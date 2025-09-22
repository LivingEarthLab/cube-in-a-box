# Standard library imports
import os
import re
import subprocess
import sys
import threading
import uuid
import xml.dom.minidom as minidom
from collections import defaultdict
from copy import deepcopy
from datetime import timedelta
from io import BytesIO
from itertools import islice
from pathlib import Path
from threading import Lock
from tqdm import tqdm
from typing import Any, Dict, Iterable, List, Optional, Pattern, Tuple, Union
from urllib.parse import unquote, urlparse

# Third-party imports
import boto3
import pystac_client
import requests
import yaml
import planetary_computer as pc
from concurrent.futures import ThreadPoolExecutor, as_completed
from dateutil import parser
from minio import Minio
from minio.error import S3Error
from osgeo import osr
from rasterio.io import MemoryFile


# remove tqdm pink background
from tqdm.auto import tqdm
os.environ['TQDM_DISABLE'] = '0'

def collect_stac_assets(
    endpoint: str,
    collection: str,
    aoi_poly: Tuple[float, float, float, float],
    time_range: Tuple[str, str],
    layers: Optional[List[str]] = None,
    t1_only: bool = True,
    modifier: Optional[callable] = None,
    platforms: Optional[List[str]] = None
) -> List[str]:
    """
    Collects STAC assets from a given endpoint with specified search parameters.
    Parameters:
    - endpoint: URL of the STAC API endpoint.
    - collection: Name of the collection to search.
    - aoi_poly: Area of interest polygon (bbox).
    - time_range: Tuple of start and end times (start_time, end_time).
    - layers: List of layer names to filter TIF assets (optional).
    - t1_only: Filter for T1 processing level only (optional).
    - modifier: Function to sign catalog items if needed (optional).
    - platforms: List of platform names to filter by (e.g., ['landsat-8', 'landsat-9']).
    Returns:
    - List of asset URLs that match the search criteria.
    """
    try:
        # Open the STAC client
        catalog = pystac_client.Client.open(endpoint)
    except Exception as e:
        print(f"Error opening STAC client: {e}")
        return []
    
    try:
        # Build the search parameters
        search_params = {
            "collections": [collection],
            "bbox": aoi_poly,
            "datetime": f"{time_range[0]}/{time_range[1]}",
        }
        
        # Add platform filtering if specified
        if platforms:
            search_params["query"] = {
                "platform": {"in": platforms}
            }
        
        # Perform the search
        items = catalog.search(**search_params).item_collection()
        
    except Exception as e:
        print(f"Error during STAC search: {e}")
        return []
    
    all_assets = []
    for item in items:
        try:
            if 'mtl.txt' not in item.assets:
                print("Warning: 'mtl.txt' asset not found.")
                continue
            # Process each asset in the item
            for asset_key, asset in item.assets.items():
                url = asset.href
                ext = urlparse(url).path.split('.')[-1].lower()
                # Skip non-TIF assets when filtering by layers
                if ('api' in url or 
                    (layers and ext == 'tif' and asset_key not in layers) or
                    (t1_only and not '_T1_' in url)):
                    continue
                all_assets.append(url)
        except Exception as e:
            print(f"Error processing item: {e}")
            continue
    
    return all_assets

def create_assets_dictionary(all_assets):
    """Create a dictionary of assets grouped by their directory path."""
    if not all_assets:
        return {}

    base_url = '/'.join(all_assets[0].split('/')[:3])
    assets_dict = defaultdict(list)

    for asset in all_assets:
        relative_path = asset.replace(base_url + '/', '').split('?')[0]
        dir_path = os.path.dirname(relative_path)
        assets_dict[dir_path].append(asset)

    return dict(assets_dict)

def find_missing_assets(
    minio_client,  # MinioClient
    bucket_name: str,
    assets_dict: Dict[str, List[str]]
) -> List[str]:
    """Compare given assets with existing objects in MinIO and return missing ones."""
    missing_assets = []

    for prefix, asset_list in assets_dict.items():
        objects = minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)
        object_names = [obj.object_name for obj in objects]

        for asset in asset_list:
            if not any(obj_name in asset for obj_name in object_names):
                missing_assets.append(asset)

    return missing_assets

def group_assets_by_parent(missing_assets: List[str], separator_pattern: Pattern) -> Dict[str, List[str]]:
    """Group assets by parent folder and extract end parts of filenames."""
    grouped_assets = defaultdict(list)

    for asset in missing_assets:
        path = unquote(urlparse(asset).path)

        filename = os.path.basename(path)
        parent_folder = os.path.basename(os.path.dirname(path))

        matches = list(re.finditer(separator_pattern, filename))
        end_part = filename[matches[-1].end():] if matches else filename
        grouped_assets[parent_folder].append(end_part)

    return dict(grouped_assets)

def invert_grouped_dictionary(grouped_dict: Dict[str, List[str]]) -> Dict[str, List[str]]:
    """Invert the grouped dictionary so that unique file lists become keys."""
    inverted_dict = defaultdict(list)

    for parent_folder, file_parts in grouped_dict.items():
        file_tuple = tuple(sorted(file_parts))
        inverted_dict[file_tuple].append(parent_folder)

    return {
        str(list(key)): sorted(folders)
        for key, folders in inverted_dict.items()
    }

def check_missing_assets(
    all_assets: List[str],
    minio_client,  # MinioClient
    bucket_name: str,
    separator_pattern: str = "_T\\d_",
    verbose: bool = True,
    dry_run: bool = False
) -> List[str]:
    """
    Process a list of assets to find missing ones in a MinIO bucket and analyze their structure.

    Parameters:
    - all_assets: List of asset URLs/paths.
    - minio_client: MinIO client instance.
    - bucket_name: Name of the bucket to search in.
    - separator_pattern: Regex pattern to find split point in filename.
    - verbose: Print summary statistics if True.
    - dry_run: Perform a dry run if True.

    Returns:
    - List of missing assets.
    """
    assets_dict = create_assets_dictionary(all_assets)
    missing_assets = find_missing_assets(minio_client, bucket_name, assets_dict)
    
    # missing_assets = []  # DEV

    grouped_assets = group_assets_by_parent(missing_assets, separator_pattern)
    inverted_dict = invert_grouped_dictionary(grouped_assets)
    
    # Show summary statistics
    if verbose or dry_run:
        if len(missing_assets) == 0:
            print('No new asset found')
            return []
        else:
            print(f"Total missing assets: {len(missing_assets)}")
            print(f"Total unique layers combinations: {len(inverted_dict)}")
            for file_part, folders in inverted_dict.items():
                print(f"  {file_part}: appears in {len(folders)} folders")
    return missing_assets


def extract_remote_file_path(url):
    """Extracts the remote file path from a given URL."""
    parsed_url = urlparse(url)
    # Get the path component of the URL
    path = parsed_url.path
    # Remove leading slash if present
    if path.startswith('/'):
        path = path[1:]
    return path

def download_and_upload_to_minio(url, minio_client, bucket_name, verbose=False, pbar=None, results=None, results_lock=None):
    """Downloads a file from a URL and uploads it directly to MinIO S3 storage."""
    remote_file_path = extract_remote_file_path(url)
    try:
        # Download the file in chunks and upload directly to MinIO
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        # Create a generator for the response content
        def stream_response():
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    yield chunk
        # Wrap the generator in a BytesIO object to make it file-like
        file_like_object = BytesIO(b''.join(stream_response()))
        # Upload the file to MinIO
        minio_client.put_object(bucket_name, remote_file_path, file_like_object, length=file_like_object.getbuffer().nbytes)
        if verbose and pbar:
            pbar.write(f"✅ File '{url}' uploaded directly to '{bucket_name}/{remote_file_path}'.")
        
        # Thread-safe success tracking
        if results and results_lock:
            with results_lock:
                results["summary"]["successful"] += 1
                
    except (requests.RequestException, S3Error) as e:
        # Thread-safe error tracking
        if results and results_lock:
            with results_lock:
                results["summary"]["failed"] += 1
                results["errors"].append(str(e))  # Only store the error message
        return False
    return True

def download_upload_assets(all_assets, minio_client, bucket_name, desc='Downloading assets', verbose=False, max_workers=5):
    """
    Downloads and uploads multiple assets to MinIO using a thread pool.
    Returns a nested dictionary with summary and detailed errors that can be collapsed/expanded in Jupyter.
    """
    results = {
        "summary": {
            "total": len(all_assets),
            "successful": 0,
            "failed": 0
        },
        "errors": []
    }
    results_lock = Lock()
    
    with tqdm(
        total=len(all_assets),
        desc= desc,
        dynamic_ncols=True,
        file=sys.stdout,
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]"
    ) as pbar:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(download_and_upload_to_minio, url, minio_client, bucket_name, verbose, pbar, results, results_lock) for url in all_assets]
            for future in as_completed(futures):
                pbar.update(1)
    
    # Clean up empty errors list if no errors occurred
    if not results["errors"]:
        del results["errors"]
    
    return results
 
def stac_to_minio(
    minio_client,  # MinioClient
    minio_bucket,
    stac_endpoint: str,
    stac_collection: str,
    platforms: Tuple[str, str],
    stac_time: Tuple[str, str],
    aoi_poly: Tuple[float, float, float, float],
    stac_layers: Optional[List[str]] = [],
    group_size: int = 5,
    t1_only: bool = True,
    overwrite: bool = False,
    complete: bool = False,
    verbose: bool = True,
    dry_run: bool = False
) -> Tuple[dict, dict]:
    """
    Entry function to find and possibly download missing STAC assets into MinIO.
    Returns:
    - Tuple containing (assets_dict, combined_download_results)
    """
    if not overwrite and not complete and not dry_run:
        print('At least one of overwrite, complete or dry_run argument need to be True')
        return {}, {"summary": {"total": 0, "successful": 0, "failed": 0}}
    
    # get STAC assets
    print('- Fetching STAC assets...')
    all_assets = collect_stac_assets(
        endpoint=stac_endpoint,
        collection=stac_collection,
        platforms=platforms,
        time_range=stac_time,
        layers=stac_layers,
        t1_only=t1_only,
        aoi_poly=aoi_poly
    )
    
    missing_assets = []
    if not overwrite or dry_run:
        print('- Checking for missing assets...')
        missing_assets = check_missing_assets(all_assets, minio_client, minio_bucket,
                                              verbose = verbose, dry_run = dry_run)
        
        if len(missing_assets) == 0 or dry_run or not complete:
            print('Stop')  # DEV
            return {}, {"summary": {"total": 0, "successful": 0, "failed": 0}}
    
    if overwrite:
        print('\nDownload all_assets')
        assets_dict = create_assets_dictionary(all_assets)
    elif complete and len(missing_assets) > 0:
        print('\nDownload missing_assets')
        assets_dict = create_assets_dictionary(missing_assets)
    
    batchs_assets = []
    assets_list = list(assets_dict.items())
    for i in range(0, len(assets_list), group_size):
        group = assets_list[i:i + group_size]
        # collect all urls from these assets
        urls = [url for _, urls in group for url in urls]
        batchs_assets.append(urls)
    
    # Initialize combined results
    combined_results = {
        "summary": {"total": 0, "successful": 0, "failed": 0},
        "errors": []
    }
    
    tot = len(batchs_assets)
    for i, batch_assets in enumerate(batchs_assets):
        # print(f"{i+1}/{tot}")
        desc = f"Downloading assets {i+1}/{tot}"
        signed_assets = []
        for url in batch_assets:
            signed_url = pc.sign_inplace(url)
            signed_assets.append(signed_url)
        result = download_upload_assets(signed_assets, minio_client, minio_bucket, desc)
        
        # Combine results from this batch
        combined_results["summary"]["total"] += result["summary"]["total"]
        combined_results["summary"]["successful"] += result["summary"]["successful"] 
        combined_results["summary"]["failed"] += result["summary"]["failed"]
        if "errors" in result:
            combined_results["errors"].extend(result["errors"])
    
    # Clean up empty errors list if no errors occurred
    if not combined_results["errors"]:
        del combined_results["errors"]
    
    print('- Done')
    return assets_dict, combined_results

def list_last_level_folders(
    client,
    bucket_name,
    prefix: str = '',
    filter_pattern: Optional[str] = None
):
    """
    List last-level folders in a MinIO bucket with optional wildcard filtering.

    Args:
        client: MinIO client instance
        bucket_name: Name of the MinIO bucket
        prefix: Prefix to filter objects (default: '')
        filter_pattern: Glob pattern for filtering ('*' for single level, '**' for multi-level)

    Returns:
        List of matching last-level folder paths (sorted)
    """
    def glob_to_regex(glob_pattern):
        pattern = glob_pattern.strip('/')
        pattern = re.escape(pattern)
        pattern = pattern.replace(r'\*\*', '.*')
        pattern = pattern.replace(r'\*', '[^/]+')
        return re.compile(pattern)

    try:
        objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
        last_level_folders = set()

        for obj in objects:
            path_parts = obj.object_name.strip('/').split('/')
            if len(path_parts) > 1:
                parent_folder = '/'.join(path_parts[:-1])
                last_level_folders.add(parent_folder)

        if filter_pattern:
            regex = glob_to_regex(filter_pattern)
            last_level_folders = {p for p in last_level_folders if regex.search(p)}

        return sorted(last_level_folders)

    except S3Error as e:
        print(f"⚠️ Error listing bucket contents: {e}")
        return []

    
    
    
    
def find_and_read_mtl(
    minio_client,
    minio_bucket: str,
    folder: str,
    suffix: str,
    yaml_file: str,
    verbose: bool = False
) -> Optional[Dict[str, Any]]:
    """
    Search for a metadata file with the given suffix in a MinIO folder and return extracted variables as dictionary.
    Args:
        minio_client: MinIO client instance
        minio_bucket: Name of the MinIO bucket
        folder: Folder path to search
        suffix: File suffix to filter by (e.g., "_mtl.xml", ".json")
        yaml_file: Path to YAML configuration file with variable mappings
        verbose: Print found file folder if True (default: False)
    Returns:
        Dictionary with extracted metadata variables, None if no file found or processing failed
    Raises:
        ValueError: If multiple files with the given suffix are found
        FileNotFoundError: If no file with the given suffix is found
    """
    # Load YAML configuration
    try:
        with open(yaml_file, 'r') as f:
            yaml_config = yaml.safe_load(f)
    except Exception as e:
        if verbose:
            print(f"❌ Error loading YAML config from {yaml_file}: {e}")
        raise Exception(f"Error loading YAML config from {yaml_file}: {e}")
    
    # Find matching files
    matches = [
        obj for obj in minio_client.list_objects(minio_bucket, prefix=folder + "/", recursive=False)
        if obj.object_name.endswith(suffix)
    ]
    
    if not matches:
        error_msg = f"No file with suffix '{suffix}' found in {folder}"
        if verbose:
            print(f"❌ {error_msg}.")
        raise FileNotFoundError(error_msg)
    
    if len(matches) > 1:
        error_msg = f"Multiple files with suffix '{suffix}' found in {folder}: {[obj.object_name for obj in matches]}"
        if verbose:
            print(f"⚠️ {error_msg}")
        raise ValueError(error_msg)
    
    obj = matches[0]
    if verbose:
        print(f"📄 Found file: {obj.object_name}")
    
    try:
        with minio_client.get_object(minio_bucket, obj.object_name) as response:
            file_content = response.read().decode("utf-8")
            
            if suffix.endswith(".xml"):
                if not file_content.strip().startswith(('<?xml', '<')):
                    error_msg = f"File {obj.object_name} is not valid XML"
                    if verbose:
                        print(f"⚠️ {error_msg}.")
                    raise ValueError(error_msg)
                
                # Parse XML and extract variables based on YAML config
                xml_doc = minidom.parseString(file_content)
                return extract_variables_from_xml(xml_doc, yaml_config)
                
            elif suffix.endswith(".json"):
                if verbose:
                    print(".json format not supported yet")
                try:
                    json_data = json.loads(file_content)
                    return extract_variables_from_json(json_data, yaml_config)
                except json.JSONDecodeError as e:
                    error_msg = f"File {obj.object_name} is not valid JSON: {e}"
                    if verbose:
                        print(f"⚠️ {error_msg}")
                    raise ValueError(error_msg)
            else:
                error_msg = f"Suffix '{suffix}' not supported. Use .xml or .json"
                if verbose:
                    print(f"⚠️ {error_msg}.")
                raise ValueError(error_msg)
                
    except Exception as e:
        if verbose:
            print(f"⚠️ Error reading/parsing {obj.object_name}: {e}")
        if isinstance(e, (FileNotFoundError, ValueError)):
            raise  # Re-raise our custom exceptions
        else:
            raise Exception(f"Error reading/parsing {obj.object_name}: {e}")
    
def _get_xml_value(xml_doc: minidom.Document, path: str) -> Optional[str]:
    """
    Extract value from XML document using a path like 'PRODUCT_CONTENTS/COLLECTION_CATEGORY'
    """
    try:
        elements = path.split('/')
        current_element = xml_doc.documentElement
        
        for element_name in elements:
            found = False
            for child in current_element.childNodes:
                if child.nodeType == minidom.Node.ELEMENT_NODE and child.nodeName == element_name:
                    current_element = child
                    found = True
                    break
            if not found:
                return None
                
        # Get the text content
        if current_element.firstChild and current_element.firstChild.nodeType == minidom.Node.TEXT_NODE:
            return current_element.firstChild.nodeValue.strip()
        return None
    except Exception:
        return None

def _get_xml_value(xml_doc: minidom.Document, xml_path: str) -> str:
    """Helper function to get a value from an XML path."""
    parts = xml_path.split('/')
    current_node = xml_doc.documentElement
    for part in parts:
        try:
            current_node = current_node.getElementsByTagName(part)[0]
        except IndexError:
            return None
    return current_node.firstChild.nodeValue if current_node.firstChild else None

def _find_file_name_by_suffix(xml_doc: minidom.Document, suffix: str) -> str:
    """Helper to find a file name whose value ends with the given suffix."""
    product_contents = xml_doc.getElementsByTagName('PRODUCT_CONTENTS')[0]
    for child in product_contents.childNodes:
        if child.nodeType == minidom.Node.ELEMENT_NODE and child.tagName.startswith('FILE_NAME_'):
            if child.firstChild and child.firstChild.nodeValue.strip().endswith(suffix + '.TIF'):
                return child.firstChild.nodeValue.strip()
    return None

def extract_variables_from_xml(xml_doc: minidom.Document, yaml_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract variables from XML based on YAML configuration
    """
    # Define keys that are NOT variable mappings
    general_info_keys = {
        'mtl_format', 'schema', 'geometry_type', 'description', 'file_format'
    }

    result = {}

    for var_name, xml_path in yaml_config.items():
        # Skip general information keys
        if var_name in general_info_keys:
            continue
        
        # Handle the special case for bands_dict
        if var_name == 'bands_dict':
            for band_name, suffix in xml_path.items():
                file_name = _find_file_name_by_suffix(xml_doc, suffix)
                result[f'bands_dict_{band_name}'] = file_name
        
        # Handle special case for datetime combination
        elif '+' in xml_path:
            parts = [part.strip() for part in xml_path.split('+')]
            if len(parts) == 2:
                date_value = _get_xml_value(xml_doc, parts[0])
                time_value = _get_xml_value(xml_doc, parts[1])
                if date_value and time_value:
                    time_value = time_value.rstrip('Z')
                    result[var_name] = f"{date_value}T{time_value}Z"
                else:
                    result[var_name] = None
            else:
                result[var_name] = None
        
        # Standard XML path extraction
        else:
            result[var_name] = _get_xml_value(xml_doc, xml_path)

    return result

def _extract_variables_from_json(json_data: Dict[str, Any], yaml_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract variables from JSON based on YAML configuration (placeholder for future implementation)
    """
    # Placeholder - implement JSON extraction logic here when needed
    print("⚠️ JSON variable extraction not implemented yet")
    return {}

def list_files(
    minio_client,
    minio_bucket: str,
    folder: str,
    valid_exts: tuple = (".tif", ".tiff"),
    notvalid_exts: tuple = (".yaml"),
    recursive: bool = True
) -> List[str]:
    images = []
    others = []
    for obj in minio_client.list_objects(minio_bucket, prefix=folder, recursive=recursive):
        if obj.object_name.lower().endswith(valid_exts):
            images.append(obj.object_name)
        elif not obj.object_name.lower().endswith(notvalid_exts):
            others.append(obj.object_name)
    # return [
    #     obj.object_name
    #     for obj in minio_client.list_objects(minio_bucket, prefix=folder, recursive=recursive)
    #     if obj.object_name.lower().endswith(valid_exts)
    # ]
    return images, others

from typing import Dict, Any, List, Optional
import rasterio
from rasterio.io import MemoryFile
from rasterio.warp import transform_bounds
from rasterio.crs import CRS

def describe_image(
    minio_client,
    minio_bucket: str,
    object_name: str
) -> Dict[str, Any]:
    """
    Get detailed spatial information for a GeoTIFF stored in MinIO.

    Args:
        minio_client: MinIO client instance
        minio_bucket: Name of the MinIO bucket
        object_name: Path of the object in the bucket

    Returns:
        A dictionary with:
        - spatial_reference (WKT)
        - epsg_code (int or None)
        - polygon (list of coordinates representing the corners)
        - transform (rasterio transform object)
        - shape (width, height)
    """
    try:
        response = minio_client.get_object(minio_bucket, object_name)
        with MemoryFile(response.read()) as memfile:
            with memfile.open() as img:
                # Get bounds and CRS
                left, bottom, right, top = img.bounds
                crs = img.crs

                # # Spatial reference (WKT)
                # spatial_reference = str(getattr(img, 'crs_wkt', None) or crs.wkt)

                # EPSG code (if available)
                epsg_code = None
                if crs and crs.is_epsg_code:
                    epsg_code = int(crs.to_epsg())

                # Polygon corners (closed loop: ul -> ur -> lr -> ll -> ul)
                polygon = [
                    [left, top],     # ul
                    [right, top],    # ur
                    [right, bottom], # lr
                    [left, bottom],  # ll
                    [left, top]      # ul (close the polygon)
                ]

                # Transform object
                transform = img.transform

                # Shape (width, height)
                shape = (img.width, img.height)

                return {
                    # 'spatial_reference': spatial_reference,
                    'epsg_code': epsg_code,
                    'polygon': polygon,
                    'transform': transform,
                    'shape': shape
                }
    finally:
        response.close()
        response.release_conn()

def dict_from_prfx(
    src_dict: Dict[str, Any],
    prefix: str,
    case_sensitive: bool = True
) -> Dict[str, Any]:
    """
    Build a new dictionary containing only the items whose keys begin with prefix.
    The returned dictionary has the same values, but the keys have the prefix removed.

    Args:
        src_dict: The original dictionary
        prefix: The prefix to filter by
        case_sensitive: If False the match ignores case (default: True)

    Returns:
        A new dictionary with the filtered and renamed keys
    """
    norm_prefix = prefix if case_sensitive else prefix.lower()
    result = {}

    for key, value in src_dict.items():
        compare_key = key if case_sensitive else key.lower()
        if compare_key.startswith(norm_prefix):
            new_key = key[len(prefix):]
            result[new_key] = value

    return result

def valid_folders(
    minio_client,
    minio_bucket: str,
    folders: List[str],
    overwrite: bool = False
) -> List[str]:
    """
    Filter folders based on whether they contain metadata YAML files.

    Args:
        minio_client: MinIO client instance
        minio_bucket: Name of the MinIO bucket
        folders: List of folder paths to check
        overwrite: If True, return all folders without filtering;
                   if False, return only folders without '-metadata.yaml' files (default: False)

    Returns:
        List of folder paths that are considered valid based on the overwrite flag
    """
    if overwrite:
        return folders

    valid_folder_list = []
    for folder in folders:
        try:
            objects = minio_client.list_objects(minio_bucket, prefix=folder + "/", recursive=False)
            if not any(obj.object_name.lower().endswith('-metadata.yaml') for obj in objects):
                valid_folder_list.append(folder)
        except Exception as e:
            print(f"⚠️ Error checking folder {folder}: {e}")
            # Optionally include problematic folders in the result or skip them
            # valid_folder_list.append(folder)  # Uncomment if you want to include folders that can't be checked

    return valid_folder_list


def map_bands_to_paths(
    bands_dict: Dict[str, str],
    image_paths: Iterable[str]
) -> Dict[str, str]:
    """
    Build a dictionary that maps each band key from bands_dict to the filename that contains the corresponding band identifier.

    Args:
        bands_dict: Dictionary mapping band names to band identifiers (e.g., {'blue': '_SR_B2', 'green': '_SR_B3'})
        image_paths: Full file paths (or just filenames) for the images

    Returns:
        Dictionary mapping band names to file information
    """
    result = {}
    for path in image_paths:
        filename = os.path.basename(path)
        for band, band_id in bands_dict.items():
            if band_id in filename:
                result[band] = {'path': filename}
                break
    return result

def get_coords(
    geo_ref_points,
    spatial_ref
):
    """
    Transform geographic reference points to latitude/longitude coordinates.

    Args:
        geo_ref_points: Dictionary of geographic reference points with x, y coordinates
        spatial_ref: Spatial reference system information

    Returns:
        Dictionary with transformed coordinates containing longitude and latitude
    """
    spatial_ref = osr.SpatialReference(spatial_ref)
    t = osr.CoordinateTransformation(spatial_ref, spatial_ref.CloneGeogCS())

    def transform(p):
        lat, lon, z = t.TransformPoint(p['x'], p['y'])
        return {'lon': lon, 'lat': lat}

    return {key: transform(p) for key, p in geo_ref_points.items()}

def yaml_bytes_from_dict(data: dict) -> BytesIO:
    """
    Serialise a dict to YAML and return a BytesIO containing the UTF‑8 bytes.
    """
    
    # Dump to a string first (yaml.safe_dump is preferred for untrusted data)
    yaml_str = yaml.safe_dump(data, default_flow_style=False, sort_keys=False, allow_unicode=True)
    
    # Remove quotes around stringed lists
    yaml_str = re.sub(r"coordinates: '(.+?)'", r"coordinates: \1", yaml_str, flags=re.DOTALL)
    yaml_str = re.sub(r"shape: '(.+?)'", r"shape: \1", yaml_str, flags=re.DOTALL)
    yaml_str = re.sub(r"transform: '(.+?)'", r"transform: \1", yaml_str, flags=re.DOTALL)

    # Encode to UTF‑8 and wrap in BytesIO so MinIO sees a file‑like object
    return BytesIO(yaml_str.encode("utf-8"))

def upload_yaml_to_minio(
    doc: dict,
    yaml_path: str,
    minio_client: Minio,
    bucket_name: str,
    minio_url: str,
    verbose: bool = False,
) -> bool:
    """
    Convert ``doc`` to YAML and upload it to ``bucket_name`` under ``yaml_path``.

    Parameters
    ----------
    doc          : dict
        The Python object you want to store as YAML.
    yaml_path    : str
        Destination “key” inside the bucket (e.g. ``landsat-c2/.../metadata.yaml``).
    minio_client : Minio
        An authenticated MinIO client instance.
    bucket_name  : str
        Target bucket name (must already exist or be created beforehand).
    verbose      : bool, optional
        Print a short status line on success/failure.

    Returns
    -------
    bool
        ``True`` on successful upload, ``False`` otherwise.
    """
    try:
        # Turn the dict into a BytesIO containing the YAML payload
        yaml_stream = yaml_bytes_from_dict(doc)

        # Upload – MinIO needs the exact byte length up front
        size = yaml_stream.getbuffer().nbytes
        minio_client.put_object(
            bucket_name,
            yaml_path,
            yaml_stream,
            length=size,
            content_type="application/x-yaml",
        )

        if verbose:
            print(
                f"✅ YAML document uploaded to {os.path.join(minio_url, bucket_name, yaml_path)}"
                f"({size:,} bytes)."
            )
        return f"✅ YAML document uploaded to {os.path.join(minio_url, bucket_name, yaml_path)}"

    except (S3Error, OSError) as exc:
        # S3Error covers most MinIO‑side problems; OSError catches stream issues
        if verbose:
            print(f"⚠️ Failed to upload YAML: {exc}")
        return f"⚠️ Failed to upload YAML: {exc}"
    
def update_bands(bands_dict: dict, images: list, minio_url: str, minio_bucket: str) -> dict:
        """
        Removes keys from a dictionary if their value (filename) is not found
        within any of the provided image paths. If found, the dictionary's value
        is replaced with the full image path.

        Args:
            bands_dict (dict): A dictionary where values are short filenames (e.g., 'SR_B1.TIF').
            images (list): A list of full image paths (e.g., 'path/to/SR_B1.TIF').

        Returns:
            dict: A new dictionary with filtered and updated key-value pairs.
        """
        updated_bands_dict = {}
        for band_name, filename in bands_dict.items():
            found_match = False
            for full_path in images:
                if filename in full_path:
                    updated_bands_dict[band_name] = {'path': os.path.join(minio_url, minio_bucket, full_path)}
                    found_match = True
                    break  # Found a match for this filename, move to the next band_name
            # If found_match is False here, it means the filename was not found in any full_path,
            # so we simply don't add it to updated_bands_dict, effectively removing it.
        return updated_bands_dict
    
def update_accessories(others: list, minio_url: str, minio_bucket: str) -> dict:
    updated_accessories_dict = {}
    for other in others:
        updated_accessories_dict[other.split('_')[-1].lower()] = {'path': os.path.join(minio_url, minio_bucket, other)}
    return updated_accessories_dict

def prepare_yaml(minio_client, minio_bucket, minio_url,
                 folder, product_name, yaml_file, verbose = True):
    scene_id = folder.split('/')[-1]
    
    with open(yaml_file, 'r') as file:
        data = yaml.safe_load(file)
        suffix = data['mtl_format']
        schema = data['schema']
        geometry_type = data['geometry_type']
        description = data['description']
        file_format = data['file_format']
        
    # Pass verbose parameter to find_and_read_mtl
    mtl_content = find_and_read_mtl(
        minio_client=minio_client,
        minio_bucket=minio_bucket,
        folder=folder,
        suffix=suffix,
        yaml_file=yaml_file,
        verbose=verbose)  # <-- Add this parameter
    
    # Rest of your function remains the same...
    bands_dict = dict_from_prfx(mtl_content, 'bands_dict_')
    images, others = list_files(minio_client, minio_bucket, folder, recursive=bool)
    bands = update_bands(bands_dict, images, minio_url, minio_bucket)
    accessories = update_accessories(others, minio_url, minio_bucket)
    image_description = describe_image(minio_client, minio_bucket, images[0])
    
    doc = {
        '$schema': schema,
        'id': str(uuid.uuid4()),
        'label': scene_id[:25] + scene_id[-6:],
        'product': {
            'name': product_name,
        },
        'location': 'http://minio:9001/browser/test/landsat-c2/level-2/standard/oli-tirs',
        'crs': f"epsg:{image_description['epsg_code']}",
        'geometry': {
            'type': geometry_type,
            'coordinates': str([image_description['polygon']]),
        },
        'grids': {
            'default': {
                'shape': str(list(image_description['shape'])),
                'transform': str(list(image_description['transform'])[:6]),
            },
        },
        'properties': {
            'created': mtl_content['created'],
            'datetime': mtl_content['datetime'],
            'description': description,
            'eo:cloud_cover': float(mtl_content['eo_cloud_cover']) / 100,
            'eo:gsd': mtl_content['eo_gsd'],
            'eo:instrument': mtl_content['eo_instrument'],
            'eo:platform': mtl_content['eo_platform'].lower().replace('_', '-'),
            'eo:sun_azimuth': float(mtl_content['eo_sun_azimuth']),
            'eo:sun_elevation': float(mtl_content['eo_sun_elevation']),
            'landsat:cloud_cover_land': float(mtl_content['landsat_cloud_cover_land']),
            'landsat:collection_category': mtl_content['landsat_collection_category'],
            'landsat:collection_number': mtl_content['landsat_collection_number'],
            'landsat:correction': mtl_content['landsat_correction'],
            'landsat:scene_id': mtl_content['landsat_scene_id'],
            'landsat:wrs_path': mtl_content['landsat_wrs_path'].zfill(3),
            'landsat:wrs_row': mtl_content['landsat_wrs_row'].zfill(3),
            'landsat:wrs_type': mtl_content['landsat_wrs_type'],
            'odc:file_format': file_format,
            'odc:processing_datetime': mtl_content['created'],
            'odc:product': product_name,
            'odc:region_code': f"{mtl_content['landsat_wrs_path'].zfill(3)}{mtl_content['landsat_wrs_row'].zfill(3)}",
            'proj:epsg': image_description['epsg_code'],
            'proj:shape': image_description['shape'],
            'proj:transform': list(image_description['transform'])[:6],
            'sci:doi': mtl_content['sci_doi'],
        },
        'measurements': bands,
        'accessories': accessories,
        'lineage': {},
    }

#     ### DEV ######################
#     def pretty_print(d, indent=0):
#         """Recursively format a dict without braces or quotes."""
#         lines = []
#         pad = "\t" * indent          # one tab per level; change to " " * 4 for spaces

#         for key, value in d.items():
#             # If the value is another dict, recurse
#             if isinstance(value, dict):
#                 lines.append(f"{pad}{key}:")
#                 lines.extend(pretty_print(value, indent + 1))
#             else:
#                 # Strip surrounding quotes from strings
#                 if isinstance(value, str):
#                     value = value.strip("'\"")
#                 lines.append(f"{pad}{key}: {value}")

#         return lines

#     # Get the formatted lines and join them
#     output = "\n".join(pretty_print(doc))
#     print(output)

#     with open("dataset.yaml", "w", encoding="utf-8") as f:
#         yaml.safe_dump(
#             doc,
#             f,
#             default_flow_style=False,   # one key per line (block style)
#             sort_keys=False,            # keep the insertion order you gave
#             allow_unicode=True,         # preserve any non‑ASCII characters
#             indent=2,                   # two spaces per nesting level
#         )
#     ##############################

    yaml_path = str(Path(folder).joinpath(f"{product_name}-metadata.yaml"))
    
    result = upload_yaml_to_minio(doc,
        yaml_path,
        minio_client,
        minio_bucket,
        minio_url,
        verbose=verbose,
    )
    return result

def prepare_folder(folder, minio_client, minio_bucket, minio_url, product_name, yaml_file, pbar=None):
    """Optimized thread-based processing with postfix updates, accepting arguments."""
    try:
        # The variables are now passed directly to the function
        result = prepare_yaml(
            minio_client=minio_client,
            minio_bucket=minio_bucket,
            minio_url=minio_url,
            folder=folder,
            product_name=product_name,
            yaml_file=yaml_file,
            verbose=False
        )
        if pbar:
            pbar.set_postfix_str(f"✓ {folder.split('/')[-1]}")
        return {
            'folder': folder,
            'result': result,
            'status': 'success'
        }
    except Exception as e:
        if pbar:
            pbar.set_postfix_str(f"✗ {folder.split('/')[-1]} failed")
        return {
            'folder': folder,
            'result': None,
            'status': 'failed',
            'error': str(e)
        }

def prepare_folders(folders, minio_client, minio_bucket, minio_url, product_name, yaml_file, max_workers=8):
    """
    Processes a list of folders using a thread pool with a progress bar,
    passing all necessary arguments to the worker function.
    """
    # Check if YAML file exists
    if not os.path.exists(yaml_file):
        raise FileNotFoundError(f"YAML file not found: {yaml_file}")
    
    print(f"Processing {len(folders)} folders with {max_workers} threads...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        with tqdm(total=len(folders), desc="Processing") as pbar:
            # Pass the arguments to executor.submit using functools.partial or a lambda,
            # but a simple for loop is clearer for this case.
            futures = {
                executor.submit(
                    prepare_folder,
                    folder,
                    minio_client,
                    minio_bucket,
                    minio_url,
                    product_name,
                    yaml_file,
                    pbar,
                ): folder for folder in folders
            }
            results = []
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                pbar.update(1)
    
    successful = [r for r in results if r['status'] == 'success']
    failed = [r for r in results if r['status'] == 'failed']
    print(f"\n✓ Successful: {len(successful)}, ✗ Failed: {len(failed)}")
    
    if failed:
        print("Failed items:")
        for item in failed:
            folder_name = item['folder'].split('/')[-1]
            print(f"  ✗ {folder_name}: {item['error']}")
            
    return results

def extract_url(text):
    url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    match = re.search(url_pattern, text)
    if match:
        url = match.group(0)
        return url

def index_minio_yaml(url, product):
    """
    Execute indexing command for a single YAML URL.
    
    Args:
        url (str): URL of the YAML file to index
        product (str): product name
        
    Returns:
        dict: Result dictionary with success status and details
    """ 
    parsed_url = urlparse(url)
    endpoint = f"{parsed_url.scheme}://{parsed_url.netloc}"
    bucket_name = parsed_url.path.strip('/').split('/')[0]
    
    command = ["python", "hcp_to_dc.py", endpoint, bucket_name, product, "--mtdsstr", url]
    
    try:
        # Execute the command and capture output
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True,
            timeout=300  # 5 minute timeout
        )
        return {"success": True, "url": url, "stdout": result.stdout, "stderr": result.stderr}
    
    except subprocess.CalledProcessError as e:
        return {"success": False, "url": url, "error": e.stderr, "returncode": e.returncode}
    
    except subprocess.TimeoutExpired:
        return {"success": False, "url": url, "error": "Timeout"}
    
    except Exception as e:
        return {"success": False, "url": url, "error": str(e)}

def index_minio_yamls(yamls, product, max_workers=4):
    """
    Execute indexing commands for multiple YAML URLs in parallel.
    
    Args:
        yamls (list): List of YAML URLs to index
        product (str): product name
        max_workers (int): Maximum number of parallel workers (default: 4)
        
    Returns:
        tuple: (execution_results, errors)
            - execution_results: List of all results
            - errors: List of failed executions only
    """
    execution_results = []
    errors = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all futures
        futures = {executor.submit(index_minio_yaml, yaml, product): yaml for yaml in yamls}
        
        # Process with progress bar
        with tqdm(total=len(yamls), desc="Processing URLs", unit="url") as pbar:
            for future in as_completed(futures):
                result = future.result()
                execution_results.append(result)
                
                if result["success"]:
                    pbar.set_postfix(status="✓", refresh=True)
                else:
                    errors.append(result)
                    pbar.set_postfix(status="✗", refresh=True)
                
                pbar.update(1)
    
    return execution_results, errors