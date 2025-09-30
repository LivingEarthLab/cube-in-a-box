# Standard library imports
import os
import re
import subprocess
import sys
import threading
import uuid
import xml.dom.minidom as minidom
from collections import defaultdict
from contextlib import contextmanager
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
import rasterio
import requests
import yaml
import planetary_computer as pc
from concurrent.futures import ThreadPoolExecutor, as_completed
from dateutil import parser
from minio import Minio
from minio.error import S3Error
from osgeo import osr
from rasterio.crs import CRS
from rasterio.warp import transform_bounds

from .nostra_stac import collect_stac_assets
from .nostra_minio import list_minio_files, describe_minio_image

# remove tqdm pink background
from tqdm.auto import tqdm
os.environ['TQDM_DISABLE'] = '0'

@contextmanager
def _progress_bar_context(total, desc='Processing', show_progress=True, progress_bar_format=None, unit='it'):
    """Context manager for optional progress bars.
    
    Args:
        total: Total number of items to process.
        desc: Description for progress bar.
        show_progress: Whether to show progress bar.
        progress_bar_format: Custom format string for progress bar.
        unit: Unit name for progress bar (e.g., 'it', 'url', 'file').
        
    Yields:
        Progress bar object or dummy object with update method.
    """
    if show_progress and total > 0:
        bar_format = progress_bar_format or "{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]"
        with tqdm(
            total=total,
            desc=desc,
            dynamic_ncols=True,
            file=sys.stdout,
            bar_format=bar_format,
            unit=unit
        ) as pbar:
            yield pbar
    else:
        # Dummy progress bar that does nothing
        class DummyProgressBar:
            def update(self, n=1):
                pass
            def write(self, s):
                if show_progress:  # Still allow writes if progress is disabled but verbose is on
                    print(s)
            def set_postfix(self, **kwargs):
                pass
            def set_postfix_str(self, s):
                pass
        yield DummyProgressBar()

def _find_missing_assets(
    minio_client: Minio,
    bucket_name: str,
    assets_dict: Dict[str, List[str]]
) -> List[str]:
    """Find assets missing from MinIO bucket by comparing with STAC assets.

    Args:
        minio_client: Authenticated MinIO client instance.
        bucket_name: MinIO bucket name to check.
        assets_dict: Dict of assets grouped by directory paths.

    Returns:
        List of asset URLs not found in the MinIO bucket.
    """
    missing_assets = []

    for prefix, asset_list in assets_dict.items():
        objects = minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)
        object_names = [obj.object_name for obj in objects]

        for asset in asset_list:
            if not any(obj_name in asset for obj_name in object_names):
                missing_assets.append(asset)

    return missing_assets

        
def _create_assets_dictionary(all_assets):
    """Create dictionary of assets grouped by parent directory path.

    Args:
        all_assets: List of full asset URLs.

    Returns:
        Dict mapping directory paths to lists of asset URLs.
        Returns empty dict if input is empty.
    """
    if not all_assets:
        return {}

    base_url = '/'.join(all_assets[0].split('/')[:3])
    assets_dict = defaultdict(list)

    for asset in all_assets:
        relative_path = asset.replace(base_url + '/', '').split('?')[0]
        dir_path = os.path.dirname(relative_path)
        assets_dict[dir_path].append(asset)

    return dict(assets_dict)

def _group_assets_by_parent(missing_assets: List[str], separator_pattern: Pattern) -> Dict[str, List[str]]:
    """Group asset URLs by parent folder and extract filename end parts.

    Args:
        missing_assets: List of full asset URLs.
        separator_pattern: Compiled regex pattern to find separator in filename.

    Returns:
        Dict mapping parent folder names to lists of filename end parts.
    """
    grouped_assets = defaultdict(list)

    for asset in missing_assets:
        path = unquote(urlparse(asset).path)

        filename = os.path.basename(path)
        parent_folder = os.path.basename(os.path.dirname(path))

        matches = list(re.finditer(separator_pattern, filename))
        end_part = filename[matches[-1].end():] if matches else filename
        grouped_assets[parent_folder].append(end_part)

    return dict(grouped_assets)

def _invert_grouped_dictionary(grouped_dict: Dict[str, List[str]]) -> Dict[str, List[str]]:
    """Invert dictionary to group parent folders with identical file parts.

    Args:
        grouped_dict: Dict mapping parent folder names to lists of file parts.

    Returns:
        Dict mapping sorted file parts lists to parent folders containing them.
    """
    inverted_dict = defaultdict(list)

    for parent_folder, file_parts in grouped_dict.items():
        file_tuple = tuple(sorted(file_parts))
        inverted_dict[file_tuple].append(parent_folder)

    return {
        str(list(key)): sorted(folders)
        for key, folders in inverted_dict.items()
    }

def _check_missing_assets(
    all_assets: List[str],
    minio_client: Minio,
    bucket_name: str,
    separator_pattern: str = "_T\\d_",
    verbose: bool = True,
    dry_run: bool = False
) -> List[str]:
    """Find missing assets in MinIO bucket and analyze their structure.

    Args:
        all_assets: List of asset URLs/paths.
        minio_client: MinIO client instance.
        bucket_name: Bucket name to search in.
        separator_pattern: Regex pattern to find split point in filename.
        verbose: Print summary statistics if True.
        dry_run: Perform dry run if True.

    Returns:
        List of missing assets.
    """
    assets_dict = _create_assets_dictionary(all_assets)
    missing_assets = _find_missing_assets(minio_client, bucket_name, assets_dict)

    grouped_assets = _group_assets_by_parent(missing_assets, separator_pattern)
    inverted_dict = _invert_grouped_dictionary(grouped_assets)
    
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

def _extract_remote_file_path(url):
    """Extract remote file path from URL.

    Args:
        url: Full URL to extract path from.

    Returns:
        Remote file path string with leading slash removed.
    """
    parsed_url = urlparse(url)
    path = parsed_url.path
    if path.startswith('/'):
        path = path[1:]
    return path

def _download_and_upload_to_minio(url, minio_client: Minio, bucket_name: str, verbose=False, pbar=None, results=None, results_lock=None):
    """Download file from URL and upload directly to MinIO bucket.

    Args:
        url: Full URL of file to download.
        minio_client: Authenticated MinIO client instance.
        bucket_name: MinIO bucket name for upload.
        verbose: Print success message if True.
        pbar: Optional progress bar for messages.
        results: Shared dictionary to record outcomes.
        results_lock: Threading lock for thread-safe access to results.

    Returns:
        True if successful, False otherwise.
    """
    remote_file_path = _extract_remote_file_path(url)
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

def _download_upload_assets(
    all_assets, 
    minio_client: Minio, 
    bucket_name: str, 
    desc='Downloading assets', 
    verbose=False, 
    max_workers=5,
    show_progress=True,
    progress_bar_format=None
):
    """Download and upload multiple assets to MinIO concurrently.

    Args:
        all_assets: List of asset URLs to download.
        minio_client: Authenticated MinIO client instance.
        bucket_name: MinIO bucket name for uploads.
        desc: Progress bar description.
        verbose: Enable verbose logging for each operation.
        max_workers: Maximum threads for parallel execution.
        show_progress: Whether to show progress bar.
        progress_bar_format: Custom format string for progress bar.

    Returns:
        Dict with summary of operation results and error details.
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
    
    with _progress_bar_context(
        total=len(all_assets),
        desc=desc,
        show_progress=show_progress,
        progress_bar_format=progress_bar_format
    ) as pbar:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(_download_and_upload_to_minio, url, minio_client, bucket_name, verbose, pbar, results, results_lock) for url in all_assets]
            for future in as_completed(futures):
                pbar.update(1)
    
    # Clean up empty errors list if no errors occurred
    if not results["errors"]:
        del results["errors"]
    
    return results

def stac_to_minio(
    minio_client: Minio,
    minio_bucket: str,
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
    dry_run: bool = False,
    show_progress: bool = True,
    progress_desc: Optional[str] = None,
    progress_bar_format: Optional[str] = None
) -> Tuple[dict, dict]:
    """Download STAC assets to MinIO bucket based on search criteria.

    Main entry point that identifies assets from STAC endpoint, checks for 
    existence in MinIO, and downloads missing or all assets to the bucket.

    Args:
        minio_client: Authenticated MinIO client instance.
        minio_bucket: MinIO bucket name.
        stac_endpoint: STAC API endpoint URL.
        stac_collection: Collection name to search.
        platforms: Platform names to filter by (e.g., ('landsat-8', 'landsat-9')).
        stac_time: Time range (start_time, end_time).
        aoi_poly: Area of interest bounding box.
        stac_layers: Layer names to filter TIF assets.
        group_size: Assets to download per batch.
        t1_only: Filter for T1 processing level only.
        overwrite: Download all assets and overwrite existing ones.
        complete: Only download missing assets from MinIO bucket.
        verbose: Enable verbose logging.
        dry_run: Search and check for missing assets without downloading.
        show_progress: Whether to show progress bars.
        progress_desc: Custom description for progress bars.
        progress_bar_format: Custom format string for progress bars.

    Returns:
        Tuple of (assets_dict, download_results).
    """
    if not overwrite and not complete and not dry_run:
        raise ValueError('At least one of overwrite, complete or dry_run argument need to be True')
    
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
        missing_assets = _check_missing_assets(all_assets, minio_client, minio_bucket,
                                              verbose = verbose, dry_run = dry_run)
        
        if len(missing_assets) == 0 or dry_run or not complete:
            print('Stop')  # DEV
            return {}, {"summary": {"total": 0, "successful": 0, "failed": 0}}
    
    if overwrite:
        print('\nDownload all_assets')
        assets_dict = _create_assets_dictionary(all_assets)
    elif complete and len(missing_assets) > 0:
        print('\nDownload missing_assets')
        assets_dict = _create_assets_dictionary(missing_assets)
    
    batchs_assets = []
    assets_list = list(assets_dict.items())
    for i in range(0, len(assets_list), group_size):
        group = assets_list[i:i + group_size]
        urls = [url for _, urls in group for url in urls]
        batchs_assets.append(urls)
    
    combined_results = {
        "summary": {"total": 0, "successful": 0, "failed": 0},
        "errors": []
    }
    
    for i, batch_assets in enumerate(batchs_assets):
        desc = progress_desc or f"Downloading assets {i+1}/{len(batchs_assets)}"
        signed_assets = []
        for url in batch_assets:
            signed_url = pc.sign_inplace(url)
            signed_assets.append(signed_url)
        result = _download_upload_assets(
            signed_assets, 
            minio_client, 
            minio_bucket, 
            desc,
            verbose=verbose,
            show_progress=show_progress,
            progress_bar_format=progress_bar_format
        )
        combined_results["summary"]["total"] += result["summary"]["total"]
        combined_results["summary"]["successful"] += result["summary"]["successful"] 
        combined_results["summary"]["failed"] += result["summary"]["failed"]
        if "errors" in result:
            combined_results["errors"].extend(result["errors"])
    
    if not combined_results["errors"]:
        del combined_results["errors"]
    
    print('- Done')
    return assets_dict, combined_results

def _update_accessories(others: list, minio_url: str, minio_bucket: str) -> dict:
    """
    Updates a dictionary of accessories with their corresponding MinIO paths.

    Args:
        others: A list of accessory filenames.
        minio_url: The base URL of the MinIO server.
        minio_bucket: The name of the MinIO bucket.

    Returns:
        A dictionary mapping the lowercase accessory name (extracted from the filename) 
        to a dictionary containing the accessory's MinIO path.
    """
    updated_accessories_dict = {}
    for other in others:
        updated_accessories_dict[other.split('_')[-1].lower()] = {'path': os.path.join(minio_url, minio_bucket, other)}
    return updated_accessories_dict

def _find_file_name_by_suffix(xml_doc: minidom.Document, suffix: str) -> str | None:
    """
    Finds a file name within an XML document that ends with a specific suffix.

    Args:
        xml_doc: The XML document to search.
        suffix: The suffix to search for (excluding the '.TIF' extension).

    Returns:
        The file name if found, otherwise None.
    """
    product_contents = xml_doc.getElementsByTagName('PRODUCT_CONTENTS')[0]
    for child in product_contents.childNodes:
        if child.nodeType == minidom.Node.ELEMENT_NODE and child.tagName.startswith('FILE_NAME_'):
            if child.firstChild and child.firstChild.nodeValue.strip().endswith(suffix + '.TIF'):
                return child.firstChild.nodeValue.strip()
    return None

def _get_xml_value(xml_doc: minidom.Document, xml_path: str) -> str | None:
    """
    Retrieves a value from an XML document given an XPath-like path.

    Args:
        xml_doc: The XML document to search.
        xml_path: The path to the desired value, using '/' as a separator for tags.

    Returns:
        The text value of the node at the given path, or None if the path is invalid or the node has no value.
    """
    parts = xml_path.split('/')
    current_node = xml_doc.documentElement
    for part in parts:
        try:
            current_node = current_node.getElementsByTagName(part)[0]
        except IndexError:
            return None
    return current_node.firstChild.nodeValue if current_node.firstChild else None

def _extract_variables_from_xml(xml_doc: minidom.Document, yaml_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extracts variables from an XML document based on a YAML configuration.

    Args:
        xml_doc: The XML document to extract data from.
        yaml_config: A dictionary defining variable names and their corresponding XML paths.

    Returns:
        A dictionary containing the extracted variables.
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

def _find_and_read_mtl(
    minio_client: Minio,
    minio_bucket: str,
    folder: str,
    suffix: str,
    yaml_file: str,
    verbose: bool = False
) -> Optional[Dict[str, Any]]:
    """
    Finds and reads an MTL file (XML or JSON) from MinIO, extracts variables based on a YAML configuration.

    Args:
        minio_client: Authenticated MinIO client instance.
        minio_bucket: The name of the MinIO bucket.
        folder: The folder within the bucket to search for the MTL file.
        suffix: The file suffix (e.g., ".xml", ".json").
        yaml_file: The path to the YAML configuration file.
        verbose: Whether to print verbose output.

    Returns:
        A dictionary containing the extracted variables, or None if the file is not found or an error occurs.
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
                return _extract_variables_from_xml(xml_doc, yaml_config)
                
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

def _dict_from_prfx(
    src_dict: Dict[str, Any],
    prefix: str,
    case_sensitive: bool = True
) -> Dict[str, Any]:
    """
    Creates a new dictionary from a source dictionary, filtering keys by a prefix.

    Args:
        src_dict: The source dictionary.
        prefix: The prefix to filter keys by.
        case_sensitive: Whether the prefix comparison should be case-sensitive.

    Returns:
        A new dictionary containing only the key-value pairs from `src_dict` 
        where the key starts with the given `prefix`. The prefix is removed from the new key.
    """
    norm_prefix = prefix if case_sensitive else prefix.lower()
    result = {}

    for key, value in src_dict.items():
        compare_key = key if case_sensitive else key.lower()
        if compare_key.startswith(norm_prefix):
            new_key = key[len(prefix):]
            result[new_key] = value

    return result

def _update_bands(bands_dict: dict, images: list, minio_url: str, minio_bucket: str) -> dict:
    """
    Updates a dictionary of band names with their corresponding MinIO paths.

    Args:
        bands_dict: A dictionary mapping band names to filenames.
        images: A list of full paths to images in MinIO.
        minio_url: The base URL of the MinIO server.
        minio_bucket: The name of the MinIO bucket.

    Returns:
        A new dictionary containing only the band names that were found in the list of images, 
        with their paths updated to include the MinIO URL and bucket name. Band names not found
        in the images list are omitted.
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

def _yaml_bytes_from_dict(data: dict) -> BytesIO:
    """
    Converts a dictionary to a YAML formatted BytesIO object.

    Args:
        data: The dictionary to convert.

    Returns:
        A BytesIO object containing the YAML formatted data.
    """
    # Dump to a string first (yaml.safe_dump is preferred for untrusted data)
    yaml_str = yaml.safe_dump(data, default_flow_style=False, sort_keys=False, allow_unicode=True)
    
    # Remove quotes around stringed lists
    yaml_str = re.sub(r"coordinates: '(.+?)'", r"coordinates: \1", yaml_str, flags=re.DOTALL)
    yaml_str = re.sub(r"shape: '(.+?)'", r"shape: \1", yaml_str, flags=re.DOTALL)
    yaml_str = re.sub(r"transform: '(.+?)'", r"transform: \1", yaml_str, flags=re.DOTALL)

    # Encode to UTF‑8 and wrap in BytesIO so MinIO sees a file‑like object
    return BytesIO(yaml_str.encode("utf-8"))

def _upload_yaml_to_minio(
    doc: dict,
    yaml_path: str,
    minio_client: Minio,
    bucket_name: str,
    minio_url: str,
    verbose: bool = False,
) -> str:
    """
    Uploads a YAML document (represented as a dictionary) to MinIO.

    Args:
        doc: The dictionary to upload as YAML.
        yaml_path: The path within the MinIO bucket where the YAML file will be stored.
        minio_client: Authenticated MinIO client instance.
        bucket_name: The name of the MinIO bucket.
        minio_url: The base URL of the MinIO server.
        verbose: Whether to print verbose output.

    Returns:
        A success or failure message string.
    """
    try:
        # Turn the dict into a BytesIO containing the YAML payload
        yaml_stream = _yaml_bytes_from_dict(doc)

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

def prepare_yaml(
    minio_client: Minio,
    minio_bucket: str,
    minio_url: str,
    folder: str,
    product_name: str,
    yaml_file: str,
    verbose: bool = True
) -> str:
    """
    Prepares a YAML document from an MTL file and uploads it to MinIO.

    Args:
        minio_client: Authenticated MinIO client instance.
        minio_bucket: The name of the MinIO bucket.
        minio_url: The base URL of the MinIO server.
        folder: The folder containing the MTL file and images.
        product_name: The name of the product.
        yaml_file: The path to the YAML configuration file.
        verbose: Whether to print verbose output.

    Returns:
        A success or failure message string from the upload operation.
    """
    scene_id = folder.split('/')[-1]
    
    with open(yaml_file, 'r') as file:
        data = yaml.safe_load(file)
        suffix = data['mtl_format']
        schema = data['schema']
        geometry_type = data['geometry_type']
        description = data['description']
        file_format = data['file_format']
        
    # Pass verbose parameter to _find_and_read_mtl
    mtl_content = _find_and_read_mtl(
        minio_client=minio_client,
        minio_bucket=minio_bucket,
        folder=folder,
        suffix=suffix,
        yaml_file=yaml_file,
        verbose=verbose)  # <-- Add this parameter
    
    # Rest of your function remains the same...
    bands_dict = _dict_from_prfx(mtl_content, 'bands_dict_')
    images, others = list_minio_files(minio_client, minio_bucket, folder, recursive=bool)
    bands = _update_bands(bands_dict, images, minio_url, minio_bucket)
    accessories = _update_accessories(others, minio_url, minio_bucket)
    image_description = describe_minio_image(minio_client, minio_bucket, images[0])
    
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

    yaml_path = str(Path(folder).joinpath(f"{product_name}-metadata.yaml"))
    
    result = _upload_yaml_to_minio(doc,
        yaml_path,
        minio_client,
        minio_bucket,
        minio_url,
        verbose=verbose,
    )
    return result

def _prepare_minio_folder(
    folder: str,
    minio_client: Minio,
    minio_bucket: str,
    minio_url: str,
    product_name: str,
    yaml_file: str,
    pbar: Any = None
) -> Dict[str, Any]:
    """
    Prepares a folder in MinIO by loading a YAML configuration and processing associated data.

    Args:
        folder: The path to the folder in MinIO.
        minio_client: Authenticated MinIO client instance.
        minio_bucket: The name of the MinIO bucket.
        minio_url: The URL of the MinIO server.
        product_name: The name of the product.
        yaml_file: The path to the YAML configuration file.
        pbar: A progress bar object (optional).

    Returns:
        A dictionary containing the folder path, the processing result, and the status (success/failed).
        If failed, the dictionary also includes an error message.
    """
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

def prepare_minio_folders(
    folders: list[str],
    minio_client: Minio,
    minio_bucket: str,
    minio_url: str,
    product_name: str,
    yaml_file: str,
    max_workers: int = 8,
    show_progress: bool = True,
) -> list[dict]:
    """Prepares folders in MinIO based on a YAML configuration.

    This function processes a list of folders, creating them in a MinIO bucket
    according to the specifications outlined in a provided YAML file. It utilizes
    a thread pool for concurrent processing and provides progress updates.

    Args:
        folders: A list of folder paths to prepare in MinIO.
        minio_client: Authenticated MinIO client instance.
        minio_bucket: The name of the MinIO bucket.
        minio_url: The URL of the MinIO server.
        product_name: The name of the product associated with the folders.
        yaml_file: The path to the YAML configuration file.
        max_workers: The maximum number of threads to use for processing. Defaults to 8.
        show_progress: Whether to display a progress bar. Defaults to True.

    Returns:
        list[dict]: A list of dictionaries, where each dictionary represents the result
                     of processing a folder. Each dictionary contains a 'status' key
                     ('success' or 'failed') and, in case of failure, an 'error' key
                     with the error message.
    """
    # Check if YAML file exists
    if not os.path.exists(yaml_file):
        raise FileNotFoundError(f"YAML file not found: {yaml_file}")

    print(f"Processing {len(folders)} folders with {max_workers} threads...")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Use the context manager to get the progress bar
        with _progress_bar_context(total=len(folders), desc="Processing", show_progress=show_progress) as pbar:
            futures = {
                executor.submit(
                    _prepare_minio_folder,
                    folder,
                    minio_client,
                    minio_bucket,
                    minio_url,
                    product_name,
                    yaml_file,
                ): folder for folder in folders
            }
            
            results = []
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                # Update the progress bar via its update method
                pbar.update(1)

    successful = [r for r in results if r['status'] == 'success']
    failed = [r for r in results if r['status'] == 'failed']
    print(f"\n✓ Successful: {len(successful)}, ✗ Failed: {len(failed)}")
    
    if failed:
        print("Failed items:")
        for item in failed:
            folder_name = os.path.basename(item['folder'])
            print(f"  ✗ {folder_name}: {item['error']}")
            
    return results   

def valid_minio_folders(
    minio_client: Minio,
    minio_bucket: str,
    folders: List[str],
    overwrite: bool = False,
) -> List[str]:
    """Validates a list of MinIO folders based on the presence of a metadata file.

    This function checks if folders in a MinIO bucket contain a '-metadata.yaml' file.
    It returns a list of folders that either do not have the metadata file or are
    allowed to be overwritten.

    Args:
        minio_client: Authenticated MinIO client instance.
        minio_bucket: The name of the MinIO bucket.
        folders: A list of folder paths to validate.
        overwrite: Whether to allow overwriting existing folders. Defaults to False.

    Returns:
        List[str]: A list of valid folder paths.
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

    return valid_folder_list

def index_minio_yaml(url: str, product: str) -> Dict[str, Any]:
    """Indexes a MinIO YAML file using a Python script.

    This function executes a Python script (`hcp_to_dc.py`) to process a YAML file
    located in a MinIO bucket and index it for a specific product. It captures the
    script's output and returns a dictionary indicating success or failure.

    Args:
        url (str): The URL of the YAML file in MinIO (e.g., 'minio://host/bucket/path/file.yaml').
        product (str): The name of the product associated with the YAML file.

    Returns:
        Dict[str, Any]: A dictionary containing the result of the indexing process.
                       If successful, it includes 'success': True, 'url', 'stdout', and 'stderr'.
                       If failed, it includes 'success': False, 'url', 'error', and optionally 'returncode'.
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

def index_minio_yamls(
    yamls: List[str],
    product: str,
    max_workers: int = 4,
    show_progress: bool = True,
) -> tuple[List[Dict[str, Any]], List[str]]:
    """Indexes a list of MinIO YAML files using a thread pool.

    This function processes a list of YAML file URLs, indexing each one using the
    `index_minio_yaml` function with a specified product name. It utilizes a thread
    pool for concurrent processing and provides progress updates.

    Args:
        yamls (List[str]): A list of YAML file URLs in MinIO.
        product (str): The name of the product associated with the YAML files.
        max_workers (int, optional): The maximum number of threads to use for processing. Defaults to 4.
        show_progress (bool, optional): Whether to display a progress bar. Defaults to True.

    Returns:
        tuple[List[Dict[str, Any]], List[str]]: A tuple containing two lists:
            - A list of dictionaries, where each dictionary represents the result of indexing a YAML file.
            - A list of error messages encountered during processing.
    """
    execution_results = []
    errors = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(index_minio_yaml, yaml, product): yaml for yaml in yamls}
        
        with _progress_bar_context(total=len(yamls), desc="Processing URLs", show_progress=show_progress, unit="url") as pbar:
            for future in as_completed(futures):
                result = future.result()
                execution_results.append(result)                
                pbar.update(1)
                
    return execution_results, errors
