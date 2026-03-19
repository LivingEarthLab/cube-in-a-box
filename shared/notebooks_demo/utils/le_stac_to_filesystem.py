# Standard library imports
import os
import json
import re
import subprocess
import sys
import threading
import uuid
import xml.dom.minidom as minidom
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from copy import deepcopy
from datetime import timedelta
from fnmatch import fnmatch
from io import BytesIO
from itertools import islice
from pathlib import Path
from threading import Lock
from tqdm import tqdm
from typing import Any, Dict, Iterable, List, Optional, Pattern, Tuple, Union
from urllib.parse import unquote, urlparse

# Third-party imports
import rasterio
import requests
import yaml
import planetary_computer as pc
from concurrent.futures import ThreadPoolExecutor, as_completed
from dateutil import parser
from osgeo import osr
from rasterio.crs import CRS
from rasterio.warp import transform_bounds

from .le_stac import collect_stac_assets

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
    base_path: str,
    assets_dict: Dict[str, List[str]]
) -> List[str]:
    """Find assets missing from filesystem by comparing with STAC assets.

    Args:
        base_path: Base filesystem path to check.
        assets_dict: Dict of assets grouped by directory paths.

    Returns:
        List of asset URLs not found in the filesystem.
    """
    missing_assets = []

    for prefix, asset_list in assets_dict.items():
        dir_path = os.path.join(base_path, prefix)
        
        # Get list of files in directory if it exists
        if os.path.exists(dir_path):
            existing_files = []
            for root, dirs, files in os.walk(dir_path):
                for file in files:
                    rel_path = os.path.relpath(os.path.join(root, file), base_path)
                    existing_files.append(rel_path)
        else:
            existing_files = []

        for asset in asset_list:
            # Extract filename from URL
            parsed_url = urlparse(asset)
            filename = os.path.basename(parsed_url.path)
            
            # Check if file exists in the directory
            if not any(filename in existing_file for existing_file in existing_files):
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
    base_path: str,
    separator_pattern: str = "_T\\d_",
    verbose: bool = True,
    dry_run: bool = False
) -> List[str]:
    """Find missing assets in filesystem and analyze their structure.

    Args:
        all_assets: List of asset URLs/paths.
        base_path: Base filesystem path to search in.
        separator_pattern: Regex pattern to find split point in filename.
        verbose: Print summary statistics if True.
        dry_run: Perform dry run if True.

    Returns:
        List of missing assets.
    """
    assets_dict = _create_assets_dictionary(all_assets)
    missing_assets = _find_missing_assets(base_path, assets_dict)

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

def _download_and_save_to_filesystem(url, base_path: str, verbose=False, pbar=None, results=None, results_lock=None):
    """Download file from URL and save to local filesystem.

    Args:
        url: Full URL of file to download.
        base_path: Base filesystem path for saving files.
        verbose: Print success message if True.
        pbar: Optional progress bar for messages.
        results: Shared dictionary to record outcomes.
        results_lock: Threading lock for thread-safe access to results.

    Returns:
        True if successful, False otherwise.
    """
    remote_file_path = _extract_remote_file_path(url)
    local_file_path = os.path.join(base_path, remote_file_path)
    
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        
        # Download the file in chunks and save to filesystem
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        
        # Write to file
        with open(local_file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        if verbose and pbar:
            pbar.write(f"✅ File '{url}' saved to '{local_file_path}'.")
        
        # Thread-safe success tracking
        if results and results_lock:
            with results_lock:
                results["summary"]["successful"] += 1
                
    except (requests.RequestException, OSError) as e:
        # Thread-safe error tracking
        if results and results_lock:
            with results_lock:
                results["summary"]["failed"] += 1
                results["errors"].append(str(e))  # Only store the error message
        return False
    return True

def _download_save_assets(
    all_assets, 
    base_path: str, 
    desc='Downloading assets', 
    verbose=False, 
    max_workers=5,
    show_progress=True,
    progress_bar_format=None
):
    """Download and save multiple assets to filesystem concurrently.

    Args:
        all_assets: List of asset URLs to download.
        base_path: Base filesystem path for saving files.
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
            futures = [executor.submit(_download_and_save_to_filesystem, url, base_path, verbose, pbar, results, results_lock) for url in all_assets]
            for future in as_completed(futures):
                pbar.update(1)
    
    # Clean up empty errors list if no errors occurred
    if not results["errors"]:
        del results["errors"]
    
    return results

def stac_to_filesystem(
    base_path: str,
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
    """Download STAC assets to local filesystem based on search criteria.

    Main entry point that identifies assets from STAC endpoint, checks for 
    existence in filesystem, and downloads missing or all assets to the local path.

    Args:
        base_path: Base filesystem path for storing files.
        stac_endpoint: STAC API endpoint URL.
        stac_collection: Collection name to search.
        platforms: Platform names to filter by (e.g., ('landsat-8', 'landsat-9')).
        stac_time: Time range (start_time, end_time).
        aoi_poly: Area of interest bounding box.
        stac_layers: Layer names to filter TIF assets.
        group_size: Assets to download per batch.
        t1_only: Filter for T1 processing level only.
        overwrite: Download all assets and overwrite existing ones.
        complete: Only download missing assets from filesystem.
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
    
    # Create base directory if it doesn't exist
    os.makedirs(base_path, exist_ok=True)
    
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
        missing_assets = _check_missing_assets(all_assets, base_path,
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
        result = _download_save_assets(
            signed_assets, 
            base_path, 
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

def list_filesystem_tree(
    base_path: str,
    prefix: str = '',
    max_recursion_level: Optional[int] = None,
    show_sizes: bool = False
) -> None:
    """Lists the contents of a filesystem directory as a tree structure.

    This function recursively lists the files and directories within a specified path, 
    displaying them in a tree-like format. It can optionally show file sizes 
    and limit the recursion depth.

    Args:
        base_path: The base filesystem path to list.
        prefix: A relative path prefix to filter by. Defaults to ''.
        max_recursion_level: The maximum recursion depth to display. 
                            Defaults to None (no limit).
        show_sizes: Whether to display file and folder sizes. 
                   Defaults to False.
    """
    try:
        full_path = os.path.join(base_path, prefix) if prefix else base_path
        
        if not os.path.exists(full_path):
            print(f"⚠️ Path does not exist: {full_path}")
            return
        
        directory_structure = {}
        file_sizes = {}
        
        # Walk the directory tree
        for root, dirs, files in os.walk(full_path):
            # Get relative path from the starting point
            rel_root = os.path.relpath(root, full_path)
            if rel_root == '.':
                rel_root = ''
            
            # Process files
            for file in files:
                file_path = os.path.join(root, file)
                rel_file_path = os.path.relpath(file_path, full_path)
                
                # Store file size
                try:
                    file_sizes[rel_file_path] = os.path.getsize(file_path)
                except OSError:
                    file_sizes[rel_file_path] = 0
                
                # Build directory structure
                parts = rel_file_path.split(os.sep)
                current_level = directory_structure
                for part in parts:
                    if part not in current_level:
                        current_level[part] = {}
                    current_level = current_level[part]
        
        def calculate_folder_sizes(node, current_path=''):
            """Recursively calculate cumulative folder sizes."""
            total_size = 0
            
            for key, value in node.items():
                full_path_key = os.path.join(current_path, key) if current_path else key
                
                if isinstance(value, dict) and value:
                    folder_size = calculate_folder_sizes(value, full_path_key)
                    total_size += folder_size
                else:
                    if full_path_key in file_sizes:
                        total_size += file_sizes[full_path_key]
            
            return total_size
        
        folder_sizes = {}
        if show_sizes:
            def build_folder_sizes(node, current_path=''):
                for key, value in node.items():
                    full_path_key = os.path.join(current_path, key) if current_path else key
                    
                    if isinstance(value, dict) and value:
                        folder_size = calculate_folder_sizes(value, full_path_key)
                        folder_sizes[full_path_key] = folder_size
                        build_folder_sizes(value, full_path_key)
            
            build_folder_sizes(directory_structure)
        
        def human_readable_bytes(size_bytes):
            """Convert bytes to human readable format."""
            for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
                if size_bytes < 1024.0:
                    return f"{size_bytes:.2f} {unit}"
                size_bytes /= 1024.0
            return f"{size_bytes:.2f} PB"
        
        def print_tree(node, current_level=0, is_last=False, indent='', current_path=''):
            if max_recursion_level is not None and current_level > max_recursion_level:
                print(indent + '... (truncated)')
                return
            
            for i, (key, value) in enumerate(node.items()):
                is_last_item = i == len(node) - 1
                full_path_key = os.path.join(current_path, key) if current_path else key
                is_file = isinstance(value, dict) and not value
                
                size_display = ''
                if show_sizes:
                    if is_file and full_path_key in file_sizes:
                        size_display = f' ({human_readable_bytes(file_sizes[full_path_key])})'
                    elif not is_file and full_path_key in folder_sizes:
                        size_display = f' ({human_readable_bytes(folder_sizes[full_path_key])})'
                
                connector = '└── ' if is_last_item else '├── '
                print(indent + connector + key + size_display)
                
                if is_last_item:
                    new_indent = indent + '    '
                else:
                    new_indent = indent + '│   '
                
                if isinstance(value, dict) and value:
                    print_tree(value, current_level + 1, is_last_item, new_indent, full_path_key)
        
        print_tree(directory_structure)
        
    except Exception as e:
        print(f"⚠️ Error listing directory contents: {e}")


def find_last_level_folders(fs_path, filter_pattern = "*"):
    """
    Find all last-level (leaf) folders matching the filter pattern.
    
    Args:
        fs_path: Root path to search from
        filter_pattern: Pattern to match folder paths (supports wildcards)
                       e.g., "oli-tirs/**/*202406*202406"
    
    Returns:
        List of Path objects for matching leaf folders
    """
    root = Path(fs_path)
    matching_folders = []
    
    # Walk through all directories
    for item in root.rglob("*"):
        if item.is_dir():
            # Check if this is a leaf directory (no subdirectories)
            has_subdirs = any(sub.is_dir() for sub in item.iterdir())
            
            if not has_subdirs:
                # Get relative path from root for pattern matching
                rel_path = item.relative_to(root)
                
                # Check if path matches the pattern
                if fnmatch(str(rel_path), filter_pattern):
                    matching_folders.append(str(item.relative_to(fs_path)))
    
    return matching_folders

def _describe_filesystem_image(image_path: str) -> Dict[str, Any]:
    """Describes the geospatial properties of an image in the filesystem.

    Args:
        image_path (str): The path to the image file.

    Returns:
        Dict[str, Any]: A dictionary containing the extracted geospatial properties:
                          'epsg_code' (int or None), 'polygon' (list of coordinates), 
                          'transform' (transform object), 'shape' (tuple of width and height).
    """
    with rasterio.open(image_path) as img:
        # Get bounds and CRS
        left, bottom, right, top = img.bounds
        crs = img.crs

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
            'epsg_code': epsg_code,
            'polygon': polygon,
            'transform': transform,
            'shape': shape
        }

def _get_filesystem_xml_value(xml_doc: minidom.Document, xml_path: str) -> str | None:
    """Retrieves a value from an XML document given an XPath-like path."""
    parts = xml_path.split('/')
    current_node = xml_doc.documentElement
    for part in parts:
        try:
            current_node = current_node.getElementsByTagName(part)[0]
        except IndexError:
            return None
    return current_node.firstChild.nodeValue if current_node.firstChild else None

def _find_file_name_by_suffix(xml_doc: minidom.Document, suffix: str) -> str | None:
    """Finds a file name within an XML document that ends with a specific suffix."""
    product_contents = xml_doc.getElementsByTagName('PRODUCT_CONTENTS')[0]
    for child in product_contents.childNodes:
        if child.nodeType == minidom.Node.ELEMENT_NODE and child.tagName.startswith('FILE_NAME_'):
            if child.firstChild and child.firstChild.nodeValue.strip().endswith(suffix + '.TIF'):
                return child.firstChild.nodeValue.strip()
    return None

def _extract_variables_from_xml(xml_doc: minidom.Document, yaml_config: Dict[str, Any]) -> Dict[str, Any]:
    """Extracts variables from an XML document based on a YAML configuration."""
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
                date_value = _get_filesystem_xml_value(xml_doc, parts[0])
                time_value = _get_filesystem_xml_value(xml_doc, parts[1])
                if date_value and time_value:
                    time_value = time_value.rstrip('Z')
                    result[var_name] = f"{date_value}T{time_value}Z"
                else:
                    result[var_name] = None
            else:
                result[var_name] = None
        
        # Standard XML path extraction
        else:
            result[var_name] = _get_filesystem_xml_value(xml_doc, xml_path)

    return result

def _find_and_read_mtl_filesystem(
    folder: str,
    suffix: str,
    yaml_file: str,
    verbose: bool = False
) -> Optional[Dict[str, Any]]:
    """Finds and reads an MTL file from filesystem, extracts variables based on config."""
    # Load YAML configuration
    try:
        with open(yaml_file, 'r') as f:
            yaml_config = yaml.safe_load(f)
    except Exception as e:
        if verbose:
            print(f"❌ Error loading YAML config from {yaml_file}: {e}")
        raise Exception(f"Error loading YAML config from {yaml_file}: {e}")
    
    # Find matching files
    matches = []
    if os.path.exists(folder):
        for f in os.listdir(folder):
            if f.endswith(suffix) and os.path.isfile(os.path.join(folder, f)):
                matches.append(f)
    
    if not matches:
        error_msg = f"No file with suffix '{suffix}' found in {folder}"
        if verbose:
            print(f"❌ {error_msg}.")
        raise FileNotFoundError(error_msg)
    
    if len(matches) > 1:
        error_msg = f"Multiple files with suffix '{suffix}' found in {folder}: {matches}"
        if verbose:
            print(f"⚠️ {error_msg}")
        raise ValueError(error_msg)
    
    filename = matches[0]
    file_path = os.path.join(folder, filename)
    if verbose:
        print(f"📄 Found file: {file_path}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            file_content = f.read()
            
            if suffix.endswith(".xml"):
                if not file_content.strip().startswith(('<?xml', '<')):
                    error_msg = f"File {filename} is not valid XML"
                    if verbose:
                        print(f"⚠️ {error_msg}.")
                    raise ValueError(error_msg)
                
                # Parse XML and extract variables based on YAML config
                xml_doc = minidom.parseString(file_content)
                return _extract_variables_from_xml(xml_doc, yaml_config)
                
            elif suffix.endswith(".json"):
                 try:
                    json_data = json.loads(file_content)
                    # Note: We don't have extract_variables_from_json implemented yet in this file
                    # If needed, it should be added. For now raising error or simple pass if not used.
                    # Assuming for now we rely on XML as per original code structure usually handling XML MTLs.
                    # If JSON support is strictly needed, need to port extract_variables_from_json logic.
                    raise NotImplementedError("JSON MTL parsing not fully implemented yet")
                 except json.JSONDecodeError as e:
                    error_msg = f"File {filename} is not valid JSON: {e}"
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
            print(f"⚠️ Error reading/parsing {filename}: {e}")
        if isinstance(e, (FileNotFoundError, ValueError, NotImplementedError)):
            raise
        else:
            raise Exception(f"Error reading/parsing {filename}: {e}")

def _dict_from_prfx(
    src_dict: Dict[str, Any],
    prefix: str,
    case_sensitive: bool = True
) -> Dict[str, Any]:
    """Creates a new dictionary from a source dictionary, filtering keys by a prefix."""
    norm_prefix = prefix if case_sensitive else prefix.lower()
    result = {}

    for key, value in src_dict.items():
        compare_key = key if case_sensitive else key.lower()
        if compare_key.startswith(norm_prefix):
            new_key = key[len(prefix):]
            result[new_key] = value

    return result

def _update_bands_filesystem(bands_dict: dict, folder: str) -> dict:
    """Updates a dictionary of band names with their corresponding filesystem paths."""
    updated_bands_dict = {}
    
    # Get all files in folder to match against
    if not os.path.exists(folder):
        return {}
        
    files = [os.path.join(folder, f) for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))]
    
    for band_name, filename in bands_dict.items():
        found_match = False
        for full_path in files:
            if filename in os.path.basename(full_path):
                # Use absolute path for safety in ODC indexing
                updated_bands_dict[band_name] = {'path': os.path.abspath(full_path)}
                found_match = True
                break
    return updated_bands_dict

def _update_accessories_filesystem(others: list, folder: str) -> dict:
    """Updates a dictionary of accessories with their corresponding filesystem paths."""
    updated_accessories_dict = {}
    for other in others:
        # others contains filenames relative to folder usually? 
        # In original code 'others' came from list_minio_files which returned relative paths?
        # Here we assume 'others' are filenames or relative paths
        full_path = os.path.abspath(os.path.join(folder, other))
        updated_accessories_dict[other.split('_')[-1].lower()] = {'path': full_path}
    return updated_accessories_dict

def _save_yaml_to_filesystem(
    doc: dict,
    yaml_path: str,
    verbose: bool = False,
) -> str:
    """Saves a dictionary as a YAML file to the filesystem."""
    try:
        # Dump to string
        yaml_str = yaml.safe_dump(doc, default_flow_style=False, sort_keys=False, allow_unicode=True)
        
        # Remove quotes around stringed lists (same regex as original)
        yaml_str = re.sub(r"coordinates: '(.+?)'", r"coordinates: \1", yaml_str, flags=re.DOTALL)
        yaml_str = re.sub(r"shape: '(.+?)'", r"shape: \1", yaml_str, flags=re.DOTALL)
        yaml_str = re.sub(r"transform: '(.+?)'", r"transform: \1", yaml_str, flags=re.DOTALL)

        with open(yaml_path, 'w', encoding='utf-8') as f:
            f.write(yaml_str)

        if verbose:
            print(f"✅ YAML document saved to {yaml_path}")
        return f"✅ YAML document saved to {yaml_path}"

    except Exception as exc:
        if verbose:
            print(f"⚠️ Failed to save YAML: {exc}")
        return f"⚠️ Failed to save YAML: {exc}"

def prepare_yaml_filesystem(
    folder: str,
    product_name: str,
    yaml_file: str,
    verbose: bool = True
) -> str:
    """Prepares a YAML document from an MTL file and saves it to filesystem.

    Args:
        folder: The folder containing the MTL file and images.
        product_name: The name of the product.
        yaml_file: The path to the YAML configuration file (rules).
        verbose: Whether to print verbose output.

    Returns:
        A success or failure message string.
    """
    scene_id = os.path.basename(folder.rstrip(os.sep))
    
    with open(yaml_file, 'r') as file:
        data = yaml.safe_load(file)
        suffix = data['mtl_format']
        schema = data['schema']
        geometry_type = data['geometry_type']
        description = data['description']
        file_format = data['file_format']
        
    # Read MTL
    mtl_content = _find_and_read_mtl_filesystem(
        folder=folder,
        suffix=suffix,
        yaml_file=yaml_file,
        verbose=verbose
    )
    
    bands_dict = _dict_from_prfx(mtl_content, 'bands_dict_')
    
    # List files in folder to separate images and accessories
    images = []
    others = []
    if os.path.exists(folder):
        for f in os.listdir(folder):
            f_path = os.path.join(folder, f)
            if os.path.isfile(f_path):
                if f.lower().endswith(('.tif', '.tiff')):
                    images.append(f)
                else:
                    others.append(f)
    
    bands = _update_bands_filesystem(bands_dict, folder)
    accessories = _update_accessories_filesystem(others, folder)
    
    # Describe first image to get geometry
    # We need to find which image corresponds to a band to ensure it exists, 
    # but any valid image in the folder typically shares the same grid in these products
    first_image_path = None
    if bands:
        first_image_path = next(iter(bands.values()))['path']
    elif images:
        first_image_path = os.path.join(folder, images[0])
        
    if not first_image_path:
        raise FileNotFoundError(f"No suitable image found in {folder} to extract geometry")
        
    image_description = _describe_filesystem_image(first_image_path)
    
    doc = {
        '$schema': schema,
        'id': str(uuid.uuid4()),
        'label': scene_id[:25] + scene_id[-6:],
        'product': {
            'name': product_name,
        },
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

    yaml_output_path = os.path.join(folder, f"{product_name}-metadata.yaml")
    
    result = _save_yaml_to_filesystem(
        doc,
        yaml_output_path,
        verbose=verbose,
    )
    return result

def _prepare_filesystem_folder(
    folder: str,
    product_name: str,
    yaml_file: str,
    pbar: Any = None
) -> Dict[str, Any]:
    """Prepares a folder in filesystem by loading a YAML configuration and processing associated data."""
    try:
        result = prepare_yaml_filesystem(
            folder=folder,
            product_name=product_name,
            yaml_file=yaml_file,
            verbose=False
        )
        if pbar:
            pbar.set_postfix_str(f"✓ {os.path.basename(folder)}")
        return {
            'folder': folder,
            'result': result,
            'status': 'success'
        }
    except Exception as e:
        if pbar:
            pbar.set_postfix_str(f"✗ {os.path.basename(folder)} failed")
        return {
            'folder': folder,
            'result': None,
            'status': 'failed',
            'error': str(e)
        }

def prepare_filesystem_folders(
    folders: List[str],
    product_name: str,
    yaml_file: str,
    max_workers: int = 8,
    show_progress: bool = True,
) -> List[dict]:
    """Prepares folders in filesystem based on a YAML configuration.

    Args:
        folders: A list of folder paths to prepare.
        product_name: The name of the product associated with the folders.
        yaml_file: The path to the YAML configuration file.
        max_workers: The maximum number of threads to use for processing. Defaults to 8.
        show_progress: Whether to display a progress bar. Defaults to True.

    Returns:
        list[dict]: A list of dictionaries, where each dictionary represents the result
                     of processing a folder.
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
                    _prepare_filesystem_folder,
                    folder,
                    product_name,
                    yaml_file,
                    pbar
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


def dc_add_dataset(
    yaml_paths: List[Union[str, Path]], 
    max_workers: int = 4,
    ignore_lineage: bool = False,
    confirm_ignore_lineage: bool = False,
    verbose: bool = False
) -> Tuple[List[str], List[str], List[Tuple[str, str]]]:
    """
    Add multiple datasets to Open Data Cube in parallel using datacube CLI.
    
    Args:
        yaml_paths: List of paths to YAML metadata files
        max_workers: Maximum number of parallel workers (default: 4)
        ignore_lineage: Add --ignore-lineage flag to skip lineage checks
        confirm_ignore_lineage: Add --confirm-ignore-lineage flag
        verbose: Print full command output for debugging
        
    Returns:
        Tuple of (newly_added, already_indexed, failed_items) where 
        failed_items contains (path, error_message)
    """
    newly_added = []
    already_indexed = []
    failed = []
    
    def add_single_dataset(yaml_path: Union[str, Path]) -> Tuple[str, str, str]:
        """Add a single dataset and return status ('added'/'already'/'failed'), path, and message."""
        yaml_path_str = str(yaml_path)
        
        try:
            cmd = ["datacube", "dataset", "add"]
            
            if ignore_lineage:
                cmd.append("--ignore-lineage")
            if confirm_ignore_lineage:
                cmd.append("--confirm-ignore-lineage")
                
            cmd.append(yaml_path_str)
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode == 0:
                output = result.stdout + result.stderr
                
                # Check if dataset was already indexed
                if "already" in output.lower() or "exists" in output.lower():
                    print(f"⊙ Already indexed: {yaml_path_str}")
                    if verbose:
                        print(f"  Output: {output.strip()}")
                    return "already", yaml_path_str, output
                else:
                    print(f"✓ Successfully added: {yaml_path_str}")
                    if verbose and output.strip():
                        print(f"  Output: {output.strip()}")
                    return "added", yaml_path_str, output
            else:
                error_msg = result.stderr or result.stdout
                print(f"✗ Failed to add {yaml_path_str}: {error_msg}")
                return "failed", yaml_path_str, error_msg
                
        except subprocess.TimeoutExpired:
            error_msg = "Command timed out after 60 seconds"
            print(f"✗ Timeout for {yaml_path_str}")
            return "failed", yaml_path_str, error_msg
        except Exception as e:
            error_msg = str(e)
            print(f"✗ Exception for {yaml_path_str}: {error_msg}")
            return "failed", yaml_path_str, error_msg
    
    print(f"Starting to add {len(yaml_paths)} datasets with {max_workers} workers...")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_path = {executor.submit(add_single_dataset, path): path 
                         for path in yaml_paths}
        
        # Process completed tasks
        for future in as_completed(future_to_path):
            status, path, message = future.result()
            if status == "added":
                newly_added.append(path)
            elif status == "already":
                already_indexed.append(path)
            else:  # failed
                failed.append((path, message))
    
    print(f"\nCompleted: {len(newly_added)} newly added, {len(already_indexed)} already indexed, {len(failed)} failed")
    
    if failed:
        print("\nFailed datasets:")
        for path, error in failed:
            print(f"  - {path}: {error[:100]}")
    
    return newly_added, already_indexed, failed