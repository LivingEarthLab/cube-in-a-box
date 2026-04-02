import rasterio
import re
import subprocess
import sys
import yaml

from concurrent.futures import ThreadPoolExecutor, as_completed
from dateutil.parser import parse
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union


def add_product(product_definition):
    """Adds a product to the datacube using the given product definition.

    Args:
        product_definition (str): The path to the product definition file.

    Returns:
        str: The result of the operation. If successful, returns 'Product added successfully'.
             If there is an error, returns the error message.
    """
    try:
        with open(product_definition, 'r') as f:
            data = yaml.safe_load(f)
        product_name = data['metadata']['product']['name']
    except FileNotFoundError:
        sys.exit(f"Error: The file {product_definition} was not found.")
    except (yaml.YAMLError, KeyError, TypeError) as e:
        sys.exit(f"Error parsing product name from YAML: {e}")
        
    
    cmd = ["datacube", "product", "add", product_definition]
    
    result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=60
                )
    
    if result.stderr != '':
        if result.returncode==1:
            sys.exit(result.stderr)
        else:
            print(result.stderr)
    else:
        print(f"{product_name} product added successfully")
    return product_name

def describe_filesystem_image(image_path: str) -> Dict[str, Any]:
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

def extract_and_format_date(input_string, output_format="%Y-%m-%dT%H:%M:%SZ"):
    # Regex pattern to match dates with month names or abbreviations
    pattern = (
        r'(\d{4}[-/._]\d{1,2}[-/._]\d{1,2})|'  # YYYY-MM-DD, YYYY/MM/DD, etc.
        r'(\d{1,2}[-/._]\d{1,2}[-/._]\d{2,4})|'  # DD-MM-YYYY, MM/DD/YYYY, etc.
        r'([a-zA-Z]{3,9}\s?\d{1,2},?\s?\d{4})|'  # "Jan 5, 2021", "January 05 2021", etc.
        r'(\d{1,2}\s?[a-zA-Z]{3,9}\s?\d{4})'  # "5 Jan 2021", "05 January 2021", etc.
    )
    matches = re.findall(pattern, input_string, re.IGNORECASE)

    for match in matches:
        # Combine the matched groups (some may be empty)
        date_str = match[0] or match[1] or match[2] or match[3]
        if not date_str:
            continue

        # Replace any non-standard separators with spaces or hyphens
        date_str = re.sub(r'[/_]', '-', date_str)

        try:
            # Parse the date using dateutil.parser
            date_obj = parse(date_str, dayfirst=False, yearfirst=True)
            # Format as YYYY-MM-DD
            return date_obj.strftime(output_format)
        except (ValueError, OverflowError):
            continue

    return None




def fill_template(template_path: Path, values: dict) -> str:
    """
    Load a YAML template as plain text, substitute all <placeholder> strings
    with values from the provided dict, and return the filled YAML string.

    The template controls the serialisation format:

    - [<key>] or [[<key>]]  →  flow/inline style.
      The template brackets are kept as-is; all bracket levels are stripped
      from the serialised value before re-wrapping.
        shape: [<shape>]           with shape=[520,560]       → shape: [520, 560]
        coordinates: [[<coords>]]  with coords=[[x,y],...]    → coordinates: [[x,y],...]

    - - <key>  (block sequence entry)  →  block style, indented under the dash.
        proj:transform:
        - <transform>              with transform=[20,0,...]  → - - 20.0
                                                                  - 0.0  ...

    - <key>  (standalone line)  →  block style at that indent level.
        measurements:
          <bands_dict>             →  ndvi_min:\n    path: ...

    Write the returned string directly to disk to preserve formatting:
        out_path.write_text(fill_template(template_path, values))

    Raises KeyError listing all missing placeholders at once.
    """
    PLACEHOLDER = re.compile(r'<(.+?)>')
    
    raw = template_path.read_text()
    raw = re.sub(r'\s*#.*', '', raw)  # strip inline comments

    keys_in_template = {m.group(1) for m in PLACEHOLDER.finditer(raw)}
    missing = keys_in_template - values.keys()
    if missing:
        raise KeyError(
            f"Template placeholder(s) {sorted(f'<{k}>' for k in missing)} "
            f"have no matching value."
        )

    result_lines = []
    for line in raw.splitlines(keepends=True):
        m = PLACEHOLDER.search(line)
        if m is None:
            result_lines.append(line)
            continue

        key = m.group(1)
        val = values[key]

        # Scalar — simple string substitution
        if not isinstance(val, (list, dict)):
            result_lines.append(PLACEHOLDER.sub(str(val), line))
            continue

        # Flow: [<key>] or [[<key>]] etc.
        flow_m = re.search(r'(\[+)<' + re.escape(key) + r'>(\]+)', line)
        if flow_m:
            n_wrap = min(len(flow_m.group(1)), len(flow_m.group(2)))
            s = yaml.dump(val, default_flow_style=True).strip()
            # strip all n_wrap bracket levels — the template brackets replace them
            for _ in range(n_wrap):
                if s.startswith('[') and s.endswith(']'):
                    s = s[1:-1]
            replacement = flow_m.group(1) + s + flow_m.group(2)
            result_lines.append(line[:flow_m.start()] + replacement + line[flow_m.end():])
            continue

        # Block sequence entry:  - <key>
        if re.match(r'^\s*- <' + re.escape(key) + r'>\s*$', line.rstrip('\n')):
            indent = re.match(r'^(\s*)', line).group(1)
            block  = yaml.dump(val, default_flow_style=False).strip().splitlines()
            result_lines.append(indent + '- ' + ('\n' + indent + '  ').join(block) + '\n')
            continue

        # Standalone:  <key>
        indent = re.match(r'^(\s*)', line).group(1)
        block  = yaml.dump(val, default_flow_style=False).strip().splitlines()
        result_lines.append(indent + ('\n' + indent).join(block) + '\n')

    return ''.join(result_lines)

def add_dataset(
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