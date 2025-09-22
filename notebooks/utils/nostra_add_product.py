import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union

import requests
import yaml
import pandas as pd
from io import StringIO
from ipywidgets import Dropdown, Button, VBox, HBox, Output
from IPython.display import display

import subprocess

# -------- Configuration --------
KEY_SEPARATOR = " - "
DEBUG_MODE = False  # Set True to surface full tracebacks for debugging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


# --------- Helpers ---------
def read_text_from_source(source: Union[str, Path]) -> str:
    """
    Read plain text from a local file or URL.

    Parameters:
    - source (str | Path): local path or http/https URL

    Returns:
    - str: text content

    Raises:
    - FileNotFoundError: local file missing
    - requests.RequestException: network/HTTP errors
    """
    src = str(source)
    if src.startswith(("http://", "https://")):
        resp = requests.get(src, timeout=15)
        resp.raise_for_status()
        return resp.text
    p = Path(src)
    if not p.exists():
        raise FileNotFoundError(f"Local file not found: {p}")
    return p.read_text(encoding="utf-8")


def parse_yaml_documents(content: str) -> List[Dict[str, Any]]:
    """
    Parse YAML content into a list of dict documents (skips non-dict docs).

    Parameters:
    - content (str): YAML text possibly containing multiple documents separated by '---'

    Returns:
    - list[dict]: parsed YAML documents

    Raises:
    - ValueError: YAML parsing error
    """
    try:
        return [doc for doc in yaml.safe_load_all(content) if isinstance(doc, dict)]
    except yaml.YAMLError as e:
        raise ValueError(f"YAML parse error: {e}") from e


def read_csv_product_definitions(csv_path: Union[str, Path]) -> Dict[str, str]:
    """
    Read a CSV mapping of product-name -> product-URL.
    Accepts several common column name variants.

    Parameters:
    - csv_path (str | Path): path or URL to CSV

    Returns:
    - dict: {product_name: product_url}

    Raises:
    - ValueError: CSV missing expected columns or parsing errors
    - FileNotFoundError / requests.RequestException: if CSV can't be read
    """
    text = read_text_from_source(csv_path)
    try:
        df = pd.read_csv(StringIO(text))
    except Exception as e:
        raise ValueError(f"Error reading CSV at {csv_path}: {e}") from e

    # Accept multiple common column name pairs
    candidate_pairs = [
        ("product", "definition"),
        ("product_name", "product_url"),
        ("product", "product_url"),
        ("name", "url"),
    ]
    for left, right in candidate_pairs:
        if {left, right} <= set(df.columns):
            return dict(zip(df[left], df[right]))

    raise ValueError(
        f"CSV at {csv_path} must contain one of the column pairs: "
        "(product,definition), (product_name,product_url), (product,product_url), (name,url)"
    )


# --------- Index builder (extracts names from YAML docs) ---------
def build_product_index(sources_dict: Dict[str, str]) -> Dict[str, Tuple[Union[str, Path], str]]:
    """
    Build an index mapping display_key -> (source, product_name).

    - Splits multi-document YAML files, using each document's 'name' field.
    - Ensures unique display keys (adds " (2)" suffix when needed).

    Parameters:
    - sources_dict (dict): {source_key: local_dir_or_csv_url}

    Returns:
    - dict: {display_key: (source_path_or_url, product_name)}

    Raises:
    - FileNotFoundError / ValueError: on missing inputs or parse problems
    """
    index: Dict[str, Tuple[Union[str, Path], str]] = {}

    for source_key, source_path in sources_dict.items():
        if isinstance(source_path, str) and source_path.endswith(".csv"):
            # CSV lists product URLs; read mapping then peek each product URL's YAML
            product_map = read_csv_product_definitions(source_path)
            for product_name_hint, product_url in product_map.items():
                content = read_text_from_source(product_url)
                docs = parse_yaml_documents(content)
                for doc in docs:
                    name = doc.get("name")
                    if not name:
                        # if the doc lacks a name, fall back to the csv product_name_hint
                        name = product_name_hint
                    base_key = f"{source_key}{KEY_SEPARATOR}{name}"
                    key = base_key
                    # make unique if needed
                    if key in index:
                        i = 2
                        while f"{base_key} ({i})" in index:
                            i += 1
                        key = f"{base_key} ({i})"
                    index[key] = (product_url, name)

        else:
            # treat source_path as local directory containing YAML files
            p = Path(source_path)
            if not p.exists() or not p.is_dir():
                raise FileNotFoundError(f"Directory not found: {p}")
            files = sorted(p.glob("*.y*ml"))
            if not files:
                raise FileNotFoundError(f"No YAML files found in directory: {p}")

            for file in files:
                content = read_text_from_source(file)
                docs = parse_yaml_documents(content)
                for doc in docs:
                    if not isinstance(doc, dict):
                        continue
                    name = doc.get("name")
                    if not name:
                        # skip docs that don't have a name (you can change to fall back if desired)
                        continue
                    base_key = f"{source_key}{KEY_SEPARATOR}{name}"
                    key = base_key
                    if key in index:
                        i = 2
                        while f"{base_key} ({i})" in index:
                            i += 1
                        key = f"{base_key} ({i})"
                    index[key] = (file, name)

    if not index:
        raise ValueError("No valid products found in the specified sources.")
    return index


# --------- YAML save utility ---------
def save_dict_as_yaml(data: Dict[str, Any], output_filepath: Union[str, Path]) -> None:
    """
    Save a dictionary as a YAML file (preserving key order).

    Parameters:
    - data (dict): the product dict to save
    - output_filepath (str | Path): destination path

    Returns:
    - None
    """
    Path(output_filepath).write_text(
        yaml.dump(data, sort_keys=False, default_flow_style=False),
        encoding="utf-8",
    )
    print(f"✅ Saved YAML to {output_filepath}")


# --------- Widget UI (lazy-load on selection) ---------
def select_and_save_product(sources_dict: Dict[str, str], output_filepath: str) -> None:
    """
    Show an interactive dropdown of discovered products (splits multi-doc YAMLs).
    The chosen document is read and saved only when the user clicks Save.

    Parameters:
    - sources_dict (dict): {source_key: local_dir_or_csv_url}
    - output_filepath (str): destination YAML path

    Returns:
    - None

    Notes:
    - Errors are printed cleanly; enable DEBUG_MODE for tracebacks.
    """
    try:
        product_index = build_product_index(sources_dict)
    except Exception as e:
        if DEBUG_MODE:
            raise
        print(f"❌ Error building product index: {e}")
        return

    dropdown = Dropdown(options=sorted(product_index.keys()), description="Product:")
    button = Button(description="Save YAML", button_style="success")
    output = Output()

    def on_click(_):
        with output:
            output.clear_output()
            sel = dropdown.value
            if sel is None:
                print("❌ No product selected")
                return
            source, product_name = product_index[sel]
            try:
                text = read_text_from_source(source)
                docs = parse_yaml_documents(text)
                # find matching doc by name
                found = None
                for doc in docs:
                    if doc.get("name") == product_name:
                        found = doc
                        break
                if found is None:
                    print(f"❌ Could not find product '{product_name}' in source {source}")
                    return
                save_dict_as_yaml(found, output_filepath)
            except Exception as e:
                if DEBUG_MODE:
                    raise
                print(f"❌ Error reading/saving product '{product_name}': {e}")

    button.on_click(on_click)
    # display(VBox([dropdown, button, output]))
    display(VBox([HBox([dropdown, button]), output]))

    
def parse_product_yaml(filename):
    """
    Parse a product YAML file and print relevant information.
    
    Parameters:
    - filename: str - Path to the YAML file
    
    Returns:
    - None
    """
    try:
        with open(filename, 'r') as f:
            product_doc = yaml.safe_load(f)
            
        print("YAML file is valid!")
        
        # Print basic information
        print(f"Product name: {product_doc.get('name', 'Not specified')}")
        print(f"Description: {product_doc.get('description', 'Not specified')}")
        print(f"Metadata type: {product_doc.get('metadata_type', 'Not specified')}")
        
        # Process measurements
        measurements = product_doc.get('measurements', [])
        print(f"Number of measurements: {len(measurements)}")
        
        if measurements:
            print("Measurements:")
            for i, measurement in enumerate(measurements[:3]):  # Show first 3
                print(f"  {i+1}. {measurement.get('name', 'Unnamed')}")
            if len(measurements) > 3:
                print(f"  ... and {len(measurements) - 3} more")
        
    except FileNotFoundError:
        print(f"YAML file '{filename}' not found!")
    except yaml.YAMLError as e:
        print(f"Invalid YAML: {e}")

def add_product_via_cli(yaml_file_path: str, update: bool = False) -> bool:
    """
    Adds or updates a product in the datacube using the CLI command.

    Args:
        yaml_file_path (str): Path to the YAML file defining the product.
        update (bool): If True, updates the product if it already exists. Defaults to False.

    Returns:
        bool: True if the product was added or updated successfully, False otherwise.
    """
    try:
        # Try to add the product
        result = subprocess.run(
            ['datacube', 'product', 'add', yaml_file_path],
            capture_output=True,
            text=True,
            check=True
        )

        if result.stdout:
            print("Success:", result.stdout)
        return True

    except subprocess.CalledProcessError as e:
        error_message = e.stderr

        # Check if the error is due to the product already existing
        if "is already in the database, checking for differences" in error_message:
            if update:
                print("Product already exists. Attempting to update...")
                try:
                    # Update the product
                    update_result = subprocess.run(
                        ['datacube', 'product', 'update', '--allow-unsafe', yaml_file_path],
                        capture_output=True,
                        text=True,
                        check=True
                    )

                    if update_result.stdout:
                        print("Update success:", update_result.stdout)
                    return True
                except subprocess.CalledProcessError as update_error:
                    print(f"Failed to update product: {update_error.stderr}")
                    return False
            else:
                print(
                    "Error: Product already exists in the database. "
                    "Set `update=True` to update the existing product."
                )
                return False
        else:
            print(f"Failed to add product: {error_message}")
            return False
