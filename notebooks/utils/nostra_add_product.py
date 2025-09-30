import logging
import requests
import subprocess
import yaml
import pandas as pd
from io import StringIO
from IPython.display import display
from ipywidgets import Dropdown, Button, VBox, HBox, Output
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union

# -------- Configuration --------
KEY_SEPARATOR = " - "
DEBUG_MODE = False  # Set True to surface full tracebacks for debugging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


# --------- Helpers ---------
def read_text_from_source(source: Union[str, Path]) -> str:
    """
    Reads text content from a given source (URL or file path).

    This function reads text from either a URL or a local file. 
    If the source is a URL, it fetches the content using a GET request. 
    If the source is a file path, it reads the content from the file.

    Args:
        source (Union[str, Path]): The source of the text. Can be a URL (string) or a file path (string or Path object).

    Returns:
        str: The text content read from the source.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        requests.exceptions.RequestException: If there is an error fetching the content from the URL.
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

def _read_csv_product_definitions(csv_path: Union[str, Path]) -> Dict[str, str]:
    """
    Reads product definitions from a CSV file.

    This function reads a CSV file and extracts product definitions based on common column name pairs.
    It attempts to identify the product and definition columns and returns a dictionary mapping product names to their definitions.

    Args:
        csv_path (Union[str, Path]): The path to the CSV file.

    Returns:
        Dict[str, str]: A dictionary mapping product names to their definitions.

    Raises:
        ValueError: If the CSV file cannot be read or does not contain the expected column pairs.
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

def parse_yaml_documents(content: str) -> List[Dict[str, Any]]:
    """
    Parses multiple YAML documents from a single string.

    This function parses a string containing multiple YAML documents and returns a list of dictionaries,
    filtering out any documents that are not dictionaries.

    Args:
        content (str): The string containing the YAML documents.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries representing the parsed YAML documents.

    Raises:
        ValueError: If there is an error parsing the YAML content.
    """
    try:
        return [doc for doc in yaml.safe_load_all(content) if isinstance(doc, dict)]
    except yaml.YAMLError as e:
        raise ValueError(f"YAML parse error: {e}") from e
    
def _build_product_index(sources_dict: Dict[str, str]) -> Dict[str, Tuple[Union[str, Path], str]]:
    """
    Builds an index of products from various sources (CSV files and YAML directories).

    This function processes a dictionary of sources, either CSV files containing product URLs or directories
    containing YAML files, to build an index mapping product names to their source file and name.

    Args:
        sources_dict (Dict[str, str]): A dictionary where keys are source identifiers and values are either
            paths to CSV files or paths to directories containing YAML files.

    Returns:
        Dict[str, Tuple[Union[str, Path], str]]: A dictionary mapping product keys to a tuple containing the
            source file path (str or Path object) and the product name (str).

    Raises:
        FileNotFoundError: If a specified directory does not exist or contains no YAML files.
        ValueError: If no valid products are found in the specified sources.
    """
    index: Dict[str, Tuple[Union[str, Path], str]] = {}

    for source_key, source_path in sources_dict.items():
        if isinstance(source_path, str) and source_path.endswith(".csv"):
            # CSV lists product URLs; read mapping then peek each product URL's YAML
            product_map = _read_csv_product_definitions(source_path)
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

def select_and_save_product(sources_dict: Dict[str, str], output_filepath: str) -> None:
    """
    Selects a product from a source dictionary and saves its YAML definition to a file.

    This function builds a product index from a dictionary of sources, presents a dropdown menu to select a product,
    and saves the corresponding YAML definition to the specified output file when a "Save YAML" button is clicked.

    Args:
        sources_dict (Dict[str, str]): A dictionary where keys are source identifiers and values are either
            paths to CSV files or paths to directories containing YAML files.
        output_filepath (str): The path to the output YAML file.

    Raises:
        Exception: If there is an error building the product index or saving the YAML file.  (If DEBUG_MODE is False, errors are printed instead of raised.)
    """
    try:
        product_index = _build_product_index(sources_dict)
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

    
def parse_product_yaml(filename: str) -> None:
    """
    Parses and displays information from a product YAML file.

    This function reads a YAML file, validates its structure, and prints basic information 
    about the product, including its name, description, metadata type, and a list of measurements.

    Args:
        filename (str): The path to the YAML file.

    Prints:
        Information about the product to the console.
        Error messages if the file is not found or the YAML is invalid.
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
    """Adds a product via the datacube CLI.

    This function attempts to add a product to the database using the `datacube product add` command.
    If the product already exists and `update` is True, it attempts to update the product using `datacube product update`.

    Args:
        yaml_file_path (str): The path to the YAML file defining the product.
        update (bool, optional): Whether to update the product if it already exists. Defaults to False.

    Returns:
        bool: True if the product was successfully added or updated, False otherwise.
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
