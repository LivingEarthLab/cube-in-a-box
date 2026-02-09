import re
from IPython.display import HTML, display

def extract_url_from_string(str: str):
    """
    Extract the first HTTP/HTTPS URL found in a given string using regex pattern matching.
    
    Args:
        str (str): Input string that may contain one or more URLs.
            
    Returns:
        str or None: The first URL found in the string, or None if no URL is found.
    """
    url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    match = re.search(url_pattern, str)
    if match:
        url = match.group(0)
        return url
    
def extract_path_from_string(str: str):
    """
    Extract the first filesystem path found in a given string using regex pattern matching.
    
    Args:
        str (str): Input string that may contain one or more paths.
            
    Returns:
        str or None: The first path found in the string, or None if no path is found.
    """
    # Pattern looks for sequences of characters commonly valid in paths, usually containing at least one separator
    path_pattern = r'(?:/?[\w\-\.]+(?:/[\w\-\.]+)+)'
    match = re.search(path_pattern, str)
    if match:
        path = match.group(0)
        return path
    
def human_readable_bytes(
    size_bytes: int
) -> str:
    """Formats a file size in bytes into a human-readable string.

    This function converts a file size in bytes to a more readable format 
    (e.g., KB, MB, GB) with appropriate units.

    Args:
        size_bytes (int): The file size in bytes. 
                           Example: 1024

    Returns:
        str: A human-readable string representing the file size.
             Example: '1.0 KB'
    """
    if size_bytes == 0:
        return "0 B"
    
    size_units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    size = float(size_bytes)
    unit_index = 0
    
    while size >= 1024.0 and unit_index < len(size_units) - 1:
        size /= 1024.0
        unit_index += 1
    
    if unit_index == 0:
        return f"{int(size)} {size_units[unit_index]}"
    else:
        return f"{size:.1f} {size_units[unit_index]}"

def style_output_cells(
    background: str = "white",
    border_radius: str = "0px",
    padding: str = "3px",
    border_width: str = "0px",
    border_color: str = "black"
) -> None:
    """
    Styles the background, border radius, padding, and border of output cells in a Jupyter Notebook.

    This function injects JavaScript code into an HTML img tag to modify the styling of the output area of the closest code cell.
    It sets the background color, border radius, padding, and border properties of the output area.  Finally, it removes the injected img tag.

    Args:
        background (str, optional): The background color for the output cell. Defaults to "white".
        border_radius (str, optional): The border radius for the output cell. Defaults to "0px".
        padding (str, optional): The padding for the output cell. Defaults to "3px".
        border_width (str, optional): The border width for the output cell. Defaults to "0px".
        border_color (str, optional): The border color for the output cell. Defaults to "black".

    Returns:
        None
    """
    js = f"""
    var cell = this.closest('.jp-CodeCell');
    var output = cell.querySelector('.jp-OutputArea');
    if (output) {{
        output.style.background = '{background}';
        output.style.borderRadius = '{border_radius}';
        output.style.padding = '{padding}';
        output.style.border = '{border_width} solid {border_color}';
    }}
    this.parentNode.removeChild(this);
    """
    html = f'<img src onerror="{js}" style="display:none">'
    display(HTML(html))