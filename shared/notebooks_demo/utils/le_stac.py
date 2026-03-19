import pystac_client
from typing import List, Optional, Tuple
from urllib.parse import urlparse

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
    """Collects STAC asset URLs based on search criteria.

    This function connects to a STAC endpoint, searches for items matching the 
    specified criteria (collection, area of interest, time range, layers, etc.), 
    and returns a list of URLs to the associated assets.

    Args:
        endpoint (str): The URL of the STAC endpoint.
        collection (str): The name of the STAC collection to search within.
                           Example: 'sentinel-2-l1c'
        aoi_poly (Tuple[float, float, float, float]): The bounding box for the area of interest 
                                                     (min_lon, min_lat, max_lon, max_lat).
        time_range (Tuple[str, str]): The time range for the search (start_date, end_date).
        layers (List[str], optional): A list of allowed layers (asset keys). 
                                       Defaults to None (no layer filtering).
        t1_only (bool, optional): Whether to only include assets containing '_T1_' in the URL.
                                  Defaults to True.
        modifier (callable, optional): An optional function to modify each asset URL.
                                       Defaults to None.
        platforms (List[str], optional): A list of platforms to filter by. Defaults to None.

    Returns:
        List[str]: A list of URLs to the collected STAC assets.
    """
    try:
        catalog = pystac_client.Client.open(endpoint)
    except Exception as e:
        print(f"Error opening STAC client: {e}")
        return []
    
    try:
        search_params = {
            "collections": [collection],
            "bbox": aoi_poly,
            "datetime": f"{time_range[0]}/{time_range[1]}",
        }
        if platforms:
            search_params["query"] = {
                "platform": {"in": platforms}
            }
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