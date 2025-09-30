import math
import xarray as xr
from ipyleaflet import basemaps, DivIcon, DrawControl, FullScreenControl, \
                       GeoJSON, LayersControl, Map, Marker, Rectangle, \
                       ScaleControl, SearchControl, WMSLayer, ZoomControl
from IPython.display import display, HTML, Javascript
from odc.geo import BoundingBox
from shapely.geometry import box, mapping, Polygon
from shapely.ops import unary_union
from typing import Union, List

from utils.deafrica_plotting import _degree_to_zoom_level

def display_crosshair() -> None:
    """
    Displays a red crosshair on a leaflet map within the Jupyter Notebook.

    This function injects JavaScript code into the notebook to create two red lines
    (vertical and horizontal) that follow the cursor's position on a leaflet map.
    It assumes the map is rendered within a div element with the class 'leaflet-container'.

    Args:
        None

    Returns:
        None
    """
    js_crosshair = """
    // Create the crosshair lines
    const crosshairV = document.createElement('div');
    crosshairV.style.position = 'absolute';
    crosshairV.style.width = '1px';
    crosshairV.style.height = '100%';
    crosshairV.style.backgroundColor = 'red';
    crosshairV.style.left = '0';
    crosshairV.style.top = '0';
    crosshairV.style.pointerEvents = 'none';
    crosshairV.style.zIndex = '1000';
    
    const crosshairH = document.createElement('div');
    crosshairH.style.position = 'absolute';
    crosshairH.style.width = '100%';
    crosshairH.style.height = '1px';
    crosshairH.style.backgroundColor = 'red';
    crosshairH.style.left = '0';
    crosshairH.style.top = '0';
    crosshairH.style.pointerEvents = 'none';
    crosshairH.style.zIndex = '1000';
    
    // Append the lines to the map container
    const mapContainer = document.querySelector('.leaflet-container');
    mapContainer.appendChild(crosshairV);
    mapContainer.appendChild(crosshairH);
    
    // Update the lines' position based on the cursor
    mapContainer.addEventListener('mousemove', (e) => {
        const rect = mapContainer.getBoundingClientRect();
        const x = e.clientX - rect.left;
        const y = e.clientY - rect.top;
    
        crosshairV.style.left = `${x}px`;
        crosshairH.style.top = `${y}px`;
    });
    """
    display(Javascript(js_crosshair))

def bbox_to_polygon(bbox: Union[list[float], BoundingBox], epsg: int = 4326) -> Polygon:
    """
    Converts a bounding box (bbox) to a Shapely Polygon.

    Args:
        bbox (Union[list[float], BoundingBox]): A list of four floats representing the bounding box in the format [min_lon, min_lat, max_lon, max_lat] or a odc.BoundingBox.
        epsg (int, optional): The EPSG code of the coordinate system. Defaults to 4326 (WGS 84).

    Returns:
        Polygon: A Shapely Polygon representing the bounding box.
    """
    min_lon, min_lat, max_lon, max_lat = bbox
    # Define the coordinates for the polygon
    coords = [(min_lon, min_lat), (max_lon, min_lat), (max_lon, max_lat), (min_lon, max_lat)]
    # Create the polygon
    polygon = Polygon(coords)
    return polygon

class MapHandler:
    def __init__(self) -> None:
        """
        Initializes the MapHandler object.

        Args:
            None

        Returns:
            None
        """
        self.aoi_tupple = None

    def handle_draw(self, _, action: str, geo_json: dict) -> tuple | None:
        """
        Handles draw events from the DrawControl.

        This function is called when a drawing action (e.g., 'created') occurs.
        It extracts the coordinates from the geo_json and stores them as the area of interest (AOI).

        Args:
            _ (any):  Unused argument passed by the DrawControl.
            action (str): The type of draw action (e.g., 'created', 'edited').
            geo_json (dict): A dictionary containing the GeoJSON data of the drawn feature.

        Returns:
            tuple | None: The bounds of the drawn polygon as a tuple, or None if the action is not 'created'.
        """
        if action == 'created':
            self.aoi_tupple = Polygon(geo_json['geometry']['coordinates'][0]).bounds
            print(f"Drawing created: {self.aoi_tupple}")  # Debugging statement
        else:
            print(f"Drawing action: {action}")  # Debugging statement
        return self.aoi_tupple
    
    def create_map(self, vect: Union[Polygon, List[Polygon], None] = None, 
                   vector_colors: Union[str, List[str], None] = None,
                   draw_rect: bool = True) -> tuple[Map, DrawControl | None]:
        """
        Creates a Leaflet map with various controls and layers.

        This function initializes a Leaflet map, adds search, full-screen, zoom, layers, and scale controls.
        It also allows for the addition of a draw control for drawing features on the map and GeoJSON layers
        for single or multiple Shapely polygons.

        Args:
            vect (Union[Polygon, List[Polygon], None], optional): 
                A single Shapely Polygon object or a list of Polygon objects to be displayed on the map. 
                Defaults to None.
            vector_colors (Union[str, List[str], None], optional): 
                Color(s) for the vector(s). Can be a single color string for all vectors, 
                or a list of colors matching the number of vectors. Defaults to None (uses default colors).
            draw_rect (bool, optional): 
                A flag indicating whether to add the draw control to the map. Defaults to True.

        Returns:
            tuple[Map, DrawControl | None]: 
                A tuple containing the created Leaflet Map object and the DrawControl object 
                (or None if draw_rect is False).
        """
        # Normalize input to always work with a list
        vectors = []
        if vect is not None:
            if isinstance(vect, list):
                vectors = vect
            else:
                vectors = [vect]
        
        # Normalize colors
        colors = self._normalize_colors(vector_colors, len(vectors))
        
        # Determine the center of the map
        if vectors:
            try:
                # Calculate combined bounds for all vectors
                if len(vectors) == 1:
                    combined_geometry = vectors[0]
                else:
                    combined_geometry = unary_union(vectors)
                
                centroid = combined_geometry.centroid
                center = (centroid.y, centroid.x)
                bounds = combined_geometry.bounds
                lat_zoom_level = _degree_to_zoom_level(bounds[0], bounds[2])
                lon_zoom_level = _degree_to_zoom_level(bounds[1], bounds[3])
                zoom_level = min(lat_zoom_level, lon_zoom_level)
                
                # Ensure we have valid values
                if zoom_level is None or not isinstance(zoom_level, (int, float)):
                    zoom_level = 10  # Default fallback
                if center[0] is None or center[1] is None:
                    center = (0, 0)
                    zoom_level = 2
                    
            except Exception as e:
                print(f"Error calculating map bounds: {e}")
                center = (0, 0)
                zoom_level = 2
        else:
            # Default center if no geometry is provided
            center = (0, 0)
            zoom_level = 2

        m = Map(center=center, zoom=zoom_level, scroll_wheel_zoom=True, zoom_control=False)

        # Add search control
        m.add_control(SearchControl(position="topleft",
                                    url='https://nominatim.openstreetmap.org/search?format=json&q={s}'))

        # Add full screen control
        m.add(FullScreenControl(position="topright"))

        # Add zoom control
        m.add_control(ZoomControl(position="topright"))

        # Add layers control
        layers_control = LayersControl(position="bottomleft")
        m.add(layers_control)

        # Add draw control if draw_rect is True
        if draw_rect:
            draw_control = DrawControl(rectangle={'shapeOptions': {'color': 'red'}},
                                       polygon={},
                                       marker={},
                                       polyline={},
                                       circle={},
                                       circlemarker={})
            draw_control.on_draw(self.handle_draw)
            m.add_control(draw_control)
        else:
            draw_control = None

        # Add scale control
        m.add(ScaleControl(position='bottomright'))

        # Add GeoJSON layers for each vector
        for i, vector in enumerate(vectors):
            # Convert Shapely geometry to GeoJSON
            geojson_data = mapping(vector)

            # Create GeoJSON layer with appropriate color
            geojson_layer = GeoJSON(
                data=geojson_data, 
                style={'color': colors[i], 'weight': 2, 'opacity': 0.8, 'fillOpacity': 0.3}
            )
            m.add_layer(geojson_layer)

        return m, draw_control
    
    def _normalize_colors(self, colors: Union[str, List[str], None], num_vectors: int) -> List[str]:
        """
        Normalize color input to a list of colors matching the number of vectors.
        
        Args:
            colors: Input colors (string, list, or None)
            num_vectors: Number of vectors that need colors
            
        Returns:
            List[str]: List of color strings
        """
        default_colors = ['blue', 'green', 'purple', 'orange', 'darkblue', 
                         'lightred', 'beige', 'darkgreen', 'cadetblue', 'darkpurple', 
                         'white', 'pink', 'lightblue', 'lightgreen', 'gray', 'black', 
                         'lightgray']
        
        if colors is None:
            # Use default colors, cycling if needed
            return [default_colors[i % len(default_colors)] for i in range(num_vectors)]
        elif isinstance(colors, str):
            # Single color for all vectors
            return [colors] * num_vectors
        elif isinstance(colors, list):
            # List of colors - extend with defaults if not enough colors provided
            if len(colors) >= num_vectors:
                return colors[:num_vectors]
            else:
                extended_colors = colors.copy()
                for i in range(len(colors), num_vectors):
                    extended_colors.append(default_colors[i % len(default_colors)])
                return extended_colors
        else:
            # Fallback to default colors
            return [default_colors[i % len(default_colors)] for i in range(num_vectors)]

def get_utm_epsg_code(latitude: float, longitude: float) -> int:
    """
    Calculates the UTM EPSG code for a given latitude and longitude.

    This function determines the Universal Transverse Mercator (UTM) zone number 
    based on the longitude and then calculates the corresponding EPSG code, 
    taking into account the hemisphere (Northern or Southern).

    Args:
        latitude (float): The latitude in decimal degrees.
        longitude (float): The longitude in decimal degrees.

    Returns:
        int: The UTM EPSG code.
    """
    # Calculate the UTM zone number
    zone_number = int((longitude + 180) / 6) + 1

    # Determine the hemisphere
    if latitude >= 0:
        # Northern hemisphere
        epsg_code = 32600 + zone_number
    else:
        # Southern hemisphere
        epsg_code = 32700 + zone_number

    return epsg_code
