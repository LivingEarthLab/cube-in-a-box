import math
from ipyleaflet import Map, DrawControl, basemaps, Rectangle
from shapely.geometry import box
from IPython.display import display, Javascript

# Shared state
last_drawn_rectangle = {}

def create_map(bbox):
    """
    Create an ipyleaflet map with draw control and a crosshair overlay.
    The last drawn rectangle is stored in `last_drawn_rectangle`.
    """
    left, bottom, right, top = bbox
    center_lat = (bottom + top) / 2
    center_lon = (left + right) / 2
    margin, zoom_bias = -0.5, 0
    zoom = _degree_to_zoom_level(max(right - left, top - bottom), margin=margin) + zoom_bias

    # Create map
    m = Map(
        basemap=basemaps.OpenStreetMap.Mapnik,
        center=(center_lat, center_lon),
        zoom=zoom,
        scroll_wheel_zoom=True
    )

    # Show initial bbox
    bbox_rect = Rectangle(
        bounds=((bottom, left), (top, right)),
        color="red",
        fill_opacity=0.1,
        weight=2
    )
    m.add_layer(bbox_rect)

    # Enable drawing rectangles only
    draw_control = DrawControl(
        polygon={}, polyline={}, circle={}, circlemarker={}, marker={},
        rectangle={"shapeOptions": {"color": "#0000FF"}}
    )

    def handle_draw(target, action, geo_json):
        if geo_json["geometry"]["type"] == "Polygon":
            coords = geo_json["geometry"]["coordinates"][0]
            lons, lats = zip(*coords[:-1])
            rect = box(min(lons), min(lats), max(lons), max(lats))
            last_drawn_rectangle.clear()
            last_drawn_rectangle.update({
                "bounds": rect.bounds,
                "shapely": rect,
                "geojson": geo_json
            })

    draw_control.on_draw(handle_draw)
    m.add_control(draw_control)

    # Inject crosshair overlay with JS
    display(Javascript("""
    setTimeout(() => {
        const containers = document.querySelectorAll('.leaflet-container');
        const container = containers[containers.length - 1];
        if (!container || container._crosshairInjected) return;
        container._crosshairInjected = true;

        const vLine = document.createElement('div');
        const hLine = document.createElement('div');

        Object.assign(vLine.style, {
            position: 'absolute',
            width: '1px',
            background: 'black',
            top: '0',
            bottom: '0',
            zIndex: '9999',
            pointerEvents: 'none',
            display: 'none'
        });

        Object.assign(hLine.style, {
            position: 'absolute',
            height: '1px',
            background: 'black',
            left: '0',
            right: '0',
            zIndex: '9999',
            pointerEvents: 'none',
            display: 'none'
        });

        container.appendChild(vLine);
        container.appendChild(hLine);

        container.addEventListener('mousemove', (e) => {
            const rect = container.getBoundingClientRect();
            const x = e.clientX - rect.left;
            const y = e.clientY - rect.top;
            vLine.style.left = x + 'px';
            hLine.style.top = y + 'px';
            vLine.style.display = 'block';
            hLine.style.display = 'block';
        });

        container.addEventListener('mouseleave', () => {
            vLine.style.display = 'none';
            hLine.style.display = 'none';
        });
    }, 500);
    """))

    return m

# modified from deafrica_plotting.py
def _degree_to_zoom_level(dl, margin=0.0):
    
    """
    Helper function to set zoom level for `display_map`
    """
    
    degree = abs(dl) * (1 + margin)
    zoom_level_int = 0
    if degree != 0:
        zoom_level_float = math.log(360 / degree) / math.log(2)
        zoom_level_int = int(zoom_level_float)
    else:
        zoom_level_int = 18
    return zoom_level_int

def get_utm_epsg_code(latitude, longitude):
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

# import xarray as xr

# from ipyleaflet import Map, WMSLayer, GeoJSON, Marker, DivIcon, \
#                        SearchControl, FullScreenControl, ZoomControl, \
#                        LayersControl, DrawControl, ScaleControl, \
#                        Polygon as LeafletPolygon
# from shapely.geometry import mapping, Polygon
# from IPython.display import HTML, display, Javascript

# def display_crosshair():
#     # Define JavaScript to add crosshair lines
#     js_crosshair = """
#     // Create the crosshair lines
#     const crosshairV = document.createElement('div');
#     crosshairV.style.position = 'absolute';
#     crosshairV.style.width = '1px';
#     crosshairV.style.height = '100%';
#     crosshairV.style.backgroundColor = 'red';
#     crosshairV.style.left = '0';
#     crosshairV.style.top = '0';
#     crosshairV.style.pointerEvents = 'none';
#     crosshairV.style.zIndex = '1000';
    
#     const crosshairH = document.createElement('div');
#     crosshairH.style.position = 'absolute';
#     crosshairH.style.width = '100%';
#     crosshairH.style.height = '1px';
#     crosshairH.style.backgroundColor = 'red';
#     crosshairH.style.left = '0';
#     crosshairH.style.top = '0';
#     crosshairH.style.pointerEvents = 'none';
#     crosshairH.style.zIndex = '1000';
    
#     // Append the lines to the map container
#     const mapContainer = document.querySelector('.leaflet-container');
#     mapContainer.appendChild(crosshairV);
#     mapContainer.appendChild(crosshairH);
    
#     // Update the lines' position based on the cursor
#     mapContainer.addEventListener('mousemove', (e) => {
#         const rect = mapContainer.getBoundingClientRect();
#         const x = e.clientX - rect.left;
#         const y = e.clientY - rect.top;
    
#         crosshairV.style.left = `${x}px`;
#         crosshairH.style.top = `${y}px`;
#     });
#     """
#     display(Javascript(js_crosshair))

# class MapHandler:
#     def __init__(self):
#         self.aoi_poly = None

#     def handle_draw(self, _, action, geo_json):
#         if action == 'created':
#             self.aoi_poly = Polygon(geo_json['geometry']['coordinates'][0]).bounds
#             print(f"Drawing created: {self.aoi_poly}")  # Debugging statement
#         else:
#             print(f"Drawing action: {action}")  # Debugging statement
#         return self.aoi_poly
    
#     def _bbox_to_polygon_coords(self, bbox):
#         """Convert BoundingBox to list of [lat, lon] coordinate pairs (closed polygon)."""
#         return [
#             [bbox.bottom, bbox.left],
#             [bbox.bottom, bbox.right],
#             [bbox.top, bbox.right],
#             [bbox.top, bbox.left],
#             [bbox.bottom, bbox.left],  # close the loop
#         ]

#     def create_map(self, draw_rect=True, bbox=None):
#         center = (0, 0)

#         m = Map(center=center, zoom=2, scroll_wheel_zoom=True, zoom_control=False)

#         # Add search control
#         m.add_control(SearchControl(position="topleft",
#                                     url='https://nominatim.openstreetmap.org/search?format=json&q={s}'))

#         # Add full screen control
#         m.add(FullScreenControl(position="topright"))

#         # Add zoom control
#         m.add_control(ZoomControl(position="topright"))

#         # Add layers control
#         layers_control = LayersControl(position="bottomleft")
#         m.add(layers_control)
        
#         if bbox is not None:
#             polygon_coords = self._bbox_to_polygon_coords(bbox)
#             bbox_polygon = LeafletPolygon(
#                 locations=polygon_coords,
#                 color="blue",
#                 fill_color="blue",
#                 fill_opacity=0.2
#             )
#             m.add_layer(bbox_polygon)

#             # Recenter map on bbox
#             center = [
#                 (bbox.bottom + bbox.top) / 2,
#                 (bbox.left + bbox.right) / 2
#             ]
#             m.center = center
#             m.zoom = 6

#         # Add draw control if draw_rect is True
#         if draw_rect:
#             draw_control = DrawControl(rectangle={'shapeOptions': {'color': 'red'}},
#                                        polygon={},
#                                        marker={},
#                                        polyline={},
#                                        circle={},
#                                        circlemarker={})
#             draw_control.on_draw(self.handle_draw)
#             m.add_control(draw_control)
#         else:
#             draw_control = None

#         # Add scale control
#         m.add(ScaleControl(position='bottomright'))

#         return m  # , draw_control
