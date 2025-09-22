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

    # Create map
    m = Map(
        basemap=basemaps.OpenStreetMap.Mapnik,
        center=(center_lat, center_lon),
        zoom=6,
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
