from shapely.geometry import box
from shapely.ops import unary_union
import random

def get_product_bbox(dc, product, query_params=None, split_size=-1, stability_threshold=2):
    """
    Compute the unified bounding box efficiently by processing small batches incrementally
    until the bounding box stabilizes (stops changing).
    
    Args:
        dc: Datacube instance.
        product (str): Product name (e.g., 'ls8_nbart_geomedian').
        query_params (dict, optional): Additional query parameters (e.g., time, region).
        split_size (int, optional): Number of datasets per split. If -1, process all datasets without splitting.
                                    Defaults to -1.
        stability_threshold (int, optional): Number of consecutive identical bboxes needed for early termination.
                                             Defaults to 2.
    
    Returns:
        shapely.geometry.Polygon: Unified bounding box in WGS84.
    """
    # Query datasets
    dss = list(dc.find_datasets(product=product, **(query_params or {})))
    if not dss:
        raise ValueError(f"No datasets found for product: {product}")
    
    # If split_size is -1, process all datasets at once without splitting
    if split_size == -1:
        bboxes = []
        for ds in dss:
            try:
                extent = ds.extent
                if extent.crs != 'epsg:4326':
                    geom = extent.to_crs('epsg:4326')
                else:
                    geom = extent
                bboxes.append(box(*geom.boundingbox))
            except:
                continue
        return unary_union(bboxes) if bboxes else None
    
    # Normal splitting approach
    random.shuffle(dss)
    splits = [dss[i:i + split_size] for i in range(0, len(dss), split_size)]
    
    def process_split(datasets):
        """Process a split of datasets and return unified bbox."""
        bboxes = []
        for ds in datasets:
            try:
                extent = ds.extent
                if extent.crs != 'epsg:4326':
                    geom = extent.to_crs('epsg:4326')
                else:
                    geom = extent
                bboxes.append(box(*geom.boundingbox))
            except:
                continue
        return unary_union(bboxes) if bboxes else None
    
    # Process first split
    unified_bbox = process_split(splits[0])
    if unified_bbox is None:
        raise ValueError("Failed to process first split")
    
    bbox_history = [unified_bbox]
    
    # Process remaining splits
    for i in range(1, len(splits)):
        current_split_bbox = process_split(splits[i])
        if current_split_bbox is None:
            continue
        
        new_unified_bbox = unary_union([unified_bbox, current_split_bbox])
        bbox_history.append(new_unified_bbox)
        
        # Check for stability
        if len(bbox_history) >= stability_threshold:
            recent_bboxes = bbox_history[-stability_threshold:]
            if all(bbox.equals(recent_bboxes[0]) for bbox in recent_bboxes):
                return new_unified_bbox
        
        unified_bbox = new_unified_bbox
    
    return unified_bbox