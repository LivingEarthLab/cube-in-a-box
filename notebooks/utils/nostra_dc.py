from shapely.geometry import box, Polygon
from shapely.ops import unary_union
from typing import Any, Dict, Optional, Tuple
import random

def get_product_bbox(
    dc: Any,
    product: str,
    query_params: Optional[Dict] = None,
    split_size: int = -1,
    stability_threshold: int = 2
) -> Optional[Tuple[float, float, float, float]]:
    """Calculates the bounding box of a product's datasets.
    Queries datasets for a given product and calculates their unified bounding box.
    Optionally splits the dataset list for processing and checks for stability in the bounding box.
    Args:
        dc: The DataCube object.
        product (str): The name of the product.
        query_params (dict, optional): Query parameters to filter datasets. Defaults to None.
        split_size (int, optional): The size of the splits for processing datasets. -1 means process all at once. Defaults to -1.
        stability_threshold (int, optional): The number of recent bounding boxes to check for stability. Defaults to 2.
    Returns:
        Optional[Tuple]: The unified bounding box as (minx, miny, maxx, maxy), or None if no bounding box could be created.
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
        return unary_union(bboxes).bounds if bboxes else None
    
    # Normal splitting approach
    random.shuffle(dss)
    splits = [dss[i:i + split_size] for i in range(0, len(dss), split_size)]
    
    def process_split(datasets):
        """Process a split of datasets and return unified bbox polygon."""
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
        
        # Merge geometries, not bounds
        new_unified_bbox = unary_union([unified_bbox, current_split_bbox])
        bbox_history.append(new_unified_bbox)
        
        # Check for stability
        if len(bbox_history) >= stability_threshold:
            recent_bboxes = bbox_history[-stability_threshold:]
            if all(bbox.equals(recent_bboxes[0]) for bbox in recent_bboxes):
                return new_unified_bbox.bounds
        
        unified_bbox = new_unified_bbox
    
    return unified_bbox.bounds