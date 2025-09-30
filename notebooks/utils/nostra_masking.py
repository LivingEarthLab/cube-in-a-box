import numpy as np
import dask.array as da
from typing import List

def _decode_bit(qa_arr: np.ndarray, bit: int) -> np.ndarray:
    """
    Decodes a specific bit from a NumPy array.

    This function converts an xarray.DataArray to a NumPy array, replaces NaN values with 0,
    and then extracts a specific bit from each element using a bitwise AND operation.

    Args:
        qa_arr (np.ndarray): The input NumPy array.
        bit (int): The bit to decode (0-indexed).

    Returns:
        np.ndarray: A NumPy array of booleans, where each element indicates whether the specified bit is set in the corresponding element of the input array.
    """
    # Convert xarray.DataArray to numpy array and replace NaNs with 0
    qa_numpy = qa_arr.values.copy()
    qa_numpy = np.nan_to_num(qa_numpy, nan=0).astype(np.uint16)
    return (qa_numpy & (1 << bit)) > 0

def qa_pixel_mask(qa_arr: np.ndarray, mask_type: str) -> np.ndarray:
    """
    Creates a boolean mask based on the specified mask type.
    - Args:
        qa_arr (np.ndarray): The quality assessment array.
        mask_type (str): The type of mask to create. Valid options are:
            "nodata", "dilated_cloud", "cirrus", "cloud", "cloud_shadow", "snow", "clear", "water",
            "cloud_confidence:low", "cloud_confidence:medium", "cloud_confidence:high",
            "cirrus_confidence:low", "cirrus_confidence:high",
            "snow_ice_confidence:low", "snow_ice_confidence:high",
            "cloud_shadow_confidence:low", "cloud_shadow_confidence:high".

    - Returns:
        np.ndarray: The boolean mask with True and False values.
    """
def qa_pixel_mask(qa_arr: np.ndarray, mask_type: str) -> np.ndarray:
    """
    Creates a boolean mask based on the specified mask type.

    This function generates a boolean mask from a quality assessment array based on a provided mask type.
    It decodes specific bits within the quality assessment array to identify different pixel conditions
    such as nodata, clouds, cirrus, snow, water, and their associated confidence levels.

    Args:
        qa_arr (np.ndarray): The quality assessment array.
        mask_type (str): The type of mask to create. Valid options are:
            "nodata", "dilated_cloud", "cirrus", "cloud", "cloud_shadow", "snow", "clear", "water",
            "cloud_confidence:low", "cloud_confidence:medium", "cloud_confidence:high",
            "cirrus_confidence:low", "cirrus_confidence:high",
            "snow_ice_confidence:low", "snow_ice_confidence:high",
            "cloud_shadow_confidence:low", "cloud_shadow_confidence:high".

    Returns:
        np.ndarray: The boolean mask with True and False values.
    """
    mask_type = mask_type.lower()  # Convert mask type to lowercase

    if ":" in mask_type:
        main_type, sub_type = mask_type.split(":")
    else:
        main_type, sub_type = mask_type, None

    if main_type == "nodata":
        return _decode_bit(qa_arr, 0)
    elif main_type == "dilated_cloud":
        return _decode_bit(qa_arr, 1)
    elif main_type == "cirrus":
        return _decode_bit(qa_arr, 2)
    elif main_type == "cloud":
        return _decode_bit(qa_arr, 3)
    elif main_type == "cloud_shadow":
        return _decode_bit(qa_arr, 4)
    elif main_type == "snow":
        return _decode_bit(qa_arr, 5)
    elif main_type == "clear":
        return _decode_bit(qa_arr, 6)
    elif main_type == "water":
        return _decode_bit(qa_arr, 7)
    elif main_type == "cloud_confidence":
        if sub_type == "low":
            return _decode_bit(qa_arr, 8) & ~_decode_bit(qa_arr, 9)
        elif sub_type == "medium":
            return ~_decode_bit(qa_arr, 8) & _decode_bit(qa_arr, 9)
        elif sub_type == "high":
            return _decode_bit(qa_arr, 8) & _decode_bit(qa_arr, 9)
    elif main_type == "cirrus_confidence":
        if sub_type == "low":
            return _decode_bit(qa_arr, 14) & ~_decode_bit(qa_arr, 15)
        elif sub_type == "high":
            return _decode_bit(qa_arr, 14) & _decode_bit(qa_arr, 15)
    elif main_type == "snow_ice_confidence":
        if sub_type == "low":
            return _decode_bit(qa_arr, 12) & ~_decode_bit(qa_arr, 13)
        elif sub_type == "high":
            return _decode_bit(qa_arr, 12) & _decode_bit(qa_arr, 13)
    elif main_type == "cloud_shadow_confidence":
        if sub_type == "low":
            return _decode_bit(qa_arr, 10) & ~_decode_bit(qa_arr, 11)
        elif sub_type == "high":
            return _decode_bit(qa_arr, 10) & _decode_bit(qa_arr, 11)
    else:
        raise ValueError(f"Invalid mask type: {mask_type}")

def scl_mask(scl:np.ndarray, valid_cats: List =[4, 5, 6, 7, 11], **kwargs) -> np.ndarray:
    """
    Create a clean mask from a list of valid categories.

    This function generates a boolean mask from a Scene Classification Layer (SCL) array,
    identifying pixels belonging to specified valid categories.

    Args:
        scl (da.Array): The input SCL data array.
        valid_cats (list[int], optional): A list of integer categories to consider valid. Defaults to [4, 5, 6, 7, 11].
        **kwargs: Additional keyword arguments (currently unused).

    Returns:
        np.ndarray: A boolean NumPy array where True indicates a valid pixel category.

    SCL Categories:
        0 - no data
        1 - saturated or defective
        2 - dark area pixels
        3 - cloud_shadows
        4 - vegetation
        5 - not vegetated
        6 - water
        7 - unclassified
        8 - cloud medium probability
        9 - cloud high probability
        10 - thin cirrus
        11 - snow
    """
    # Convert valid_cats to a set for faster membership testing
    valid_cats_set = set(valid_cats)

    # Ensure scl is a Dask array for parallel processing
    scl_data = scl.data if isinstance(scl.data, da.Array) else da.from_array(scl.data, chunks=scl.shape)

    # Use Dask's map_blocks for parallel processing
    clean_mask = da.map_blocks(lambda x: np.isin(x, list(valid_cats_set)), scl_data, dtype=bool)

    return clean_mask.compute()