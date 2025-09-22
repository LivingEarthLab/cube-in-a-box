import numpy as np
import dask.array as da

def decode_bit(qa_arr: np.ndarray, bit: int) -> np.ndarray:
    """
    Decodes the QA Bit
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

    mask_type = mask_type.lower()  # Convert mask type to lowercase

    if ":" in mask_type:
        main_type, sub_type = mask_type.split(":")
    else:
        main_type, sub_type = mask_type, None

    if main_type == "nodata":
        return decode_bit(qa_arr, 0)
    elif main_type == "dilated_cloud":
        return decode_bit(qa_arr, 1)
    elif main_type == "cirrus":
        return decode_bit(qa_arr, 2)
    elif main_type == "cloud":
        return decode_bit(qa_arr, 3)
    elif main_type == "cloud_shadow":
        return decode_bit(qa_arr, 4)
    elif main_type == "snow":
        return decode_bit(qa_arr, 5)
    elif main_type == "clear":
        return decode_bit(qa_arr, 6)
    elif main_type == "water":
        return decode_bit(qa_arr, 7)
    elif main_type == "cloud_confidence":
        if sub_type == "low":
            return decode_bit(qa_arr, 8) & ~decode_bit(qa_arr, 9)
        elif sub_type == "medium":
            return ~decode_bit(qa_arr, 8) & decode_bit(qa_arr, 9)
        elif sub_type == "high":
            return decode_bit(qa_arr, 8) & decode_bit(qa_arr, 9)
    elif main_type == "cirrus_confidence":
        if sub_type == "low":
            return decode_bit(qa_arr, 14) & ~decode_bit(qa_arr, 15)
        elif sub_type == "high":
            return decode_bit(qa_arr, 14) & decode_bit(qa_arr, 15)
    elif main_type == "snow_ice_confidence":
        if sub_type == "low":
            return decode_bit(qa_arr, 12) & ~decode_bit(qa_arr, 13)
        elif sub_type == "high":
            return decode_bit(qa_arr, 12) & decode_bit(qa_arr, 13)
    elif main_type == "cloud_shadow_confidence":
        if sub_type == "low":
            return decode_bit(qa_arr, 10) & ~decode_bit(qa_arr, 11)
        elif sub_type == "high":
            return decode_bit(qa_arr, 10) & decode_bit(qa_arr, 11)
    else:
        raise ValueError(f"Invalid mask type: {mask_type}")


def scl_mask(scl, valid_cats=[4, 5, 6, 7, 11], **kwargs):
    """
    Create a clean mask from a list of valid categories.

    Args:
        scl: xarray data array to extract clean categories from.
        valid_cats: array of ints representing what category should be considered valid.
        * category selected by default
        ###################################
        # SCL categories:                 #
        #   0 - no data                   #
        #   1 - saturated or defective    #
        #   2 - dark area pixels          #
        #   3 - cloud_shadows             #
        #   4 * vegetation                #
        #   5 * not vegetated             #
        #   6 * water                     #
        #   7 * unclassified              #
        #   8 - cloud medium probability  #
        #   9 - cloud high probability    #
        #  10 - thin cirrus               #
        #  11 * snow                      #
        ###################################
    Output:
      clean_mask (boolean numpy array)
    """
    # Convert valid_cats to a set for faster membership testing
    valid_cats_set = set(valid_cats)

    # Ensure scl is a Dask array for parallel processing
    scl_data = scl.data if isinstance(scl.data, da.Array) else da.from_array(scl.data, chunks=scl.shape)

    # Use Dask's map_blocks for parallel processing
    clean_mask = da.map_blocks(lambda x: np.isin(x, list(valid_cats_set)), scl_data, dtype=bool)

    return clean_mask.compute()