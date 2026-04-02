# Notebooks Demo

This directory contains a collection of Jupyter notebooks demonstrating various functionalities, products and tools of the Cube in a Bix (CiaB).

## Overview

- Loading and visualizing satellite imagery (Sentinel, Landsat).
- Handling elevation data (NASADEM).
- Utilizing Dask for parallel processing and large-scale data handling.
- Indexing new datasets from STAC APIs.
- Working with local storage and filesystem-based data.

## Demo Notebook Serie

| Notebook | Description | Key Modules/Tools |
| :--- | :--- | :--- |
| **[ESRI_Land_Cover.ipynb](./ESRI_Land_Cover.ipynb)** | Demonstrates loading and plotting ESRI Land Cover data. | `datacube`, `matplotlib` |
| **[ESA_Worldcover.ipynb](./ESA_Worldcover.ipynb)** | Demonstrates loading and plotting ESA Worldcover data. | `datacube`, `matplotlib` |
| **[Landsat_Collection_2_Level-2_Science_Products.ipynb](./Landsat_Collection_2_Level-2_Science_Products.ipynb)** | Covers processing Landsat C2 L2 products, including DN to SR conversion. | `datacube`, `dask` |
| **[NASADEM.ipynb](./NASADEM.ipynb)** | Focuses on loading NASADEM products and computing hillshade for elevation display. | `datacube`, `matplotlib`, `rich_dem` |
| **[Sentinel_1_rtc.ipynb](./Sentinel_1_rtc.ipynb)** | Covers loading and exporting Sentinel-1 Radiometrically Terrain Corrected (RTC) images. | `datacube`, `odc.geo.xr.write_cog` |
| **[Sentinel_2.ipynb](./Sentinel_2.ipynb)** | Introduction to Sentinel-2 L2A surface reflectance data, including cloud masking, time statistics and export as COGs. | `datacube`, `load_ard`, `rio.to_raster` |

## Tools Notebook Serie

| Notebook | Description | Key Modules/Tools |
| :--- | :--- | :--- |
| **[cogs_fs_indexation.ipynb](./cogs_fs_indexation.ipynb)** | Prepare metadata and index existing COGs as a new product. | `add_product`, `add_dataset` |
| **[STAC_to_fs.ipynb](./STAC_to_fs.ipynb)** | Demonstrates converting STAC metadata to a filesystem structure for local indexing. | `pystac`, `odc.geo` |
| **[Test_fs_indexation.ipynb](./Test_fs_indexation.ipynb)** | Verification and exploration of data recently indexed into the datacube from local storage. | `datacube`, `ipyleaflet` |

## Quick Start

**Jupyter Notebooks form the Demo Serie are supposed to work without any input from the user as long as the CiaB was created with default setup**.

In the case it wasn't the case, useer might need to:
- draw it'own Area of Interest (AoI)
- check the requested product is available by using the [CiaB Explorer](http://localhost/explorer).

**Remember all file sand folder in the `./shared` folder are read only at the exception of user own folder (`./shared/all_users/<OWN_FOLDER>`). Then all Jupyter Notebooks in `./shared/notebooks_demo` can be executed and modified, but cannot be saved. To do so, user need to copy the folder to JupyterLab root (as well as any other file or folder from shared by another user**.