# Notebooks Demo

This directory contains a collection of Jupyter notebooks demonstrating various functionalities, products and tools of the Cube in a Box (CiaB).

## Overview

- Loading and visualizing satellite imagery (Sentinel, Landsat).
- Handling elevation data (NASADEM).
- Utilizing Dask for parallel processing and large-scale data handling.
- Indexing new datasets from STAC APIs.
- Working with local storage and filesystem-based data.

## Demo Notebook Serie

| Notebook | Description | Key Modules/Tools |
| :--- | :--- | :--- |
| **[CLMS_CLCplus_Europe.ipynb](./CLMS_CLCplus_Europe.ipynb)** | Demonstrates loading and plotting CLMS CLCplus LULUCF Instance Europe 100 m. | `datacube`, `matplotlib` |
| **[AlphaEarth_AEF_annual.ipynb](./AlphaEarth_AEF_annual.ipynb)** | Demonstrates loading and visualizing AlphaEarth Foundations annual 64-band satellite embeddings (10 m), including a PCA RGB preview. | `datacube`, `matplotlib`, `numpy` |
| **[ESRI_Land_Cover.ipynb](./ESRI_Land_Cover.ipynb)** | Demonstrates loading and plotting ESRI Land Cover data. | `datacube`, `matplotlib` |
| **[ESA_Worldcover.ipynb](./ESA_Worldcover.ipynb)** | Demonstrates loading and plotting ESA Worldcover data. | `datacube`, `matplotlib` |
| **[Landsat_Collection_2_Level-2_Science_Products.ipynb](./Landsat_Collection_2_Level-2_Science_Products.ipynb)** | Covers processing Landsat C2 L2 products, including DN to SR conversion. | `datacube`, `dask` |
| **[NASADEM.ipynb](./NASADEM.ipynb)** | Focuses on loading NASADEM products and computing hillshade for elevation display. | `datacube`, `matplotlib`, `rich_dem` |
| **[Sentinel_1_rtc.ipynb](./Sentinel_1_rtc.ipynb)** | Covers loading and exporting Sentinel-1 Radiometrically Terrain Corrected (RTC) images. | `datacube`, `odc.geo.xr.write_cog` |
| **[Sentinel_2.ipynb](./Sentinel_2.ipynb)** | Introduction to Sentinel-2 L2A surface reflectance data, including cloud masking, time statistics and export as COGs. | `datacube`, `load_ard`, `rio.to_raster` |

## Tools Notebook Serie

| Notebook | Description | Key Modules/Tools |
| :--- | :--- | :--- |
| **[cogs_fs_indexation.ipynb](./cogs_fs_indexation.ipynb)** | ADMIN ONLY! Prepare metadata and index existing COGs as a new product. | `add_product`, `add_dataset` |
| **[STAC_to_fs.ipynb](./STAC_to_fs.ipynb)** | ADMIN ONLY! Demonstrates converting STAC metadata to a filesystem structure for local indexing. | `pystac`, `odc.geo` |
| **[Test_fs_indexation.ipynb](./Test_fs_indexation.ipynb)** | Verification and exploration of data recently indexed into the datacube from local storage. | `datacube`, `ipyleaflet` |

## Utils

The [`utils/`](./utils/) directory contains shared Python helpers imported by the demo notebooks (`le_dc`, `le_mapping`, `deafrica_plotting`, `le_cdse_s3`, etc.).

- **Admins:** edit files on the host at `./shared/notebooks_demo/utils/`. Changes are available to all users via the JupyterHub shared mount (no image rebuild). Notebook helpers such as `le_dc` / `le_cdse_s3` update live; the Explorer image still needs a rebuild if you change the copy baked into Explorer (`cdse_s3`).
- **Users:** when copying this folder to your workspace (`cp -r /notebooks/shared/notebooks_demo ~/my_notebooks_demo`), `utils/` is included automatically.
- **CDSE products:** use `get_patch_url` from `utils.le_dc` with `dc.load(..., patch_url=...)`. Requires `CDSE_S3_ACCESS_KEY` / `CDSE_S3_SECRET_KEY` in `.env` (set by the admin).

## Product availability

Not every CiaB deployment indexes the same products. Before running a notebook, check that its product appears in the [CiaB Explorer](http://localhost/explorer).

These demos need Copernicus Data Space (CDSE) products and will not work if those were not indexed:

- **[CLMS_CLCplus_Europe.ipynb](./CLMS_CLCplus_Europe.ipynb)** — needs `clms_clcplus_europe_100m`
- **[Sentinel_2.ipynb](./Sentinel_2.ipynb)** — `s2_l2a_pc` is fine when present; `s2_l2a_cdse` only works if that product was indexed (keep `product = 's2_l2a_pc'` otherwise)

This demo needs a non-CDSE product to be indexed (no CDSE credentials required):

- **[AlphaEarth_AEF_annual.ipynb](./AlphaEarth_AEF_annual.ipynb)** — needs `aef_annual` (Source Cooperative, unsigned HTTPS COGs; no `patch_url`/credentials involved)

## Quick Start

**Jupyter Notebooks from the Demo Serie are supposed to work without any input from the user as long as the CiaB was set up with the usual full product set for your site.**

If products or coverage differ, you might need to:
- draw your own Area of Interest (AoI)
- confirm the requested product is available in the [CiaB Explorer](http://localhost/explorer)
- skip notebooks whose products are missing (see above)

**Remember:** files and folders under `./shared` are read-only except your own folder (`./shared/all_users/<OWN_FOLDER>`). Notebooks in `./shared/notebooks_demo` can be executed and modified in the session, but cannot be saved there — copy the folder to your JupyterLab root (same for any other shared path) to keep edits.