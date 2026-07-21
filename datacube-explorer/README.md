# Datacube Explorer Multi-Architecture Image

## Overview

The official Docker image for **Datacube Explorer** is not currently published as a multi-architecture (multi-arch) image on Docker Hub. This limits its usability on non-`amd64` platforms, particularly ARM-based systems.

## Motivation

To address this limitation and support a wider range of deployment environments, UNIGE/GRID-Geneva provides a multi-architecture build targeting:

- `amd64`
- `arm64`

## Image Build

This image is built directly from the upstream Datacube Explorer repository:

- [https://github.com/opendatacube/datacube-explorer](https://github.com/opendatacube/datacube-explorer)

The build process uses the original `Dockerfile` **without modification**, ensuring consistency with upstream code and behavior.

The resulting image is published at:

```bash
git.unepgrid.ch/nostradamus/explorer:3.1.5
```

## Downstream Usage

This image serves as the base image for a customized version of Datacube Explorer used within the *Cube in a Box (CiaB)* framework.
This implementation includes adaptations for the **Microsoft Planetary Computer** and **Copernicus Data Space Ecosystem (CDSE)**; these adaptations are maintained in this repository.

The image is published at:

```bash
git.unepgrid.ch/nostradamus/explorer:3.1.5
```

### Customizations and Patches

This directory contains patches to the standard Explorer codebase to support the **Cube in a Box (CiaB)** environment:

1.  **`cubedash/_api.py`**:
    -   **`/api/data/<filename>`**: Serves files from the `/local_data` directory bridged from the host.
    -   **`/api/cdse/<object_key>`**: Streams CDSE `eodata` objects through a short-lived signed download proxy (requires `CDSE_S3_*` credentials on the Explorer service). Signing uses an internal secret auto-generated in the container when unset (optional `CDSE_PROXY_SECRET` override).
    -   **`/api/pc/<blob_ref>`**: Planetary Computer Azure blob proxy — verifies a short-lived token, then **302 redirects** to a freshly SAS-signed blob URL (optional `PC_PROXY_SECRET` override).
2.  **`cubedash/_utils.py`**:
    -   **Planetary Computer Integration**: Rewrites `*.blob.core.windows.net` asset URLs to `/explorer/api/pc/...`; aliases `rendered_preview` to `thumbnail` for map overlays; leaves public Data API URLs unsigned.
    -   **CDSE Integration**: Rewrites CDSE `s3://eodata/...` asset URLs to the local `/explorer/api/cdse/...` proxy path.
    -   **Local URL Resolution**: Correctly resolves and redirects `file:///local_data/` paths to the `/api/data/` endpoint so they can be viewed in the browser.
    -   **Shared-path Location dedup**: When every measurement on a dataset points at the same file (e.g. multi-band COGs such as AEF), the dataset Location table shows one download row with a band-range label instead of repeating the same URL per band. Accessories (e.g. thumbnails) are unchanged.
3.  **`cubedash/templates/dataset.html`**: Embeds thumbnail URLs with Jinja `tojson` so Planetary Computer preview query strings keep `&` (HTML-escaping `&amp;` otherwise breaks the Data API).
4.  **`pc_proxy.py`** (installed into the image as `pc_proxy`): Token mint/verify and path rewrite for Planetary Computer blob downloads.
5.  **`shared/notebooks_demo/utils/le_cdse_s3.py`** (installed into the image as `cdse_s3`): Shared CDSE S3 helpers and proxy token mint/verify used by the patches above.

## Upstream Relationship

This project is **not officially maintained by the Open Data Cube team**.

It is an independent effort by **UNIGE/GRID-Geneva** to extend distribution capabilities (multi-architecture support) while remaining fully aligned with the upstream Datacube Explorer codebase.