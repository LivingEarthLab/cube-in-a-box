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
This implementation includes additional adaptations to support integration with the **Microsoft Planetary Computer**; these adaptations are maintained in this repository.

The image is published at:

```bash
git.unepgrid.ch/nostradamus/explorer:3.1.5
```

### Customizations and Patches

This directory contains two specific patches to the standard Explorer codebase to support the **Cube in a Box (CiaB)** environment:

1.  **`cubedash/_api.py`**: Adds a new `/api/data/<filename>` endpoint. This allows the Explorer to serve files directly from the `/local_data` directory, which is bridged from the host system.
2.  **`cubedash/_utils.py`**:
    -   **Planetary Computer Integration**: Automatically signs data URLs using the `planetary-computer` library. This allows the Explorer to access and display datasets from Microsoft's Planetary Computer using temporary SAS tokens (SAS tokens).
    -   **Local URL Resolution**: Correctly resolves and redirects `file:///local_data/` paths to the new `/api/data/` endpoint so they can be viewed in the browser.

## Upstream Relationship

This project is **not officially maintained by the Open Data Cube team**.

It is an independent effort by **UNIGE/GRID-Geneva** to extend distribution capabilities (multi-architecture support) while remaining fully aligned with the upstream Datacube Explorer codebase.