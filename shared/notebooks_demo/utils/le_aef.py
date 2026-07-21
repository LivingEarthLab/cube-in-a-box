"""GDAL /vsicurl tuning for AlphaEarth Foundations (AEF) COG reads.

AEF tiles are unsigned HTTPS COGs on Source Cooperative
(us-west-2.opendata.source.coop). ``dc.load()`` reads the plain ``https://``
hrefs through GDAL ``/vsicurl/`` — no AWS credentials or ``patch_url`` needed.

Call ``ensure_aef_gdal_env()`` once before ``dc.load()`` (and on Dask workers
when using a distributed client) to coalesce range requests and cache COG
headers/blocks across the 64 int8 bands.
"""

from __future__ import annotations

import os

# GDAL env for anonymous HTTPS COG reads (matches scripts/index_aef.py subset).
_AEF_GDAL_ENV: dict[str, str] = {
    "AWS_NO_SIGN_REQUEST": "YES",
    "GDAL_HTTP_MAX_RETRY": "10",
    "GDAL_HTTP_RETRY_DELAY": "3",
    "GDAL_HTTP_TIMEOUT": "300",
    "GDAL_HTTP_MERGE_CONSECUTIVE_RANGES": "YES",
    "GDAL_HTTP_MULTIPART": "YES",
    "CPL_VSIL_CURL_CACHE_SIZE": "268435456",  # 256 MB
    "GDAL_CACHEMAX": "512",
    "GDAL_DISABLE_READDIR_ON_OPEN": "YES",
    "CPL_VSIL_CURL_ALLOWED_EXTENSIONS": ".tif,.tiff",
    "VSI_CACHE": "TRUE",
    "VSI_CACHE_SIZE": "536870912",  # 512 MB
}


def ensure_aef_gdal_env() -> None:
    """Configure process env for fast anonymous /vsicurl/ reads of AEF COGs."""
    for key, value in _AEF_GDAL_ENV.items():
        os.environ.setdefault(key, value)
