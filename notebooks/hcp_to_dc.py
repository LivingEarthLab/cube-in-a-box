#!/usr/bin/env python3

# Adaptation of https://github.com/opendatacube/odc-tools/blob/develop/apps/dc_tools/odc/apps/dc_tools/fs_to_dc.py
# to index from Hitachi Content Platform
# (https://www.hitachivantara.com/en-us/products/storage/object-storage/content-platform.html) storage.
# The stac indexation code was removed to keep it simple.

import click
from datacube import Datacube
from datacube.index.hl import Doc2Dataset
from odc.apps.dc_tools.utils import (
    index_update_dataset,
    # update_if_exists,
    allow_unsafe,
)
from typing import Generator, Optional, Any
import logging

import yaml
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s: %(levelname)s: %(message)s",
    datefmt="%m/%d/%Y %I:%M:%S",
)

# HCP specific
import boto3
from fnmatch import fnmatch
from urllib.parse import urlparse
import requests



from datetime import datetime
def convert_datetime_to_string(obj: Any) -> Any:
    """Recursively convert datetime objects to ISO format strings"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {key: convert_datetime_to_string(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_datetime_to_string(item) for item in obj]
    else:
        return obj





def _find_files(
    endpoint: str, tenant: str, glob: Optional[str] = None
) -> Generator[str, None, None]:
    if glob is None:
        glob = "**/*.yaml"
    
    uu = urlparse(endpoint)
    d_prfx = f"{uu.scheme}://{tenant}.{uu.netloc}/rest"
    # d_prfx = f"{uu.scheme}://{tenant}.{uu.netloc}"
    # > ERROR: Failed to add dataset
    # https://sdc-storage.unepgrid.s3.unige.ch/lsc2/landsat_etm_c2_l2/LE07_L2SP_193027_20121002/lsc2-metadata.yaml
    # with error Exceeded 30 redirects.
    
    r_list = []
    s3 = boto3.client(service_name='s3', endpoint_url=endpoint)
    pp = s3.get_paginator( "list_objects_v2" )
    for o in pp.paginate(Bucket = tenant, Prefix = glob.split('*')[0]):
        for d in o.get("Contents", []):
            d = d.get("Key")
            if fnmatch(d, glob):
                r_list.append(f"{d_prfx}/{d}")
    
    return(r_list)

@click.command("hcp-to-dc")
@click.argument("endpoint", type=str, nargs=1)
@click.argument("tenant", type=str, nargs=1)
@click.argument("product", type=str, nargs=1)
# @update_if_exists
@allow_unsafe
@click.option(
    "--glob",
    default=None,
    help="Metadata glob to use, defaults to **/*.yaml.",
)
@click.option(
    "--mtdsstr",
    default=None,
    help="List of metadata url on HCP as a comma separated string.",
)
def cli(endpoint, tenant, product, allow_unsafe, glob, mtdsstr):
# def cli(endpoint, tenant, product, update_if_exists, allow_unsafe, glob):
    """ Iterate through files in an HCP tenant and index them on datacube"""

    dc = Datacube()
    doc2ds = Doc2Dataset(dc.index, products = product.split())

    if glob is None:
        glob = "**/*.yaml"

    if mtdsstr is None:
        files_to_process = _find_files(endpoint, tenant, glob)
    else:
        files_to_process = mtdsstr.split(',')

    added, failed = 0, 0

    for in_file in files_to_process:
        try:
            r = requests.get(in_file)
            metadata = yaml.safe_load(r.content.decode())
            
            # Convert datetime objects to strings
            metadata = convert_datetime_to_string(metadata)
            
            # DEV note: update_if_exists & allow_unsafe not tested
            index_update_dataset(
                metadata,
                in_file,
                dc=dc,
                doc2ds=doc2ds,
                # update_if_exists=False,
                # update_if_exists=update_if_exists,
                allow_unsafe=False
                # allow_unsafe=allow_unsafe,
            )
            added += 1
        except Exception as e:
            logging.exception(f"Failed to add dataset {in_file} with error {e}")
            failed += 1

    logging.info(f"Added {added} and failed {failed} datasets.")

if __name__ == "__main__":
    cli()
