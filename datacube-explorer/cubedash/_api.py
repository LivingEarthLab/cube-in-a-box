from datetime import date, datetime
import os
from zoneinfo import ZoneInfo

import flask
import structlog
from flask import Blueprint, Response, abort, request, send_from_directory, stream_with_context

from cubedash import _utils

from . import _model
from ._utils import as_geojson, as_json
from .summary import ItemSort

_LOG = structlog.stdlib.get_logger()
bp = Blueprint("api", __name__, url_prefix="/api")


@bp.route("/datasets/<product_name>")
@bp.route("/datasets/<product_name>/<int:year>")
@bp.route("/datasets/<product_name>/<int:year>/<int:month>")
@bp.route("/datasets/<product_name>/<int:year>/<int:month>/<int:day>")
def datasets_geojson(
    product_name: str,
    year: int | None = None,
    month: int | None = None,
    day: int | None = None,
):
    limit = request.args.get(
        "limit",
        default=flask.current_app.config["CUBEDASH_DEFAULT_API_LIMIT"],
        type=int,
    )
    hard_limit = flask.current_app.config["CUBEDASH_HARD_API_LIMIT"]
    if limit > hard_limit:
        limit = hard_limit

    time = _utils.as_time_range(
        year, month, day, tzinfo=ZoneInfo(_model.DEFAULT_GROUPING_TIMEZONE)
    )

    return as_geojson(
        {
            "type": "FeatureCollection",
            "features": [
                s.as_geojson()
                for s in _model.STORE.search_items(
                    product_names=[product_name],
                    time=time,
                    limit=limit,
                    order=ItemSort.UNSORTED,
                )
                if s.geom_geojson is not None
            ],
        },
        downloadable_filename_prefix=_utils.api_path_as_filename_prefix(),
    )

    # TODO: replace this api with stac?
    #       Stac includes much more information in records, so has to join the
    #       dataset table, so is slower, but does it matter?
    # Can trivially redirect to stac as its return value is still geojson:
    # return flask.redirect(
    #     flask.url_for(
    #         'stac.stac_search',
    #         product_name=product_name,
    #         time=_unparse_time_range(time) if time else None,
    #         limit=limit,
    #     )
    # )


@bp.route("/footprint/<product_name>")
@bp.route("/footprint/<product_name>/<int:year>")
@bp.route("/footprint/<product_name>/<int:year>/<int:month>")
@bp.route("/footprint/<product_name>/<int:year>/<int:month>/<int:day>")
def footprint_geojson(
    product_name: str,
    year: int | None = None,
    month: int | None = None,
    day: int | None = None,
):
    return as_geojson(
        _model.get_footprint_geojson(product_name, year, month, day),
        downloadable_filename_prefix=_utils.api_path_as_filename_prefix(),
    )


@bp.route("/regions/<product_name>")
@bp.route("/regions/<product_name>/<int:year>")
@bp.route("/regions/<product_name>/<int:year>/<int:month>")
@bp.route("/regions/<product_name>/<int:year>/<int:month>/<int:day>")
def regions_geojson(
    product_name: str,
    year: int | None = None,
    month: int | None = None,
    day: int | None = None,
):
    regions = _model.get_regions_geojson(product_name, year, month, day)
    if regions is None:
        abort(404, f"{product_name} does not have regions")
    return as_geojson(
        regions, downloadable_filename_prefix=_utils.api_path_as_filename_prefix()
    )


@bp.route("/dataset-timeline/<product_name>")
@bp.route("/dataset-timeline/<product_name>/<int:year>")
@bp.route("/dataset-timeline/<product_name>/<int:year>/<int:month>")
@bp.route("/dataset-timeline/<product_name>/<int:year>/<int:month>/<int:day>")
def dataset_timeline(
    product_name: str,
    year: int | None = None,
    month: int | None = None,
    day: int | None = None,
):
    summary = _model.get_time_summary(product_name, year, month, day)
    if summary is None:
        abort(
            404,
            "No known information for product "
            f"{product_name!r} {year or 'all'} {month or 'all'} {day or 'all'}",
        )

    def _datekey(k):
        # The timezone is the global grouping timezone: we don't want it in json.
        if type(k) is date:
            k = datetime(k.year, k.month, k.day)
        return k.replace(tzinfo=None).isoformat()

    return as_json(
        {_datekey(k): v for k, v in summary.timeline_dataset_counts.items()},
        downloadable_filename_prefix=_utils.api_path_as_filename_prefix(),
    )


@bp.route("/data/<path:filename>")
def data_file(filename):
    return send_from_directory("/local_data", filename)


@bp.route("/cdse/<path:object_key>")
def cdse_file(object_key):
    # Require a valid, unexpired, key-bound token before touching credentials.
    # Fails closed: unsigned/tampered/expired/mismatched -> 403.
    from cdse_s3 import verify_token

    if not verify_token(object_key, request.args.get("sig")):
        abort(403, "Invalid or expired CDSE proxy token")

    # Lazy imports so a missing boto3/botocore breaks only this route.
    import boto3
    from botocore.client import Config
    from botocore.exceptions import ClientError

    endpoint = os.environ.get("AWS_S3_ENDPOINT", "https://eodata.dataspace.copernicus.eu")
    if not endpoint.startswith("http"):
        endpoint = f"https://{endpoint}"

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", ""),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        region_name="default",
        config=Config(signature_version="s3v4"),
    )
    try:
        obj = s3.get_object(Bucket="eodata", Key=object_key)
    except ClientError as e:
        _LOG.warning("cdse_proxy_get_failed", key=object_key, error=str(e))
        abort(404, f"CDSE object not accessible: {object_key}")

    filename = object_key.rsplit("/", 1)[-1]
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    if "ContentLength" in obj:
        headers["Content-Length"] = str(obj["ContentLength"])

    return Response(
        stream_with_context(obj["Body"].iter_chunks(chunk_size=1024 * 1024)),
        content_type=obj.get("ContentType", "application/octet-stream"),
        headers=headers,
    )
