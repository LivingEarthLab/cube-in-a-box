"""Access Copernicus Data Space Ecosystem (CDSE) S3 URLs from notebooks.

CDSE does not support presigned URLs (query-string auth). GDAL/rasterio must read
``s3://eodata/...`` paths using header-based SigV4 via AWS_* environment variables.

Explorer installs this module as ``cdse_s3`` in site-packages (``proxy_path`` only).
"""

from __future__ import annotations

import os
import secrets
from pathlib import Path
from urllib.parse import quote, urlparse

CDSE_BUCKET = "eodata"
CDSE_ENDPOINT = "https://eodata.dataspace.copernicus.eu"

# Signed-proxy token settings. The Explorer mints short-lived tokens bound to the
# object key so only links it hands out (UI downloads) can use the deployment's
# CDSE credentials. Scripts reconstructing /cdse/<key> from STAC have no valid
# token and cannot forge one without the secret. When CDSE_PROXY_SECRET is unset,
# a secret is generated once under /tmp so gunicorn workers share it; it rotates
# on container restart (tokens are short-lived anyway).
_PROXY_SALT = "cdse-proxy"
PROXY_TOKEN_MAX_AGE = 300  # seconds; harvested links expire quickly
_PROXY_SECRET_FILE = Path("/tmp/cdse_proxy_secret")
_generated_secret: str | None = None


def _proxy_secret() -> str:
    """Return signing secret: env override, else generate once for this container."""
    global _generated_secret
    env_secret = os.environ.get("CDSE_PROXY_SECRET")
    if env_secret:
        return env_secret
    if _generated_secret:
        return _generated_secret
    try:
        fd = os.open(
            _PROXY_SECRET_FILE,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            0o600,
        )
        try:
            secret = secrets.token_hex(32)
            os.write(fd, secret.encode())
        finally:
            os.close(fd)
    except FileExistsError:
        secret = _PROXY_SECRET_FILE.read_text(encoding="utf-8").strip()
        if not secret:
            # Corrupt/empty file from a previous race — regenerate.
            _PROXY_SECRET_FILE.unlink(missing_ok=True)
            return _proxy_secret()
    _generated_secret = secret
    return secret


def _serializer():
    """Return an itsdangerous serializer (secret is always available)."""
    try:
        from itsdangerous import URLSafeTimedSerializer
    except ImportError:
        return None
    return URLSafeTimedSerializer(_proxy_secret(), salt=_PROXY_SALT)


def make_token(key: str) -> str | None:
    """Mint a signed token bound to the (decoded) object key."""
    serializer = _serializer()
    if serializer is None:
        return None
    return serializer.dumps({"key": key})


def verify_token(key: str, token: str | None, max_age: int = PROXY_TOKEN_MAX_AGE) -> bool:
    """Return True only if token is valid, unexpired, and bound to this key."""
    if not token:
        return False
    serializer = _serializer()
    if serializer is None:
        return False
    try:
        from itsdangerous import BadSignature, SignatureExpired
    except ImportError:
        return False
    try:
        payload = serializer.loads(token, max_age=max_age)
    except (SignatureExpired, BadSignature):
        return False
    except Exception:
        return False
    return isinstance(payload, dict) and payload.get("key") == key


def _access_key() -> str:
    return os.environ.get("AWS_ACCESS_KEY_ID") or os.environ.get("CDSE_S3_ACCESS_KEY", "")


def _secret_key() -> str:
    return os.environ.get("AWS_SECRET_ACCESS_KEY") or os.environ.get(
        "CDSE_S3_SECRET_KEY", ""
    )


def _endpoint() -> str:
    endpoint = (
        os.environ.get("AWS_S3_ENDPOINT")
        or os.environ.get("CDSE_S3_ENDPOINT")
        or CDSE_ENDPOINT
    )
    if not endpoint.startswith("http"):
        endpoint = f"https://{endpoint}"
    return endpoint.rstrip("/")


def ensure_cdse_gdal_env() -> None:
    """Configure GDAL/rasterio and Dask workers for CDSE S3 header-based auth."""
    os.environ["AWS_S3_ENDPOINT"] = _endpoint()
    if _access_key():
        os.environ["AWS_ACCESS_KEY_ID"] = _access_key()
    if _secret_key():
        os.environ["AWS_SECRET_ACCESS_KEY"] = _secret_key()
    os.environ["AWS_VIRTUAL_HOSTING"] = "FALSE"
    os.environ["AWS_HTTPS"] = "YES"
    os.environ["GDAL_HTTP_UNSAFESSL"] = "YES"
    # Retry/backoff for CDSE rate limits (HTTP 429) during parallel reads.
    os.environ.setdefault("GDAL_HTTP_MAX_RETRY", "10")
    os.environ.setdefault("GDAL_HTTP_RETRY_DELAY", "3")
    os.environ.setdefault("GDAL_HTTP_MULTIPLEX", "NO")
    os.environ.setdefault("GDAL_HTTP_VERSION", "2")
    os.environ.setdefault("GDAL_HTTP_TIMEOUT", "300")
    # Anonymous access must be off when using CDSE credentials.
    if _access_key() and _secret_key():
        os.environ.pop("AWS_NO_SIGN_REQUEST", None)


def _parse_s3_url(url: str) -> tuple[str, str] | None:
    parsed = urlparse(url)
    if parsed.scheme == "s3" and parsed.netloc == CDSE_BUCKET:
        return CDSE_BUCKET, parsed.path.lstrip("/")
    if parsed.scheme in ("http", "https") and parsed.netloc.startswith(
        ("eodata.", CDSE_BUCKET)
    ):
        path = parsed.path.lstrip("/")
        if path.startswith(f"{CDSE_BUCKET}/"):
            path = path[len(CDSE_BUCKET) + 1 :]
        return CDSE_BUCKET, path
    return None


def extract_key(url: str) -> str | None:
    """Extract CDSE object key from supported URL forms."""
    parsed = _parse_s3_url(url)
    if parsed is None:
        return None
    _, key = parsed
    return key


def proxy_path(url: str) -> str:
    """Return explorer API proxy path for CDSE object URLs.

    Appends a signed, short-lived token bound to the (decoded) object key.
    The signing secret is taken from ``CDSE_PROXY_SECRET`` when set, otherwise
    generated once per container under ``/tmp/cdse_proxy_secret``.
    """
    key = extract_key(url)
    if not key:
        return url
    path = f"/explorer/api/cdse/{quote(key, safe='/')}"
    token = make_token(key)
    if token:
        return f"{path}?sig={quote(token, safe='')}"
    return path


def sign(url: str) -> str:
    """Return an ``s3://`` URL for GDAL/rasterio (CDSE rejects presigned URLs)."""
    parsed = _parse_s3_url(url)
    if parsed is None:
        return url
    bucket, key = parsed
    ensure_cdse_gdal_env()
    return f"s3://{bucket}/{key}"


def sign_url(url: str) -> str:
    """Alias for sign(), matching planetary_computer.sign_url patch_url usage."""
    return sign(url)
