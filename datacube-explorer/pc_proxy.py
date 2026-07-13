"""Planetary Computer Azure blob download proxy helpers for Explorer.

Rewrites ``*.blob.core.windows.net`` HTTPS URLs to short-lived
``/explorer/api/pc/...`` links. The API route verifies the token and 302-redirects
to a freshly SAS-signed blob URL (unlike CDSE, which must stream).
"""

from __future__ import annotations

import os
import secrets
from pathlib import Path
from urllib.parse import quote, urlparse, urlunparse

_PROXY_SALT = "pc-proxy"
PROXY_TOKEN_MAX_AGE = 300  # seconds
_PROXY_SECRET_FILE = Path("/tmp/pc_proxy_secret")
_generated_secret: str | None = None


def _proxy_secret() -> str:
    """Return signing secret: env override, else generate once for this container."""
    global _generated_secret
    env_secret = os.environ.get("PC_PROXY_SECRET")
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
            _PROXY_SECRET_FILE.unlink(missing_ok=True)
            return _proxy_secret()
    _generated_secret = secret
    return secret


def _serializer():
    try:
        from itsdangerous import URLSafeTimedSerializer
    except ImportError:
        return None
    return URLSafeTimedSerializer(_proxy_secret(), salt=_PROXY_SALT)


def make_token(canonical_url: str) -> str | None:
    """Mint a signed token bound to the canonical blob HTTPS URL (no query)."""
    serializer = _serializer()
    if serializer is None:
        return None
    return serializer.dumps({"url": canonical_url})


def verify_token(
    canonical_url: str, token: str | None, max_age: int = PROXY_TOKEN_MAX_AGE
) -> bool:
    """Return True only if token is valid, unexpired, and bound to this URL."""
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
    return isinstance(payload, dict) and payload.get("url") == canonical_url


def canonical_blob_url(url: str) -> str | None:
    """Return ``https://host/path`` for Azure blob URLs, or None if not a PC blob."""
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        return None
    host = (parsed.hostname or "").lower()
    if not host.endswith(".blob.core.windows.net"):
        return None
    # Drop query/fragment (e.g. leftover SAS) and force https.
    path = parsed.path or "/"
    return urlunparse(("https", host, path, "", "", ""))


def blob_ref_from_url(url: str) -> str | None:
    """Return ``host/path`` (no leading slash on path join) for proxy routes."""
    canonical = canonical_blob_url(url)
    if not canonical:
        return None
    parsed = urlparse(canonical)
    host = parsed.hostname or ""
    path = (parsed.path or "/").lstrip("/")
    return f"{host}/{path}" if path else host


def url_from_blob_ref(blob_ref: str) -> str | None:
    """Rebuild canonical HTTPS blob URL from a proxy path ``host/path``."""
    blob_ref = blob_ref.lstrip("/")
    if not blob_ref or "/" not in blob_ref:
        return None
    host, _, path = blob_ref.partition("/")
    if not host.lower().endswith(".blob.core.windows.net"):
        return None
    return canonical_blob_url(f"https://{host}/{path}")


def proxy_path(url: str) -> str:
    """Return explorer API proxy path for Planetary Computer blob URLs.

    Appends a signed, short-lived token bound to the canonical blob URL.
    Non-blob URLs are returned unchanged.
    """
    canonical = canonical_blob_url(url)
    if not canonical:
        return url
    ref = blob_ref_from_url(canonical)
    if not ref:
        return url
    path = f"/explorer/api/pc/{quote(ref, safe='/')}"
    token = make_token(canonical)
    if token:
        return f"{path}?sig={quote(token, safe='')}"
    return path
