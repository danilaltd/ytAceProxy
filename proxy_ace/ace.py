import os
from urllib.parse import urlencode, urlsplit


def get_acexy_base_url() -> str:
    return (os.environ.get("ACEXY_BASE_URL") or "").strip().rstrip("/")


def build_acexy_stream_url(base_url: str, content_id: str) -> str:
    base = (base_url or "").strip().rstrip("/")
    if not base:
        raise ValueError("ACEXY_BASE_URL is not configured")

    parts = urlsplit(base)
    if parts.scheme.lower() not in ("http", "https") or not parts.netloc:
        raise ValueError("ACEXY_BASE_URL must be an absolute http(s) URL")
    if parts.path not in ("", "/") or parts.query or parts.fragment:
        raise ValueError("ACEXY_BASE_URL must be an origin without path or query")

    if not content_id:
        raise ValueError("content id is empty")

    return f"{base}/ace/getstream?{urlencode({'id': content_id})}"
