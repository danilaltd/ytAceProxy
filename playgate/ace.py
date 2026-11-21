import os
from urllib.parse import urlencode, urlsplit


def get_acexy_base_url() -> str:
    """Return the client-facing acexy origin.

    ``ACEXY_BASE_URL`` is an optional override; by default the origin is
    computed from the tailnet address and the published acexy port, both of
    which already exist in the host ``.env``.
    """
    override = (os.environ.get("ACEXY_BASE_URL") or "").strip().rstrip("/")
    if override:
        return override

    host = (os.environ.get("TAILSCALE_IP") or "").strip()
    port = (os.environ.get("ACEXY_PORT") or "").strip()
    if host and port:
        return f"http://{host}:{port}"
    return ""


def build_acexy_stream_url(base_url: str, content_id: str) -> str:
    base = (base_url or "").strip().rstrip("/")
    if not base:
        raise ValueError("acexy base URL is not configured")

    parts = urlsplit(base)
    if parts.scheme.lower() not in ("http", "https") or not parts.netloc:
        raise ValueError("acexy base URL must be an absolute http(s) URL")
    if parts.path not in ("", "/") or parts.query or parts.fragment:
        raise ValueError("acexy base URL must be an origin without path or query")

    if not content_id:
        raise ValueError("ACE content id is empty")

    return f"{base}/ace/getstream?{urlencode({'id': content_id})}"
