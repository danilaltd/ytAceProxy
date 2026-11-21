import re
from dataclasses import dataclass

ACE_SCHEME = "acestream://"
ACE_HASH_RE = re.compile(r"^[0-9a-f]{40}$")
HTTP_SCHEMES = ("http://", "https://")

KIND_YT_DLP = "yt_dlp"
KIND_ACE = "ace"


@dataclass(frozen=True)
class Source:
    kind: str
    value: str


def _ace_hash(value: str) -> str | None:
    candidate = value.strip()
    if candidate.lower().startswith(ACE_SCHEME):
        candidate = candidate[len(ACE_SCHEME) :]
    candidate = candidate.lower()
    return candidate if ACE_HASH_RE.fullmatch(candidate) else None


def parse_source(url: str | None) -> Source | None:
    """Parse a stored source. Unknown or malformed sources return None."""
    if not url:
        return None
    value = url.strip()
    if value.lower().startswith(ACE_SCHEME):
        digest = _ace_hash(value)
        return Source(KIND_ACE, digest) if digest else None
    if value.lower().startswith(HTTP_SCHEMES):
        return Source(KIND_YT_DLP, value)
    return None


def source_kind(url: str | None) -> str:
    """Template helper: yt_dlp, ace, or unknown for malformed input."""
    source = parse_source(url)
    if source is not None:
        return source.kind
    lower = (url or "").strip().lower()
    if lower.startswith(ACE_SCHEME) or ACE_HASH_RE.fullmatch(lower):
        return KIND_ACE
    return "unknown"


def normalize_source_input(raw: str) -> str:
    """Normalize admin input to the stored form.

    Accepts an http(s) URL, ``acestream://<40 hex>``, or a bare 40-hex
    content id (which is prefixed with ``acestream://``).
    """
    value = (raw or "").strip()
    if not value:
        raise ValueError("Source is required")

    lower = value.lower()
    if lower.startswith(ACE_SCHEME) or ACE_HASH_RE.fullmatch(lower):
        digest = _ace_hash(value)
        if digest is None:
            raise ValueError(
                "ACE source must be acestream://<40 hex characters> "
                "or a 40-character content id"
            )
        return f"{ACE_SCHEME}{digest}"

    if lower.startswith(HTTP_SCHEMES):
        return value

    raise ValueError(
        "Source must be an http(s) URL, acestream://<40 hex characters>, "
        "or a 40-character content id"
    )
