import pytest

from playgate.sources import (
    KIND_ACE,
    KIND_YT_DLP,
    normalize_source_input,
    parse_source,
    source_kind,
)

HASH = "a" * 40


def test_parse_http_source():
    source = parse_source("https://www.youtube.com/watch?v=abc")
    assert source is not None
    assert source.kind == KIND_YT_DLP
    assert source.value == "https://www.youtube.com/watch?v=abc"


def test_parse_ace_source_normalizes_hash():
    source = parse_source(f"acestream://{HASH.upper()}")
    assert source is not None
    assert source.kind == KIND_ACE
    assert source.value == HASH


@pytest.mark.parametrize(
    "url",
    [
        None,
        "",
        "ftp://example.com/x",
        f"acestream://{HASH[:39]}",
        f"acestream://{'z' * 40}",
        f"acestream://{HASH}/path",
        HASH,
    ],
)
def test_parse_invalid_source_returns_none(url):
    assert parse_source(url) is None


def test_normalize_bare_hash():
    assert normalize_source_input(HASH) == f"acestream://{HASH}"
    assert normalize_source_input(f"  {HASH.upper()}  ") == f"acestream://{HASH}"


def test_normalize_ace_scheme():
    assert normalize_source_input(f"acestream://{HASH.upper()}") == (
        f"acestream://{HASH}"
    )


def test_normalize_http_source_is_unchanged():
    assert normalize_source_input(" https://example.com/a ") == "https://example.com/a"


@pytest.mark.parametrize(
    "raw",
    ["", "   ", "ftp://example.com/x", f"acestream://{HASH[:39]}", "not a source"],
)
def test_normalize_invalid_source(raw):
    with pytest.raises(ValueError, match=r"(?i)source"):
        normalize_source_input(raw)


def test_source_kind_bare_hash_is_ace():
    assert source_kind(HASH) == KIND_ACE
    assert source_kind(f"acestream://{HASH}") == KIND_ACE
    assert source_kind("https://example.com") == KIND_YT_DLP
    assert source_kind("ftp://example.com") == "unknown"
