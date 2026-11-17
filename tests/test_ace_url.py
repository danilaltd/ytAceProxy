import pytest

from proxy_ace.ace import build_acexy_stream_url, get_acexy_base_url


CONTENT_ID = "dd1e67078381739d14beca697356ab76d49d1a20"


def test_builds_acexy_stream_url():
    assert (
        build_acexy_stream_url("http://acexy:8090", CONTENT_ID)
        == f"http://acexy:8090/ace/getstream?id={CONTENT_ID}"
    )


def test_strips_trailing_slash():
    assert (
        build_acexy_stream_url("http://acexy:8090/", CONTENT_ID)
        == f"http://acexy:8090/ace/getstream?id={CONTENT_ID}"
    )


def test_encodes_content_id():
    assert (
        build_acexy_stream_url("http://acexy:8090", "a b&c")
        == "http://acexy:8090/ace/getstream?id=a+b%26c"
    )


@pytest.mark.parametrize(
    "base_url",
    ["", "   ", "acexy:8090", "ftp://acexy:8090", "http://acexy:8090/prefix"],
)
def test_rejects_invalid_base_url(base_url):
    with pytest.raises(ValueError):
        build_acexy_stream_url(base_url, CONTENT_ID)


def test_rejects_empty_content_id():
    with pytest.raises(ValueError):
        build_acexy_stream_url("http://acexy:8090", "")


def test_reads_base_url_from_environment(monkeypatch):
    monkeypatch.setenv("ACEXY_BASE_URL", " http://acexy:8090/ ")
    assert get_acexy_base_url() == "http://acexy:8090"


def test_base_url_is_empty_when_unset(monkeypatch):
    monkeypatch.delenv("ACEXY_BASE_URL", raising=False)
    assert get_acexy_base_url() == ""
