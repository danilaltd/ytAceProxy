import pytest

from playgate.ttl import humanize_duration, ttl_state

NOW = 1_800_000_000


def test_ttl_unknown():
    assert ttl_state(None) == {"state": "unknown", "label": "unknown"}


def test_ttl_eternal():
    assert ttl_state(-1) == {"state": "eternal", "label": "eternal"}


def test_ttl_expired():
    assert ttl_state(NOW - 1, now=NOW) == {"state": "expired", "label": "expired"}


@pytest.mark.parametrize(
    ("seconds", "label"),
    [
        (30, "expires in <1m"),
        (5 * 60, "expires in 5m"),
        (2 * 3600 + 5 * 60, "expires in 2h 5m"),
        (3 * 86400 + 4 * 3600, "expires in 3d 4h"),
    ],
)
def test_ttl_valid(seconds, label):
    assert ttl_state(NOW + seconds, now=NOW) == {"state": "valid", "label": label}


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [
        (0, "0m"),
        (59, "<1m"),
        (60, "1m"),
        (3600, "1h 0m"),
        (86400, "1d 0h"),
    ],
)
def test_humanize_duration(seconds, expected):
    assert humanize_duration(seconds) == expected
