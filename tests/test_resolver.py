import asyncio
import multiprocessing

import pytest

import playgate.config as config
import playgate.repo as repo
from playgate.state import appContext

RESOLVED = "http://hidden.example/stream?expire=1893456000"


async def _fake_resolve(url, fmt, stop_event, timeout=60):
    return url, fmt, RESOLVED


async def test_resolve_channels_saves_redirect_and_ttl(app, monkeypatch):
    monkeypatch.setattr(config, "run_in_process_with_timeout", _fake_resolve)
    await repo.add_channel("nsf", "http://source", None, None, None)

    channels = await repo.get_channels()
    resolved = await config.resolve_channels(channels, asyncio.Event())
    await repo.update_channels(resolved)

    assert channels[0].id is not None
    saved = await repo.get_channel_by_id(channels[0].id)
    assert saved is not None
    assert saved.redirect_url == RESOLVED
    assert saved.ttl == 1893456000


async def test_update_special_channel_persists(app, monkeypatch):
    monkeypatch.setattr(config, "run_in_process_with_timeout", _fake_resolve)
    await repo.add_channel("nsf", "http://source", None, None, None)

    await config.update_special_channel("nsf", appContext)

    saved = (await repo.get_channels())[0]
    assert saved.redirect_url == RESOLVED
    assert saved.ttl == 1893456000


async def test_sync_channels_persists(app, monkeypatch):
    monkeypatch.setattr(config, "run_in_process_with_timeout", _fake_resolve)
    await repo.add_channel("a", "http://one", None, None, None)
    await repo.add_channel("b", "http://two", None, None, None)

    await config.sync_channels()

    channels = await repo.get_channels()
    assert all(channel.redirect_url == RESOLVED for channel in channels)


async def test_resolve_channels_skips_valid_ttl_unless_forced(app, monkeypatch):
    calls = []

    async def fake_resolve(url, fmt, stop_event, timeout=60):
        calls.append((url, fmt))
        return url, fmt, RESOLVED

    monkeypatch.setattr(config, "run_in_process_with_timeout", fake_resolve)
    await repo.add_channel("valid", "http://source", "http://old", 1893456000, None)

    channels = await repo.get_channels()
    await config.resolve_channels(channels, asyncio.Event())
    assert calls == []

    await config.resolve_channels(channels, asyncio.Event(), force_update=["valid"])
    assert calls == [("http://source", config.DEFAULT_FORMAT)]


async def test_resolve_channels_failure_keeps_redirect_empty(app, monkeypatch):
    async def failing_resolve(url, fmt, stop_event, timeout=60):
        raise RuntimeError("resolver down")

    monkeypatch.setattr(config, "run_in_process_with_timeout", failing_resolve)
    await repo.add_channel("nsf", "http://source", None, None, None)

    channels = await repo.get_channels()
    await config.resolve_channels(channels, asyncio.Event())

    assert channels[0].redirect_url is None
    assert channels[0].dirty is False


async def test_resolve_channels_dedups_by_url_and_format(app, monkeypatch):
    calls = []

    async def fake_resolve(url, fmt, stop_event, timeout=60):
        calls.append((url, fmt))
        return url, fmt, RESOLVED

    monkeypatch.setattr(config, "run_in_process_with_timeout", fake_resolve)
    await repo.add_channel("a", "http://same", None, None, None)
    await repo.add_channel("b", "http://same", None, None, None)
    await repo.add_channel("c", "http://same", None, None, "best")

    channels = await repo.get_channels()
    await config.resolve_channels(channels, asyncio.Event())

    assert sorted(calls) == sorted(
        [
            ("http://same", config.DEFAULT_FORMAT),
            ("http://same", "best"),
        ]
    )
    assert all(channel.redirect_url == RESOLVED for channel in channels)


@pytest.mark.skipif(
    multiprocessing.get_start_method() != "fork",
    reason="the resolver worker is tested with the fork start method",
)
async def test_worker_returns_url_format_and_link(monkeypatch):
    monkeypatch.setattr(
        config, "python_yt_dlp_get_link", lambda url, fmt: "http://link"
    )

    result = await config.run_in_process_with_timeout(
        "http://source", "best", asyncio.Event(), timeout=5
    )

    assert result == ("http://source", "best", "http://link")


@pytest.mark.skipif(
    multiprocessing.get_start_method() != "fork",
    reason="the resolver worker is tested with the fork start method",
)
async def test_worker_uses_default_format(monkeypatch):
    monkeypatch.setattr(
        config, "python_yt_dlp_get_link", lambda url, fmt: "http://link"
    )

    result = await config.run_in_process_with_timeout(
        "http://source", None, asyncio.Event(), timeout=5
    )

    assert result == ("http://source", config.DEFAULT_FORMAT, "http://link")


def test_worker_errors_are_sanitized(monkeypatch):
    def boom(url, fmt):
        raise RuntimeError("failed https://secret.example/x?token=1")

    monkeypatch.setattr(config, "python_yt_dlp_get_link", boom)

    with pytest.raises(Exception, match="<url>"):
        asyncio.run(
            config.run_in_process_with_timeout(
                "http://source", None, asyncio.Event(), timeout=5
            )
        )


def test_rejects_merge_format():
    with pytest.raises(ValueError, match="ffmpeg"):
        config.python_yt_dlp_get_link("http://source", "bestvideo+bestaudio")


def test_safe_error_strips_urls():
    assert (
        config._safe_error(RuntimeError("see https://x.example/y?token=1 now"))
        == "see <url> now"
    )


HASH = "a" * 40


async def test_sync_channels_skips_ace_sources(app, monkeypatch):
    calls = []

    async def fake_resolve(url, fmt, stop_event, timeout=60):
        calls.append(url)
        return url, fmt, RESOLVED

    monkeypatch.setattr(config, "run_in_process_with_timeout", fake_resolve)
    await repo.add_channel("yt", "http://source", None, None, None)
    await repo.add_channel("ace1", f"acestream://{HASH}", None, None, None)

    await config.sync_channels()

    assert calls == ["http://source"]
    ace = await repo.get_channel_by_name("ace1")
    assert ace is not None
    assert ace.redirect_url is None


async def test_update_special_channel_on_ace_is_noop(app, monkeypatch):
    calls = []

    async def fake_resolve(url, fmt, stop_event, timeout=60):
        calls.append(url)
        return url, fmt, RESOLVED

    monkeypatch.setattr(config, "run_in_process_with_timeout", fake_resolve)
    await repo.add_channel("ace1", f"acestream://{HASH}", None, None, None)

    await config.update_special_channel("ace1", appContext)

    assert calls == []
