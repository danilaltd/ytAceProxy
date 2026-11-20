import asyncio

import playgate.config as config
import playgate.repo as repo


RESOLVED = "http://hidden.example/stream?expire=1893456000"


async def _fake_resolve(url, fmt, stop_event, timeout=60):
    return url, fmt, RESOLVED


async def test_resolve_channels_saves_redirect_and_ttl(app, monkeypatch):
    monkeypatch.setattr(config, "run_in_process_with_timeout", _fake_resolve)
    await repo.add_channel("nsf", "http://source", None, None, None)

    channels = await repo.get_channels()
    resolved = await config.resolve_channels(channels, asyncio.Event())
    await repo.update_channels(resolved)

    saved = await repo.get_channel_by_id(channels[0].id)
    assert saved.redirect_url == RESOLVED
    assert saved.ttl == 1893456000


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
