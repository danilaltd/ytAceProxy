import datetime
from multiprocessing import Process, Queue
from collections import defaultdict
import asyncio
import logging
import re
import time
from typing import Any, Coroutine, Protocol

from yt_dlp import YoutubeDL, parse_options

from .models import Channel
from .repo import get_channels, update_channels
from .state import AppContext, appContext

MAX_PROCESSES = 2
DEFAULT_FORMAT = "best[acodec!=none]"

logger = logging.getLogger(__name__)


def _safe_error(exc: Exception) -> str:
    """Strip URLs (which can carry tokens) from an error message."""
    return re.sub(r"https?://\S+", "<url>", str(exc))


def python_yt_dlp_get_link(url: str, fmt: str | None = None) -> str:
    selected = (fmt or DEFAULT_FORMAT).strip()
    if "+" in selected:
        raise ValueError("merge formats are not supported: the image has no ffmpeg")

    with YoutubeDL(
        parse_options(
            [
                "--quiet",
                "--skip-download",
                "--format",
                selected,
                url,
            ]
        ).ydl_opts
    ) as ydl:
        info = ydl.extract_info(url, download=False)

    if "requested_formats" in info:
        raise Exception("expected one link, got multiple formats")
    res = info.get("url")
    if isinstance(res, str):
        return res
    raise Exception("yt-dlp returned a non-string url")


def worker_target(url: str, fmt: str | None, out_q: "Queue[tuple[str, str]]"):
    try:
        out_q.put(("ok", python_yt_dlp_get_link(url, fmt)))
    except KeyboardInterrupt:
        out_q.put(("err", "stopped"))
    except Exception as exc:
        out_q.put(("err", _safe_error(exc)))


async def run_in_process_with_timeout(
    url: str,
    fmt: str | None,
    stop_event: asyncio.Event,
    timeout: int = 60,
) -> tuple[str, str]:
    q: "Queue[tuple[str, str]]" = Queue(1)
    p = Process(target=worker_target, args=(url, fmt, q))
    p.start()

    try:
        elapsed = 0
        interval = 0.5
        while True:
            if stop_event.is_set():
                p.terminate()
                p.join(timeout=2)
                raise Exception("stopped")

            if not q.empty():
                tag, payload = q.get_nowait()
                p.join()
                if tag == "ok":
                    return url, payload
                raise Exception(payload)

            if elapsed >= timeout:
                p.terminate()
                p.join(timeout=2)
                raise asyncio.TimeoutError("resolve timeout")

            await asyncio.sleep(interval)
            elapsed += interval

    finally:
        if p.is_alive():
            p.terminate()
            p.join(timeout=2)
            if p.is_alive():
                p.kill()
                p.join()


def _key(channel: Channel) -> tuple[str, str]:
    return channel.url, (channel.format or DEFAULT_FORMAT).strip()


async def resolve_channels(
    channels: list[Channel],
    stop_event: asyncio.Event,
    update_eternal_channels: bool = False,
    force_update: list[str] | None = None,
) -> list[Channel]:
    if force_update is None:
        force_update = []

    now = time.time()
    name_to_channel = {channel.name: channel for channel in channels}

    if update_eternal_channels:
        valid_keys = {
            _key(channel)
            for channel in channels
            if channel.ttl is not None and channel.ttl != -1
        }
    else:
        valid_keys = {
            _key(channel)
            for channel in channels
            if channel.ttl is not None and (channel.ttl > now or channel.ttl == -1)
        }

    key_to_names: dict[tuple[str, str], list[str]] = defaultdict(list)
    for channel in channels:
        if (not channel.url) or (
            channel.name not in force_update and _key(channel) in valid_keys
        ):
            continue
        key_to_names[_key(channel)].append(channel.name)

    semaphore = asyncio.Semaphore(MAX_PROCESSES)

    async def bounded_fetch(url: str, fmt: str):
        if stop_event.is_set():
            return url, fmt, None

        async with semaphore:
            try:
                return await run_in_process_with_timeout(url, fmt, stop_event)
            except Exception as exc:
                logger.error(
                    "resolve failed for %s: %s",
                    ",".join(key_to_names[(url, fmt)]),
                    _safe_error(exc),
                )
                return url, fmt, None

    tasks = [asyncio.create_task(bounded_fetch(url, fmt)) for url, fmt in key_to_names]

    for task in asyncio.as_completed(tasks):
        if stop_event.is_set():
            break
        url, fmt, redirect_url = await task
        if not redirect_url:
            continue
        match = re.search(r"(expire|validto|exp)(=|/)(\d{10})", redirect_url)
        timestamp = int(match.group(3)) if match else -1
        for name in key_to_names[(url, fmt)]:
            channel = name_to_channel[name]
            channel.redirect_url = redirect_url
            channel.ttl = timestamp
            channel.dirty = True
            logger.info("resolved %s (ttl=%s)", name, timestamp)

    return channels


async def sync_channels():
    logger.info("start sync")
    channels: list[Channel] = await get_channels()

    await resolve_channels(channels, appContext.stop_event)
    await update_channels(channels)

    logger.info("end sync")


async def update_eternal_channels(appContext: AppContext):
    logger.info("start update_eternal_channels")
    channels: list[Channel] = await get_channels()

    await resolve_channels(
        channels, appContext.stop_event, update_eternal_channels=True
    )
    await update_channels(channels)
    logger.info("end update_eternal_channels")


async def update_special_channel(channel_name: str, appContext: AppContext):
    logger.info("start update_special_channel")
    channels: list[Channel] = await get_channels()
    await resolve_channels(channels, appContext.stop_event, force_update=[channel_name])
    await update_channels(channels)
    logger.info("end update_special_channel")


class UpdateEternalChannelsCallback(Protocol):
    def __call__(
        self,
        appContext: AppContext,
    ) -> Coroutine[Any, Any, None]: ...


async def daily_routine(
    coro: UpdateEternalChannelsCallback,
    appContext: AppContext,
    target_hour: int = 3,
):
    while True:
        now = datetime.datetime.now()
        target = now.replace(hour=target_hour, minute=0, second=0, microsecond=0)
        if target <= now:
            target += datetime.timedelta(days=1)

        sleep_seconds = (target - now).total_seconds()
        logger.info(f"next daily task in {sleep_seconds / 3600:.2f} h")

        await asyncio.sleep(sleep_seconds)

        try:
            await coro(appContext)
        except Exception as exc:
            logger.error(f"Error on daily_routine: {_safe_error(exc)}")
