import datetime
from multiprocessing  import Process, Queue
from collections import defaultdict
import asyncio
import logging
import time
from typing import Any, Coroutine, Protocol

from .repo import get_redirects, update_redirects

from .state import appContext
from .models import RedirectChannel
from yt_dlp import YoutubeDL, parse_options
import re

MAX_PROCESSES = 2

logger = logging.getLogger(__name__)

def python_yt_dlp_get_link(url: str) -> str:
    with YoutubeDL(parse_options(["--quiet", "--skip-download", "--format", "best[acodec!=none]", url]).ydl_opts) as ydl:
        info = ydl.extract_info(url, download=False)

    if "requested_formats" in info:
        raise Exception(f"expected 1 link, got multiple: {info}")
    res = info.get("url")
    if isinstance(res, str):
        return res
    raise Exception(f"yt-dlp returned non-string url: {info}")

def worker_target(url: str, out_q: "Queue[tuple[str, str]]"):
    try:
        res = python_yt_dlp_get_link(url)
        out_q.put(("ok", res))
    except KeyboardInterrupt:
        out_q.put(("err", "stopped"))
        return
    except Exception as e:
        out_q.put(("err", str(e)))

async def run_in_process_with_timeout(url: str, stop_event: asyncio.Event, timeout: int = 60) -> tuple[str, str]:
    q: "Queue[tuple[str, str]]" = Queue(1)
    p = Process(target=worker_target, args=(url, q))
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
                else:
                    raise Exception(payload)

            if elapsed >= timeout:
                p.terminate()
                p.join(timeout=2)
                raise asyncio.TimeoutError(f"{url}: task timeout")

            await asyncio.sleep(interval)
            elapsed += interval

    finally:
        if p.is_alive():
            p.terminate()
            p.join(timeout=2)
            if p.is_alive():
                p.kill()
                p.join()

async def parallel_fill_redirects(
    redirects: list[RedirectChannel],
    stop_event: asyncio.Event,
    update_eternal_channels: bool = False,
    force_update: list[str] | None = None,
) -> list[RedirectChannel]:
    if force_update is None:
        force_update = []
    now = time.time()
    name_to_obj = {r.name: r for r in redirects}
    url_to_names: dict[str, list[str]] = defaultdict(list)
    if update_eternal_channels:
        valid_urls = {r.url for r in redirects if r.ttl is not None and r.ttl != -1}
    else:
        valid_urls = {r.url for r in redirects if (r.ttl is not None and (r.ttl > now or r.ttl == -1))}
    for r in redirects:
        if (not r.url) or (r.name not in force_update) and (r.url in valid_urls):
            continue
        url_to_names[r.url].append(r.name)
    sem = asyncio.Semaphore(MAX_PROCESSES)
    async def bounded_fetch(url: str):
        if stop_event.is_set():
            return url, None

        async with sem:
            try:
                return await run_in_process_with_timeout(url, stop_event)
            except Exception as e:
                logger.error(f"Error fetching {url}: {e}")
                return url, None

    tasks = [
        asyncio.create_task(bounded_fetch(url))
        for url in url_to_names.keys()
    ]

    for task in asyncio.as_completed(tasks):
        if stop_event.is_set():
            break
        url, url_redirect = await task
        if not url_redirect:
            continue
        match = re.search(r"(expire|validto|exp)(=|/)(\d{10})", url_redirect)
        timestamp = int(match.group(3)) if match else -1
        for name in url_to_names[url]:
            r=name_to_obj[name]
            r.redirect_url=url_redirect
            r.ttl=timestamp
            r.dirty=True
            logger.info(f"Link for {name} ({url}): {url_redirect}")
    return redirects

async def load_channels():
    # return await get_channels()

    return {
        "ace": {""},
        "yt-dlp": {""},
    }

async def sync_channels():
    # appContext: AppContext
    logger.info(f"start sync")
    redirects: list[RedirectChannel] = await get_redirects()

    await parallel_fill_redirects(redirects, appContext.stop_event)
    await update_redirects(redirects)

    logger.info("end sync")

async def update_eternal_channels(appContext: AppContext):
    logger.info(f"start update_eternal_channels")
    redirects: list[RedirectChannel] = await get_redirects()

    await parallel_fill_redirects(redirects, appContext.stop_event, update_eternal_channels=True)
    await update_redirects(redirects)
    logger.info(f"end update_eternal_channels")

async def update_special_channel(channel_name: str, appContext: AppContext):
    logger.info(f"start update_special_channel")
    redirects: list[RedirectChannel] = await get_redirects()
    await parallel_fill_redirects(redirects, appContext.stop_event, force_update=[channel_name])
    await update_redirects(redirects)
    logger.info(f"end update_special_channel")


class UpdateEternalChannelsCallback(Protocol):
    def __call__(
        self,
        appContext: AppContext,
    ) -> Coroutine[Any, Any, None]:
        ...


async def daily_routine(coro: UpdateEternalChannelsCallback, appContext: AppContext, target_hour: int = 3):
    while True:
        now = datetime.datetime.now()
        target = now.replace(hour=target_hour, minute=0, second=0, microsecond=0)
        if target <= now:
            target += datetime.timedelta(days=1)

        sleep_seconds = (target - now).total_seconds()
        logger.info(f"next daily task in {sleep_seconds/3600:.2f} h")

        await asyncio.sleep(sleep_seconds)

        try:
            await coro(appContext)
        except Exception as e:
            logger.error(f"Error on daily_routine: {e}")