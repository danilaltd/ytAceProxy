from multiprocessing  import Process, Queue
from collections import defaultdict
import hashlib
import json
import os
import asyncio
import logging
import time
from .models import Channel, RedirectChannel
from yt_dlp import YoutubeDL, parse_options
import re

MAX_PROCESSES = 2
CHANNELS_FILE: str = "/home/daniil/proxy_ace/channels.json"

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
    redirect_dict: dict[str, str], 
    redirects: dict[str, RedirectChannel], 
    stop_event: asyncio.Event,
    update_eternal_channels: bool = False,
    force_update: list[str]|None = None
):
    if force_update is None:
        force_update = []
    results: dict[str, RedirectChannel] = {}
    url_to_names: defaultdict[str, list[str]] = defaultdict(list)
    
    now = time.time()
    if update_eternal_channels:
        active_cache = {res.url: res for res in redirects.values() if res.ttl != -1}
    else:
        active_cache = {res.url: res for res in redirects.values() if (res.ttl > now or res.ttl == -1)}
    for name, url in redirect_dict.items():
        if not url:
            continue
        if (name not in force_update) and (url in active_cache):
            results[name] = active_cache[url]
            continue
        url_to_names[url].append(name)
    
    async def bounded_fetch(url: str):
        async with sem:
            return await run_in_process_with_timeout(url, stop_event)

    sem = asyncio.Semaphore(MAX_PROCESSES)
    tasks = {
        url: asyncio.create_task(bounded_fetch(url))
        for url in url_to_names.keys()
    }

    for task in asyncio.as_completed(tasks.values()):
        try:
            url, url_redirect = await task
        except Exception as e:
            logger.error(f"Error fetching: {e}")
            continue
        match = re.search(r"(expire|validto|exp)(=|/)(\d{10})", url_redirect)
        timestamp = int(match.group(3) if match else -1)
        ch = RedirectChannel(url=url, redirect_url=url_redirect, ttl=timestamp)
        for name in url_to_names[url]:
            results[name] = ch
            logger.info(f"Link for {name} ({url}): {url_redirect}")
    return results

def load_channels_sync() -> dict[str, dict[str, str]]:
    if not os.path.exists(CHANNELS_FILE):
        return {"ace": {}, "yt-dlp": {}}

    try:
        with open(CHANNELS_FILE, "r", encoding="utf-8") as f:
            data: dict[str, object] = json.load(f)
    except json.JSONDecodeError:
        return {"ace": {}, "yt-dlp": {}}

    ace = data.get("ace") 
    yt = data.get("yt-dlp")
    return {
        "ace": ace if isinstance(ace, dict) else {},
        "yt-dlp": yt if isinstance(yt, dict) else {},
    }

async def load_channels() -> dict[str, dict[str, str]]:
    return await asyncio.to_thread(load_channels_sync)

async def sync_channels(channels: dict[str, Channel], channels_lock: asyncio.Lock, redirects: dict[str, RedirectChannel], redirects_lock: asyncio.Lock, stop_event: asyncio.Event):
    logger.info(f"start sync")
    config = await load_channels()
    ace_dict: dict[str, str] = config["ace"]
    redirect_dict: dict[str, str] = config["yt-dlp"]
    async with channels_lock:
        for name, url in ace_dict.items():
            head = 'http://localhost:6878/ace/getstream?id='
            pid = f"&pid=splitter_{hashlib.sha1(url.encode()).hexdigest()[:10]}"
            url = head + url + pid
            if name not in channels:
                channels[name] = Channel(url=url)
                continue

            ch = channels[name]

            if ch.url != url:
                logger.info(f"[{name}] URL changed. Restarting producer.")
                ch.url = url

                if ch.producer and not ch.producer.done():
                    ch.producer.cancel()

        for name in list(channels.keys()):
            if name not in config["ace"]:
                chan = channels.pop(name)
                if chan.producer and not chan.producer.done():
                    chan.producer.cancel()

    updates = await parallel_fill_redirects(redirect_dict, redirects, stop_event)
    async with redirects_lock:
        redirects.update(updates)

    logger.info("end sync")

async def update_eternal_channels(redirects: dict[str, RedirectChannel], redirects_lock: asyncio.Lock, stop_event: asyncio.Event):
    logger.info(f"start update_eternal_channels")
    config = await load_channels()
    redirect_dict: dict[str, str] = config["yt-dlp"]
    updates = await parallel_fill_redirects(redirect_dict, redirects, stop_event, update_eternal_channels=True)
    async with redirects_lock:
        redirects.clear()
        redirects.update(updates)
    logger.info(f"end update_eternal_channels")

async def update_special_channel(channel: str, redirects: dict[str, RedirectChannel], redirects_lock: asyncio.Lock, stop_event: asyncio.Event):
    logger.info(f"start update_special_channel")
    config = await load_channels()
    redirect_dict: dict[str, str] = config["yt-dlp"]
    updates = await parallel_fill_redirects(redirect_dict, redirects, stop_event, force_update=[channel])
    async with redirects_lock:
        redirects.update(updates)
    logger.info(f"end update_special_channel")
