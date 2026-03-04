from multiprocessing  import Process, Queue
from collections import defaultdict
import hashlib
import json
import os
import asyncio
import logging
from .models import Channel, RedirectChannel
from yt_dlp import YoutubeDL, parse_options
from concurrent.futures import ThreadPoolExecutor

QUEUE_MAX_SIZE = 25000
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
                raise asyncio.TimeoutError("task timeout")

            await asyncio.sleep(interval)
            elapsed += interval

    finally:
        if p.is_alive():
            p.terminate()
            p.join()

async def parallel_fill_redirects(
    redirect_dict: dict[str, str], 
    redirects: dict[str, RedirectChannel], 
    stop_event: asyncio.Event, 
    full_update: bool
):
    executor = ThreadPoolExecutor(max_workers=8)
    
    results: dict[str, RedirectChannel] = {}
    url_to_names: defaultdict[str,list[str]] = defaultdict(list)
    for name, url in redirect_dict.items():
        if not url:
            continue
            
        is_already_known = False
        if not full_update:
            for res in redirects.values():
                if res.url == url:
                    results[name] = res
                    is_already_known = True
                    break
        
        if full_update or not is_already_known:
            url_to_names[url].append(name)

    tasks = {
        url: asyncio.create_task(run_in_process_with_timeout (url, stop_event))
        for url in url_to_names.keys()
    }

    for task in asyncio.as_completed(tasks.values()):
        try:
            url, url_redirect = await task
        except Exception as e:
            logger.error(f"Error fetching: {e}")
            continue
        ch = RedirectChannel(url=url, redirect_url=url_redirect)
        for name in url_to_names[url]:
            results[name] = ch
            logger.info(f"Link for {name} ({url}): {url_redirect}")
    executor.shutdown(wait=False)
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

async def sync_channels(channels: dict[str, Channel], channels_lock: asyncio.Lock, redirects: dict[str, RedirectChannel], redirects_lock: asyncio.Lock, stop_event: asyncio.Event, full_update: bool = False):
    logger.info(f"start sync, full upd: {full_update}")
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

    updates = await parallel_fill_redirects(redirect_dict, redirects, stop_event, full_update)
    async with redirects_lock:
        if full_update: 
            redirects.clear()
        redirects.update(updates)

    logger.info("end sync")