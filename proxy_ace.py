import asyncio
from dataclasses import asdict
import json
import logging
import time
import debugpy
import os
from aiohttp import web
import signal
from watchdog.observers import Observer
from .state import AppContext

from .config import ConfigWatcher, sync_channels, CHANNELS_FILE, update_eternal_channels, daily_routine, periodic_sync
from .handler import handle_client, handle_yt_dlp, handle_yt_dlp_upd
from .models import RedirectChannel 

LISTEN_PORT = 8081

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

appContext = AppContext()

def load_redirects(dump_filename: str="proxy_ace/redirects_dump.json") -> dict[str, RedirectChannel]:
    try:
        with open(dump_filename, "r") as f:
            data = json.load(f)
        
        now = time.time()
        loaded_redirects: dict[str, RedirectChannel] = {}
        
        for k, v in data.items():
            if v['ttl'] == -1 or v['ttl'] > now:
                loaded_redirects[k] = RedirectChannel(**v)
        
        return loaded_redirects
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_redirects(redirects: dict[str, RedirectChannel], dump_filename: str="proxy_ace/redirects_dump.json"):
    data = {k: asdict(v) for k, v in redirects.items()}
    with open(dump_filename, "w") as f:
        json.dump(data, f, indent=4)

app = web.Application()
async def ace_handler(request: web.Request) -> web.Response | web.StreamResponse:
    return await handle_client(request, appContext)

async def yt_dlp_handler(request: web.Request):
    return await handle_yt_dlp(request, appContext)

async def yt_dlp_upd_handler(request: web.Request):
    return await handle_yt_dlp_upd(request, appContext)

app.router.add_get("/ace/{channel}", ace_handler)
app.router.add_get("/yt_dlp/{channel}", yt_dlp_handler)
app.router.add_get("/yt_dlp/{channel}/upd", yt_dlp_upd_handler)

async def main():
    loop = asyncio.get_running_loop()
    def handle_sigterm():
        loop.call_soon_threadsafe(appContext.stop_event.set)
    appContext.redirects.update(load_redirects())
    loop.add_signal_handler(signal.SIGTERM, handle_sigterm)
    loop.add_signal_handler(signal.SIGINT, handle_sigterm)
    await sync_channels(appContext)
    asyncio.create_task(periodic_sync(appContext))
    asyncio.create_task(daily_routine(update_eternal_channels, appContext))
    event_handler = ConfigWatcher(sync_channels, appContext, loop)
    observer = Observer()
    observer.schedule(event_handler, CHANNELS_FILE, recursive=False)
    observer.start()

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, port=LISTEN_PORT)
    await site.start()

    try:
        await appContext.stop_event.wait()
    finally:
        observer.stop()
        observer.join()
        save_redirects(appContext.redirects)
    await runner.cleanup()

if __name__ == "__main__":
    port = os.environ.get('PYTHON_DEBUG_PORT')
    if port:
        debugpy.listen(("127.0.0.1", int(port)))
    asyncio.run(main())