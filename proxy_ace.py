import asyncio
import datetime
import logging
from aiohttp import web
from typing import Any, Coroutine, Dict, Protocol
import signal
from watchdog.observers import Observer
from watchdog.events import DirModifiedEvent, DirMovedEvent, FileModifiedEvent, FileMovedEvent, FileSystemEventHandler

from .config import sync_channels, CHANNELS_FILE, update_eternal_channels
from .handler import handle_client, handle_yt_dlp
from .models import Channel, RedirectChannel 

LISTEN_PORT = 8081

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

stop_event = asyncio.Event()
channels: Dict[str, Channel] = {}
redirects: Dict[str, RedirectChannel] = {}
channels_lock = asyncio.Lock()
redirects_lock = asyncio.Lock()

class SyncChannelsCallback(Protocol):
    def __call__(
        self,
        channels: dict[str, Channel],
        channels_lock: asyncio.Lock,
        redirects: dict[str, RedirectChannel],
        redirects_lock: asyncio.Lock,
        stop_event: asyncio.Event,
    ) -> Coroutine[Any, Any, None]:
        ...


class ConfigWatcher(FileSystemEventHandler):
    def __init__(self, callback: SyncChannelsCallback, channels: dict[str, Channel], channels_lock: asyncio.Lock, redirects: dict[str, RedirectChannel], redirects_lock: asyncio.Lock, loop: asyncio.AbstractEventLoop, stop_event: asyncio.Event):
        self.callback = callback
        self.channels = channels
        self.channels_lock = channels_lock
        self.redirects = redirects
        self.redirects_lock = redirects_lock
        self.loop = loop
        self.stop_event = stop_event
        self._debounce_handle = None

    def _trigger(self):
        self._debounce_handle = None
        self.loop.call_soon_threadsafe(
            lambda: asyncio.create_task(
                self.callback(
                    self.channels,
                    self.channels_lock,
                    self.redirects,
                    self.redirects_lock,
                    self.stop_event,
                )
            )
        )

    def _schedule(self):
        if self._debounce_handle is not None:
            self._debounce_handle.cancel()

        def schedule_inside_loop():
            self._debounce_handle = self.loop.call_later(0.1, self._trigger)

        self.loop.call_soon_threadsafe(schedule_inside_loop)

    def on_modified(self, event: DirModifiedEvent | FileModifiedEvent):
        raw = event.src_path 
        if isinstance(raw, str):
            src = raw
        else:
            src = bytes(raw).decode("utf-8", errors="replace")
        if src.endswith(CHANNELS_FILE):
            self._schedule()

    def on_moved(self, event: DirMovedEvent | FileMovedEvent):
        raw = event.src_path 
        if isinstance(raw, str):
            src = raw
        else:
            src = bytes(raw).decode("utf-8", errors="replace")
        if src.endswith(CHANNELS_FILE):
            self._schedule()

class UpdateEternalChannelsCallback(Protocol):
    def __call__(
        self,
        redirects: dict[str, RedirectChannel],
        redirects_lock: asyncio.Lock,
        stop_event: asyncio.Event,
    ) -> Coroutine[Any, Any, None]:
        ...

async def daily_routine(coro: UpdateEternalChannelsCallback, target_hour: int = 3):
    while True:
        now = datetime.datetime.now()
        target = now.replace(hour=target_hour, minute=0, second=0, microsecond=0)
        if target <= now:
            target += datetime.timedelta(days=1)
        
        sleep_seconds = (target - now).total_seconds()
        logger.info(f"next daily task in {sleep_seconds/3600:.2f} h")
        
        await asyncio.sleep(sleep_seconds)
        
        try:
            await coro(redirects, redirects_lock, stop_event)
        except Exception as e:
            logger.error(f"Error on daily_routine: {e}")

async def periodic_sync(stop_event: asyncio.Event):
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=60)
            continue
        except asyncio.TimeoutError:
            pass
        await sync_channels(channels, channels_lock, redirects, redirects_lock, stop_event)


app = web.Application()
async def ace_handler(request: web.Request) -> web.Response | web.StreamResponse:
    return await handle_client(request, channels, channels_lock, stop_event)

async def yt_dlp_handler(request: web.Request):
    return await handle_yt_dlp(request, redirects, redirects_lock)

app.router.add_get("/ace/{channel}", ace_handler)
app.router.add_get("/yt_dlp/{channel}", yt_dlp_handler)

async def main():
    loop = asyncio.get_running_loop()
    def handle_sigterm():
        loop.call_soon_threadsafe(stop_event.set)

    loop.add_signal_handler(signal.SIGTERM, handle_sigterm)
    loop.add_signal_handler(signal.SIGINT, handle_sigterm)
    await sync_channels(channels, channels_lock, redirects, redirects_lock, stop_event)
    asyncio.create_task(periodic_sync(stop_event))
    asyncio.create_task(daily_routine(update_eternal_channels))

    event_handler = ConfigWatcher(sync_channels, channels, channels_lock, redirects, redirects_lock, loop, stop_event)
    observer = Observer()
    observer.schedule(event_handler, CHANNELS_FILE, recursive=False)
    observer.start()

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, port=LISTEN_PORT)
    await site.start()

    try:
        await stop_event.wait()
    finally:
        observer.stop()
        observer.join()
    await runner.cleanup()

if __name__ == "__main__":
    asyncio.run(main())