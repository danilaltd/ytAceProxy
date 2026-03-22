import asyncio
from .models import Channel, RedirectChannel

class AppContext:
    def __init__(self):
        self.channels: dict[str, Channel] = {}
        self.redirects: dict[str, RedirectChannel] = {}
        self.channels_lock = asyncio.Lock()
        self.redirects_lock = asyncio.Lock()
        self.stop_event = asyncio.Event()