import asyncio
from .models import Channel

class AppContext:
    def __init__(self):
        self.channels_streaming_now: dict[str, Channel] = {}
        self.channels_lock = asyncio.Lock()
        self.stop_event = asyncio.Event()

appContext = AppContext()