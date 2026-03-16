from dataclasses import dataclass, field
import asyncio

from aiohttp.web import StreamResponse

@dataclass
class Client:
    response: StreamResponse
    queue: asyncio.Queue[bytes]
@dataclass
class Channel:
    url: str
    clients: dict[int, Client] = field(default_factory=lambda: {})
    producer: asyncio.Task[None] | None = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    stop_event: asyncio.Event = field(default_factory=asyncio.Event)

@dataclass
class RedirectChannel:
    url: str
    redirect_url: str
    ttl: int