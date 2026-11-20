import time
from dataclasses import dataclass


@dataclass
class Channel:
    name: str
    url: str
    redirect_url: str | None = None
    format: str | None = None
    ttl: int | None = None
    created_at: float | None = time.time()
    id: int | None = None
    dirty: bool | None = False
