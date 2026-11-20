from dataclasses import dataclass
import time
from typing import Optional


@dataclass
class Channel:
    name: str
    url: str
    redirect_url: Optional[str] = None
    format: Optional[str] = None
    ttl: Optional[int] = None
    created_at: Optional[float] = time.time()
    id: Optional[int] = None
    dirty: Optional[bool] = False
