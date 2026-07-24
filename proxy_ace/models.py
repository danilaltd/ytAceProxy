from dataclasses import dataclass
import time
from typing import Optional

@dataclass
class RedirectChannel:
    name: str
    url: str
    redirect_url: Optional[str]
    ttl: Optional[int] = None
    created_at: Optional[float] = time.time()
    id: Optional[int] = None
    dirty: Optional[bool] = False