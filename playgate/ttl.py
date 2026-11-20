import time
from typing import Optional


def humanize_duration(seconds: int) -> str:
    seconds = int(seconds)
    if seconds <= 0:
        return "0m"
    days, remainder = divmod(seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes = remainder // 60
    if days:
        return f"{days}d {hours}h"
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m"
    return "<1m"


def ttl_state(ttl: Optional[int], now: Optional[float] = None) -> dict[str, str]:
    if ttl is None:
        return {"state": "unknown", "label": "unknown"}
    if ttl == -1:
        return {"state": "eternal", "label": "eternal"}
    now = time.time() if now is None else now
    if ttl <= now:
        return {"state": "expired", "label": "expired"}
    return {
        "state": "valid",
        "label": f"expires in {humanize_duration(int(ttl - now))}",
    }
