import asyncio
import aiohttp
from aiohttp.web_exceptions import HTTPBadRequest
import logging
from .models import Channel

logger = logging.getLogger(__name__)

async def ensure_producer(channel_name: str, channels: dict[str, Channel], stop_event: asyncio.Event):
    ch = channels.get(channel_name)
    if ch is None:
        return
    async with ch.lock:
        if ch.producer is None or ch.producer.done():
            ch.producer = asyncio.create_task(stream_producer(channel_name, channels, stop_event))


async def stream_producer(channel_name: str, channels: dict[str, Channel], stop_event: asyncio.Event):
    if channel_name not in channels:
        return
    ch = channels[channel_name]
    retry_delay = 1.0

    while not stop_event.is_set():
        async with ch.lock:
            has_clients = bool(ch.clients)
        if not has_clients:
            break

        try:
            url = ch.url
            async with aiohttp.ClientSession(headers={"User-Agent": "proxy_ace/1.0"}) as session:
                logger.info(f"[{channel_name}] Connecting to {url}")
                try:
                    async with session.get(url, timeout=None) as resp:
                        if not (200 <= resp.status < 300):
                            logger.warning(f"[{channel_name}] upstream returned status {resp.status}")
                            raise RuntimeError(f"Upstream returned status {resp.status}")

                        logger.info(f"[{channel_name}] Stream started")
                        retry_delay = 1.0

                        async for raw_chunk in resp.content.iter_any():
                            if not raw_chunk:
                                logger.info(f"[{channel_name}] Upstream closed (empty iter chunk)")
                                break

                            async with ch.lock:
                                items = list(ch.clients.items())

                            disconnected: list[int] = []
                            for client_id, ctx in items:
                                q = ctx.queue
                                try:
                                    q.put_nowait(raw_chunk)
                                except asyncio.QueueFull:
                                    try:
                                        q.get_nowait()
                                    except asyncio.QueueEmpty:
                                        pass
                                    try:
                                        q.put_nowait(raw_chunk)
                                    except asyncio.QueueFull:
                                        logger.warning(f"[{channel_name}] queue still full for client {client_id}; dropping client")
                                        disconnected.append(client_id)
                                except Exception as e:
                                    logger.exception(f"[{channel_name}] Error putting chunk to client {client_id}: {e}")
                                    disconnected.append(client_id)

                            if disconnected:
                                async with ch.lock:
                                    for cid in disconnected:
                                        ch.clients.pop(cid, None)

                except HTTPBadRequest as e:
                    logger.exception(f"[{channel_name}] Bad HTTP message from upstream: {e}")
                    raise RuntimeError("Bad HTTP message from upstream") from e

        except asyncio.CancelledError:
            logger.info(f"[{channel_name}] Producer cancelled")
            break
        except Exception as e:
            logger.exception(f"[{channel_name}] Producer error: {e}")
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 30.0)

    logger.info(f"[{channel_name}] No clients or stopped. Producer finishing.")
    async with ch.lock:
        ch.producer = None