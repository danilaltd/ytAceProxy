import asyncio
import aiohttp
from aiohttp.web_exceptions import HTTPBadRequest
from aiohttp.client_exceptions import ClientResponseError
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

    try_number: int = 0
    timeout = aiohttp.ClientTimeout(sock_read=10)
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
                    async with session.get(url, timeout=timeout) as resp:
                        if not (200 <= resp.status < 300):
                            logger.warning(f"[{channel_name}] upstream returned status {resp.status}")
                            raise RuntimeError(f"Upstream returned status {resp.status}")

                        logger.info(f"[{channel_name}] Stream started")

                        async for raw_chunk in resp.content.iter_any():
                            if not raw_chunk:
                                logger.info(f"[{channel_name}] Upstream closed (empty iter chunk)")
                                break

                            async with ch.lock:
                                items = list(ch.clients.items())
                            if len(items) == 0:
                                break
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
                                        logger.warning(f"[{channel_name}] queue was full for client {client_id}; but its ok after dropping chunk")
                                    except asyncio.QueueFull:
                                        logger.warning(f"[{channel_name}] queue still full after dropping chunk for client {client_id}")
                                except Exception as e:
                                    logger.exception(f"[{channel_name}] Error putting chunk to client {client_id}: {e}")

                except asyncio.TimeoutError as e: 
                    logger.error(f"[{channel_name}] timeout error from upstream: {e}")
                    raise RuntimeError("timeout error from upstream") from e

                except HTTPBadRequest as e:
                    logger.exception(f"[{channel_name}] Bad HTTP message from upstream: {e}")
                    raise RuntimeError("Bad HTTP message from upstream") from e

                except ClientResponseError as e:
                    logger.error(f"[{channel_name}] Client Response Error from upstream: {e}")
                    raise RuntimeError("Client Response Error from upstream") from e

        except asyncio.CancelledError:
            logger.info(f"[{channel_name}] Producer cancelled")
            break
        except Exception as e:
            logger.error(f"[{channel_name}] Producer error: {e.__class__} {e}")
            await asyncio.sleep(2 ** try_number)
            try_number += 1
            if try_number >= 3:
                break

    logger.info(f"[{channel_name}] No clients or stopped. Producer finishing.")
    async with ch.lock:
        logger.info(f"[{channel_name}] cleared producer.")
        ch.stop_event.set()
        ch.producer = None