import asyncio
from http.client import HTTPException
import logging
from aiohttp import web

from .models import Channel, Client, RedirectChannel
from .producer import ensure_producer

logger = logging.getLogger(__name__)

QUEUE_MAX_SIZE = 25000

async def handle_client(request: web.Request, channels: dict[str, Channel], channels_lock: asyncio.Lock, stop_event: asyncio.Event):
    channel_name = request.match_info["channel"]
    async with channels_lock:
        ch = channels.get(channel_name)
    if ch is None:
        return web.Response(status=404, text="Channel not found")

    response = web.StreamResponse(
        status=200,
        headers={
            "Content-Type": "video/mp2t",
        }
    )
    await response.prepare(request)

    queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
    client_id = id(response)
    async with ch.lock:
        ch.clients[client_id] = Client(response=response, queue=queue)
        total = len(ch.clients)

    logger.info(f"[{channel_name}] Client connected ({client_id}). Total: {total}")

    await ensure_producer(channel_name, channels, stop_event)
    try:
        while not stop_event.is_set():
            chunk = await queue.get()
            await response.write(chunk)

    except (ConnectionResetError, asyncio.CancelledError, RuntimeError, HTTPException) as e:
        logger.info(f"[{channel_name}] client {client_id} disconnected: {e}")
    except Exception as e:
        logger.exception(f"[{channel_name}] unexpected error for client {client_id}: {e}")
    finally:
        async with ch.lock:
            ch.clients.pop(client_id, None)
            left = len(ch.clients)
        logger.info(f"[{channel_name}] Client disconnected ({client_id}). Left: {left}")
        try:
            await response.write_eof()
        except Exception:
            pass

    return response

async def handle_yt_dlp(request: web.Request, redirects: dict[str, RedirectChannel], redirects_lock: asyncio.Lock):
    channel = request.match_info["channel"]
    async with redirects_lock:
        channel_obj = redirects.get(channel) or redirects.get("placeholder")
        if channel_obj is None:
            return web.Response(status=404)

        url = channel_obj.redirect_url
    if not url:
        return web.Response(status=404, text="Cannot get stream URL")

    return web.Response(
    status=302,
    headers={
        "Location": url,
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
        "Connection": "keep-alive", 
    }
)