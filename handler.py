import asyncio
import logging
from aiohttp import web

from .state import AppContext
from .config import update_special_channel
from .models import Client
from .producer import ensure_producer

logger = logging.getLogger(__name__)

QUEUE_MAX_SIZE = 5

async def handle_client(request: web.Request, appContext: AppContext):
    channel_name = request.match_info["channel"]
    async with appContext.channels_lock:
        ch = appContext.channels.get(channel_name)
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
        channel_stop_event = ch.stop_event
        channel_stop_event.clear()
        total = len(ch.clients)

    logger.info(f"[{channel_name}] Client connected ({client_id}). Total: {total}")

    await ensure_producer(channel_name, appContext)
    stop_event = appContext.stop_event
    try:
        while not stop_event.is_set() and not channel_stop_event.is_set():
            try:
                chunk = await asyncio.wait_for(queue.get(), 5)
                queue.task_done()
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                continue
            await response.write(chunk)

    except (ConnectionResetError, asyncio.CancelledError, RuntimeError) as e:
        logger.info(f"[{channel_name}] client {client_id} disconnected: {e}")
    except Exception as e:
        logger.error(f"[{channel_name}] unexpected error for client {client_id}: {e}")
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

async def handle_yt_dlp(request: web.Request, appContext: AppContext):
    channel = request.match_info["channel"]
    async with appContext.redirects_lock:
        channel_obj = appContext.redirects.get(channel) or appContext.redirects.get("placeholder")
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

async def handle_yt_dlp_upd(request: web.Request, appContext: AppContext):
    channel = request.match_info["channel"]
    await update_special_channel(channel, appContext)
    return web.Response(
        text=f"Channel '{channel}' updated successfully",
        content_type="text/plain",
        status=200
    )