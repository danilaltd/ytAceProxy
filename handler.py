import asyncio
import hashlib
import logging
from aiohttp import web

from .repo import get_redirect_url, get_streaming_channel_url
from .config import update_special_channel
from .models import Channel, Client
from .producer import ensure_producer
from .state import AppContext, appContext

logger = logging.getLogger(__name__)

QUEUE_MAX_SIZE = 5

async def ace_handler(request: web.Request) -> web.Response | web.StreamResponse:
    return await handle_client(request, appContext)

async def yt_dlp_handler(request: web.Request):
    return await handle_yt_dlp(request, appContext)

async def yt_dlp_upd_handler(request: web.Request):
    return await handle_yt_dlp_upd(request, appContext)


async def handle_client(request: web.Request, appContext: AppContext):
    channel_name = request.match_info["channel"]
    async with appContext.channels_lock:
        ch = appContext.channels_streaming_now.get(channel_name)
        if ch is None:
            channel_url = await get_streaming_channel_url(channel_name)
            if channel_url is None:
                return web.Response(status=404, text="Channel not found")
            head = 'http://localhost:6878/ace/getstream?id='
            pid = f"&pid=splitter_{hashlib.sha1(channel_url.encode()).hexdigest()[:10]}"
            channel_url = head + channel_url + pid
            ch = Channel(url=channel_url)
            appContext.channels_streaming_now[channel_name] = ch

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

def redirect_response(redirect_url: str) -> web.Response:
    if not redirect_url:
        return web.Response(status=404, text="Cannot get stream URL")

    return web.Response(
        status=302,
        headers={
            "Location": redirect_url,
            "User-Agent": "Mozilla/5.0",
            "Accept": "*/*",
            "Connection": "keep-alive",
        }
    )

async def handle_yt_dlp(request: web.Request, appContext: AppContext):
    channel_name = request.match_info["channel"]
    
    redirect_url = await get_redirect_url(channel_name)
    if redirect_url is not None: 
        return redirect_response(redirect_url)

    redirect_url = await get_redirect_url("placeholder")
    if redirect_url is not None:
        return redirect_response(redirect_url)
    
    return web.Response(status=404)

async def handle_yt_dlp_upd(request: web.Request, appContext: AppContext):
    channel = request.match_info["channel"]
    await update_special_channel(channel, appContext)
    return web.Response(
        text=f"Channel '{channel}' updated successfully",
        content_type="text/plain",
        status=200
    )