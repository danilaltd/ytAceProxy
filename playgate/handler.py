import logging
from aiohttp import web

from .db import check_db
from .repo import get_redirect_url
from .config import update_special_channel
from .state import AppContext, appContext

logger = logging.getLogger(__name__)


async def channel_handler(request: web.Request):
    return await handle_channel(request, appContext)


async def channel_upd_handler(request: web.Request):
    return await handle_channel_update(request, appContext)


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
        },
    )


async def handle_channel(request: web.Request, appContext: AppContext):
    channel_name = request.match_info["channel"]

    redirect_url = await get_redirect_url(channel_name)
    if redirect_url is not None:
        return redirect_response(redirect_url)

    redirect_url = await get_redirect_url("placeholder")
    if redirect_url is not None:
        return redirect_response(redirect_url)

    return web.Response(status=404)


async def handle_channel_update(request: web.Request, appContext: AppContext):
    channel = request.match_info["channel"]
    await update_special_channel(channel, appContext)
    return web.Response(
        text=f"Channel '{channel}' updated successfully",
        content_type="text/plain",
        status=200,
    )


async def health_handler(request: web.Request):
    try:
        await check_db()
    except Exception as exc:
        logger.error("health check failed: %s", exc)
        return web.Response(status=503, text="unhealthy")

    return web.Response(status=200, text="ok")
