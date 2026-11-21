import logging

from aiohttp import web

from .ace import build_acexy_stream_url, get_acexy_base_url
from .config import update_special_channel
from .db import check_db
from .models import Channel
from .repo import get_channel_by_name
from .sources import KIND_ACE, KIND_YT_DLP, parse_source
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


async def _channel_response(channel: Channel) -> web.Response | None:
    source = parse_source(channel.url)
    if source is None:
        logger.warning("channel %s has an unsupported source; skipping", channel.name)
        return None

    if source.kind == KIND_ACE:
        try:
            return redirect_response(
                build_acexy_stream_url(get_acexy_base_url(), source.value)
            )
        except ValueError as exc:
            logger.error("ACE redirect is unavailable for %s: %s", channel.name, exc)
            return web.Response(status=503, text="ACE playback is not configured")

    if channel.redirect_url is not None:
        return redirect_response(channel.redirect_url)

    return None


async def handle_channel(request: web.Request, appContext: AppContext):
    channel_name = request.match_info["channel"]

    channel = await get_channel_by_name(channel_name)
    if channel is not None:
        response = await _channel_response(channel)
        if response is not None:
            return response

    placeholder = await get_channel_by_name("placeholder")
    if placeholder is not None:
        response = await _channel_response(placeholder)
        if response is not None:
            return response

    return web.Response(status=404)


async def handle_channel_update(request: web.Request, appContext: AppContext):
    channel_name = request.match_info["channel"]
    channel = await get_channel_by_name(channel_name)
    source = parse_source(channel.url) if channel is not None else None

    if channel is not None and not (source is not None and source.kind == KIND_YT_DLP):
        return web.Response(
            text=f"Channel '{channel_name}' is not a yt-dlp source; nothing to resolve",
            content_type="text/plain",
            status=200,
        )

    await update_special_channel(channel_name, appContext)
    return web.Response(
        text=f"Channel '{channel_name}' updated successfully",
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
