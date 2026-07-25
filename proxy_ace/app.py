import asyncio
import logging
import aiohttp_jinja2
import jinja2
from aiohttp import web
from aiohttp.web_request import Request
from aiohttp.web_response import StreamResponse
from typing import Any, Callable, Awaitable
import signal

from .db import init_db
from .state import appContext
from .admin_routes import routes

from .handler import yt_dlp_upd_handler, yt_dlp_handler

LISTEN_PORT = 80

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@web.middleware
async def prefix_middleware(
    request: Request,
    handler: Callable[[Request], Awaitable[StreamResponse]]
) -> StreamResponse:
    prefix = request.headers.get('X-Forwarded-Prefix', '')
    request['app_prefix'] = prefix
    
    return await handler(request)

async def prefix_context_processor(request: Request) -> dict[str, Any]:
    def url_for(name: str, **kwargs: dict[str, Any]) -> str:
        route = request.app.router[name]
        return str(route.url_for(**{k: str(v) for k, v in kwargs.items()}))

    return {
        "app_prefix": request.get("app_prefix", ""),
        "url_for": url_for
    }
async def create_app():
    app = web.Application(middlewares=[prefix_middleware])
    
    app.router.add_get("/yt_dlp/{channel}", yt_dlp_handler)
    app.router.add_get("/yt_dlp/{channel}/upd", yt_dlp_upd_handler)    

    app.add_routes(routes)
    aiohttp_jinja2.setup(
        app,
        loader=jinja2.FileSystemLoader('proxy_ace/forms'),
        context_processors=[prefix_context_processor]
    )

    return app


async def main():
    loop = asyncio.get_running_loop()
    def handle_sigterm():
        loop.call_soon_threadsafe(appContext.stop_event.set)
    loop.add_signal_handler(signal.SIGTERM, handle_sigterm)
    loop.add_signal_handler(signal.SIGINT, handle_sigterm)
    await init_db()
    # await sync_channels(appContext)
    # asyncio.create_task(daily_routine(update_eternal_channels, appContext))
    app = await create_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, port=LISTEN_PORT)
    await site.start()

    try:
        await appContext.stop_event.wait()
    finally:
        pass
    await runner.cleanup()

if __name__ == "__main__":
    asyncio.run(main())