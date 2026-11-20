import asyncio
import sqlite3
from collections.abc import Coroutine
from typing import Any

import aiohttp_jinja2
from aiohttp import web

from .config import sync_channels, update_special_channel
from .forms import (
    ChannelForm,
    effective_format,
    format_form_data,
    normalize_channel_form,
)
from .models import Channel
from .repo import (
    add_channel,
    delete_channel,
    get_channel_by_id,
    get_channels,
    update_channel,
)
from .state import appContext

routes = web.RouteTableDef()

MESSAGES = {
    "added": "Channel added; resolving in the background.",
    "updated": "Channel updated.",
    "deleted": "Channel deleted.",
    "update-started": "Refresh started in the background.",
    "sync-started": "Sync started in the background.",
}

_background_tasks: set[asyncio.Task[None]] = set()


def _spawn(coro: Coroutine[Any, Any, None]) -> None:
    task = asyncio.create_task(coro)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


def _redirect(request: web.Request, path: str, message: str | None = None):
    location = request.get("app_prefix", "") + path
    if message:
        location += "?msg=" + message
    return web.HTTPFound(location)


def _channel_id(request: web.Request) -> int:
    try:
        return int(request.match_info["id"])
    except (KeyError, ValueError):
        raise web.HTTPNotFound() from None


def _render_form(request: web.Request, form: ChannelForm, title: str):
    return aiohttp_jinja2.render_template(
        "channel_form.html", request, {"form": form, "title": title}
    )


@routes.get("/admin", name="admin_index")
async def admin_index(request: web.Request):
    raise _redirect(request, "/admin/channels")


@routes.get("/admin/channels", name="channel_list")
@aiohttp_jinja2.template("channels.html")
async def channels_page(request: web.Request):
    return {
        "channels": await get_channels(),
        "message": MESSAGES.get(request.query.get("msg", "")),
    }


@routes.get("/admin/channels/add", name="channel_add")
@aiohttp_jinja2.template("channel_form.html")
async def add_channel_page(request: web.Request) -> dict[str, Any]:
    return {"form": ChannelForm(), "title": "Add channel"}


@routes.post("/admin/channels/add")
async def add_channel_handler(request: web.Request):
    form = ChannelForm(await request.post())
    normalize_channel_form(form)

    if form.validate():
        try:
            await add_channel(
                form.name.data,
                form.url.data,
                form.redirect_url.data or None,
                None,
                effective_format(form),
            )
        except sqlite3.IntegrityError:
            # WTForms stubs type Field.errors as tuple[()]; it is a list at runtime.
            form.name.errors = [  # ty: ignore[invalid-assignment]
                *form.name.errors,
                "A channel with this name already exists",
            ]
        else:
            _spawn(update_special_channel(form.name.data, appContext))
            raise _redirect(request, "/admin/channels", "added")

    return _render_form(request, form, "Add channel")


@routes.get("/admin/channels/{id}/edit", name="channel_edit")
@aiohttp_jinja2.template("channel_form.html")
async def edit_channel_page(request: web.Request) -> dict[str, Any]:
    channel = await get_channel_by_id(_channel_id(request))
    if channel is None:
        raise web.HTTPNotFound()

    data = {
        "name": channel.name,
        "url": channel.url,
        "redirect_url": channel.redirect_url or "",
        **format_form_data(channel.format),
    }
    return {"form": ChannelForm(data=data), "title": "Edit channel"}


@routes.post("/admin/channels/{id}/edit")
async def edit_channel_handler(request: web.Request):
    channel_id = _channel_id(request)
    existing = await get_channel_by_id(channel_id)
    if existing is None:
        raise web.HTTPNotFound()

    form = ChannelForm(await request.post())
    normalize_channel_form(form)

    if form.validate():
        updated = Channel(
            id=channel_id,
            name=form.name.data,
            url=form.url.data,
            redirect_url=form.redirect_url.data or None,
            format=effective_format(form),
        )
        try:
            await update_channel(updated)
        except sqlite3.IntegrityError:
            # WTForms stubs type Field.errors as tuple[()]; it is a list at runtime.
            form.name.errors = [  # ty: ignore[invalid-assignment]
                *form.name.errors,
                "A channel with this name already exists",
            ]
        else:
            if existing.url != updated.url or (existing.format or "") != (
                updated.format or ""
            ):
                _spawn(update_special_channel(updated.name, appContext))
            raise _redirect(request, "/admin/channels", "updated")

    return _render_form(request, form, "Edit channel")


@routes.post("/admin/channels/{id}/delete", name="channel_del")
async def delete_channel_handler(request: web.Request):
    await delete_channel(_channel_id(request))
    raise _redirect(request, "/admin/channels", "deleted")


@routes.post("/admin/channels/{id}/update", name="channel_update")
async def update_channel_handler(request: web.Request):
    channel = await get_channel_by_id(_channel_id(request))
    if channel is None:
        raise web.HTTPNotFound()

    _spawn(update_special_channel(channel.name, appContext))
    raise _redirect(request, "/admin/channels", "update-started")


@routes.post("/admin/channels/sync", name="channel_sync")
async def sync_channels_handler(request: web.Request):
    _spawn(sync_channels())
    raise _redirect(request, "/admin/channels", "sync-started")
