import asyncio
import sqlite3
from typing import Any, Dict

import aiohttp_jinja2
from aiohttp import web

from .config import sync_channels

from .models import AceChannel, RedirectChannel
from .forms import AceChannelForm, RedirectForm
from .repo import (
    add_ace_channel,
    add_redirect,
    delete_ace_channel,
    delete_redirect,
    get_ace_channel_by_id,
    get_ace_channels,
    get_redirect_by_id,
    get_redirects,
    update_ace_channel,
    update_redirect,
)

routes = web.RouteTableDef()


def _redirect(request: web.Request, path: str) -> web.HTTPFound:
    return web.HTTPFound(request.get("app_prefix", "") + path)


def _normalize_ace_form(form: AceChannelForm) -> None:
    form.name.data = (form.name.data or "").strip()
    form.content_id.data = (form.content_id.data or "").strip().lower()


@routes.get("/admin/redirects")
@aiohttp_jinja2.template("redirects.html")
async def redirects_page(request: web.Request):
    redirects = await get_redirects()
    return {"redirects": redirects}


@routes.get("/admin/redirects/add", name="redirect_add")
@aiohttp_jinja2.template("redirect_form.html")
async def add_redirect_form_page(request: web.Request) -> Dict[str, Any]:
    return {"form": RedirectForm(), "title": "Add Channel"}


@routes.post("/admin/redirects/add")
async def add_redirect_handler(request: web.Request):
    data = await request.post()
    form = RedirectForm(data)
    if form.name.data is None or form.url.data is None:
        return aiohttp_jinja2.render_template(
            "redirect_form.html", request, {"form": form, "title": "Add Channel"}
        )

    if form.validate():
        await add_redirect(form.name.data, form.url.data, form.redirect_url.data, None)
        asyncio.create_task(sync_channels())
        raise _redirect(request, "/admin/redirects")

    return aiohttp_jinja2.render_template(
        "redirect_form.html", request, {"form": form, "title": "Add Channel"}
    )


@routes.get("/admin/redirects/edit/{id}", name="redirect_edit")
@aiohttp_jinja2.template("redirect_form.html")
async def edit_redirect_page(request: web.Request) -> Dict[str, Any]:
    ch_id = int(request.match_info["id"])
    redirect = await get_redirect_by_id(ch_id)
    if redirect is None:
        raise web.HTTPNotFound()

    form = RedirectForm(
        data={
            "name": redirect.name,
            "url": redirect.url,
            "redirect_url": redirect.redirect_url,
        }
    )
    return {"form": form, "title": "Edit Channel"}


@routes.post("/admin/redirects/edit/{id}")
async def edit_redirect_handler(request: web.Request):
    ch_id = int(request.match_info["id"])
    data = await request.post()
    form = RedirectForm(data)
    if form.name.data is None or form.url.data is None:
        return aiohttp_jinja2.render_template(
            "redirect_form.html", request, {"form": form, "title": "Edit Channel"}
        )
    if form.validate():
        redirect = RedirectChannel(
            id=ch_id,
            name=form.name.data,
            url=form.url.data,
            redirect_url=form.redirect_url.data,
        )

        await update_redirect(redirect)
        raise _redirect(request, "/admin/redirects")

    return aiohttp_jinja2.render_template(
        "redirect_form.html", request, {"form": form, "title": "Edit Channel"}
    )


@routes.get("/admin/redirects/delete/{id}", name="redirect_del")
async def delete_redirect_handler(request: web.Request):
    await delete_redirect(int(request.match_info["id"]))
    raise _redirect(request, "/admin/redirects")


@routes.get("/admin/redirects/sync", name="redirect_sync")
async def sync_redirects_handler(request: web.Request):
    asyncio.create_task(sync_channels())
    raise _redirect(request, "/admin/redirects")


@routes.get("/admin/ace", name="ace_list")
@aiohttp_jinja2.template("ace_channels.html")
async def ace_channels_page(request: web.Request):
    channels = await get_ace_channels()
    return {"channels": channels}


@routes.get("/admin/ace/add", name="ace_add")
@aiohttp_jinja2.template("ace_form.html")
async def add_ace_form_page(request: web.Request) -> Dict[str, Any]:
    return {"form": AceChannelForm(), "title": "Add ACE channel"}


@routes.post("/admin/ace/add")
async def add_ace_handler(request: web.Request):
    form = AceChannelForm(await request.post())
    _normalize_ace_form(form)

    if form.validate():
        try:
            await add_ace_channel(form.name.data, form.content_id.data)
        except sqlite3.IntegrityError:
            form.name.errors.append("A channel with this name already exists")
        else:
            raise _redirect(request, "/admin/ace")

    return aiohttp_jinja2.render_template(
        "ace_form.html", request, {"form": form, "title": "Add ACE channel"}
    )


@routes.get("/admin/ace/edit/{id}", name="ace_edit")
@aiohttp_jinja2.template("ace_form.html")
async def edit_ace_page(request: web.Request) -> Dict[str, Any]:
    channel = await get_ace_channel_by_id(int(request.match_info["id"]))
    if channel is None:
        raise web.HTTPNotFound()

    form = AceChannelForm(data={"name": channel.name, "content_id": channel.content_id})
    return {"form": form, "title": "Edit ACE channel"}


@routes.post("/admin/ace/edit/{id}")
async def edit_ace_handler(request: web.Request):
    ch_id = int(request.match_info["id"])
    if await get_ace_channel_by_id(ch_id) is None:
        raise web.HTTPNotFound()

    form = AceChannelForm(await request.post())
    _normalize_ace_form(form)

    if form.validate():
        channel = AceChannel(
            id=ch_id,
            name=form.name.data,
            content_id=form.content_id.data,
        )
        try:
            await update_ace_channel(channel)
        except sqlite3.IntegrityError:
            form.name.errors.append("A channel with this name already exists")
        else:
            raise _redirect(request, "/admin/ace")

    return aiohttp_jinja2.render_template(
        "ace_form.html", request, {"form": form, "title": "Edit ACE channel"}
    )


@routes.get("/admin/ace/delete/{id}", name="ace_del")
async def delete_ace_handler(request: web.Request):
    await delete_ace_channel(int(request.match_info["id"]))
    raise _redirect(request, "/admin/ace")
