from typing import Any, Dict

import aiohttp_jinja2
from aiohttp import web

from .config import sync_channels

from .models import RedirectChannel
from .forms import AceChannelForm, RedirectForm
from .repo import (
    add_ace_channel,
    add_redirect, 
    delete_ace_channel,
    delete_redirect,
    get_redirect_by_id,
    get_redirects, 
    get_streaming_channels, 
    get_ace_channel_by_id, 
    update_ace_channel,
    update_redirect
)

routes = web.RouteTableDef()

@routes.get("/admin/ace")
@aiohttp_jinja2.template("channels.html")
async def channels_page(request: web.Request):
    ace = await get_streaming_channels()
    return {"ace": ace}

@routes.get("/admin/ace/add", name="ace_add")
@aiohttp_jinja2.template("ace_form.html")
async def add_ace_form_page(request: web.Request) -> Dict[str, Any]:
    return {"form": AceChannelForm(), "title": "Add Channel"}

@routes.post("/admin/ace/add")
async def add_ace_handler(request: web.Request):
    data = await request.post()
    form = AceChannelForm(data)
    if form.name.data is None or form.hash.data is None:
        return aiohttp_jinja2.render_template(
            "ace_form.html", request, {"form": form, "title": "Add Channel"}
        )

    if form.validate():
        await add_ace_channel(form.name.data, form.hash.data)
        raise web.HTTPFound("/admin/ace")
    
    return aiohttp_jinja2.render_template("ace_form.html", request, {"form": form, "title": "Add Channel"})

@routes.get("/admin/ace/edit/{id}", name="ace_edit")
@aiohttp_jinja2.template("ace_form.html")
async def edit_ace_page(request: web.Request) -> Dict[str, Any]:
    ch_id = int(request.match_info["id"])
    channel = await get_ace_channel_by_id(ch_id)
    if not channel:
        raise web.HTTPNotFound()
    
    form = AceChannelForm(data={'name': channel['name'], 'hash': channel['url']})
    return {"form": form, "title": "Edit Channel"}

@routes.post("/admin/ace/edit/{id}")
async def edit_ace_handler(request: web.Request):
    ch_id = int(request.match_info["id"])
    data = await request.post()
    form = AceChannelForm(data)
    if form.name.data is None or form.hash.data is None:
        return aiohttp_jinja2.render_template(
            "ace_form.html", request, {"form": form, "title": "Edit Channel"}
        )
    if form.validate():
        await update_ace_channel(ch_id, form.name.data, form.hash.data)
        raise web.HTTPFound("/admin/ace")
        
    return aiohttp_jinja2.render_template("ace_form.html", request, {"form": form, "title": "Edit Channel"})

@routes.get("/admin/ace/delete/{id}", name="ace_del")
async def delete_ace_handler(request: web.Request):
    await delete_ace_channel(int(request.match_info["id"]))
    raise web.HTTPFound("/admin/ace")



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
        raise web.HTTPFound("/admin/redirects")
    
    return aiohttp_jinja2.render_template("redirect_form.html", request, {"form": form, "title": "Add Channel"})

@routes.get("/admin/redirects/edit/{id}", name="redirect_edit")
@aiohttp_jinja2.template("redirect_form.html")
async def edit_redirect_page(request: web.Request) -> Dict[str, Any]:
    ch_id = int(request.match_info["id"])
    redirect = await get_redirect_by_id(ch_id)
    if redirect is None:
        raise web.HTTPNotFound()
    
    form = RedirectForm(data={'name': redirect.name, 'url': redirect.url, 'redirect_url': redirect.redirect_url})
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
            redirect_url=form.redirect_url.data
        )

        await update_redirect(redirect)
        raise web.HTTPFound("/admin/redirects")
        
    return aiohttp_jinja2.render_template("redirect_form.html", request, {"form": form, "title": "Edit Channel"})

@routes.get("/admin/redirects/delete/{id}", name="redirect_del")
async def delete_redirect_handler(request: web.Request):
    await delete_redirect(int(request.match_info["id"]))
    raise web.HTTPFound("/admin/redirects")

@routes.get("/admin/redirects/sync", name="redirect_sync")
async def sync_redirects_handler(request: web.Request):
    await sync_channels()
    raise web.HTTPFound("/admin/redirects")