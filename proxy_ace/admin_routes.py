import asyncio
from typing import Any, Dict

import aiohttp_jinja2
from aiohttp import web

from .config import sync_channels

from .models import RedirectChannel
from .forms import RedirectForm
from .repo import (
    add_redirect, 
    delete_redirect,
    get_redirect_by_id,
    get_redirects, 
    update_redirect
)

routes = web.RouteTableDef()

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
    asyncio.create_task(sync_channels())
    raise web.HTTPFound("/admin/redirects")