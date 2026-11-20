import asyncio

import playgate.admin_routes as admin_routes
import playgate.repo as repo


async def test_admin_index_redirects_to_channels(aiohttp_client, app):
    client = await aiohttp_client(app)

    response = await client.get("/admin", allow_redirects=False)

    assert response.status == 302
    assert response.headers["Location"] == "/admin/channels"


async def test_channels_page_lists_created_channel(aiohttp_client, app):
    await repo.add_channel("nsf", "http://source", "http://target", -1, "best")
    client = await aiohttp_client(app)

    response = await client.get("/admin/channels")

    assert response.status == 200
    body = await response.text()
    assert "nsf" in body
    assert "best" in body


async def test_add_channel_stores_format_and_redirects(aiohttp_client, app):
    client = await aiohttp_client(app)

    response = await client.post(
        "/admin/channels/add",
        data={
            "name": "nsf",
            "url": "http://source",
            "format": "best",
            "format_custom": "",
            "redirect_url": "",
        },
        allow_redirects=False,
    )

    assert response.status == 302
    assert response.headers["Location"] == "/admin/channels?msg=added"
    channels = await repo.get_channels()
    assert len(channels) == 1
    assert channels[0].format == "best"


async def test_add_channel_starts_background_resolve(aiohttp_client, app, monkeypatch):
    calls = []

    async def fake_update(name, context=None):
        calls.append(name)

    monkeypatch.setattr(admin_routes, "update_special_channel", fake_update)
    client = await aiohttp_client(app)

    response = await client.post(
        "/admin/channels/add",
        data={
            "name": "nsf",
            "url": "http://source",
            "format": "",
            "format_custom": "",
            "redirect_url": "",
        },
        allow_redirects=False,
    )
    await asyncio.sleep(0)

    assert response.status == 302
    assert calls == ["nsf"]


async def test_add_channel_uses_custom_format(aiohttp_client, app):
    client = await aiohttp_client(app)

    await client.post(
        "/admin/channels/add",
        data={
            "name": "nsf",
            "url": "http://source",
            "format": "",
            "format_custom": "best[height<=720]",
            "redirect_url": "",
        },
        allow_redirects=False,
    )

    channels = await repo.get_channels()
    assert channels[0].format == "best[height<=720]"


async def test_add_channel_rejects_duplicate_name(aiohttp_client, app):
    await repo.add_channel("nsf", "http://source", None, None, None)
    client = await aiohttp_client(app)

    response = await client.post(
        "/admin/channels/add",
        data={
            "name": "nsf",
            "url": "http://other",
            "format": "",
            "format_custom": "",
            "redirect_url": "",
        },
        allow_redirects=False,
    )

    assert response.status == 200
    assert "already exists" in await response.text()


async def test_add_channel_rejects_merge_format(aiohttp_client, app):
    client = await aiohttp_client(app)

    response = await client.post(
        "/admin/channels/add",
        data={
            "name": "nsf",
            "url": "http://source",
            "format": "",
            "format_custom": "bestvideo+bestaudio",
            "redirect_url": "",
        },
        allow_redirects=False,
    )

    assert response.status == 200
    assert "ffmpeg" in await response.text()


async def test_add_channel_rejects_url_separators_in_name(aiohttp_client, app):
    client = await aiohttp_client(app)

    response = await client.post(
        "/admin/channels/add",
        data={
            "name": "bad/name",
            "url": "http://source",
            "format": "",
            "format_custom": "",
            "redirect_url": "",
        },
        allow_redirects=False,
    )

    assert response.status == 200
    assert "must not contain" in await response.text()


async def test_edit_channel_updates_fields(aiohttp_client, app):
    await repo.add_channel("nsf", "http://source", "http://target", -1, None)
    channel = (await repo.get_channels())[0]
    client = await aiohttp_client(app)

    response = await client.post(
        f"/admin/channels/{channel.id}/edit",
        data={
            "name": "renamed",
            "url": "http://new-source",
            "format": "18",
            "format_custom": "",
            "redirect_url": "",
        },
        allow_redirects=False,
    )

    assert response.status == 302
    updated = await repo.get_channel_by_id(channel.id)
    assert updated.name == "renamed"
    assert updated.url == "http://new-source"
    assert updated.format == "18"


async def test_delete_channel_removes_row(aiohttp_client, app):
    await repo.add_channel("nsf", "http://source", None, None, None)
    channel = (await repo.get_channels())[0]
    client = await aiohttp_client(app)

    response = await client.post(
        f"/admin/channels/{channel.id}/delete",
        allow_redirects=False,
    )

    assert response.status == 302
    assert await repo.get_channels() == []


async def test_update_now_starts_background_refresh(aiohttp_client, app, monkeypatch):
    calls = []

    async def fake_update(name, context=None):
        calls.append(name)

    monkeypatch.setattr(admin_routes, "update_special_channel", fake_update)
    await repo.add_channel("nsf", "http://source", None, None, None)
    channel = (await repo.get_channels())[0]
    client = await aiohttp_client(app)

    response = await client.post(
        f"/admin/channels/{channel.id}/update",
        allow_redirects=False,
    )

    assert response.status == 302
    assert response.headers["Location"] == "/admin/channels?msg=update-started"
    assert calls == ["nsf"]


async def test_sync_starts_background_task(aiohttp_client, app, monkeypatch):
    calls = []

    async def fake_sync():
        calls.append("sync")

    monkeypatch.setattr(admin_routes, "sync_channels", fake_sync)
    client = await aiohttp_client(app)

    response = await client.post("/admin/channels/sync", allow_redirects=False)

    assert response.status == 302
    assert response.headers["Location"] == "/admin/channels?msg=sync-started"
    assert calls == ["sync"]


async def test_unknown_edit_id_returns_404(aiohttp_client, app):
    client = await aiohttp_client(app)

    response = await client.get("/admin/channels/999/edit")

    assert response.status == 404
