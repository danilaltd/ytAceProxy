import pytest

import proxy_ace.db as db
import proxy_ace.repo as repo
from proxy_ace.app import create_app


CONTENT_ID = "dd1e67078381739d14beca697356ab76d49d1a20"


@pytest.fixture
async def app(tmp_path, monkeypatch):
    db_path = tmp_path / "proxies.db"
    monkeypatch.setattr(db, "DB_PATH", str(db_path))
    monkeypatch.setattr(repo, "DB_PATH", str(db_path))
    await db.init_db()
    return await create_app()


async def test_ace_admin_creates_and_lists_channel(aiohttp_client, app):
    client = await aiohttp_client(app)

    created = await client.post(
        "/admin/ace/add",
        data={"name": "nsf", "content_id": CONTENT_ID},
        allow_redirects=False,
    )
    assert created.status == 302
    assert created.headers["Location"] == "/admin/ace"

    listing = await client.get("/admin/ace")
    assert listing.status == 200
    body = await listing.text()
    assert "nsf" in body
    assert CONTENT_ID in body


async def test_ace_admin_rejects_duplicate_name(aiohttp_client, app):
    await repo.add_ace_channel("nsf", CONTENT_ID)
    client = await aiohttp_client(app)

    response = await client.post(
        "/admin/ace/add",
        data={"name": "nsf", "content_id": CONTENT_ID},
        allow_redirects=False,
    )

    assert response.status == 200
    assert "already exists" in await response.text()


async def test_ace_admin_edits_and_deletes_channel(aiohttp_client, app):
    await repo.add_ace_channel("nsf", CONTENT_ID)
    channel = (await repo.get_ace_channels())[0]
    client = await aiohttp_client(app)

    edited = await client.post(
        f"/admin/ace/edit/{channel.id}",
        data={"name": "x_nsf", "content_id": CONTENT_ID},
        allow_redirects=False,
    )
    assert edited.status == 302

    channels = await repo.get_ace_channels()
    assert len(channels) == 1
    assert channels[0].name == "x_nsf"

    deleted = await client.get(
        f"/admin/ace/delete/{channel.id}",
        allow_redirects=False,
    )
    assert deleted.status == 302
    assert await repo.get_ace_channels() == []


async def test_ace_admin_rejects_url_separators_in_name(aiohttp_client, app):
    client = await aiohttp_client(app)

    response = await client.post(
        "/admin/ace/add",
        data={"name": "bad/name", "content_id": CONTENT_ID},
        allow_redirects=False,
    )

    assert response.status == 200
    assert 'must not contain' in await response.text()


async def test_ace_admin_rejects_bad_content_id(aiohttp_client, app):
    client = await aiohttp_client(app)

    response = await client.post(
        "/admin/ace/add",
        data={"name": "nsf", "content_id": "not-a-hash"},
        allow_redirects=False,
    )

    assert response.status == 200
    assert "40 hexadecimal characters" in await response.text()


async def test_yt_dlp_redirect_and_placeholder_still_work(aiohttp_client, app):
    await repo.add_redirect("nsf", "http://source", "http://target", -1)
    await repo.add_redirect("placeholder", "http://source", "http://fallback", -1)
    client = await aiohttp_client(app)

    known = await client.get("/yt_dlp/nsf", allow_redirects=False)
    assert known.status == 302
    assert known.headers["Location"] == "http://target"

    fallback = await client.get("/yt_dlp/missing", allow_redirects=False)
    assert fallback.status == 302
    assert fallback.headers["Location"] == "http://fallback"
