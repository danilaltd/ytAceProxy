import playgate.handler as handler
import playgate.repo as repo


async def test_play_redirects_to_resolved_url(aiohttp_client, app):
    await repo.add_channel("nsf", "http://source", "http://target", -1, None)
    client = await aiohttp_client(app)

    response = await client.get("/play/nsf", allow_redirects=False)

    assert response.status == 302
    assert response.headers["Location"] == "http://target"


async def test_unknown_channel_returns_404(aiohttp_client, app):
    client = await aiohttp_client(app)

    response = await client.get("/play/missing", allow_redirects=False)

    assert response.status == 404


async def test_unknown_channel_falls_back_to_placeholder(aiohttp_client, app):
    await repo.add_channel("placeholder", "http://source", "http://fallback", -1, None)
    client = await aiohttp_client(app)

    response = await client.get("/play/missing", allow_redirects=False)

    assert response.status == 302
    assert response.headers["Location"] == "http://fallback"


async def test_expired_ttl_still_returns_the_stored_redirect(aiohttp_client, app):
    await repo.add_channel("nsf", "http://source", "http://old", 1, None)
    client = await aiohttp_client(app)

    response = await client.get("/play/nsf", allow_redirects=False)

    assert response.status == 302
    assert response.headers["Location"] == "http://old"


async def test_update_endpoint_calls_resolver(aiohttp_client, app, monkeypatch):
    calls = []

    async def fake_update(name, context):
        calls.append(name)

    monkeypatch.setattr(handler, "update_special_channel", fake_update)
    await repo.add_channel("nsf", "http://source", "http://target", -1, None)
    client = await aiohttp_client(app)

    response = await client.get("/play/nsf/upd")

    assert response.status == 200
    assert calls == ["nsf"]
