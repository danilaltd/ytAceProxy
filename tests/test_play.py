import playgate.handler as handler
import playgate.repo as repo

HASH = "a" * 40


def _configure_ace(monkeypatch):
    monkeypatch.delenv("ACEXY_BASE_URL", raising=False)
    monkeypatch.setenv("TAILSCALE_IP", "100.77.6.55")
    monkeypatch.setenv("ACEXY_PORT", "8091")


async def test_ace_channel_redirects_to_acexy(aiohttp_client, app, monkeypatch):
    _configure_ace(monkeypatch)
    await repo.add_channel("ace1", f"acestream://{HASH}", None, None, None)
    client = await aiohttp_client(app)

    response = await client.get("/play/ace1", allow_redirects=False)

    assert response.status == 302
    assert response.headers["Location"] == (
        f"http://100.77.6.55:8091/ace/getstream?id={HASH}"
    )


async def test_ace_channel_without_configuration_returns_503(
    aiohttp_client, app, monkeypatch
):
    monkeypatch.delenv("ACEXY_BASE_URL", raising=False)
    monkeypatch.delenv("TAILSCALE_IP", raising=False)
    monkeypatch.delenv("ACEXY_PORT", raising=False)
    await repo.add_channel("ace1", f"acestream://{HASH}", None, None, None)
    client = await aiohttp_client(app)

    response = await client.get("/play/ace1", allow_redirects=False)

    assert response.status == 503


async def test_ace_channel_does_not_fall_back_to_placeholder(
    aiohttp_client, app, monkeypatch
):
    monkeypatch.delenv("ACEXY_BASE_URL", raising=False)
    monkeypatch.delenv("TAILSCALE_IP", raising=False)
    monkeypatch.delenv("ACEXY_PORT", raising=False)
    await repo.add_channel("ace1", f"acestream://{HASH}", None, None, None)
    await repo.add_channel("placeholder", "http://source", "http://fallback", -1, None)
    client = await aiohttp_client(app)

    response = await client.get("/play/ace1", allow_redirects=False)

    assert response.status == 503


async def test_unsupported_source_returns_404(aiohttp_client, app):
    await repo.add_channel("weird", "ftp://example.com/x", None, None, None)
    client = await aiohttp_client(app)

    response = await client.get("/play/weird", allow_redirects=False)

    assert response.status == 404


async def test_update_endpoint_skips_ace(aiohttp_client, app, monkeypatch):
    calls = []

    async def fake_update(name, context):
        calls.append(name)

    monkeypatch.setattr(handler, "update_special_channel", fake_update)
    await repo.add_channel("ace1", f"acestream://{HASH}", None, None, None)
    client = await aiohttp_client(app)

    response = await client.get("/play/ace1/upd")

    assert response.status == 200
    assert "nothing to resolve" in await response.text()
    assert calls == []


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
