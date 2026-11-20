import playgate.db as db


async def test_health_ok(aiohttp_client, app):
    client = await aiohttp_client(app)

    response = await client.get("/health")

    assert response.status == 200
    assert await response.text() == "ok"


async def test_health_unhealthy_without_database(
    aiohttp_client, app, monkeypatch, tmp_path
):
    monkeypatch.setattr(db, "DB_PATH", str(tmp_path / "missing" / "channels.db"))
    client = await aiohttp_client(app)

    response = await client.get("/health")

    assert response.status == 503
    assert await response.text() == "unhealthy"
