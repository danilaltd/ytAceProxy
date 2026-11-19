import pytest

import proxy_ace.db as db
from proxy_ace.app import create_app


@pytest.fixture
async def app(tmp_path, monkeypatch):
    db_path = tmp_path / "proxies.db"
    monkeypatch.setattr(db, "DB_PATH", str(db_path))
    await db.init_db()
    return await create_app()


async def test_health_ok(aiohttp_client, app):
    client = await aiohttp_client(app)

    response = await client.get("/health")

    assert response.status == 200
    assert await response.text() == "ok"


async def test_health_unhealthy_without_database(
    aiohttp_client, app, monkeypatch, tmp_path
):
    monkeypatch.setattr(db, "DB_PATH", str(tmp_path / "missing" / "proxies.db"))
    client = await aiohttp_client(app)

    response = await client.get("/health")

    assert response.status == 503
    assert await response.text() == "unhealthy"
