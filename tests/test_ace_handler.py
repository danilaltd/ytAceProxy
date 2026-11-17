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


async def test_ace_redirects_to_acexy(aiohttp_client, app, monkeypatch):
    monkeypatch.setenv("ACEXY_BASE_URL", "http://acexy:8090")
    await repo.add_ace_channel("nsf", CONTENT_ID)

    client = await aiohttp_client(app)
    response = await client.get("/ace/nsf", allow_redirects=False)

    assert response.status == 302
    assert response.headers["Location"] == (
        f"http://acexy:8090/ace/getstream?id={CONTENT_ID}"
    )


async def test_unknown_ace_channel_returns_404(aiohttp_client, app, monkeypatch):
    monkeypatch.setenv("ACEXY_BASE_URL", "http://acexy:8090")

    client = await aiohttp_client(app)
    response = await client.get("/ace/missing", allow_redirects=False)

    assert response.status == 404


async def test_missing_acexy_base_url_returns_503(aiohttp_client, app, monkeypatch):
    monkeypatch.delenv("ACEXY_BASE_URL", raising=False)
    await repo.add_ace_channel("nsf", CONTENT_ID)

    client = await aiohttp_client(app)
    response = await client.get("/ace/nsf", allow_redirects=False)

    assert response.status == 503


async def test_fresh_database_has_only_active_tables(tmp_path, monkeypatch):
    db_path = tmp_path / "proxies.db"
    monkeypatch.setattr(db, "DB_PATH", str(db_path))
    await db.init_db()

    import sqlite3

    with sqlite3.connect(db_path) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }

    assert tables == {"redirect_channels", "ace_channels"}
