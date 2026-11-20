import pytest

import playgate.db as db
from playgate.app import create_app


@pytest.fixture(autouse=True)
async def no_background_resolution(monkeypatch):
    async def fake_update(name, context=None):
        return None

    async def fake_sync():
        return None

    monkeypatch.setattr("playgate.admin_routes.update_special_channel", fake_update)
    monkeypatch.setattr("playgate.admin_routes.sync_channels", fake_sync)


@pytest.fixture
async def app(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DB_PATH", str(tmp_path / "channels.db"))
    await db.init_db()
    return await create_app()
