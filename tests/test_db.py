import sqlite3

import pytest

import playgate.db as db


async def test_init_db_creates_only_the_channels_table(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DB_PATH", str(tmp_path / "channels.db"))

    await db.init_db()

    with sqlite3.connect(db.DB_PATH) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        columns = [row[1] for row in connection.execute("PRAGMA table_info(channels)")]

    assert tables == {"channels"}
    assert columns == [
        "id",
        "name",
        "url",
        "format",
        "redirect_url",
        "ttl",
        "created_at",
    ]


async def test_check_db_raises_without_a_database(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DB_PATH", str(tmp_path / "missing" / "channels.db"))

    with pytest.raises(Exception):
        await db.check_db()
