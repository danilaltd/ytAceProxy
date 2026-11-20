import aiosqlite

from . import db
from .models import Channel


async def get_channels() -> list[Channel]:
    async with aiosqlite.connect(db.DB_PATH) as connection:
        connection.row_factory = aiosqlite.Row
        async with connection.execute(
            "SELECT id, name, url, format, redirect_url, ttl, created_at FROM channels"
        ) as cursor:
            return [Channel(**dict(row)) async for row in cursor]


async def get_channel_by_id(id_: int) -> Channel | None:
    async with aiosqlite.connect(db.DB_PATH) as connection:
        connection.row_factory = aiosqlite.Row
        async with connection.execute(
            "SELECT id, name, url, format, redirect_url, ttl, created_at "
            "FROM channels WHERE id = ?",
            (id_,),
        ) as cursor:
            row = await cursor.fetchone()
            return Channel(**dict(row)) if row else None


async def get_redirect_url(name: str) -> str | None:
    async with (
        aiosqlite.connect(db.DB_PATH) as connection,
        connection.execute(
            "SELECT redirect_url FROM channels WHERE name = ?", (name,)
        ) as cursor,
    ):
        row = await cursor.fetchone()
        return row[0] if row else None


async def add_channel(
    name: str,
    url: str,
    redirect_url: str | None,
    ttl: int | None,
    format: str | None,
):
    async with aiosqlite.connect(db.DB_PATH) as connection:
        await connection.execute(
            "INSERT INTO channels (name, url, format, redirect_url, ttl) "
            "VALUES (?, ?, ?, ?, ?)",
            (name, url, format, redirect_url, ttl),
        )
        await connection.commit()


async def update_channel(channel: Channel):
    async with aiosqlite.connect(db.DB_PATH) as connection:
        await connection.execute(
            """UPDATE channels
               SET name = ?, url = ?, format = ?, redirect_url = ?
               WHERE id = ?""",
            (
                channel.name,
                channel.url,
                channel.format,
                channel.redirect_url,
                channel.id,
            ),
        )
        await connection.commit()


async def delete_channel(id_: int):
    async with aiosqlite.connect(db.DB_PATH) as connection:
        await connection.execute("DELETE FROM channels WHERE id = ?", (id_,))
        await connection.commit()


async def update_channels(channels: list[Channel]):
    to_update = [channel for channel in channels if channel.dirty]

    if not to_update:
        return

    async with aiosqlite.connect(db.DB_PATH) as connection:
        await connection.executemany(
            """
            UPDATE channels
            SET redirect_url = ?, ttl = ?
            WHERE id = ?
            """,
            [(channel.redirect_url, channel.ttl, channel.id) for channel in to_update],
        )
        await connection.commit()
