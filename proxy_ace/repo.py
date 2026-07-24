import aiosqlite

from .models import RedirectChannel
from .db import DB_PATH

async def get_redirects() -> list[RedirectChannel]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT id, name, url, redirect_url, ttl, created_at FROM redirect_channels"
        ) as cursor:
            return [
                RedirectChannel(**dict(row))
                async for row in cursor
            ]

async def get_redirect_by_id(id_: int) -> RedirectChannel | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM redirect_channels WHERE id = ?", 
            (id_,)
        ) as cursor:
            row = await cursor.fetchone()
            return RedirectChannel(**dict(row)) if row else None

async def get_redirect_url(name: str) -> str | None:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT redirect_url FROM redirect_channels WHERE name = (?)", 
            (name,)
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None

async def add_redirect(name: str, url: str, redirect_url: str | None, ttl: int | None):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO redirect_channels (name, url, redirect_url, ttl) VALUES (?, ?, ?, ?)",
            (name, url, redirect_url, ttl),
        )
        await db.commit()

async def update_redirect(r: RedirectChannel):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """UPDATE redirect_channels 
               SET name = ?, url = ?, redirect_url = ? 
               WHERE id = ?""",
            (r.name, r.url, r.redirect_url, r.id)
        )
        await db.commit()

async def delete_redirect(id_: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM redirect_channels WHERE id=?", (id_,))
        await db.commit()

async def update_redirects(redirects: list[RedirectChannel]):
    # db = await get_db()

    to_update = [r for r in redirects if r.dirty]

    if not to_update:
        return

    async with aiosqlite.connect(DB_PATH) as db:
        await db.executemany(
            """
            UPDATE redirect_channels
            SET redirect_url = ?, ttl = ?
            WHERE id = ?
            """,
            [
                (r.redirect_url, r.ttl, r.id)
                for r in to_update
            ]
        )

        await db.commit()