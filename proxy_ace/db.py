import aiosqlite

DB_PATH = "/app/data/proxies.db"

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS redirect_channels (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    url TEXT NOT NULL,
    redirect_url TEXT,
    ttl INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ace_channels (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    content_id TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(CREATE_TABLES_SQL)
        await db.commit()


async def check_db() -> None:
    """Raise if the SQLite database or the active table is unreadable."""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT count(*) FROM redirect_channels")
        await cursor.fetchone()
