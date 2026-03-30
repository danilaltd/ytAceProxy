import aiosqlite

DB_PATH = "channels.db"

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS ace_streaming_channels (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    url TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS redirect_channels (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    url TEXT NOT NULL,
    redirect_url TEXT,
    ttl INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(CREATE_TABLES_SQL)
        await db.commit()