"""Database layer for the coldpipe project.

Usage:
    async with get_db() as db:
        leads = await queries.get_leads(db)
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import aiosqlite

from .migrate import migrate

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "coldpipe.db"


async def _setup_connection(db: aiosqlite.Connection) -> None:
    """Configure pragmas on a fresh connection."""
    await db.execute("PRAGMA journal_mode = WAL")
    await db.execute("PRAGMA foreign_keys = ON")
    await db.execute("PRAGMA busy_timeout = 5000")
    db.row_factory = aiosqlite.Row


@asynccontextmanager
async def get_db(db_path: str | Path | None = None) -> AsyncIterator[aiosqlite.Connection]:
    """Async context manager that yields an aiosqlite connection.

    On first use the schema is auto-created / migrated.
    """
    path = Path(db_path) if db_path else DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)

    db = await aiosqlite.connect(str(path))
    try:
        await _setup_connection(db)
        await migrate(db)
        yield db
    finally:
        await db.close()


class DBPool:
    """Lightweight connection pool for aiosqlite.

    Keeps a small pool of connections to avoid open/close overhead in
    long-running processes (TUI, scheduler, etc.).

    Usage:
        pool = DBPool()
        await pool.open()
        async with pool.acquire() as db:
            ...
        await pool.close()
    """

    def __init__(self, db_path: str | Path | None = None, *, size: int = 3):
        self._path = Path(db_path) if db_path else DB_PATH
        self._size = size
        self._pool: asyncio.Queue[aiosqlite.Connection] = asyncio.Queue(maxsize=size)
        self._connections: list[aiosqlite.Connection] = []
        self._opened = False

    async def open(self) -> None:
        if self._opened:
            return
        self._path.parent.mkdir(parents=True, exist_ok=True)
        for _ in range(self._size):
            conn = await aiosqlite.connect(str(self._path))
            await _setup_connection(conn)
            self._connections.append(conn)
            await self._pool.put(conn)
        # Run migrations on the first connection
        first = await self._pool.get()
        await migrate(first)
        await self._pool.put(first)
        self._opened = True

    async def close(self) -> None:
        for conn in self._connections:
            await conn.close()
        self._connections.clear()
        self._opened = False

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[aiosqlite.Connection]:
        if not self._opened:
            await self.open()
        conn = await self._pool.get()
        try:
            yield conn
        finally:
            await self._pool.put(conn)

    async def __aenter__(self) -> DBPool:
        await self.open()
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()
