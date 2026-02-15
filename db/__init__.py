"""Database layer for the coldpipe project.

Piccolo ORM-based. Backward-compatible DBPool shim for transition period.

Usage:
    # New style (preferred):
    await init_db()
    leads = await queries.get_leads()
    await close_db()

    # Legacy style (still works during transition):
    async with get_db() as db:
        leads = await queries.get_leads(db)
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from piccolo.engine.sqlite import SQLiteEngine

from .tables import _POST_CREATE_SQL

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "coldpipe.db"

# Global engine reference
_engine: SQLiteEngine | None = None


async def init_db(db_path: str | Path | None = None) -> SQLiteEngine:
    """Initialize Piccolo engine with pragmas and create tables."""
    global _engine
    path = Path(db_path) if db_path else DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)

    _engine = SQLiteEngine(path=str(path))
    await _engine.start_connection_pool()

    # Set pragmas
    await _engine.run_ddl("PRAGMA journal_mode = WAL")
    await _engine.run_ddl("PRAGMA foreign_keys = ON")
    await _engine.run_ddl("PRAGMA busy_timeout = 5000")

    # Create tables if not exist
    from .tables import (
        Campaign,
        CampaignLead,
        DailySendLog,
        Deal,
        EmailSent,
        Lead,
        Mailbox,
        SchemaVersion,
        SequenceStep,
        Session,
        TrackingEvent,
        User,
    )

    tables = [
        Lead,
        Mailbox,
        Campaign,
        SequenceStep,
        CampaignLead,
        EmailSent,
        Deal,
        TrackingEvent,
        DailySendLog,
        SchemaVersion,
        User,
        Session,
    ]

    # Bind engine to all table classes so queries use the right database
    for table_cls in tables:
        table_cls._meta._db = _engine

    for table_cls in tables:
        await table_cls.create_table(if_not_exists=True).run()

    # Apply post-creation SQL (indexes, triggers)
    for sql in _POST_CREATE_SQL:
        await _engine.run_ddl(sql)

    # Ensure schema version is set
    try:
        from .migrate import migrate_legacy

        await migrate_legacy(_engine)
    except Exception:
        pass

    # Insert current schema version if table is empty
    existing = await SchemaVersion.select().run()
    if not existing:
        await SchemaVersion.insert(SchemaVersion(version=3)).run()

    return _engine


async def close_db() -> None:
    """Close the Piccolo engine connection pool."""
    global _engine
    if _engine is not None:
        await _engine.close_connection_pool()
        _engine = None


def get_engine() -> SQLiteEngine:
    """Get the active engine (call init_db first)."""
    if _engine is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    return _engine


@asynccontextmanager
async def get_db(db_path: str | Path | None = None) -> AsyncIterator[None]:
    """Backward-compatible context manager.

    Initializes the Piccolo engine on entry, yields None (queries use engine
    directly), closes on exit.
    """
    await init_db(db_path)
    try:
        yield None
    finally:
        await close_db()


class DBPool:
    """Backward-compatible pool shim wrapping Piccolo engine lifecycle.

    The acquire() context manager yields None — query functions accept
    an optional `db` parameter but ignore it (all queries go through
    the Piccolo engine).
    """

    def __init__(self, db_path: str | Path | None = None, *, size: int = 3):
        self._path = Path(db_path) if db_path else DB_PATH
        self._opened = False

    async def open(self) -> None:
        if self._opened:
            return
        await init_db(self._path)
        self._opened = True

    async def close(self) -> None:
        if self._opened:
            await close_db()
            self._opened = False

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[None]:
        if not self._opened:
            await self.open()
        yield None

    async def __aenter__(self) -> DBPool:
        await self.open()
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()
