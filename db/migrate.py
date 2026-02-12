"""Version-based migration system for the dentists database."""

from __future__ import annotations

from pathlib import Path

import aiosqlite

SCHEMA_PATH = Path(__file__).parent / "schema.sql"


async def get_version(db: aiosqlite.Connection) -> int:
    """Get the current schema version. Returns 0 if table doesn't exist."""
    try:
        cursor = await db.execute(
            "SELECT MAX(version) FROM schema_version"
        )
        row = await cursor.fetchone()
        return row[0] if row and row[0] is not None else 0
    except aiosqlite.OperationalError:
        return 0


async def init_schema(db: aiosqlite.Connection) -> int:
    """Apply the base schema. Returns the version applied."""
    sql = SCHEMA_PATH.read_text()
    await db.executescript(sql)
    return await get_version(db)


# Migrations are keyed by target version number.
# Each migration receives the db connection and upgrades from version N-1 to N.
MIGRATIONS: dict[int, str] = {
    # Example: 2: "ALTER TABLE leads ADD COLUMN linkedin TEXT NOT NULL DEFAULT '';",
}


async def migrate(db: aiosqlite.Connection) -> int:
    """Run all pending migrations. Returns final version."""
    current = await get_version(db)

    if current == 0:
        current = await init_schema(db)

    target_versions = sorted(v for v in MIGRATIONS if v > current)
    for version in target_versions:
        await db.executescript(MIGRATIONS[version])
        await db.execute(
            "INSERT INTO schema_version (version) VALUES (?)", (version,)
        )
        await db.commit()
        current = version

    return current
