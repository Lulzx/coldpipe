"""Version-based migration system for the coldpipe database."""

from __future__ import annotations

from pathlib import Path

import aiosqlite

SCHEMA_PATH = Path(__file__).parent / "schema.sql"


async def get_version(db: aiosqlite.Connection) -> int:
    """Get the current schema version. Returns 0 if table doesn't exist."""
    try:
        cursor = await db.execute("SELECT MAX(version) FROM schema_version")
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
    2: """
        ALTER TABLE leads ADD COLUMN email_confidence REAL NOT NULL DEFAULT 0.0;
        ALTER TABLE leads ADD COLUMN email_source TEXT NOT NULL DEFAULT '';
        ALTER TABLE leads ADD COLUMN email_provider TEXT NOT NULL DEFAULT '';
    """,
    3: """
        CREATE TABLE IF NOT EXISTS users (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            username              TEXT NOT NULL UNIQUE,
            webauthn_credential_id TEXT NOT NULL DEFAULT '',
            webauthn_public_key   TEXT NOT NULL DEFAULT '',
            webauthn_sign_count   INTEGER NOT NULL DEFAULT 0,
            onboarding_completed  INTEGER NOT NULL DEFAULT 0,
            created_at            TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
        );
        CREATE TABLE IF NOT EXISTS sessions (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            token       TEXT NOT NULL UNIQUE,
            user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            expires_at  TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_sessions_token ON sessions(token);
    """,
}


async def migrate(db: aiosqlite.Connection) -> int:
    """Run all pending migrations. Returns final version."""
    current = await get_version(db)

    if current == 0:
        current = await init_schema(db)

    target_versions = sorted(v for v in MIGRATIONS if v > current)
    for version in target_versions:
        await db.executescript(MIGRATIONS[version])
        await db.execute("INSERT INTO schema_version (version) VALUES (?)", (version,))
        await db.commit()
        current = version

    return current
