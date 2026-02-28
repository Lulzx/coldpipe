"""Legacy migration support for pre-v3 databases.

This module handles upgrading old databases that were created before the
Piccolo ORM migration. New databases are created directly by Piccolo's
create_table() in db/__init__.py.
"""

from __future__ import annotations

import contextlib
from pathlib import Path

from piccolo.querystring import QueryString

SCHEMA_PATH = Path(__file__).parent / "schema.sql"

# Legacy migrations for databases created before Piccolo adoption
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
    4: """
        CREATE TABLE IF NOT EXISTS mcp_activity (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            tool_name  TEXT NOT NULL,
            params     TEXT NOT NULL DEFAULT '',
            result_summary TEXT NOT NULL DEFAULT '',
            status     TEXT NOT NULL DEFAULT 'running',
            error      TEXT,
            duration_ms INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
        );
        CREATE INDEX IF NOT EXISTS idx_mcp_created ON mcp_activity(created_at);
    """,
    5: """
        CREATE TABLE IF NOT EXISTS mcp_notes (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            key        TEXT NOT NULL UNIQUE,
            value      TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
        );
        CREATE INDEX IF NOT EXISTS idx_notes_key ON mcp_notes(key);
    """,
}


async def migrate_legacy(engine) -> int:
    """Run legacy migrations on an existing database via Piccolo engine.

    Only needed for databases created before the Piccolo migration.
    Returns final version.
    """
    # Check if schema_version table has data
    try:
        rows = await engine.run_querystring(QueryString("SELECT MAX(version) FROM schema_version"))
        current = rows[0]["max(version)"] if rows and rows[0].get("max(version)") is not None else 0
    except Exception:
        current = 0

    if current == 0:
        # No version data — this is either a new DB (Piccolo-created) or pre-migration.
        # Check if leads table exists to distinguish
        try:
            rows = await engine.run_querystring(
                QueryString("SELECT name FROM sqlite_master WHERE type='table' AND name='leads'")
            )
            if not rows:
                # New DB — Piccolo handles everything
                return 0
        except Exception:
            return 0

        # Insert initial version
        await engine.run_querystring(
            QueryString("INSERT OR IGNORE INTO schema_version (version) VALUES ({})", 1)
        )
        current = 1

    target_versions = sorted(v for v in MIGRATIONS if v > current)
    for version in target_versions:
        try:
            for statement in MIGRATIONS[version].strip().split(";"):
                statement = statement.strip()
                if statement:
                    await engine.run_ddl(statement)
            await engine.run_querystring(
                QueryString(
                    "INSERT INTO schema_version (version) VALUES ({})",
                    version,
                )
            )
            current = version
        except Exception:
            # Column/table may already exist from Piccolo create_table
            with contextlib.suppress(Exception):
                await engine.run_querystring(
                    QueryString(
                        "INSERT OR IGNORE INTO schema_version (version) VALUES ({})",
                        version,
                    )
                )

    return current
