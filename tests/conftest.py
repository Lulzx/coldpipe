"""Shared test fixtures for the coldpipe test suite."""

from __future__ import annotations

import aiosqlite
import pytest
import pytest_asyncio

from db.migrate import migrate


@pytest_asyncio.fixture
async def db():
    """In-memory SQLite database, initialized with schema + migrations."""
    conn = await aiosqlite.connect(":memory:")
    await conn.execute("PRAGMA journal_mode = WAL")
    await conn.execute("PRAGMA foreign_keys = ON")
    await conn.execute("PRAGMA busy_timeout = 5000")
    conn.row_factory = aiosqlite.Row
    await migrate(conn)
    yield conn
    await conn.close()


@pytest.fixture
def sample_lead_data() -> list[dict]:
    """Sample CSV-like lead data dicts."""
    return [
        {
            "email": "alice@smile.com",
            "first_name": "Alice",
            "last_name": "Smith",
            "company": "Smile Dental",
            "website": "https://smile.com",
            "job_title": "Dentist",
            "location": "Austin, TX",
            "source_file": "test.csv",
        },
        {
            "email": "bob@jones-dds.com",
            "first_name": "Bob",
            "last_name": "Jones",
            "company": "Jones DDS",
            "website": "https://jonesdds.com",
            "job_title": "Orthodontist",
            "location": "Dallas, TX",
            "source_file": "test.csv",
        },
        {
            "email": "carol@brightteeth.com",
            "first_name": "Carol",
            "last_name": "Lee",
            "company": "Bright Teeth",
            "website": "https://brightteeth.com",
            "job_title": "Dentist",
            "location": "New York, NY",
            "source_file": "test.csv",
        },
    ]
