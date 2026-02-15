"""Shared test fixtures for the coldpipe test suite."""

from __future__ import annotations

from pathlib import Path

import pytest
import pytest_asyncio

from db import close_db, init_db


@pytest_asyncio.fixture
async def db(tmp_path: Path):
    """Temp-file SQLite database, initialized with Piccolo tables + indexes."""
    db_file = tmp_path / "test.db"
    await init_db(db_file)
    yield None  # queries use Piccolo engine directly, db param is ignored
    await close_db()


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
