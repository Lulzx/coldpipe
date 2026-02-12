"""Tests for the scrapers module: CSV import, base protocol, dedup."""

from __future__ import annotations

import pytest

from db import queries
from db.models import Lead
from scrapers.base import BaseScraper
from scrapers.csv_import import CsvImporter, _parse_location

# ---------------------------------------------------------------------------
# CSV Import
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_csv_import(db, tmp_path):
    """Import a CSV file and verify leads are upserted into DB."""
    csv_content = (
        "email,First Name,Last Name,Company Name (Result),URL,Job Title,Location (Result)\n"
        'alice@smile.com,Alice,Smith,Smile Dental,https://smile.com,Dentist,"Austin, TX"\n'
        'bob@jones.com,Bob,Jones,Jones DDS,https://jones.com,Orthodontist,"Dallas, TX"\n'
    )
    csv_file = tmp_path / "test_leads.csv"
    csv_file.write_text(csv_content)

    importer = CsvImporter()
    leads = await importer.scrape(db, data_dir=str(tmp_path))

    assert len(leads) == 2
    assert await queries.count_leads(db) == 2

    lead = await queries.get_lead_by_email(db, "alice@smile.com")
    assert lead is not None
    assert lead.first_name == "Alice"
    assert lead.company == "Smile Dental"
    assert lead.city == "Austin"
    assert lead.state == "TX"


@pytest.mark.asyncio
async def test_csv_import_skips_empty_rows(db, tmp_path):
    """Rows with no email, company, or website should be skipped."""
    csv_content = (
        "email,First Name,Last Name,Company Name (Result),URL,Job Title,Location (Result)\n"
        "alice@smile.com,Alice,Smith,Smile Dental,https://smile.com,Dentist,Austin TX\n"
        ",,,,,,\n"
    )
    csv_file = tmp_path / "test_skip.csv"
    csv_file.write_text(csv_content)

    importer = CsvImporter()
    leads = await importer.scrape(db, data_dir=str(tmp_path))

    assert len(leads) == 1


@pytest.mark.asyncio
async def test_csv_import_dedup_on_email(db, tmp_path):
    """Two rows with the same email should result in one lead (upsert)."""
    csv_content = (
        "email,First Name,Last Name,Company Name (Result),URL,Job Title,Location (Result)\n"
        "alice@smile.com,Alice,Smith,Smile Dental,https://smile.com,Dentist,Austin TX\n"
        "alice@smile.com,Alicia,Smith,Smile Dental Updated,,Dentist,Austin TX\n"
    )
    csv_file = tmp_path / "test_dedup.csv"
    csv_file.write_text(csv_content)

    importer = CsvImporter()
    await importer.scrape(db, data_dir=str(tmp_path))

    assert await queries.count_leads(db) == 1
    lead = await queries.get_lead_by_email(db, "alice@smile.com")
    assert lead is not None
    # Second row has non-empty first_name "Alicia", should overwrite
    assert lead.first_name == "Alicia"
    assert lead.company == "Smile Dental Updated"


# ---------------------------------------------------------------------------
# Location parsing
# ---------------------------------------------------------------------------


def test_parse_location_city_state():
    assert _parse_location("Austin, TX") == ("Austin", "TX")


def test_parse_location_city_full_state():
    assert _parse_location("San Francisco, California") == ("San Francisco", "CA")


def test_parse_location_city_state_zip():
    assert _parse_location("Austin, TX 78701") == ("Austin", "TX")


def test_parse_location_state_only():
    assert _parse_location("TX") == ("", "TX")


def test_parse_location_full_state_only():
    assert _parse_location("California") == ("", "CA")


def test_parse_location_empty():
    assert _parse_location("") == ("", "")


def test_parse_location_city_only():
    assert _parse_location("Springfield") == ("Springfield", "")


# ---------------------------------------------------------------------------
# Base scraper protocol
# ---------------------------------------------------------------------------


def test_csv_importer_is_base_scraper():
    """CsvImporter should satisfy the BaseScraper protocol."""
    assert isinstance(CsvImporter(), BaseScraper)


def test_custom_scraper_protocol():
    """A class with async scrape(db, **kwargs) -> list[Lead] should satisfy BaseScraper."""

    class MyScraper:
        async def scrape(self, db, **kwargs) -> list[Lead]:
            return []

    assert isinstance(MyScraper(), BaseScraper)
