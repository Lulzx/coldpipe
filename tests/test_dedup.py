"""Tests for the fuzzy deduplication logic."""

from __future__ import annotations

import pytest

from db import queries
from db.tables import Lead

# Import directly to avoid pulling in exa_py dependency
from scrapers.dedup import deduplicate_leads


async def _insert_null_email_lead(db, first_name: str, last_name: str, company: str) -> int:
    """Insert a lead with NULL email directly (bypasses upsert_lead's email lookup)."""
    result = await Lead.insert(
        Lead(email=None, first_name=first_name, last_name=last_name, company=company)
    ).run()
    return result[0]["id"]


@pytest.mark.asyncio
async def test_dedup_exact_email(db):
    """Leads with the same email are handled by DB UNIQUE constraint, not dedup."""
    await queries.upsert_lead(db, Lead(email="same@test.com", company="A Corp"))
    await queries.upsert_lead(db, Lead(email="same@test.com", company="B Corp"))

    # Only 1 lead due to upsert
    assert await queries.count_leads(db) == 1

    removed = await deduplicate_leads(db)
    assert removed == 0


@pytest.mark.asyncio
async def test_dedup_fuzzy_company_name(db):
    """Leads without email that share similar company names should be deduped."""
    await _insert_null_email_lead(db, "A", "Smith", "Smith Family Dental")
    await _insert_null_email_lead(db, "B", "Jones", "Smith Family Dental LLC")

    assert await queries.count_leads(db) == 2

    removed = await deduplicate_leads(db)
    assert removed == 1
    assert await queries.count_leads(db) == 1


@pytest.mark.asyncio
async def test_dedup_different_companies(db):
    """Leads with very different company names should not be deduped."""
    await _insert_null_email_lead(db, "A", "A", "Smith Family Dental")
    await _insert_null_email_lead(db, "B", "B", "Totally Different Practice")

    removed = await deduplicate_leads(db)
    assert removed == 0
    assert await queries.count_leads(db) == 2


@pytest.mark.asyncio
async def test_dedup_prefers_lead_with_email(db):
    """When deduping, keep the lead that has an email."""
    # Lead with email
    await queries.upsert_lead(db, Lead(email="doc@smith.com", company="Jones Family Dental"))
    # Lead without email but similar company
    await _insert_null_email_lead(db, "X", "Y", "Jones Family Dental Care")

    assert await queries.count_leads(db) == 2

    removed = await deduplicate_leads(db)
    assert removed == 1

    # The one with email should survive
    remaining = await queries.get_leads(db)
    assert len(remaining) == 1
    assert remaining[0].email == "doc@smith.com"


@pytest.mark.asyncio
async def test_dedup_empty_db(db):
    """Dedup on empty DB should return 0."""
    removed = await deduplicate_leads(db)
    assert removed == 0


@pytest.mark.asyncio
async def test_dedup_no_company_match(db):
    """Leads without company names should not be matched."""
    await _insert_null_email_lead(db, "A", "A", "")
    await _insert_null_email_lead(db, "B", "B", "")

    removed = await deduplicate_leads(db)
    assert removed == 0
