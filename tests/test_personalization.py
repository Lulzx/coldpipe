"""Tests for template-based email personalization."""

from __future__ import annotations

import pytest

from config.settings import LlmSettings
from mailer.personalize import (
    _build_user_prompt,
    _fallback_opener,
    batch_personalize,
    personalize_opener,
)

# ---------------------------------------------------------------------------
# Fallback opener (template-based)
# ---------------------------------------------------------------------------


def test_fallback_company_city():
    lead = {"first_name": "Alice", "company": "Smile Dental", "city": "Austin"}
    result = _fallback_opener(lead)
    assert "Smile Dental" in result
    assert "Austin" in result


def test_fallback_job_title_company():
    lead = {"company": "Smile Dental", "job_title": "Orthodontist"}
    result = _fallback_opener(lead)
    assert "Orthodontist" in result
    assert "Smile Dental" in result


def test_fallback_company_only():
    lead = {"company": "Smile Dental"}
    result = _fallback_opener(lead)
    assert "Smile Dental" in result


def test_fallback_name_title():
    lead = {"first_name": "Alice", "job_title": "Dentist"}
    result = _fallback_opener(lead)
    assert "Alice" in result


def test_fallback_name_only():
    lead = {"first_name": "Alice"}
    result = _fallback_opener(lead)
    assert "Alice" in result


def test_fallback_empty():
    lead = {}
    result = _fallback_opener(lead)
    assert result == ""


# ---------------------------------------------------------------------------
# Build user prompt
# ---------------------------------------------------------------------------


def test_build_user_prompt():
    lead = {
        "first_name": "Alice",
        "last_name": "Smith",
        "company": "Smile Dental",
        "city": "Austin",
        "state": "TX",
    }
    prompt = _build_user_prompt(lead)
    assert "first_name: Alice" in prompt
    assert "company: Smile Dental" in prompt
    assert "city: Austin" in prompt


def test_build_user_prompt_empty_fields():
    lead = {"first_name": "Alice", "company": ""}
    prompt = _build_user_prompt(lead)
    assert "first_name: Alice" in prompt
    assert "company" not in prompt


# ---------------------------------------------------------------------------
# personalize_opener — always returns template fallback
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_personalize_opener_returns_fallback():
    """personalize_opener always uses template fallback (LLM via MCP tool)."""
    lead = {"first_name": "Alice", "company": "Smile Dental", "city": "Austin"}
    result = await personalize_opener(lead, api_key="")
    assert "Smile Dental" in result


@pytest.mark.asyncio
async def test_personalize_opener_ignores_api_key():
    """api_key param is accepted for backward compat but not used."""
    lead = {"first_name": "Alice", "company": "Smile Dental", "city": "Austin"}
    result_no_key = await personalize_opener(lead, api_key="")
    result_with_key = await personalize_opener(lead, api_key="sk-ant-ignored")
    assert result_no_key == result_with_key


@pytest.mark.asyncio
async def test_personalize_opener_empty_lead():
    lead = {}
    result = await personalize_opener(lead, api_key="")
    assert result == ""


# ---------------------------------------------------------------------------
# batch_personalize — concurrency + template fallback
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_batch_personalize_returns_all():
    leads = [
        {"first_name": "Alice", "company": "Smile", "city": "Austin"},
        {"first_name": "Bob", "company": "Jones", "city": "Dallas"},
        {"first_name": "Carol", "company": "Bright", "city": "Houston"},
    ]
    results = await batch_personalize(leads, api_key="")
    assert len(results) == 3
    assert "Smile" in results[0]
    assert "Jones" in results[1]
    assert "Bright" in results[2]


@pytest.mark.asyncio
async def test_batch_personalize_respects_semaphore():
    """batch_personalize should still respect max_concurrent (semaphore path)."""
    leads = [{"first_name": f"Lead{i}", "company": f"Co{i}"} for i in range(6)]
    llm = LlmSettings(max_concurrent=2)
    results = await batch_personalize(leads, api_key="", llm=llm)
    assert len(results) == 6
    assert all(isinstance(r, str) for r in results)
