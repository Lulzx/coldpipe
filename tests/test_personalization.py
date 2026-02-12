"""Tests for LLM personalization with Claude API mocking."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from config.settings import LlmSettings
from email_engine.personalize import (
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
# Personalize opener (mock Claude API)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_personalize_opener_no_api_key():
    """Without API key, should fall back to template."""
    lead = {"first_name": "Alice", "company": "Smile Dental", "city": "Austin"}
    result = await personalize_opener(lead, api_key="")
    assert "Smile Dental" in result


@pytest.mark.asyncio
async def test_personalize_opener_with_mock_api():
    """With API key, should call Claude and return the response."""
    mock_response = MagicMock()
    mock_response.content = [
        MagicMock(text="I noticed your Austin practice uses cutting-edge tech.")
    ]

    mock_client = MagicMock()
    mock_client.messages.create = AsyncMock(return_value=mock_response)

    with patch("email_engine.personalize.anthropic.AsyncAnthropic", return_value=mock_client):
        lead = {"first_name": "Alice", "company": "Smile Dental", "city": "Austin"}
        result = await personalize_opener(lead, api_key="test-key")

    assert "Austin" in result
    mock_client.messages.create.assert_called_once()


@pytest.mark.asyncio
async def test_personalize_opener_api_failure_fallback():
    """On API failure, should fall back to template."""
    mock_client = MagicMock()
    mock_client.messages.create = AsyncMock(side_effect=RuntimeError("API down"))

    with patch("email_engine.personalize.anthropic.AsyncAnthropic", return_value=mock_client):
        lead = {"first_name": "Alice", "company": "Smile Dental", "city": "Austin"}
        result = await personalize_opener(lead, api_key="test-key")

    # Should get the template fallback
    assert "Smile Dental" in result


@pytest.mark.asyncio
async def test_personalize_opener_word_limit():
    """Response exceeding max_opener_words should be truncated."""
    long_text = " ".join(["word"] * 50)
    mock_response = MagicMock()
    mock_response.content = [MagicMock(text=long_text)]

    mock_client = MagicMock()
    mock_client.messages.create = AsyncMock(return_value=mock_response)

    llm = LlmSettings(max_opener_words=10)

    with patch("email_engine.personalize.anthropic.AsyncAnthropic", return_value=mock_client):
        lead = {"first_name": "Alice"}
        result = await personalize_opener(lead, api_key="test-key", llm=llm)

    assert len(result.split()) <= 10


# ---------------------------------------------------------------------------
# Batch personalize (mock + semaphore)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_batch_personalize_with_mock():
    """batch_personalize should process all leads with concurrency limit."""
    mock_response = MagicMock()
    mock_response.content = [MagicMock(text="Great practice!")]

    mock_client = MagicMock()
    mock_client.messages.create = AsyncMock(return_value=mock_response)

    with patch("email_engine.personalize.anthropic.AsyncAnthropic", return_value=mock_client):
        leads = [
            {"first_name": "Alice", "company": "Smile"},
            {"first_name": "Bob", "company": "Jones"},
            {"first_name": "Carol", "company": "Bright"},
        ]
        results = await batch_personalize(leads, api_key="test-key")

    assert len(results) == 3
    assert all(r == "Great practice!" for r in results)
    assert mock_client.messages.create.call_count == 3


@pytest.mark.asyncio
async def test_batch_personalize_no_api_key():
    """Without API key, all leads should get template fallback."""
    leads = [
        {"first_name": "Alice", "company": "Smile", "city": "Austin"},
        {"first_name": "Bob", "company": "Jones", "city": "Dallas"},
    ]
    results = await batch_personalize(leads, api_key="")

    assert len(results) == 2
    assert "Smile" in results[0]
    assert "Jones" in results[1]


@pytest.mark.asyncio
async def test_batch_personalize_semaphore_limit():
    """Semaphore should limit concurrency to max_concurrent."""
    call_count = 0
    max_concurrent_seen = 0
    current_concurrent = 0

    async def mock_create(**kwargs):
        nonlocal call_count, max_concurrent_seen, current_concurrent
        current_concurrent += 1
        max_concurrent_seen = max(max_concurrent_seen, current_concurrent)
        call_count += 1
        # Small yield to allow other tasks to run
        import asyncio

        await asyncio.sleep(0.01)
        current_concurrent -= 1
        resp = MagicMock()
        resp.content = [MagicMock(text="Opener")]
        return resp

    mock_client = MagicMock()
    mock_client.messages.create = mock_create

    llm = LlmSettings(max_concurrent=2)

    with patch("email_engine.personalize.anthropic.AsyncAnthropic", return_value=mock_client):
        leads = [{"first_name": f"Lead{i}"} for i in range(6)]
        results = await batch_personalize(leads, api_key="test-key", llm=llm)

    assert len(results) == 6
    assert call_count == 6
    assert max_concurrent_seen <= 2
