"""LLM-powered email personalization with template fallback."""

from __future__ import annotations

import asyncio
import logging

import anthropic

from config.settings import LlmSettings

log = logging.getLogger(__name__)

SYSTEM_PROMPT = (
    "Generate a unique 1-2 sentence email opener. "
    "Reference ONE specific thing about the practice. "
    "Under 30 words. No sycophancy."
)


def _fallback_opener(lead: dict) -> str:
    """Template-based fallback from tools/outreach.py strategy."""
    first_name = lead.get("first_name", "").strip()
    company = lead.get("company", "").strip()
    city = lead.get("city", "").strip()
    job_title = lead.get("job_title", "").strip()

    if company and city:
        return f"I noticed {company} serves patients in {city} -- "
    if job_title and company:
        return f"As a {job_title} at {company}, "
    if company:
        return f"I came across {company} and "
    if first_name and job_title:
        return f"Hi {first_name}, as a {job_title}, "
    if first_name:
        return f"Hi {first_name}, "
    return ""


def _build_user_prompt(lead: dict) -> str:
    """Format lead data into a concise prompt for the LLM."""
    parts = []
    for key in ("first_name", "last_name", "company", "job_title", "website", "city", "state"):
        val = lead.get(key, "")
        if val:
            parts.append(f"{key}: {val}")
    return "\n".join(parts)


async def personalize_opener(
    lead: dict,
    *,
    api_key: str,
    llm: LlmSettings | None = None,
) -> str:
    """Generate a personalized opener for one lead via Claude API.

    Falls back to template-based opener on any error.
    """
    if not api_key:
        return _fallback_opener(lead)

    settings = llm or LlmSettings()
    client = anthropic.AsyncAnthropic(api_key=api_key)

    try:
        response = await client.messages.create(
            model=settings.model,
            max_tokens=100,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": _build_user_prompt(lead)}],
        )
        text = response.content[0].text.strip()  # type: ignore[union-attr]
        # Enforce word limit
        words = text.split()
        if len(words) > settings.max_opener_words:
            text = " ".join(words[: settings.max_opener_words])
        return text
    except Exception as exc:
        log.warning("LLM personalization failed for %s: %s", lead.get("email", "?"), exc)
        return _fallback_opener(lead)


async def batch_personalize(
    leads: list[dict],
    *,
    api_key: str,
    llm: LlmSettings | None = None,
) -> list[str]:
    """Personalize openers for a batch of leads with concurrency limit.

    Returns a list of openers in the same order as the input leads.
    """
    settings = llm or LlmSettings()
    semaphore = asyncio.Semaphore(settings.max_concurrent)

    async def _one(lead: dict) -> str:
        async with semaphore:
            return await personalize_opener(lead, api_key=api_key, llm=settings)

    return await asyncio.gather(*[_one(lead) for lead in leads])
