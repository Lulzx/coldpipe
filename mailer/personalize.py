"""Email personalization with template fallback.

LLM-powered personalization is available via the coldpipe MCP server
(`personalize_opener` tool) when running under Claude Code. The CLI/daemon
path uses template-based openers — reliable, zero API cost.
"""

from __future__ import annotations

import asyncio
import logging

from config.settings import LlmSettings

log = logging.getLogger(__name__)


def _fallback_opener(lead: dict) -> str:
    """Template-based opener derived from lead fields."""
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
    """Format lead data into a concise prompt string."""
    parts = []
    for key in ("first_name", "last_name", "company", "job_title", "website", "city", "state"):
        val = lead.get(key, "")
        if val:
            parts.append(f"{key}: {val}")
    return "\n".join(parts)


async def personalize_opener(
    lead: dict,
    *,
    api_key: str = "",
    llm: LlmSettings | None = None,
) -> str:
    """Return a personalized opener for one lead.

    Uses the template fallback. For LLM-powered openers, call the
    `personalize_opener` MCP tool via Claude Code instead.
    """
    return _fallback_opener(lead)


async def batch_personalize(
    leads: list[dict],
    *,
    api_key: str = "",
    llm: LlmSettings | None = None,
) -> list[str]:
    """Personalize openers for a batch of leads with concurrency limit."""
    settings = llm or LlmSettings()
    semaphore = asyncio.Semaphore(settings.max_concurrent)

    async def _one(lead: dict) -> str:
        async with semaphore:
            return await personalize_opener(lead, api_key=api_key, llm=settings)

    return await asyncio.gather(*[_one(lead) for lead in leads])
