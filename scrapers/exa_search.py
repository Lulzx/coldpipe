"""Exa.ai API scraper for finding business websites."""

from __future__ import annotations

import os
from urllib.parse import urlparse

import aiosqlite
from exa_py import Exa

from db.models import Lead
from db.queries import upsert_lead


class ExaScraper:
    """Search for businesses using the Exa.ai API."""

    async def scrape(
        self,
        db: aiosqlite.Connection,
        *,
        query: str = "local business",
        city: str = "",
        max_results: int = 20,
    ) -> list[Lead]:
        api_key = os.getenv("EXA_API_KEY", "")
        if not api_key:
            return []

        exa = Exa(api_key=api_key)
        search_query = f"{query} {city}".strip() if city else query

        result = exa.search_and_contents(
            search_query,
            type="neural",
            use_autoprompt=True,
            num_results=max_results,
            text={"max_characters": 2000},
            highlights={"num_sentences": 3},
        )

        leads: list[Lead] = []

        for item in result.results:
            parsed = urlparse(item.url)
            domain = parsed.hostname or ""

            # Skip aggregator sites — we want individual practice sites
            if any(
                skip in domain
                for skip in (
                    "yelp.com",
                    "healthgrades.com",
                    "zocdoc.com",
                    "facebook.com",
                    "instagram.com",
                )
            ):
                continue

            lead = Lead(
                company=item.title or "",
                website=item.url,
                source="exa",
                source_url=item.url,
                city=city,
                notes=(item.highlights[0] if item.highlights else "")[:500],
            )
            if lead.company and lead.website:
                await upsert_lead(db, lead)
                leads.append(lead)

        return leads
