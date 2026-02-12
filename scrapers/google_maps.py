"""Google Maps scraper using Crawl4AI with LLM extraction."""

from __future__ import annotations

import json
import os

import aiosqlite
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from crawl4ai.extraction_strategy import LLMExtractionStrategy

from db.models import Lead
from db.queries import upsert_lead

_SYSTEM_PROMPT = """Extract dentist/dental practice information from Google Maps search results.
For each result, extract:
- name: practice or dentist name
- address: full street address
- city: city name
- state: state abbreviation
- zip: zip/postal code
- phone: phone number
- website: website URL
- rating: star rating (as string)

Return a JSON array of objects with these keys. Only include results that are dental practices."""

_SCHEMA = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "address": {"type": "string"},
            "city": {"type": "string"},
            "state": {"type": "string"},
            "zip": {"type": "string"},
            "phone": {"type": "string"},
            "website": {"type": "string"},
            "rating": {"type": "string"},
        },
    },
}


class GoogleMapsScraper:
    """Scrape dentist listings from Google Maps using Crawl4AI + LLM extraction."""

    async def scrape(
        self,
        db: aiosqlite.Connection,
        *,
        city: str = "New York",
        max_results: int = 20,
    ) -> list[Lead]:
        query = f"dentists in {city}"
        url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"

        extraction = LLMExtractionStrategy(
            provider=os.getenv("LLM_PROVIDER", "anthropic/claude-haiku-4-5-20251001"),
            api_token=os.getenv("ANTHROPIC_API_KEY", ""),
            schema=_SCHEMA,
            instruction=_SYSTEM_PROMPT,
        )

        browser_cfg = BrowserConfig(headless=True)
        run_cfg = CrawlerRunConfig(
            extraction_strategy=extraction,
            js_code=[
                # Scroll to load more results
                """
                (async () => {
                    const feed = document.querySelector('div[role="feed"]');
                    if (!feed) return;
                    for (let i = 0; i < 5; i++) {
                        feed.scrollTop = feed.scrollHeight;
                        await new Promise(r => setTimeout(r, 2000));
                    }
                })();
                """
            ],
            wait_for="css:div[role='feed'] a[href*='/maps/place/']",
            page_timeout=30000,
        )

        leads: list[Lead] = []

        async with AsyncWebCrawler(config=browser_cfg) as crawler:
            result = await crawler.arun(url=url, config=run_cfg)

            if not result.extracted_content:
                return leads

            try:
                items = json.loads(result.extracted_content)
            except json.JSONDecodeError, TypeError:
                return leads

            for item in items[:max_results]:
                if not isinstance(item, dict):
                    continue
                lead = Lead(
                    company=item.get("name", ""),
                    address=item.get("address", ""),
                    city=item.get("city", "") or city,
                    state=item.get("state", ""),
                    zip=item.get("zip", ""),
                    phone=item.get("phone", ""),
                    website=item.get("website", ""),
                    source="google_maps",
                    source_url=url,
                    notes=f"rating:{item.get('rating', '')}",
                )
                if lead.company:
                    await upsert_lead(db, lead)
                    leads.append(lead)

        return leads
