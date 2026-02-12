"""Website enricher — deep-crawl practice websites to extract contact details."""

from __future__ import annotations

import json
import os
from urllib.parse import urlparse

import aiosqlite
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from crawl4ai.deep_crawling import BFSDeepCrawlStrategy
from crawl4ai.extraction_strategy import LLMExtractionStrategy

from db.models import Lead
from db.queries import get_leads, upsert_lead
from shared.email_utils import extract_emails, is_junk
from shared.http import create_sessions
from shared.scraping import scrape_site_for_emails

_ENRICHMENT_PROMPT = """Extract contact information for this dental practice from the page content.
Return a JSON object with these fields (use empty string if not found):
- emails: array of email addresses found
- phone: primary phone number
- address: street address
- city: city name
- state: state abbreviation
- zip: zip code
- dentist_names: array of dentist names found
- specialties: array of dental specialties (e.g. "orthodontics", "cosmetic dentistry")
- services: array of services offered
- about: brief description of the practice (max 200 chars)"""

_ENRICHMENT_SCHEMA = {
    "type": "object",
    "properties": {
        "emails": {"type": "array", "items": {"type": "string"}},
        "phone": {"type": "string"},
        "address": {"type": "string"},
        "city": {"type": "string"},
        "state": {"type": "string"},
        "zip": {"type": "string"},
        "dentist_names": {"type": "array", "items": {"type": "string"}},
        "specialties": {"type": "array", "items": {"type": "string"}},
        "services": {"type": "array", "items": {"type": "string"}},
        "about": {"type": "string"},
    },
}


def _parse_name(name: str) -> tuple[str, str]:
    """Split 'Dr. First Last' into (first, last)."""
    cleaned = name.strip()
    for prefix in ("Dr.", "Dr", "DDS", "DMD"):
        if cleaned.startswith(prefix):
            cleaned = cleaned[len(prefix):].strip(", ")
    parts = cleaned.split(None, 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    if len(parts) == 1:
        return parts[0], ""
    return "", ""


class WebsiteEnricher:
    """Deep-crawl practice websites and enrich lead records with extracted data."""

    async def scrape(
        self,
        db: aiosqlite.Connection,
        *,
        limit: int = 50,
    ) -> list[Lead]:
        """Enrich leads that have a website but are missing email/phone/address."""
        # Fetch leads needing enrichment
        all_leads: list[Lead] = []
        offset = 0
        while True:
            batch = await get_leads(db, limit=100, offset=offset)
            if not batch:
                break
            all_leads.extend(batch)
            offset += 100

        # Filter to leads with website but missing key data
        targets = [
            l for l in all_leads
            if l.website and (not l.email or not l.phone)
        ][:limit]

        if not targets:
            return []

        enriched: list[Lead] = []
        browser_cfg = BrowserConfig(headless=True)

        extraction = LLMExtractionStrategy(
            provider=os.getenv("LLM_PROVIDER", "anthropic/claude-haiku-4-5-20251001"),
            api_token=os.getenv("ANTHROPIC_API_KEY", ""),
            schema=_ENRICHMENT_SCHEMA,
            instruction=_ENRICHMENT_PROMPT,
        )

        deep_crawl = BFSDeepCrawlStrategy(max_depth=2, max_pages=8)

        run_cfg = CrawlerRunConfig(
            extraction_strategy=extraction,
            deep_crawl_strategy=deep_crawl,
            page_timeout=30000,
        )

        async with AsyncWebCrawler(config=browser_cfg) as crawler:
            for lead in targets:
                url = lead.website
                if not url.startswith("http"):
                    url = f"https://{url}"

                data = await self._crawl_site(crawler, url, run_cfg)

                if not data:
                    # Fallback: use shared scraping module
                    data = await self._fallback_scrape(url)

                if not data:
                    continue

                # Apply enrichment data to lead
                emails = [e for e in data.get("emails", []) if not is_junk(e)]
                if emails and not lead.email:
                    lead = Lead(**{
                        **{k: getattr(lead, k) for k in lead.__struct_fields__},
                        "email": emails[0],
                    })

                updates: dict = {}
                if data.get("phone") and not lead.phone:
                    updates["phone"] = data["phone"]
                if data.get("address") and not lead.address:
                    updates["address"] = data["address"]
                if data.get("city") and not lead.city:
                    updates["city"] = data["city"]
                if data.get("state") and not lead.state:
                    updates["state"] = data["state"]
                if data.get("zip") and not lead.zip:
                    updates["zip"] = data["zip"]

                # Extract dentist names into first/last if missing
                names = data.get("dentist_names", [])
                if names and not lead.first_name:
                    first, last = _parse_name(names[0])
                    updates["first_name"] = first
                    updates["last_name"] = last

                # Build notes from specialties/services/about
                notes_parts = []
                if data.get("specialties"):
                    notes_parts.append(f"specialties:{','.join(data['specialties'])}")
                if data.get("services"):
                    notes_parts.append(f"services:{','.join(data['services'][:5])}")
                if data.get("about"):
                    notes_parts.append(data["about"][:200])
                if notes_parts and not lead.notes:
                    updates["notes"] = " | ".join(notes_parts)

                updates["enriched_at"] = __import__("datetime").datetime.now(
                    __import__("datetime").UTC
                ).strftime("%Y-%m-%dT%H:%M:%SZ")

                if updates:
                    updated = Lead(**{
                        **{k: getattr(lead, k) for k in lead.__struct_fields__},
                        **updates,
                    })
                    await upsert_lead(db, updated)
                    enriched.append(updated)

        return enriched

    async def _crawl_site(
        self,
        crawler: AsyncWebCrawler,
        url: str,
        run_cfg: CrawlerRunConfig,
    ) -> dict | None:
        """Run Crawl4AI deep crawl + LLM extraction on a site."""
        try:
            results = await crawler.arun(url=url, config=run_cfg)
            if not results.extracted_content:
                return None
            data = json.loads(results.extracted_content)
            if isinstance(data, list):
                # Merge multiple page extractions
                merged: dict = {
                    "emails": [], "dentist_names": [],
                    "specialties": [], "services": [],
                }
                for item in data:
                    if not isinstance(item, dict):
                        continue
                    for key in ("emails", "dentist_names", "specialties", "services"):
                        merged[key].extend(item.get(key, []))
                    for key in ("phone", "address", "city", "state", "zip", "about"):
                        if item.get(key) and not merged.get(key):
                            merged[key] = item[key]
                return merged
            return data if isinstance(data, dict) else None
        except Exception:
            return None

    async def _fallback_scrape(self, url: str) -> dict | None:
        """Use shared/scraping.py as fallback for email extraction."""
        try:
            sessions = create_sessions()
            ssl_session, nossl_session = sessions
            try:
                emails = await scrape_site_for_emails(
                    [ssl_session, nossl_session], url
                )
                if emails:
                    return {"emails": list(emails)}
                return None
            finally:
                await ssl_session.close()
                await nossl_session.close()
        except Exception:
            return None
