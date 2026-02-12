"""Directory scrapers for Yelp, Healthgrades, and Zocdoc using Crawl4AI CSS extraction."""

from __future__ import annotations

import json

import aiosqlite
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy

from db.models import Lead
from db.queries import upsert_lead

# ---------------------------------------------------------------------------
# CSS extraction schemas per directory site
# ---------------------------------------------------------------------------

_YELP_SCHEMA = {
    "name": "yelp_results",
    "baseSelector": "li.y-css-1iy1dwt",
    "fields": [
        {"name": "company", "selector": "a.css-19v1rkv", "type": "text"},
        {"name": "phone", "selector": "p.css-1p9ibgf", "type": "text"},
        {"name": "address", "selector": "p.css-qyp8bo", "type": "text"},
        {"name": "website", "selector": "a.css-1idmmu3", "type": "attribute", "attribute": "href"},
        {"name": "rating", "selector": "div.y-css-dnttlc span.y-css-jf9frv", "type": "text"},
        {"name": "url", "selector": "a.css-19v1rkv", "type": "attribute", "attribute": "href"},
    ],
}

_HEALTHGRADES_SCHEMA = {
    "name": "healthgrades_results",
    "baseSelector": "div.provider-card",
    "fields": [
        {"name": "first_name", "selector": "a.provider-name", "type": "text"},
        {"name": "address", "selector": "div.provider-address", "type": "text"},
        {"name": "phone", "selector": "a.phone-link", "type": "text"},
        {"name": "rating", "selector": "span.star-rating-text", "type": "text"},
        {"name": "url", "selector": "a.provider-name", "type": "attribute", "attribute": "href"},
    ],
}

_ZOCDOC_SCHEMA = {
    "name": "zocdoc_results",
    "baseSelector": "div[data-test='provider-card']",
    "fields": [
        {"name": "first_name", "selector": "h2[data-test='provider-name']", "type": "text"},
        {"name": "address", "selector": "div[data-test='provider-address']", "type": "text"},
        {"name": "phone", "selector": "a[data-test='provider-phone']", "type": "text"},
        {
            "name": "url",
            "selector": "a[data-test='provider-name-link']",
            "type": "attribute",
            "attribute": "href",
        },
    ],
}

_DIRECTORY_CONFIGS = {
    "yelp": {
        "url_template": "https://www.yelp.com/search?find_desc=Dentist&find_loc={city}",
        "schema": _YELP_SCHEMA,
        "source": "yelp",
    },
    "healthgrades": {
        "url_template": "https://www.healthgrades.com/dentistry/dentist-directory/{city}",
        "schema": _HEALTHGRADES_SCHEMA,
        "source": "healthgrades",
    },
    "zocdoc": {
        "url_template": "https://www.zocdoc.com/dentists/{city}",
        "schema": _ZOCDOC_SCHEMA,
        "source": "zocdoc",
    },
}


def _parse_name(full_name: str) -> tuple[str, str]:
    """Split 'Dr. First Last' into (first, last), stripping title prefixes."""
    name = full_name.strip()
    for prefix in ("Dr.", "Dr", "DDS", "DMD"):
        if name.startswith(prefix):
            name = name[len(prefix) :].strip(", ")
    parts = name.split(None, 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    if len(parts) == 1:
        return parts[0], ""
    return "", ""


def _parse_address(raw: str) -> tuple[str, str, str, str]:
    """Best-effort split of a single-line address into (address, city, state, zip)."""
    parts = [p.strip() for p in raw.rsplit(",", 2)]
    if len(parts) >= 3:
        addr = parts[0]
        city = parts[1]
        state_zip = parts[2].split()
        state = state_zip[0] if state_zip else ""
        zipcode = state_zip[1] if len(state_zip) > 1 else ""
        return addr, city, state, zipcode
    return raw, "", "", ""


class DirectoryScraper:
    """Scrape directories (Yelp, Healthgrades, Zocdoc) via Crawl4AI CSS extraction."""

    async def scrape(
        self,
        db: aiosqlite.Connection,
        *,
        city: str = "New York",
        directories: list[str] | None = None,
        max_results: int = 20,
    ) -> list[Lead]:
        targets = directories or list(_DIRECTORY_CONFIGS.keys())
        all_leads: list[Lead] = []

        browser_cfg = BrowserConfig(headless=True)

        async with AsyncWebCrawler(config=browser_cfg) as crawler:
            for dir_name in targets:
                cfg = _DIRECTORY_CONFIGS.get(dir_name)
                if not cfg:
                    continue

                url = str(cfg["url_template"]).format(city=city.replace(" ", "-").lower())
                extraction = JsonCssExtractionStrategy(schema=cfg["schema"])  # type: ignore[arg-type]
                run_cfg = CrawlerRunConfig(
                    extraction_strategy=extraction,
                    page_timeout=30000,
                )

                result = await crawler.arun(url=url, config=run_cfg)
                if not result.extracted_content:
                    continue

                try:
                    items = json.loads(result.extracted_content)
                except (json.JSONDecodeError, TypeError):
                    continue

                for item in items[:max_results]:
                    if not isinstance(item, dict):
                        continue

                    company = item.get("company", "")
                    first_name = ""
                    last_name = ""

                    # Some directories return individual names, not practice names
                    raw_name = item.get("first_name", "")
                    if raw_name and not company:
                        first_name, last_name = _parse_name(raw_name)
                        company = raw_name  # Use full name as company placeholder
                    elif raw_name:
                        first_name, last_name = _parse_name(raw_name)

                    addr_raw = item.get("address", "")
                    address, addr_city, state, zipcode = _parse_address(addr_raw)

                    lead = Lead(
                        first_name=first_name,
                        last_name=last_name,
                        company=company,
                        phone=item.get("phone", ""),
                        website=item.get("website", ""),
                        address=address,
                        city=addr_city or city,
                        state=state,
                        zip=zipcode,
                        source=str(cfg["source"]),
                        source_url=url,
                        notes=f"rating:{item.get('rating', '')}",
                    )
                    if lead.company or lead.first_name:
                        await upsert_lead(db, lead)
                        all_leads.append(lead)

        return all_leads
