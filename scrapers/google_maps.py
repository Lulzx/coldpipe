"""Google Maps scraper using Crawl4AI with regex-based extraction."""

from __future__ import annotations

import re
from typing import Any

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig

from db.queries import upsert_lead
from db.tables import Lead


def _parse_maps_markdown(markdown: str, city: str, url: str) -> list[Lead]:
    """Parse Crawl4AI markdown output from a Google Maps search page.

    Google Maps listings appear in the rendered markdown as blocks
    containing a business name, address, phone, website, and rating.
    """
    leads: list[Lead] = []

    # Split into candidate blocks on double-newlines
    blocks = re.split(r"\n{2,}", markdown)

    for block in blocks:
        lines = [l.strip() for l in block.splitlines() if l.strip()]
        if not lines:
            continue

        # Heuristic: a block is a listing if it has at least a name-like
        # first line and at least one of: phone, address pattern, or website.
        name = ""
        phone = ""
        address = ""
        website = ""
        state = ""
        zip_code = ""
        rating = ""

        # First non-empty line often is the business name (strip markdown link syntax)
        raw_name = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", lines[0])
        raw_name = re.sub(r"[#*`]", "", raw_name).strip()

        # Skip if it looks like a UI element / nav item
        if len(raw_name) < 3 or raw_name.lower() in ("menu", "search", "directions"):
            continue

        name = raw_name

        for line in lines[1:]:
            # Phone pattern
            if not phone:
                m = re.search(r"\(?\d{3}\)?[\s\-\.]\d{3}[\s\-\.]\d{4}", line)
                if m:
                    phone = m.group().strip()

            # Website
            if not website:
                m = re.search(r"https?://[^\s\)\"']+", line)
                if m:
                    website = m.group().rstrip(".,")

            # Rating
            if not rating:
                m = re.search(r"(\d\.\d)\s*(?:stars?|★|·)", line, re.IGNORECASE)
                if not m:
                    m = re.search(r"★\s*(\d\.\d)", line)
                if m:
                    rating = m.group(1)

            # State / zip from address lines
            m = re.search(r",\s*([A-Z]{2})\s+(\d{5})", line)
            if m:
                state = m.group(1)
                zip_code = m.group(2)
                # Address is the bit before the city/state/zip
                addr_candidate = re.sub(r",\s*[A-Z]{2}\s+\d{5}.*", "", line).strip()
                if addr_candidate and not address:
                    address = addr_candidate

        # Require a name and at least one piece of contact data
        if name and (phone or website or address):
            lead = Lead(
                company=name,
                phone=phone,
                website=website,
                address=address,
                city=city,
                state=state,
                zip=zip_code,
                source="google_maps",
                source_url=url,
                notes=f"rating:{rating}" if rating else "",
            )
            leads.append(lead)

    return leads


class GoogleMapsScraper:
    """Scrape business listings from Google Maps using Crawl4AI."""

    async def scrape(
        self,
        db: Any = None,
        *,
        city: str = "New York",
        max_results: int = 20,
    ) -> list[Lead]:
        query = f"businesses in {city}"
        url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"

        browser_cfg = BrowserConfig(headless=True)
        run_cfg = CrawlerRunConfig(
            js_code=[
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

            markdown = getattr(result, "markdown", None) or getattr(result, "markdown_v2", None)
            if not markdown:
                return leads

            # markdown may be a string or an object with a .raw_markdown attribute
            if not isinstance(markdown, str):
                markdown = getattr(markdown, "raw_markdown", str(markdown))

            parsed = _parse_maps_markdown(markdown, city, url)

            for lead in parsed[:max_results]:
                if lead.company:
                    try:
                        await upsert_lead(db, lead)
                    except Exception:
                        pass
                    leads.append(lead)

        return leads
