import asyncio
from urllib.parse import urlparse

from .constants import PATHS, SKIP_DOMAINS
from .email_utils import extract_emails
from .http import fetch, fetch_many

from lxml.html import fromstring as parse_html


def build_urls(base_url: str) -> list[str]:
    """Build every URL variant we want to try for a site."""
    parsed = urlparse(base_url)
    host = parsed.hostname or ""
    bases = [base_url]
    if not host.startswith("www."):
        bases.append(base_url.replace("://", "://www.", 1))
    urls = []
    for b in bases:
        for p in PATHS:
            urls.append(b.rstrip("/") + p)
    return list(dict.fromkeys(urls))  # dedup, preserve order


def find_contact_links(html: str, base_url: str) -> list[str]:
    """Find contact-related links in HTML."""
    try:
        doc = parse_html(html)
        doc.make_links_absolute(base_url)
        seen, out = set(), []
        for el in doc.xpath("//a[@href]"):
            href = el.get("href", "")
            text = (el.text_content() or "").lower()
            if "contact" in href.lower() or "contact" in text or "email" in text:
                if href not in seen and not href.endswith("#"):
                    seen.add(href)
                    out.append(href)
        return out[:5]
    except Exception:
        return []


async def scrape_site_for_emails(sessions: list, base_url: str) -> set[str]:
    """Full site scrape pipeline: blitz + phase2 contact links + http fallback.

    Returns set of discovered email addresses.
    """
    domain = urlparse(base_url).hostname or ""
    if any(domain == s or domain.endswith("." + s) for s in SKIP_DOMAINS):
        return set()

    urls = build_urls(base_url)
    found = set()
    pages = 0
    homepage_html = None

    # Phase 1: Blast all URL variants across all sessions
    results = await fetch_many(sessions, urls)

    seen_urls = set()
    for url, html, final_url in results:
        if html and url not in seen_urls:
            seen_urls.add(url)
            pages += 1
            found.update(extract_emails(html))
            if url == base_url or (final_url and final_url.rstrip("/") == base_url.rstrip("/")):
                homepage_html = html

    # Phase 2: Follow contact links if no emails found
    if not found and homepage_html:
        contact = [c for c in find_contact_links(homepage_html, base_url) if c not in seen_urls]
        if contact:
            tasks = [fetch(sessions[0], u) for u in contact]
            r2 = await asyncio.gather(*tasks)
            for html, _ in r2:
                if html:
                    pages += 1
                    found.update(extract_emails(html))

    # Phase 3: HTTP fallback if nothing loaded at all
    if pages == 0:
        parsed = urlparse(base_url)
        if parsed.scheme == "https":
            http_urls = [u.replace("https://", "http://", 1) for u in urls[:3]]
            tasks = [fetch(sessions[0], u) for u in http_urls]
            r3 = await asyncio.gather(*tasks)
            for html, _ in r3:
                if html:
                    pages += 1
                    found.update(extract_emails(html))

    return found
