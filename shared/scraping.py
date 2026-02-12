import asyncio
from urllib.parse import urljoin, urlparse

from lxml.html import fromstring as parse_html

from .constants import CONTACT_KW, PATHS, SKIP_DOMAINS
from .email_utils import extract_emails
from .http import fetch, fetch_many


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
    """Find contact/about/team links in HTML using broad keyword matching."""
    try:
        doc = parse_html(html)
        doc.make_links_absolute(base_url)
    except Exception:
        return []
    base_dom = urlparse(base_url).netloc.lower()
    found, seen = [], set()
    for el in doc.xpath("//a[@href]"):
        href = el.get("href", "").strip()
        text = (el.text_content() or "").lower()
        if not (CONTACT_KW.search(text) or CONTACT_KW.search(href.lower())):
            continue
        full = urljoin(base_url, href)
        p = urlparse(full)
        if p.netloc.lower() != base_dom or p.scheme not in ("http", "https"):
            continue
        path = p.path.lower()
        if any(path.endswith(x) for x in (".pdf", ".doc", ".jpg", ".png", ".zip")):
            continue
        norm = f"{p.scheme}://{p.netloc}{p.path}"
        if norm not in seen:
            seen.add(norm)
            found.append(full)
    return found[:8]


async def scrape_site_for_emails(sessions: list, base_url: str) -> set[str]:
    """Full site scrape pipeline: blitz + contact links + sub-links + http fallback.

    Returns set of discovered email addresses.
    """
    domain = urlparse(base_url).hostname or ""
    if any(domain == s or domain.endswith("." + s) for s in SKIP_DOMAINS):
        return set()

    clean_domain = domain.replace("www.", "")
    urls = build_urls(base_url)
    found: set[str] = set()
    pages = 0
    homepage_html = None

    # Phase 1: Blast all URL variants across all sessions
    results = await fetch_many(sessions, urls)

    seen_urls: set[str] = set()
    for url, html, final_url in results:
        if html and url not in seen_urls:
            seen_urls.add(url)
            pages += 1
            found.update(extract_emails(html, clean_domain))
            if url == base_url or (final_url and final_url.rstrip("/") == base_url.rstrip("/")):
                homepage_html = html

    if found:
        return found

    # Phase 2: Follow contact/about/team links from homepage
    if homepage_html:
        nav_links = find_contact_links(homepage_html, base_url)
        contact_urls = [c for c in nav_links if c not in seen_urls]
        if contact_urls:
            tasks = [fetch(sessions[0], u) for u in contact_urls]
            r2 = await asyncio.gather(*tasks)
            for html, _ in r2:
                if html:
                    pages += 1
                    found.update(extract_emails(html, clean_domain))
                if found:
                    break

    if found:
        return found

    # Phase 3: Sub-links from contact pages (one level deeper)
    if homepage_html:
        nav_links = find_contact_links(homepage_html, base_url)
        all_phase2_urls = [c for c in nav_links if c not in seen_urls]
        sub_urls: list[str] = []
        visited = {base_url} | seen_urls | set(all_phase2_urls)
        # Re-fetch phase 2 pages to find sub-links
        for url in all_phase2_urls:
            html, _ = await fetch(sessions[0], url)
            if html:
                for sl in find_contact_links(html, url)[:3]:
                    if sl not in visited:
                        visited.add(sl)
                        sub_urls.append(sl)
        if sub_urls:
            sub_tasks = [fetch(sessions[0], u) for u in sub_urls[:10]]
            sub_pages = await asyncio.gather(*sub_tasks)
            for html, _ in sub_pages:
                if html:
                    found.update(extract_emails(html, clean_domain))
                if found:
                    break

    if found:
        return found

    # Phase 4: HTTP fallback if nothing loaded at all
    if pages == 0:
        parsed = urlparse(base_url)
        if parsed.scheme == "https":
            http_urls = [u.replace("https://", "http://", 1) for u in urls[:3]]
            tasks = [fetch(sessions[0], u) for u in http_urls]
            r3 = await asyncio.gather(*tasks)
            for html, _ in r3:
                if html:
                    pages += 1
                    found.update(extract_emails(html, clean_domain))

    return found
