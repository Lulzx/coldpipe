"""Blast every URL variant in parallel. One round-trip latency to scrape all sites."""
import asyncio
import csv
import re
import ssl
import time
import urllib.parse
from urllib.parse import urlparse

import aiohttp
from lxml.html import fromstring as parse_html

# ---------------------------------------------------------------------------
TIMEOUT = 5
MAX_CONN = 500
EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
PATHS = ["", "/contact", "/contact-us", "/about", "/about-us", "/team", "/our-team"]
SKIP_DOMAINS = {"exa.ai"}
JUNK_DOMAINS = {
    "sentry.io", "wixpress.com", "example.com", "domain.com", "yoursite.com",
    "email.com", "yourdomain.com", "test.com", "sentry-next.wixpress.com",
    "change.me", "exa.ai", "myftpupload.com", "googleapis.com", "w3.org",
    "schema.org", "gravatar.com", "wordpress.org", "wordpress.com",
}
JUNK_PREFIXES = ("noreply@", "no-reply@", "webmaster@", "root@", "admin@wordpress")
HDRS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
}

# ---------------------------------------------------------------------------
def is_junk(e: str) -> bool:
    e = e.lower()
    local, _, domain = e.partition("@")
    if not domain:
        return True
    if domain in JUNK_DOMAINS or any(domain.endswith("." + j) for j in JUNK_DOMAINS):
        return True
    if e.startswith(JUNK_PREFIXES):
        return True
    if re.search(r"\.(png|jpg|jpeg|gif|svg|webp|css|js|woff2?|ttf|eot)$", e):
        return True
    if len(local) < 2:
        return True
    if len(local) > 20 and all(c in "0123456789abcdef" for c in local):
        return True
    tld = domain.rsplit(".", 1)[-1] if "." in domain else ""
    if len(tld) < 2 or len(domain) < 5:
        return True
    if domain.count(".") >= 3:
        return True
    return False


def extract_emails(html: str) -> set[str]:
    emails = set()
    try:
        doc = parse_html(html)
        for href in doc.xpath("//a[starts-with(@href,'mailto:')]/@href"):
            raw = href[7:].split("?")[0].strip()
            if EMAIL_RE.fullmatch(raw):
                emails.add(raw.lower())
    except Exception:
        pass
    for m in EMAIL_RE.findall(html):
        emails.add(m.lower())
    return {e for e in emails if not is_junk(e)}


def find_contact_links(html: str, base_url: str) -> list[str]:
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

# ---------------------------------------------------------------------------
async def _get(session, url, timeout=TIMEOUT):
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout),
                               allow_redirects=True) as r:
            if r.status == 200:
                return await r.text(errors="replace"), str(r.url)
    except Exception:
        pass
    return None, None

# ---------------------------------------------------------------------------
def build_urls(base_url: str) -> list[str]:
    """Build every URL variant we want to try for a site."""
    parsed = urlparse(base_url)
    host = parsed.hostname or ""
    bases = [base_url]
    # Add www variant
    if not host.startswith("www."):
        bases.append(base_url.replace("://", "://www.", 1))
    urls = []
    for b in bases:
        for p in PATHS:
            urls.append(b.rstrip("/") + p)
    return list(dict.fromkeys(urls))  # dedup, preserve order


async def scrape_site(sessions, base_url: str, idx: int, total: int):
    lines = []
    log = lambda m, i=0: lines.append("  " * i + m)

    log(f"[{idx}/{total}] {base_url}")

    domain = urlparse(base_url).hostname or ""
    if any(domain == s or domain.endswith("." + s) for s in SKIP_DOMAINS):
        log(f"  SKIP (third-party)", 1)
        print("\n".join(lines), flush=True)
        return {"url": base_url, "emails": "", "count": 0, "pages_checked": 0}

    urls = build_urls(base_url)
    found = set()
    pages = 0
    homepage_html = None

    # BLAST: fire all URL variants across both sessions simultaneously
    async def try_url(session, url):
        return url, *await _get(session, url)

    tasks = []
    for url in urls:
        for s in sessions:
            tasks.append(try_url(s, url))

    log(f"  Firing {len(tasks)} requests...", 1)
    results = await asyncio.gather(*tasks)

    seen_urls = set()
    for url, html, final_url in results:
        if html and url not in seen_urls:
            seen_urls.add(url)
            pages += 1
            emails = extract_emails(html)
            found.update(emails)
            if emails:
                log(f"  {url} -> {emails}", 1)
            # Cache homepage html for Phase 2
            if url == base_url or (final_url and final_url.rstrip("/") == base_url.rstrip("/")):
                homepage_html = html

    # Phase 2: if no emails, follow contact links (parallel)
    if not found and homepage_html:
        contact = find_contact_links(homepage_html, base_url)
        contact = [c for c in contact if c not in seen_urls]
        if contact:
            log(f"  Phase 2: {len(contact)} contact links...", 1)
            tasks2 = [try_url(sessions[0], u) for u in contact]
            r2 = await asyncio.gather(*tasks2)
            for url, html, _ in r2:
                if html:
                    pages += 1
                    emails = extract_emails(html)
                    found.update(emails)
                    if emails:
                        log(f"  {url} -> {emails}", 1)

    # Also try http:// if we got nothing at all
    if pages == 0:
        parsed = urlparse(base_url)
        if parsed.scheme == "https":
            http_urls = [u.replace("https://", "http://", 1) for u in urls[:3]]
            log(f"  HTTP fallback: {len(http_urls)} urls...", 1)
            tasks3 = [try_url(sessions[0], u) for u in http_urls]
            r3 = await asyncio.gather(*tasks3)
            for url, html, _ in r3:
                if html:
                    pages += 1
                    found.update(extract_emails(html))

    if found:
        log(f"  => {len(found)} email(s): {'; '.join(sorted(found))}", 1)
    else:
        log(f"  => no emails (checked {pages} pages)", 1)

    print("\n".join(lines), flush=True)
    return {"url": base_url, "emails": "; ".join(sorted(found)), "count": len(found), "pages_checked": pages}


async def main():
    csv_file = "data/webset_dental_implants_all_on_4_full_arch_teeth_in_a_day_phoenix.csv"
    output_file = "data/dentist_emails.csv"

    urls = []
    with open(csv_file, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            url = row.get("URL", "").strip()
            if url and url.startswith("http"):
                urls.append(url)

    print(f"Scraping {len(urls)} sites...\n", flush=True)

    nossl = ssl.create_default_context()
    nossl.check_hostname = False
    nossl.verify_mode = ssl.CERT_NONE

    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=MAX_CONN, ssl=True),
        headers=HDRS,
    ) as s1, aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=MAX_CONN, ssl=nossl),
        headers=HDRS,
    ) as s2:
        sessions = [s1, s2]
        start = time.time()
        results = await asyncio.gather(*[
            scrape_site(sessions, url, i, len(urls))
            for i, url in enumerate(urls, 1)
        ])
        elapsed = time.time() - start

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["url", "emails", "count", "pages_checked"])
        w.writeheader()
        w.writerows(results)

    hit = sum(1 for r in results if r["emails"])
    total_e = sum(r["count"] for r in results)
    print(f"\n{'='*50}", flush=True)
    print(f"Done in {elapsed:.1f}s | {hit}/{len(results)} sites | {total_e} emails", flush=True)
    print(f"{'='*50}", flush=True)
    for r in results:
        m = "OK" if r["emails"] else "--"
        print(f"  [{m}] {r['url']} -> {r['emails'] or '(none)'}", flush=True)
    print(f"\nSaved to {output_file}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
