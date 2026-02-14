import asyncio
import ssl
import time
from collections import defaultdict
from urllib.parse import urlparse

import aiohttp

from .constants import DOMAIN_CONCURRENCY, HDRS, MAX_CONN, TIMEOUT

DOMAIN_DELAY = 0.5

_domain_semaphores: dict[str, asyncio.Semaphore] = defaultdict(
    lambda: asyncio.Semaphore(DOMAIN_CONCURRENCY)
)
_domain_last_request: dict[str, float] = defaultdict(float)


def create_sessions():
    """Create (ssl_session, nossl_session) aiohttp pair. Must be used as async context managers."""
    nossl = ssl.create_default_context()
    nossl.check_hostname = False
    nossl.verify_mode = ssl.CERT_NONE

    ssl_session = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=MAX_CONN, ssl=True),
        headers=HDRS,
    )
    nossl_session = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=MAX_CONN, ssl=nossl),
        headers=HDRS,
    )
    return ssl_session, nossl_session


async def fetch(session: aiohttp.ClientSession, url: str, timeout: int = TIMEOUT):
    """Async GET with per-domain rate limiting. Returns (html, final_url) or (None, None)."""
    domain = urlparse(url).netloc
    sem = _domain_semaphores[domain]
    async with sem:
        now = time.monotonic()
        last = _domain_last_request.get(domain, 0.0)
        wait = DOMAIN_DELAY - (now - last)
        if wait > 0:
            await asyncio.sleep(wait)
        _domain_last_request[domain] = time.monotonic()
        try:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=timeout),
                allow_redirects=True,
            ) as r:
                if r.status == 200:
                    return await r.text(errors="replace"), str(r.url)
        except Exception:
            pass
        return None, None


async def fetch_many(sessions: list, urls: list[str], timeout: int = TIMEOUT):
    """Fire all URLs across all sessions in parallel. Returns list of (url, html, final_url)."""

    async def _try(session, url):
        html, final_url = await fetch(session, url, timeout)
        return url, html, final_url

    tasks = []
    for url in urls:
        for s in sessions:
            tasks.append(_try(s, url))
    return await asyncio.gather(*tasks)
