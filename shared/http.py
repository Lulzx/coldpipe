import ssl

import aiohttp

from .constants import HDRS, MAX_CONN, TIMEOUT


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
    """Async GET with error handling. Returns (html, final_url) or (None, None)."""
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
    import asyncio

    async def _try(session, url):
        html, final_url = await fetch(session, url, timeout)
        return url, html, final_url

    tasks = []
    for url in urls:
        for s in sessions:
            tasks.append(_try(s, url))
    return await asyncio.gather(*tasks)
