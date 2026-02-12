"""BaseScraper protocol — structural typing for all scraper backends."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

import aiosqlite

from db.models import Lead


@runtime_checkable
class BaseScraper(Protocol):
    """Every scraper must expose an async ``scrape`` method.

    The method should return a list of Lead structs ready for upsert.
    """

    async def scrape(self, db: aiosqlite.Connection, **kwargs) -> list[Lead]: ...
