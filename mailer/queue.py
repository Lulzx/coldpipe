"""Send queue with daily-limit checks, timezone-aware send windows, and backpressure."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, time
from zoneinfo import ZoneInfo

import aiosqlite

from config.settings import SendSettings
from db import queries

log = logging.getLogger(__name__)

DEFAULT_QUEUE_SIZE = 50


def warmup_daily_limit(warmup_day: int) -> int:
    """Return warmup limit using the shared DB/query logic."""
    return queries.get_warmup_limit(warmup_day)


def _in_send_window(send_settings: SendSettings) -> bool:
    """Check whether the current time falls within the configured send window."""
    tz = ZoneInfo(send_settings.timezone)
    now = datetime.now(tz).time()

    start_h, start_m = map(int, send_settings.send_window_start.split(":"))
    end_h, end_m = map(int, send_settings.send_window_end.split(":"))

    start = time(start_h, start_m)
    end = time(end_h, end_m)

    return start <= now <= end


class SendQueue:
    """Fetches eligible campaign-leads and applies rate/window limits.

    Usage:
        queue = SendQueue(db, campaign_id, mailbox_id, send_settings)
        async for item in queue:
            # item is a dict with lead + step info
            ...
    """

    def __init__(
        self,
        db: aiosqlite.Connection,
        campaign_id: int,
        mailbox_id: int,
        send_settings: SendSettings | None = None,
        *,
        max_queue_size: int = DEFAULT_QUEUE_SIZE,
        warmup_day: int | None = None,
    ):
        self._db = db
        self._campaign_id = campaign_id
        self._mailbox_id = mailbox_id
        self._settings = send_settings or SendSettings()
        self._max_queue_size = max_queue_size
        self._warmup_day = warmup_day
        self._buffer: asyncio.Queue[dict] = asyncio.Queue(maxsize=max_queue_size)
        self._stop = False

    async def _effective_limit(self) -> int:
        """Determine how many more emails can be sent today."""
        sent_today, mailbox_limit = await queries.check_daily_limit(self._db, self._mailbox_id)
        daily_cap = mailbox_limit
        if self._warmup_day is not None:
            daily_cap = min(daily_cap, warmup_daily_limit(self._warmup_day))
        remaining = max(0, daily_cap - sent_today)
        return remaining

    async def fill(self) -> None:
        """Fetch eligible items from DB and push into the buffer.

        Respects send window, daily limit, and backpressure (buffer size).
        """
        if not _in_send_window(self._settings):
            log.info("Outside send window, skipping fetch")
            return

        remaining = await self._effective_limit()
        if remaining <= 0:
            log.info("Daily limit reached for mailbox %d", self._mailbox_id)
            return

        # Don't fetch more than the buffer can hold
        space = self._max_queue_size - self._buffer.qsize()
        fetch_limit = min(remaining, space)
        if fetch_limit <= 0:
            log.debug("Queue full, backpressure applied")
            return

        items = await queries.get_send_queue(self._db, self._campaign_id, limit=fetch_limit)
        for item in items:
            await self._buffer.put(item)

        log.info(
            "Queued %d items for campaign %d (remaining=%d)",
            len(items),
            self._campaign_id,
            remaining - len(items),
        )

    def stop(self) -> None:
        """Signal the queue to stop yielding items."""
        self._stop = True

    async def get(self) -> dict | None:
        """Get next item from the queue, or None if stopped/empty."""
        if self._stop:
            return None
        try:
            return self._buffer.get_nowait()
        except asyncio.QueueEmpty:
            return None

    def __aiter__(self):
        return self

    async def __anext__(self) -> dict:
        if self._stop:
            raise StopAsyncIteration
        try:
            item = self._buffer.get_nowait()
            return item
        except asyncio.QueueEmpty as exc:
            raise StopAsyncIteration from exc
