"""IMAP reply watcher: polls for UNSEEN messages and matches to sent emails."""

from __future__ import annotations

import asyncio
import contextlib
import email
import logging
from email.message import Message

import aioimaplib
import aiosqlite

from config.settings import ImapSettings
from db import queries

from .sequences import handle_reply

log = logging.getLogger(__name__)

POLL_INTERVAL = 60  # seconds


class ReplyWatcher:
    """Polls IMAP for unseen messages and processes replies.

    Matches In-Reply-To / References headers against emails_sent.message_id.
    On match: marks email as replied, stops sequence, creates deal.
    """

    def __init__(
        self,
        db: aiosqlite.Connection,
        imap_settings: ImapSettings,
        *,
        poll_interval: int = POLL_INTERVAL,
    ):
        self._db = db
        self._imap = imap_settings
        self._poll_interval = poll_interval
        self._stop = False
        self._client: aioimaplib.IMAP4_SSL | None = None

    async def connect(self) -> None:
        """Connect and authenticate to IMAP server."""
        self._client = aioimaplib.IMAP4_SSL(
            host=self._imap.host,
            port=self._imap.port,
        )
        await self._client.wait_hello_from_server()
        await self._client.login(self._imap.user, self._imap.password)
        await self._client.select("INBOX")
        log.info("IMAP connected to %s:%d", self._imap.host, self._imap.port)

    async def disconnect(self) -> None:
        """Close IMAP connection."""
        if self._client:
            with contextlib.suppress(Exception):
                await self._client.logout()
            self._client = None

    def stop(self) -> None:
        """Signal the watcher to stop polling."""
        self._stop = True

    def _parse_message(self, raw_bytes: bytes) -> Message:
        """Parse raw email bytes into an email.message.Message."""
        return email.message_from_bytes(raw_bytes)

    def _extract_reply_to(self, msg: Message) -> str | None:
        """Get the In-Reply-To or first References header value."""
        in_reply_to = msg.get("In-Reply-To", "").strip()
        if in_reply_to:
            return in_reply_to
        refs = msg.get("References", "").strip()
        if refs:
            # Take the first reference (the original message)
            return refs.split()[0]
        return None

    async def _process_message(self, uid: str) -> bool:
        """Fetch and process a single message by UID.

        Returns True if the message matched a sent email.
        """
        assert self._client is not None
        result, data = await self._client.uid("fetch", uid, "(RFC822)")
        if result != "OK" or not data:
            return False

        # aioimaplib returns data as list of (header, body) tuples
        raw = None
        for item in data:
            if isinstance(item, tuple) and len(item) >= 2:
                raw = item[1]
                break
            elif isinstance(item, bytes):
                raw = item
                break

        if raw is None:
            return False

        msg = self._parse_message(raw if isinstance(raw, bytes) else raw.encode())
        reply_to_id = self._extract_reply_to(msg)
        if not reply_to_id:
            return False

        # Look up the original sent email
        sent_email = await queries.get_email_by_message_id(self._db, reply_to_id)
        if sent_email is None:
            return False

        # Handle the reply
        await handle_reply(
            self._db,
            email_sent_id=sent_email.id,
            campaign_id=sent_email.campaign_id,
            lead_id=sent_email.lead_id,
        )

        log.info(
            "Reply matched: uid=%s, message_id=%s, lead_id=%d",
            uid,
            reply_to_id,
            sent_email.lead_id,
        )
        return True

    async def poll_once(self) -> int:
        """Check for unseen messages and process them.

        Returns the number of replies matched.
        """
        if self._client is None:
            await self.connect()

        assert self._client is not None
        result, data = await self._client.uid("search", "UNSEEN")
        if result != "OK":
            log.warning("IMAP search failed: %s", result)
            return 0

        uids = []
        for item in data:
            if isinstance(item, bytes):
                uids.extend(item.decode().split())
            elif isinstance(item, str):
                uids.extend(item.split())

        if not uids:
            return 0

        matched = 0
        for uid in uids:
            try:
                if await self._process_message(uid):
                    matched += 1
            except Exception as exc:
                log.error("Error processing uid %s: %s", uid, exc)

        log.info("Polled %d unseen, %d matched replies", len(uids), matched)
        return matched

    async def run(self) -> None:
        """Run the polling loop until stopped."""
        log.info("Reply watcher started (interval=%ds)", self._poll_interval)
        while not self._stop:
            try:
                await self.poll_once()
            except Exception as exc:
                log.error("Reply poll error: %s", exc)
                # Reconnect on failure
                self._client = None
            await asyncio.sleep(self._poll_interval)

    async def __aenter__(self) -> ReplyWatcher:
        await self.connect()
        return self

    async def __aexit__(self, *exc) -> None:
        self.stop()
        await self.disconnect()


async def check_replies(db: aiosqlite.Connection, mb: object) -> int:
    """Thin wrapper for daemon: poll a mailbox for replies. Returns matched count."""
    watcher = ReplyWatcher(
        db,
        ImapSettings(
            host=getattr(mb, "imap_host", ""),
            port=getattr(mb, "imap_port", 993),
            user=getattr(mb, "imap_user", ""),
            password=getattr(mb, "imap_pass", ""),
        ),
    )
    await watcher.connect()
    try:
        return await watcher.poll_once()
    finally:
        await watcher.disconnect()
