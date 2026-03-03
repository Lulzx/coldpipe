"""IMAP reply watcher: polls for UNSEEN messages and matches to sent emails."""

from __future__ import annotations

import asyncio
import contextlib
import email
import logging
from email.message import Message
from typing import Any

import aioimaplib

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
        db: Any,
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

    def _extract_body(self, msg: Message) -> str:
        """Extract plain-text body from an email message."""
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == "text/plain":
                    payload = part.get_payload(decode=True)
                    if isinstance(payload, bytes):
                        return payload.decode("utf-8", errors="replace")
            return ""
        payload = msg.get_payload(decode=True)
        if isinstance(payload, bytes):
            return payload.decode("utf-8", errors="replace")
        return ""

    async def _apply_triage(self, deal_id: int, lead_id: int, body: str) -> None:
        """Run rule-based triage and apply the result."""
        from db.tables import CampaignLead, Deal

        from .triage import triage_reply_text

        result = triage_reply_text(body)
        classification = result["classification"]
        action = result["action"]

        try:
            if action == "mark_unsubscribed":
                # Update all campaign leads for this lead to unsubscribed
                await (
                    CampaignLead.update({CampaignLead.status: "unsubscribed"})
                    .where(CampaignLead.lead_id == lead_id)
                    .run()
                )
                log.info("Triage: lead %d marked unsubscribed", lead_id)
            elif action == "move_to_deals":
                # Advance deal stage to interested
                await Deal.update({Deal.stage: "interested"}).where(Deal.id == deal_id).run()
                log.info("Triage: deal %d advanced to interested", deal_id)
            elif action == "follow_up_later":
                # Pause campaign leads for this lead (out of office)
                await (
                    CampaignLead.update({CampaignLead.status: "paused"})
                    .where((CampaignLead.lead_id == lead_id) & (CampaignLead.status == "active"))
                    .run()
                )
                log.info("Triage: lead %d paused (out of office)", lead_id)

            # Store triage result in deal notes
            note = f"[auto-triage] {classification} (confidence={result['confidence']})"
            await (
                Deal.update({Deal.notes: Deal.notes + "\n" + note}).where(Deal.id == deal_id).run()
            )
        except Exception:
            log.error("Triage action failed for deal %d", deal_id, exc_info=True)

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
        if self._client is None:
            raise RuntimeError("IMAP client not connected")
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

        # Extract reply body text for triage
        reply_body = self._extract_body(msg)

        # Handle the reply (creates deal, stops sequence)
        deal_id = await handle_reply(
            self._db,
            email_sent_id=sent_email.id,
            campaign_id=sent_email.campaign_id,
            lead_id=sent_email.lead_id,
        )

        # Run rule-based triage on the reply body
        if reply_body and deal_id is not None:
            await self._apply_triage(deal_id, sent_email.lead_id, reply_body)

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

        if self._client is None:
            raise RuntimeError("IMAP client not connected")
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

        sem = asyncio.Semaphore(5)

        async def _safe_process(uid: str) -> bool:
            async with sem:
                return await self._process_message(uid)

        results = await asyncio.gather(
            *[_safe_process(uid) for uid in uids], return_exceptions=True
        )
        for uid, exc in zip(uids, results, strict=True):
            if isinstance(exc, Exception):
                log.error("Error processing uid %s: %s", uid, exc)
        matched = sum(1 for r in results if r is True)

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


async def check_replies(db: Any, mb: object) -> int:
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
