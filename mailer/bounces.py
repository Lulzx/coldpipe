"""DSN (Delivery Status Notification) bounce parser and handler."""

from __future__ import annotations

import contextlib
import email
import logging
import re
from email.message import Message
from typing import Any

import aioimaplib

from db import queries

log = logging.getLogger(__name__)

MAX_SOFT_RETRIES = 3

# Regex to extract status codes from DSN reports (e.g. "5.1.1" or "4.0.0")
_STATUS_RE = re.compile(r"\b([45])\.\d+\.\d+\b")
# Regex to extract SMTP reply codes (e.g. "550", "421")
_SMTP_CODE_RE = re.compile(r"\b([45]\d{2})\b")


def parse_dsn(raw_bytes: bytes) -> dict | None:
    """Parse a DSN email and extract bounce info.

    Returns a dict with keys:
        - bounce_type: "hard" (5xx) or "soft" (4xx)
        - status_code: e.g. "5.1.1"
        - original_message_id: the Message-ID of the bounced email
        - diagnostic: human-readable diagnostic text
    Or None if the message is not a recognizable DSN.
    """
    msg = email.message_from_bytes(raw_bytes)

    # Check for DSN content type
    content_type = msg.get_content_type()
    is_dsn = content_type == "multipart/report"

    original_message_id = ""
    status_code = ""
    diagnostic = ""
    bounce_type = ""

    if is_dsn:
        # Walk the MIME parts looking for message/delivery-status
        for part in msg.walk():
            ct = part.get_content_type()

            if ct == "message/delivery-status":
                payload = part.get_payload()
                if isinstance(payload, list):
                    for sub in payload:
                        text = str(sub)
                        fields: dict = {}
                        _extract_dsn_fields(text, fields)
                        status_code = status_code or fields.get("status_code", "")
                        diagnostic = diagnostic or fields.get("diagnostic", "")
                elif isinstance(payload, str):
                    fields = {}
                    _extract_dsn_fields(payload, fields)
                    status_code = fields.get("status_code", "")
                    diagnostic = fields.get("diagnostic", "")

            elif ct == "message/rfc822":
                # Extract original Message-ID
                inner = part.get_payload()
                if isinstance(inner, list) and inner:
                    original_message_id = inner[0].get("Message-ID", "").strip()  # type: ignore[union-attr]
                elif isinstance(inner, Message):
                    original_message_id = inner.get("Message-ID", "").strip()
    else:
        # Fallback: check the body for bounce patterns
        body = _get_text_body(msg)
        if not body:
            return None
        status_match = _STATUS_RE.search(body)
        smtp_match = _SMTP_CODE_RE.search(body)
        if not status_match and not smtp_match:
            return None
        if status_match:
            status_code = status_match.group(0)
        elif smtp_match:
            status_code = smtp_match.group(0)
        diagnostic = body[:500]

    if not status_code:
        return None

    # Determine bounce type from status code
    first_digit = status_code[0]
    if first_digit == "5":
        bounce_type = "hard"
    elif first_digit == "4":
        bounce_type = "soft"
    else:
        return None

    # Try to find original message ID from headers if not found in DSN parts
    if not original_message_id:
        # Check In-Reply-To or References
        original_message_id = (
            msg.get("In-Reply-To", "").strip()
            or (msg.get("References", "").strip().split() or [""])[0]
        )

    return {
        "bounce_type": bounce_type,
        "status_code": status_code,
        "original_message_id": original_message_id,
        "diagnostic": diagnostic,
    }


def _extract_dsn_fields(text: str, out: dict) -> None:
    """Extract Status and Diagnostic-Code from DSN text block."""
    for line in text.splitlines():
        lower = line.lower().strip()
        if lower.startswith("status:"):
            code = line.split(":", 1)[1].strip()
            out["status_code"] = code
        elif lower.startswith("diagnostic-code:"):
            out["diagnostic"] = line.split(":", 1)[1].strip()


def _get_text_body(msg: Message) -> str:
    """Extract plain-text body from a message."""
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


async def process_bounce(
    db: Any,
    dsn: dict,
) -> None:
    """Process a parsed DSN bounce.

    Hard bounce (5xx):
        - Set email_status='invalid' on the lead
        - Set all campaign_leads for that lead to status='bounced'
    Soft bounce (4xx):
        - Increment retry count on the sent email
        - After MAX_SOFT_RETRIES, treat as hard bounce
    """
    message_id = dsn.get("original_message_id", "")
    if not message_id:
        log.warning("Bounce DSN has no original message ID, skipping")
        return

    sent_email = await queries.get_email_by_message_id(db, message_id)
    if sent_email is None:
        log.warning("No matching sent email for message_id=%s", message_id)
        return

    lead_id = sent_email.lead_id

    if dsn["bounce_type"] == "hard":
        await _handle_hard_bounce(db, sent_email.id, lead_id)
    else:
        await _handle_soft_bounce(db, sent_email.id, lead_id)


async def _handle_hard_bounce(
    db,
    email_sent_id: int,
    lead_id: int,
) -> None:
    """Hard bounce: invalidate lead email, cancel all campaigns for that lead."""
    from db.tables import CampaignLead, Lead

    # Mark the sent email as bounced
    await queries.update_email_status(db, email_sent_id, "bounced")

    # Mark lead email as invalid
    await Lead.update({Lead.email_status: "invalid"}).where(Lead.id == lead_id).run()

    # Cancel all campaign_leads for this lead
    await (
        CampaignLead.update({CampaignLead.status: "bounced"})
        .where(CampaignLead.lead_id == lead_id)
        .run()
    )

    log.info("Hard bounce: lead %d marked invalid, all campaigns bounced", lead_id)


async def _handle_soft_bounce(
    db,
    email_sent_id: int,
    lead_id: int,
) -> None:
    """Soft bounce: increment retry, escalate to hard after MAX_SOFT_RETRIES."""
    from db.tables import EmailSent

    # Check how many times this email has bounced
    rows = await EmailSent.raw(
        "SELECT COUNT(*) AS cnt FROM emails_sent WHERE lead_id = {} AND status = 'bounced'",
        lead_id,
    ).run()
    bounce_count = (rows[0]["cnt"] if rows else 0) + 1

    if bounce_count >= MAX_SOFT_RETRIES:
        log.info(
            "Soft bounce count %d >= %d for lead %d, escalating to hard bounce",
            bounce_count,
            MAX_SOFT_RETRIES,
            lead_id,
        )
        await _handle_hard_bounce(db, email_sent_id, lead_id)
    else:
        # Just mark this email as bounced
        await queries.update_email_status(db, email_sent_id, "bounced")
        log.info(
            "Soft bounce %d/%d for lead %d (email_sent=%d)",
            bounce_count,
            MAX_SOFT_RETRIES,
            lead_id,
            email_sent_id,
        )


async def check_bounces(db: Any, mb: object) -> int:
    """Poll IMAP for bounce DSNs and apply bounce processing.

    Returns the number of bounce messages processed.
    """
    host = getattr(mb, "imap_host", "")
    port = int(getattr(mb, "imap_port", 993))
    user = getattr(mb, "imap_user", "")
    password = getattr(mb, "imap_pass", "")

    if not host or not user or not password:
        return 0

    client: aioimaplib.IMAP4_SSL | None = None
    processed = 0

    try:
        client = aioimaplib.IMAP4_SSL(host=host, port=port)
        await client.wait_hello_from_server()
        await client.login(user, password)
        await client.select("INBOX")

        result, data = await client.uid("search", "UNSEEN")
        if result != "OK":
            log.warning("Bounce IMAP search failed for %s: %s", user, result)
            return 0

        uids: list[str] = []
        for item in data:
            if isinstance(item, bytes):
                uids.extend(item.decode(errors="ignore").split())
            elif isinstance(item, str):
                uids.extend(item.split())

        for uid in uids:
            fetch_result, fetch_data = await client.uid("fetch", uid, "(RFC822)")
            if fetch_result != "OK" or not fetch_data:
                continue

            raw: bytes | None = None
            for item in fetch_data:
                if isinstance(item, tuple) and len(item) >= 2:
                    body = item[1]
                    if isinstance(body, bytes):
                        raw = body
                        break
                    if isinstance(body, str):
                        raw = body.encode()
                        break
                elif isinstance(item, bytes):
                    raw = item
                    break

            if raw is None:
                continue

            dsn = parse_dsn(raw)
            if dsn is None:
                continue

            await process_bounce(db, dsn)
            processed += 1

            # Best effort: prevent repeated processing.
            with contextlib.suppress(Exception):
                await client.uid("store", uid, "+FLAGS", "(\\Seen)")

        return processed
    finally:
        if client is not None:
            with contextlib.suppress(Exception):
                await client.logout()
