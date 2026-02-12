"""Tests for bounce parsing and processing."""

from __future__ import annotations

import pytest

from db import queries
from db.models import Campaign, EmailSent, Lead
from mailer.bounces import parse_dsn, process_bounce


def test_parse_dsn_fallback_plaintext():
    raw = (
        b"From: MAILER-DAEMON@example.com\r\n"
        b"In-Reply-To: <msg-plain-1>\r\n"
        b"Content-Type: text/plain; charset=utf-8\r\n"
        b"\r\n"
        b"550 5.1.1 User unknown\r\n"
    )
    dsn = parse_dsn(raw)
    assert dsn is not None
    assert dsn["bounce_type"] == "hard"
    assert dsn["status_code"] == "5.1.1"
    assert dsn["original_message_id"] == "<msg-plain-1>"


@pytest.mark.asyncio
async def test_process_hard_bounce_updates_records(db):
    lead_id = await queries.upsert_lead(
        db, Lead(email="hard-bounce@test.com", email_status="valid")
    )
    campaign_id = await queries.create_campaign(db, Campaign(name="Bounce Campaign"))
    await queries.enroll_lead(db, campaign_id, lead_id)
    await queries.log_send(
        db,
        EmailSent(
            campaign_id=campaign_id,
            lead_id=lead_id,
            message_id="<msg-hard-1>",
            to_email="hard-bounce@test.com",
            from_email="sender@test.com",
            body_text="Hello",
        ),
    )

    await process_bounce(
        db,
        {
            "bounce_type": "hard",
            "status_code": "5.1.1",
            "original_message_id": "<msg-hard-1>",
            "diagnostic": "User unknown",
        },
    )

    lead = await queries.get_lead_by_id(db, lead_id)
    assert lead is not None
    assert lead.email_status == "invalid"

    sent = await queries.get_email_by_message_id(db, "<msg-hard-1>")
    assert sent is not None
    assert sent.status == "bounced"

    campaign_leads = await queries.get_campaign_leads(db, campaign_id)
    assert campaign_leads[0].status == "bounced"
