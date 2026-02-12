"""Sequence state machine: advance steps, log sends, handle replies."""

from __future__ import annotations

import logging

import aiosqlite

from db import queries
from db.models import Deal, EmailSent

log = logging.getLogger(__name__)


async def advance_sequence(
    db: aiosqlite.Connection,
    *,
    campaign_lead_id: int,
    campaign_id: int,
    lead_id: int,
    mailbox_id: int,
    step_number: int,
    subject: str,
    body: str,
    message_id: str,
    delay_days: int,
) -> int:
    """Advance a campaign-lead to the next step after a send.

    1. INSERT into emails_sent
    2. UPDATE campaign_leads (advance current_step, set last_sent_at)
    3. INCREMENT daily_send_log

    Returns the emails_sent row id.
    """
    # 1. Log the sent email
    es = EmailSent(
        campaign_id=campaign_id,
        lead_id=lead_id,
        mailbox_id=mailbox_id,
        step_number=step_number,
        subject=subject,
        body_text=body,
        message_id=message_id,
        status="sent",
    )
    email_id = await queries.log_send(db, es)

    # 2. Advance the campaign-lead step
    await queries.advance_step(db, campaign_lead_id)

    # 3. Increment daily send count
    await queries.increment_daily_send(db, mailbox_id)

    log.info(
        "Advanced campaign_lead %d to step %d (email_id=%d)",
        campaign_lead_id,
        step_number + 1,
        email_id,
    )
    return email_id


async def complete_sequence(
    db: aiosqlite.Connection,
    campaign_lead_id: int,
) -> None:
    """Mark a campaign-lead as completed (all steps finished)."""
    await queries.update_campaign_lead_status(db, campaign_lead_id, "completed")
    log.info("Sequence completed for campaign_lead %d", campaign_lead_id)


async def handle_reply(
    db: aiosqlite.Connection,
    *,
    email_sent_id: int,
    campaign_id: int | None,
    lead_id: int,
) -> int | None:
    """Handle an inbound reply.

    1. Update email status to 'replied'
    2. Set campaign_lead status to 'replied' (stops the sequence)
    3. Auto-create a deal with stage='replied'

    Returns the deal id, or None if no campaign context.
    """
    # 1. Mark the email as replied
    await queries.update_email_status(db, email_sent_id, "replied")

    # 2. Stop the sequence for this lead in the campaign
    if campaign_id is not None:
        cursor = await db.execute(
            "SELECT id FROM campaign_leads WHERE campaign_id = ? AND lead_id = ?",
            (campaign_id, lead_id),
        )
        row = await cursor.fetchone()
        if row:
            await queries.update_campaign_lead_status(db, row[0], "replied")

    # 3. Auto-create deal
    deal = Deal(
        lead_id=lead_id,
        campaign_id=campaign_id,
        stage="replied",
        notes="Auto-created from email reply",
    )
    deal_id = await queries.upsert_deal(db, deal)

    log.info(
        "Reply handled: email_sent=%d, lead=%d, deal=%d",
        email_sent_id,
        lead_id,
        deal_id,
    )
    return deal_id
