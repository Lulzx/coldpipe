"""Sequence state machine: advance steps, log sends, handle replies."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from db import queries
from db.tables import CampaignLead, Deal, EmailSent

log = logging.getLogger(__name__)


async def advance_sequence(
    db: Any = None,
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
    to_email: str = "",
    from_email: str = "",
) -> int:
    """Advance a campaign-lead to the next step after a send.

    1. INSERT into emails_sent
    2. UPDATE campaign_leads (advance current_step, set last_sent_at, schedule next_send_at)
    3. INCREMENT daily_send_log

    Returns the emails_sent row id.
    """
    # 1. Log the sent email
    es = EmailSent(
        campaign_lead_id=campaign_lead_id,
        campaign_id=campaign_id,
        lead_id=lead_id,
        mailbox_id=mailbox_id,
        step_number=step_number,
        subject=subject,
        body_text=body,
        message_id=message_id,
        to_email=to_email,
        from_email=from_email,
        status="sent",
    )
    email_id = await queries.log_send(db, es)

    # 2. Advance the campaign-lead step and schedule the next send if another step exists.
    steps = await queries.get_sequence_steps(db, campaign_id)
    next_step = next((s for s in steps if s.step_number == step_number + 1), None)

    next_send_at: str | None = None
    if next_step:
        # delay_days is modeled on the step being sent next.
        delay = max(0, next_step.delay_days)
        send_at = datetime.now(UTC) + timedelta(days=delay)
        next_send_at = send_at.strftime("%Y-%m-%dT%H:%M:%SZ")

    await queries.advance_step(db, campaign_lead_id, next_send_at=next_send_at)
    if next_step is None:
        await queries.update_campaign_lead_status(db, campaign_lead_id, "completed")

    # 3. Increment daily send count
    await queries.increment_daily_send(db, mailbox_id)

    log.info(
        "Advanced campaign_lead %d to step %d (email_id=%d, next_send_at=%s)",
        campaign_lead_id,
        step_number + 1,
        email_id,
        next_send_at or "-",
    )
    return email_id


async def complete_sequence(
    db: Any = None,
    campaign_lead_id: int = 0,
) -> None:
    """Mark a campaign-lead as completed (all steps finished)."""
    await queries.update_campaign_lead_status(db, campaign_lead_id, "completed")
    log.info("Sequence completed for campaign_lead %d", campaign_lead_id)


async def handle_reply(
    db: Any = None,
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
        rows = (
            await CampaignLead.select(CampaignLead.id)
            .where((CampaignLead.campaign_id == campaign_id) & (CampaignLead.lead_id == lead_id))
            .run()
        )
        if rows:
            await queries.update_campaign_lead_status(db, rows[0]["id"], "replied")

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
