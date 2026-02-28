"""Async query functions using Piccolo ORM.

All functions accept an optional `db` parameter for backward compatibility
with the transition period. This parameter is ignored — queries go through
the Piccolo engine.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from shared.crypto import decrypt, encrypt

from .tables import (
    Campaign,
    CampaignLead,
    DailySendLog,
    Deal,
    EmailSent,
    Lead,
    Mailbox,
    McpActivity,
    SequenceStep,
    Session,
    TrackingEvent,
    User,
)


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _escape_like(value: str) -> str:
    """Escape special LIKE characters (%, _, \\) for safe use in LIKE clauses."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


# ---------------------------------------------------------------------------
# Leads
# ---------------------------------------------------------------------------


async def upsert_lead(db: Any = None, lead: Any = None, **kw) -> int:
    """Insert or update a lead by email. Returns the lead id."""
    # Support both positional (db, lead) and keyword (lead=lead) calling
    if lead is None and db is not None and hasattr(db, "email"):
        lead = db
        db = None

    await Lead.raw(
        """INSERT INTO leads (email, first_name, last_name, company, job_title, website,
                              phone, address, city, state, zip, source, source_url,
                              email_status, enriched_at, validated_at, tags, notes,
                              email_confidence, email_source, email_provider)
           VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})
           ON CONFLICT(email) DO UPDATE SET
               first_name       = CASE WHEN excluded.first_name   != '' THEN excluded.first_name   ELSE leads.first_name   END,
               last_name        = CASE WHEN excluded.last_name    != '' THEN excluded.last_name    ELSE leads.last_name    END,
               company          = CASE WHEN excluded.company      != '' THEN excluded.company      ELSE leads.company      END,
               job_title        = CASE WHEN excluded.job_title != '' THEN excluded.job_title ELSE leads.job_title END,
               website          = CASE WHEN excluded.website      != '' THEN excluded.website      ELSE leads.website      END,
               phone            = CASE WHEN excluded.phone        != '' THEN excluded.phone        ELSE leads.phone        END,
               address          = CASE WHEN excluded.address      != '' THEN excluded.address      ELSE leads.address      END,
               city             = CASE WHEN excluded.city         != '' THEN excluded.city         ELSE leads.city         END,
               state            = CASE WHEN excluded.state        != '' THEN excluded.state        ELSE leads.state        END,
               zip              = CASE WHEN excluded.zip          != '' THEN excluded.zip          ELSE leads.zip          END,
               source           = CASE WHEN excluded.source       != '' THEN excluded.source       ELSE leads.source       END,
               source_url       = CASE WHEN excluded.source_url   != '' THEN excluded.source_url   ELSE leads.source_url   END,
               email_status     = CASE WHEN excluded.email_status != 'unknown' THEN excluded.email_status ELSE leads.email_status END,
               enriched_at      = COALESCE(excluded.enriched_at, leads.enriched_at),
               validated_at     = COALESCE(excluded.validated_at, leads.validated_at),
               tags             = CASE WHEN excluded.tags         != '' THEN excluded.tags         ELSE leads.tags         END,
               notes            = CASE WHEN excluded.notes        != '' THEN excluded.notes        ELSE leads.notes        END,
               email_confidence = CASE WHEN excluded.email_confidence > 0.0 THEN excluded.email_confidence ELSE leads.email_confidence END,
               email_source     = CASE WHEN excluded.email_source     != '' THEN excluded.email_source     ELSE leads.email_source     END,
               email_provider   = CASE WHEN excluded.email_provider   != '' THEN excluded.email_provider   ELSE leads.email_provider   END
        """,
        lead.email,
        lead.first_name,
        lead.last_name,
        lead.company,
        lead.job_title,
        lead.website,
        lead.phone,
        lead.address,
        lead.city,
        lead.state,
        lead.zip,
        lead.source,
        lead.source_url,
        lead.email_status,
        lead.enriched_at,
        lead.validated_at,
        lead.tags,
        lead.notes,
        lead.email_confidence,
        lead.email_source,
        lead.email_provider,
    ).run()

    rows = await Lead.select(Lead.id).where(Lead.email == lead.email).run()
    if not rows:
        raise RuntimeError(f"Lead not found after upsert: {lead.email}")
    return rows[0]["id"]


async def upsert_leads_batch(db: Any = None, leads: list | None = None, **kw) -> int:
    """Batch upsert leads. Returns count of rows affected."""
    if leads is None and db is not None and isinstance(db, list):
        leads = db
        db = None

    count = 0
    for lead in leads or []:
        await upsert_lead(lead=lead)
        count += 1
    return count


async def get_leads(
    db: Any = None,
    *,
    limit: int = 100,
    offset: int = 0,
    email_status: str | None = None,
    source: str | None = None,
) -> list[Lead]:
    """Fetch leads with optional filters."""
    query = Lead.select().order_by(Lead.id, ascending=False)
    if email_status:
        query = query.where(Lead.email_status == email_status)
    if source:
        query = query.where(Lead.source == source)
    query = query.limit(limit).offset(offset)
    rows = await query.run()
    return [Lead(**r) for r in rows]


async def get_lead_by_id(db: Any = None, lead_id: int = 0) -> Lead | None:
    if lead_id == 0 and db is not None and isinstance(db, int):
        lead_id = db
        db = None
    rows = await Lead.select().where(Lead.id == lead_id).run()
    if not rows:
        return None
    return Lead(**rows[0])


async def get_lead_by_email(db: Any = None, email: str = "") -> Lead | None:
    if email == "" and db is not None and isinstance(db, str):
        email = db
        db = None
    rows = await Lead.select().where(Lead.email == email).run()
    if not rows:
        return None
    return Lead(**rows[0])


async def search_leads(db: Any = None, query: str = "", *, limit: int = 50) -> list[Lead]:
    """Search leads by email, name, or company (LIKE match)."""
    if query == "" and db is not None and isinstance(db, str):
        query = db
        db = None
    pattern = f"%{_escape_like(query)}%"
    rows = await Lead.raw(
        """SELECT * FROM leads
           WHERE email LIKE {} ESCAPE '\\' OR first_name LIKE {} ESCAPE '\\'
                 OR last_name LIKE {} ESCAPE '\\' OR company LIKE {} ESCAPE '\\'
                 OR website LIKE {} ESCAPE '\\'
           ORDER BY id DESC LIMIT {}""",
        pattern,
        pattern,
        pattern,
        pattern,
        pattern,
        limit,
    ).run()
    return [Lead(**r) for r in rows]


async def count_leads(db: Any = None, *, email_status: str | None = None) -> int:
    query = Lead.count()
    if email_status:
        query = query.where(Lead.email_status == email_status)
    return await query.run()


async def delete_lead(db: Any = None, lead_id: int = 0) -> bool:
    if lead_id == 0 and db is not None and isinstance(db, int):
        lead_id = db
        db = None
    result = await Lead.delete().where(Lead.id == lead_id).run()
    return len(result) >= 0  # delete always succeeds if no error


# ---------------------------------------------------------------------------
# Mailboxes
# ---------------------------------------------------------------------------


async def upsert_mailbox(db: Any = None, mb: Any = None, **kw) -> int:
    if mb is None and db is not None and hasattr(db, "smtp_host"):
        mb = db
        db = None

    smtp_pass = encrypt(mb.smtp_pass)
    imap_pass = encrypt(mb.imap_pass)
    await Mailbox.raw(
        """INSERT INTO mailboxes (email, smtp_host, smtp_port, smtp_user, smtp_pass,
                                   imap_host, imap_port, imap_user, imap_pass,
                                   daily_limit, warmup_day, is_active, display_name)
           VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})
           ON CONFLICT(email) DO UPDATE SET
               smtp_host = excluded.smtp_host, smtp_port = excluded.smtp_port,
               smtp_user = excluded.smtp_user, smtp_pass = excluded.smtp_pass,
               imap_host = excluded.imap_host, imap_port = excluded.imap_port,
               imap_user = excluded.imap_user, imap_pass = excluded.imap_pass,
               daily_limit = excluded.daily_limit, warmup_day = excluded.warmup_day,
               is_active = excluded.is_active, display_name = excluded.display_name
        """,
        mb.email,
        mb.smtp_host,
        mb.smtp_port,
        mb.smtp_user,
        smtp_pass,
        mb.imap_host,
        mb.imap_port,
        mb.imap_user,
        imap_pass,
        mb.daily_limit,
        mb.warmup_day,
        mb.is_active,
        mb.display_name,
    ).run()

    rows = await Mailbox.select(Mailbox.id).where(Mailbox.email == mb.email).run()
    if not rows:
        raise RuntimeError(f"Mailbox not found after upsert: {mb.email}")
    return rows[0]["id"]


async def get_mailboxes(db: Any = None, *, active_only: bool = False) -> list[Mailbox]:
    query = Mailbox.select().order_by(Mailbox.id)
    if active_only:
        query = query.where(Mailbox.is_active == 1)
    rows = await query.run()
    mailboxes = [Mailbox(**r) for r in rows]
    for m in mailboxes:
        m.smtp_pass = decrypt(m.smtp_pass)
        m.imap_pass = decrypt(m.imap_pass)
    return mailboxes


async def get_mailbox_by_id(db: Any = None, mailbox_id: int = 0) -> Mailbox | None:
    if mailbox_id == 0 and db is not None and isinstance(db, int):
        mailbox_id = db
        db = None
    rows = await Mailbox.select().where(Mailbox.id == mailbox_id).run()
    if not rows:
        return None
    mb = Mailbox(**rows[0])
    mb.smtp_pass = decrypt(mb.smtp_pass)
    mb.imap_pass = decrypt(mb.imap_pass)
    return mb


async def encrypt_existing_passwords(db: Any = None) -> int:
    """One-time migration: encrypt any plaintext mailbox passwords."""
    rows = await Mailbox.select(Mailbox.id, Mailbox.smtp_pass, Mailbox.imap_pass).run()
    count = 0
    for row in rows:
        mid, smtp_pass, imap_pass = row["id"], row["smtp_pass"], row["imap_pass"]
        new_smtp = encrypt(smtp_pass)
        new_imap = encrypt(imap_pass)
        if new_smtp != smtp_pass or new_imap != imap_pass:
            await (
                Mailbox.update(
                    {
                        Mailbox.smtp_pass: new_smtp,
                        Mailbox.imap_pass: new_imap,
                    }
                )
                .where(Mailbox.id == mid)
                .run()
            )
            count += 1
    return count


# ---------------------------------------------------------------------------
# Campaigns
# ---------------------------------------------------------------------------


async def create_campaign(db: Any = None, camp: Any = None, **kw) -> int:
    if camp is None and db is not None and hasattr(db, "name"):
        camp = db
        db = None

    result = await Campaign.insert(
        Campaign(
            name=camp.name,
            status=camp.status,
            mailbox_id=camp.mailbox_id,
            daily_limit=camp.daily_limit,
            timezone=camp.timezone,
            send_window_start=camp.send_window_start,
            send_window_end=camp.send_window_end,
        )
    ).run()
    return result[0]["id"]


async def get_campaigns(db: Any = None, *, status: str | None = None) -> list[Campaign]:
    query = Campaign.select().order_by(Campaign.id, ascending=False)
    if status:
        query = query.where(Campaign.status == status)
    rows = await query.run()
    return [Campaign(**r) for r in rows]


async def get_campaign_by_id(db: Any = None, campaign_id: int = 0) -> Campaign | None:
    if campaign_id == 0 and db is not None and isinstance(db, int):
        campaign_id = db
        db = None
    rows = await Campaign.select().where(Campaign.id == campaign_id).run()
    if not rows:
        return None
    return Campaign(**rows[0])


async def update_campaign_status(db: Any = None, campaign_id: int = 0, status: str = "") -> bool:
    if campaign_id == 0 and db is not None and isinstance(db, int):
        campaign_id = db
        db = None
        status = status or ""
    await Campaign.update({Campaign.status: status}).where(Campaign.id == campaign_id).run()
    return True


# ---------------------------------------------------------------------------
# Sequence steps
# ---------------------------------------------------------------------------


async def add_sequence_step(db: Any = None, step: Any = None, **kw) -> int:
    if step is None and db is not None and hasattr(db, "step_number"):
        step = db
        db = None

    result = await SequenceStep.insert(
        SequenceStep(
            campaign_id=step.campaign_id,
            step_number=step.step_number,
            template_name=step.template_name,
            subject=step.subject,
            delay_days=step.delay_days,
            is_reply=step.is_reply,
        )
    ).run()
    return result[0]["id"]


async def get_sequence_steps(db: Any = None, campaign_id: int = 0) -> list[SequenceStep]:
    if campaign_id == 0 and db is not None and isinstance(db, int):
        campaign_id = db
        db = None
    rows = (
        await SequenceStep.select()
        .where(SequenceStep.campaign_id == campaign_id)
        .order_by(SequenceStep.step_number)
        .run()
    )
    return [SequenceStep(**r) for r in rows]


# ---------------------------------------------------------------------------
# Campaign leads
# ---------------------------------------------------------------------------


async def enroll_lead(db: Any = None, campaign_id: int = 0, lead_id: int = 0) -> int:
    if campaign_id == 0 and db is not None and isinstance(db, int):
        campaign_id = db
        db = None

    await CampaignLead.raw(
        """INSERT INTO campaign_leads (campaign_id, lead_id) VALUES ({}, {})
           ON CONFLICT(campaign_id, lead_id) DO NOTHING""",
        campaign_id,
        lead_id,
    ).run()

    rows = (
        await CampaignLead.select(CampaignLead.id)
        .where((CampaignLead.campaign_id == campaign_id) & (CampaignLead.lead_id == lead_id))
        .run()
    )
    return rows[0]["id"] if rows else 0


async def get_campaign_leads(
    db: Any = None,
    campaign_id: int = 0,
    *,
    status: str | None = None,
) -> list[CampaignLead]:
    if campaign_id == 0 and db is not None and isinstance(db, int):
        campaign_id = db
        db = None

    query = (
        CampaignLead.select()
        .where(CampaignLead.campaign_id == campaign_id)
        .order_by(CampaignLead.id)
    )
    if status:
        query = query.where(CampaignLead.status == status)
    rows = await query.run()
    return [CampaignLead(**r) for r in rows]


async def advance_step(
    db: Any = None,
    campaign_lead_id: int = 0,
    *,
    next_send_at: str | None = None,
) -> bool:
    """Increment current_step and update last_sent_at."""
    if campaign_lead_id == 0 and db is not None and isinstance(db, int):
        campaign_lead_id = db
        db = None

    await CampaignLead.raw(
        """UPDATE campaign_leads
           SET current_step = current_step + 1,
               last_sent_at = {},
               next_send_at = {}
           WHERE id = {}""",
        _now(),
        next_send_at,
        campaign_lead_id,
    ).run()
    return True


async def update_campaign_lead_status(
    db: Any = None, campaign_lead_id: int = 0, status: str = ""
) -> bool:
    if campaign_lead_id == 0 and db is not None and isinstance(db, int):
        campaign_lead_id = db
        db = None
        status = status or ""
    await (
        CampaignLead.update(
            {
                CampaignLead.status: status,
            }
        )
        .where(CampaignLead.id == campaign_lead_id)
        .run()
    )
    return True


# ---------------------------------------------------------------------------
# Send queue
# ---------------------------------------------------------------------------


async def get_send_queue(
    db: Any = None,
    campaign_id: int = 0,
    *,
    limit: int = 50,
) -> list[dict]:
    """Get leads ready to receive the next email in a campaign sequence."""
    if campaign_id == 0 and db is not None and isinstance(db, int):
        campaign_id = db
        db = None

    rows = await CampaignLead.raw(
        """SELECT cl.id AS cl_id, cl.campaign_id, cl.lead_id, cl.current_step,
                  l.email, l.first_name, l.last_name, l.company, l.website,
                  ss.template_name, ss.subject, ss.delay_days, ss.is_reply
           FROM campaign_leads cl
           JOIN leads l ON l.id = cl.lead_id
           JOIN sequence_steps ss ON ss.campaign_id = cl.campaign_id
                                  AND ss.step_number = cl.current_step
           WHERE cl.campaign_id = {}
             AND cl.status = 'active'
             AND (cl.next_send_at IS NULL OR cl.next_send_at <= strftime('%Y-%m-%dT%H:%M:%SZ','now'))
           ORDER BY cl.id
           LIMIT {}""",
        campaign_id,
        limit,
    ).run()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Emails sent
# ---------------------------------------------------------------------------


async def log_send(db: Any = None, es: Any = None, **kw) -> int:
    if es is None and db is not None and hasattr(db, "message_id"):
        es = db
        db = None

    result = await EmailSent.insert(
        EmailSent(
            campaign_lead_id=es.campaign_lead_id,
            campaign_id=es.campaign_id,
            lead_id=es.lead_id,
            mailbox_id=es.mailbox_id,
            step_number=es.step_number,
            message_id=es.message_id,
            subject=es.subject,
            to_email=es.to_email,
            from_email=es.from_email,
            body_text=es.body_text,
            status=es.status,
        )
    ).run()
    return result[0]["id"]


async def update_email_status(db: Any = None, email_id: int = 0, status: str = "") -> bool:
    if email_id == 0 and db is not None and isinstance(db, int):
        email_id = db
        db = None
        status = status or ""

    updates: dict = {EmailSent.status: status}
    if status == "replied":
        updates[EmailSent.replied_at] = _now()
    elif status == "bounced":
        updates[EmailSent.bounced_at] = _now()

    await EmailSent.update(updates).where(EmailSent.id == email_id).run()
    return True


async def get_emails_for_lead(db: Any = None, lead_id: int = 0) -> list[EmailSent]:
    if lead_id == 0 and db is not None and isinstance(db, int):
        lead_id = db
        db = None
    rows = (
        await EmailSent.select()
        .where(EmailSent.lead_id == lead_id)
        .order_by(EmailSent.sent_at, ascending=False)
        .run()
    )
    return [EmailSent(**r) for r in rows]


async def get_emails_sent(
    db: Any = None,
    *,
    limit: int = 50,
    offset: int = 0,
    status: str | None = None,
) -> list[EmailSent]:
    """Fetch sent emails with optional status filter."""
    query = EmailSent.select().order_by(EmailSent.id, ascending=False)
    if status:
        query = query.where(EmailSent.status == status)
    query = query.limit(limit).offset(offset)
    rows = await query.run()
    return [EmailSent(**r) for r in rows]


async def count_emails_sent(db: Any = None, *, status: str | None = None) -> int:
    """Count sent emails with optional status filter."""
    query = EmailSent.count()
    if status:
        query = query.where(EmailSent.status == status)
    return await query.run()


async def get_email_by_message_id(db: Any = None, message_id: str = "") -> EmailSent | None:
    if message_id == "" and db is not None and isinstance(db, str):
        message_id = db
        db = None
    rows = await EmailSent.select().where(EmailSent.message_id == message_id).run()
    if not rows:
        return None
    return EmailSent(**rows[0])


# ---------------------------------------------------------------------------
# Daily send limit
# ---------------------------------------------------------------------------


async def check_daily_limit(db: Any = None, mailbox_id: int = 0) -> tuple[int, int]:
    """Returns (sent_today, daily_limit)."""
    if mailbox_id == 0 and db is not None and isinstance(db, int):
        mailbox_id = db
        db = None

    today = datetime.now(UTC).strftime("%Y-%m-%d")
    rows = (
        await DailySendLog.select(DailySendLog.send_count)
        .where((DailySendLog.mailbox_id == mailbox_id) & (DailySendLog.send_date == today))
        .run()
    )
    sent = rows[0]["count"] if rows else 0

    rows2 = await Mailbox.select(Mailbox.daily_limit).where(Mailbox.id == mailbox_id).run()
    limit = rows2[0]["daily_limit"] if rows2 else 30
    return sent, limit


async def increment_daily_send(db: Any = None, mailbox_id: int = 0) -> int:
    """Increment today's send count. Returns new count."""
    if mailbox_id == 0 and db is not None and isinstance(db, int):
        mailbox_id = db
        db = None

    today = datetime.now(UTC).strftime("%Y-%m-%d")
    await DailySendLog.raw(
        """INSERT INTO daily_send_log (mailbox_id, send_date, count) VALUES ({}, {}, 1)
           ON CONFLICT(mailbox_id, send_date) DO UPDATE SET count = count + 1""",
        mailbox_id,
        today,
    ).run()

    rows = (
        await DailySendLog.select(DailySendLog.send_count)
        .where((DailySendLog.mailbox_id == mailbox_id) & (DailySendLog.send_date == today))
        .run()
    )
    return rows[0]["count"] if rows else 0


# ---------------------------------------------------------------------------
# Deals
# ---------------------------------------------------------------------------


async def upsert_deal(db: Any = None, deal: Any = None, **kw) -> int:
    if deal is None and db is not None and hasattr(db, "stage"):
        deal = db
        db = None

    if isinstance(deal.id, int) and deal.id:
        await (
            Deal.update(
                {
                    Deal.stage: deal.stage,
                    Deal.value: deal.value,
                    Deal.close_date: deal.close_date,
                    Deal.loss_reason: deal.loss_reason,
                    Deal.notes: deal.notes,
                }
            )
            .where(Deal.id == deal.id)
            .run()
        )
        return deal.id

    result = await Deal.insert(
        Deal(
            lead_id=deal.lead_id,
            campaign_id=deal.campaign_id,
            stage=deal.stage,
            value=deal.value,
            close_date=deal.close_date,
            loss_reason=deal.loss_reason,
            notes=deal.notes,
        )
    ).run()
    return result[0]["id"]


async def get_deals(db: Any = None, *, stage: str | None = None) -> list[Deal]:
    query = Deal.select().order_by(Deal.id, ascending=False)
    if stage:
        query = query.where(Deal.stage == stage)
    rows = await query.run()
    return [Deal(**r) for r in rows]


async def get_deal_by_id(db: Any = None, deal_id: int = 0) -> Deal | None:
    if deal_id == 0 and db is not None and isinstance(db, int):
        deal_id = db
        db = None
    rows = await Deal.select().where(Deal.id == deal_id).run()
    if not rows:
        return None
    return Deal(**rows[0])


# ---------------------------------------------------------------------------
# Tracking events
# ---------------------------------------------------------------------------


async def log_tracking_event(db: Any = None, evt: Any = None, **kw) -> int:
    if evt is None and db is not None and hasattr(db, "event_type"):
        evt = db
        db = None

    result = await TrackingEvent.insert(
        TrackingEvent(
            email_sent_id=evt.email_sent_id,
            event_type=evt.event_type,
            metadata=evt.metadata,
        )
    ).run()
    return result[0]["id"]


# ---------------------------------------------------------------------------
# Stats / reporting
# ---------------------------------------------------------------------------


async def get_pipeline_stats(db: Any = None) -> dict:
    """Get deal pipeline counts by stage."""
    rows = await Deal.raw(
        "SELECT stage, COUNT(*) as cnt FROM deals GROUP BY stage ORDER BY stage"
    ).run()
    return {row["stage"]: row["cnt"] for row in rows}


async def get_daily_stats(db: Any = None, days: int = 30) -> list[dict]:
    """Get email send stats per day for the last N days."""
    rows = await EmailSent.raw(
        """SELECT DATE(sent_at) as day, status, COUNT(*) as cnt
           FROM emails_sent
           WHERE sent_at >= DATE('now', {})
           GROUP BY day, status
           ORDER BY day DESC""",
        f"-{days} days",
    ).run()
    return [dict(r) for r in rows]


async def get_lead_stats(db: Any = None) -> dict:
    """Get lead counts grouped by email_status and source."""
    result: dict = {}

    rows = await Lead.raw(
        "SELECT email_status, COUNT(*) as cnt FROM leads GROUP BY email_status"
    ).run()
    result["by_status"] = {row["email_status"]: row["cnt"] for row in rows}

    rows = await Lead.raw(
        "SELECT source, COUNT(*) as cnt FROM leads GROUP BY source ORDER BY cnt DESC"
    ).run()
    result["by_source"] = {row["source"]: row["cnt"] for row in rows}

    rows = await Lead.raw(
        """SELECT city, state, COUNT(*) as cnt FROM leads WHERE city != ''
           GROUP BY city, state ORDER BY cnt DESC LIMIT 20"""
    ).run()
    result["by_city"] = {f"{row['city']}, {row['state']}": row["cnt"] for row in rows}

    total = await Lead.count().run()
    result["total"] = total
    return result


async def tag_leads(db: Any = None, lead_ids: list[int] | None = None, tag: str = "") -> int:
    """Add a tag to multiple leads. Returns count updated."""
    if lead_ids is None and db is not None and isinstance(db, list):
        lead_ids = db
        db = None

    count = 0
    for lid in lead_ids or []:
        rows = await Lead.select(Lead.tags).where(Lead.id == lid).run()
        if not rows:
            continue
        existing = rows[0]["tags"] or ""
        tags_list = [t.strip() for t in existing.split(",") if t.strip()]
        if tag not in tags_list:
            tags_list.append(tag)
            await Lead.update({Lead.tags: ",".join(tags_list)}).where(Lead.id == lid).run()
            count += 1
    return count


async def deactivate_mailbox(db: Any = None, mailbox_id: int = 0) -> bool:
    if mailbox_id == 0 and db is not None and isinstance(db, int):
        mailbox_id = db
        db = None
    await Mailbox.update({Mailbox.is_active: 0}).where(Mailbox.id == mailbox_id).run()
    return True


async def delete_campaign(db: Any = None, campaign_id: int = 0) -> bool:
    if campaign_id == 0 and db is not None and isinstance(db, int):
        campaign_id = db
        db = None
    await Campaign.delete().where(Campaign.id == campaign_id).run()
    return True


async def enroll_leads_by_filter(
    db: Any = None,
    campaign_id: int = 0,
    *,
    city: str | None = None,
    state: str | None = None,
    email_status: str | None = None,
    tag: str | None = None,
) -> int:
    """Enroll leads matching filters into a campaign. Returns count enrolled."""
    if campaign_id == 0 and db is not None and isinstance(db, int):
        campaign_id = db
        db = None

    query = Lead.select(Lead.id)
    if city:
        query = query.where(Lead.city == city)
    if state:
        query = query.where(Lead.state == state)
    if email_status:
        query = query.where(Lead.email_status == email_status)
    if tag:
        query = query.where(Lead.tags.like(f"%{tag}%"))

    rows = await query.run()
    count = 0
    for row in rows:
        result = await enroll_lead(campaign_id=campaign_id, lead_id=row["id"])
        if result:
            count += 1
    return count


async def get_all_send_queues(db: Any = None, *, limit: int = 50) -> list[dict]:
    """Get send queue across all active campaigns."""
    rows = await CampaignLead.raw(
        """SELECT cl.id AS cl_id, cl.campaign_id, cl.lead_id, cl.current_step,
                  l.email, l.first_name, l.last_name, l.company, l.website,
                  l.city, l.state, l.job_title,
                  ss.template_name, ss.subject, ss.delay_days, ss.is_reply,
                  c.mailbox_id
           FROM campaign_leads cl
           JOIN leads l ON l.id = cl.lead_id
           JOIN campaigns c ON c.id = cl.campaign_id
           JOIN sequence_steps ss ON ss.campaign_id = cl.campaign_id
                                  AND ss.step_number = cl.current_step
           WHERE cl.status = 'active'
             AND c.status = 'active'
             AND l.email IS NOT NULL
             AND l.email_status IN ('valid', 'catch_all', 'unknown')
             AND (cl.next_send_at IS NULL OR cl.next_send_at <= {})
           ORDER BY cl.id
           LIMIT {}""",
        _now(),
        limit,
    ).run()
    return [dict(r) for r in rows]


def get_warmup_limit(warmup_day: int) -> int:
    """Return daily send limit based on warmup day."""
    if warmup_day <= 3:
        return 5
    if warmup_day <= 7:
        return 10
    if warmup_day <= 14:
        return 20
    if warmup_day <= 21:
        return 30
    return min(40 + (warmup_day - 22), 50)


async def get_deal_stats(db: Any = None) -> dict:
    """Get deal pipeline stats."""
    rows = await Deal.raw(
        "SELECT stage, COUNT(*) as count, SUM(value) as total_value FROM deals GROUP BY stage ORDER BY stage"
    ).run()
    stages = {}
    for row in rows:
        stages[row["stage"]] = {"count": row["count"], "value": row["total_value"] or 0.0}
    total_pipeline = sum(s["value"] for s in stages.values())
    total_closed = stages.get("closed_won", {}).get("value", 0.0)
    return {"stages": stages, "pipeline_value": total_pipeline, "closed_value": total_closed}


async def get_today_activity(db: Any = None) -> dict:
    """Get today's email activity summary."""
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    rows = await EmailSent.raw(
        """SELECT
               COUNT(*) as sent,
               SUM(CASE WHEN status = 'replied' THEN 1 ELSE 0 END) as replies,
               SUM(CASE WHEN status = 'bounced' THEN 1 ELSE 0 END) as bounces
           FROM emails_sent WHERE DATE(sent_at) = {}""",
        today,
    ).run()
    if not rows:
        return {"sent": 0, "replies": 0, "bounces": 0}
    row = rows[0]
    return {
        "sent": row["sent"] or 0,
        "replies": row["replies"] or 0,
        "bounces": row["bounces"] or 0,
    }


async def get_email_status_distribution(db: Any = None) -> dict[str, int]:
    """Get count of emails by status."""
    rows = await EmailSent.raw(
        "SELECT status, COUNT(*) as cnt FROM emails_sent GROUP BY status"
    ).run()
    return {row["status"]: row["cnt"] for row in rows}


async def get_campaign_step_distribution(
    db: Any = None, campaign_id: int = 0
) -> dict[int, dict[str, int]]:
    """Get lead counts by step and status for a campaign."""
    if campaign_id == 0 and db is not None and isinstance(db, int):
        campaign_id = db
        db = None

    rows = await CampaignLead.raw(
        """SELECT current_step, status, COUNT(*) as cnt
           FROM campaign_leads
           WHERE campaign_id = {}
           GROUP BY current_step, status""",
        campaign_id,
    ).run()
    result: dict[int, dict[str, int]] = {}
    for row in rows:
        step, status, cnt = row["current_step"], row["status"], row["cnt"]
        if step not in result:
            result[step] = {}
        result[step][status] = cnt
    return result


async def get_campaign_stats(db: Any = None, campaign_id: int = 0) -> dict:
    """Get stats for a specific campaign."""
    if campaign_id == 0 and db is not None and isinstance(db, int):
        campaign_id = db
        db = None

    rows = await CampaignLead.raw(
        """SELECT
               COUNT(*) as total,
               SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) as active,
               SUM(CASE WHEN status = 'replied' THEN 1 ELSE 0 END) as replied,
               SUM(CASE WHEN status = 'bounced' THEN 1 ELSE 0 END) as bounced,
               SUM(CASE WHEN status = 'unsubscribed' THEN 1 ELSE 0 END) as unsubscribed,
               SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
               SUM(CASE WHEN status = 'paused' THEN 1 ELSE 0 END) as paused
           FROM campaign_leads WHERE campaign_id = {}""",
        campaign_id,
    ).run()
    if not rows:
        return {}
    return dict(rows[0])


# ---------------------------------------------------------------------------
# Users & Sessions (auth)
# ---------------------------------------------------------------------------


async def get_user_count(db: Any = None) -> int:
    return await User.count().run()


async def get_user_by_id(db: Any = None, user_id: int = 0) -> User | None:
    if user_id == 0 and db is not None and isinstance(db, int):
        user_id = db
        db = None
    rows = await User.select().where(User.id == user_id).run()
    if not rows:
        return None
    return User(**rows[0])


async def get_user_by_username(db: Any = None, username: str = "") -> User | None:
    if username == "" and db is not None and isinstance(db, str):
        username = db
        db = None
    rows = await User.select().where(User.username == username).run()
    if not rows:
        return None
    return User(**rows[0])


async def create_user(db: Any = None, user: Any = None, **kw) -> int:
    if user is None and db is not None and hasattr(db, "username"):
        user = db
        db = None

    result = await User.insert(
        User(
            username=user.username,
            webauthn_credential_id=user.webauthn_credential_id,
            webauthn_public_key=user.webauthn_public_key,
            webauthn_sign_count=user.webauthn_sign_count,
            onboarding_completed=user.onboarding_completed,
        )
    ).run()
    return result[0]["id"]


async def update_user_credential(
    db: Any = None,
    user_id: int = 0,
    *,
    credential_id: str,
    public_key: str,
    sign_count: int,
) -> None:
    if user_id == 0 and db is not None and isinstance(db, int):
        user_id = db
        db = None
    await (
        User.update(
            {
                User.webauthn_credential_id: credential_id,
                User.webauthn_public_key: public_key,
                User.webauthn_sign_count: sign_count,
            }
        )
        .where(User.id == user_id)
        .run()
    )


async def update_user_sign_count(db: Any = None, user_id: int = 0, sign_count: int = 0) -> None:
    if user_id == 0 and db is not None and isinstance(db, int):
        user_id = db
        db = None
        sign_count = sign_count or 0
    await (
        User.update(
            {
                User.webauthn_sign_count: sign_count,
            }
        )
        .where(User.id == user_id)
        .run()
    )


async def set_onboarding_completed(db: Any = None, user_id: int = 0) -> None:
    if user_id == 0 and db is not None and isinstance(db, int):
        user_id = db
        db = None
    await (
        User.update(
            {
                User.onboarding_completed: 1,
            }
        )
        .where(User.id == user_id)
        .run()
    )


async def create_session(
    db: Any = None, token: str = "", user_id: int = 0, expires_at: str = ""
) -> int:
    if token == "" and db is not None and isinstance(db, str):
        token = db
        db = None

    result = await Session.insert(
        Session(token=token, user_id=user_id, expires_at=expires_at)
    ).run()
    return result[0]["id"]


async def get_session_by_token(db: Any = None, token: str = "") -> Session | None:
    if token == "" and db is not None and isinstance(db, str):
        token = db
        db = None
    rows = await Session.select().where(Session.token == token).run()
    if not rows:
        return None
    return Session(**rows[0])


async def delete_session(db: Any = None, token: str = "") -> None:
    if token == "" and db is not None and isinstance(db, str):
        token = db
        db = None
    await Session.delete().where(Session.token == token).run()


async def cleanup_expired_sessions(db: Any = None) -> int:
    rows = await Session.raw(
        "DELETE FROM sessions WHERE expires_at < {} RETURNING id",
        _now(),
    ).run()
    return len(rows)


# ---------------------------------------------------------------------------
# MCP Activity logging
# ---------------------------------------------------------------------------


async def log_mcp_activity(
    tool_name: str,
    params: str = "",
    result_summary: str = "",
    status: str = "running",
    error: str | None = None,
    duration_ms: int = 0,
) -> int:
    """Insert an MCP activity row and return its id."""
    result = await McpActivity.insert(
        McpActivity(
            tool_name=tool_name,
            params=params,
            result_summary=result_summary,
            status=status,
            error=error,
            duration_ms=duration_ms,
        )
    ).run()
    return result[0]["id"]


async def update_mcp_activity(
    row_id: int,
    status: str,
    result_summary: str = "",
    duration_ms: int = 0,
    error: str | None = None,
) -> None:
    """Update an existing MCP activity row after completion."""
    updates: dict = {
        McpActivity.status: status,
        McpActivity.result_summary: result_summary,
        McpActivity.duration_ms: duration_ms,
    }
    if error is not None:
        updates[McpActivity.error] = error
    await McpActivity.update(updates).where(McpActivity.id == row_id).run()


async def get_mcp_activity(limit: int = 50, offset: int = 0) -> list[McpActivity]:
    """Fetch MCP activity rows ordered by most recent first."""
    rows = (
        await McpActivity.select()
        .order_by(McpActivity.created_at, ascending=False)
        .limit(limit)
        .offset(offset)
        .run()
    )
    return [McpActivity(**r) for r in rows]


async def count_mcp_activity() -> int:
    return await McpActivity.count().run()


async def get_mcp_stats() -> dict:
    """Get MCP activity stats for the dashboard panel."""
    today = datetime.now(UTC).strftime("%Y-%m-%d")

    total_rows = await McpActivity.raw(
        "SELECT COUNT(*) as cnt FROM mcp_activity WHERE DATE(created_at) = {}",
        today,
    ).run()
    total_today = total_rows[0]["cnt"] if total_rows else 0

    tool_rows = await McpActivity.raw(
        "SELECT tool_name, COUNT(*) as cnt FROM mcp_activity GROUP BY tool_name ORDER BY cnt DESC"
    ).run()
    by_tool = {r["tool_name"]: r["cnt"] for r in tool_rows}

    status_rows = await McpActivity.raw(
        "SELECT status, COUNT(*) as cnt FROM mcp_activity GROUP BY status"
    ).run()
    by_status = {r["status"]: r["cnt"] for r in status_rows}

    return {"total_today": total_today, "by_tool": by_tool, "by_status": by_status}
