"""Async query functions for all database operations."""

from __future__ import annotations

from datetime import UTC, datetime

import aiosqlite

from shared.crypto import decrypt, encrypt

from .models import (
    Campaign,
    CampaignLead,
    Deal,
    EmailSent,
    Lead,
    Mailbox,
    SequenceStep,
    Session,
    TrackingEvent,
    User,
)


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _row_to_struct[T](row: aiosqlite.Row, cls: type[T], keys: list[str]) -> T:
    return cls(**dict(zip(keys, row, strict=False)))


def _escape_like(value: str) -> str:
    """Escape special LIKE characters (%, _, \\) for safe use in LIKE clauses."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


# ---------------------------------------------------------------------------
# Leads
# ---------------------------------------------------------------------------

_LEAD_COLS = [
    "id",
    "email",
    "first_name",
    "last_name",
    "company",
    "job_title",
    "website",
    "phone",
    "address",
    "city",
    "state",
    "zip",
    "source",
    "source_url",
    "email_status",
    "enriched_at",
    "validated_at",
    "tags",
    "notes",
    "created_at",
    "updated_at",
    "email_confidence",
    "email_source",
    "email_provider",
]


async def upsert_lead(db: aiosqlite.Connection, lead: Lead) -> int:
    """Insert or update a lead by email. Returns the lead id."""
    await db.execute(
        """INSERT INTO leads (email, first_name, last_name, company, job_title, website,
                              phone, address, city, state, zip, source, source_url,
                              email_status, enriched_at, validated_at, tags, notes,
                              email_confidence, email_source, email_provider)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        (
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
        ),
    )
    await db.commit()
    cursor = await db.execute("SELECT id FROM leads WHERE email = ?", (lead.email,))
    row = await cursor.fetchone()
    if row is None:
        raise RuntimeError(f"Lead not found after upsert: {lead.email}")
    return row[0]


async def upsert_leads_batch(db: aiosqlite.Connection, leads: list[Lead]) -> int:
    """Batch upsert leads. Returns count of rows affected."""
    count = 0
    for lead in leads:
        await upsert_lead(db, lead)
        count += 1
    await db.commit()
    return count


async def get_leads(
    db: aiosqlite.Connection,
    *,
    limit: int = 100,
    offset: int = 0,
    email_status: str | None = None,
    source: str | None = None,
) -> list[Lead]:
    """Fetch leads with optional filters."""
    q = "SELECT * FROM leads WHERE 1=1"
    params: list = []
    if email_status:
        q += " AND email_status = ?"
        params.append(email_status)
    if source:
        q += " AND source = ?"
        params.append(source)
    q += " ORDER BY id DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    cursor = await db.execute(q, params)
    rows = await cursor.fetchall()
    return [_row_to_struct(r, Lead, _LEAD_COLS) for r in rows]


async def get_lead_by_id(db: aiosqlite.Connection, lead_id: int) -> Lead | None:
    cursor = await db.execute("SELECT * FROM leads WHERE id = ?", (lead_id,))
    row = await cursor.fetchone()
    if row is None:
        return None
    return _row_to_struct(row, Lead, _LEAD_COLS)


async def get_lead_by_email(db: aiosqlite.Connection, email: str) -> Lead | None:
    cursor = await db.execute("SELECT * FROM leads WHERE email = ?", (email,))
    row = await cursor.fetchone()
    if row is None:
        return None
    return _row_to_struct(row, Lead, _LEAD_COLS)


async def search_leads(db: aiosqlite.Connection, query: str, *, limit: int = 50) -> list[Lead]:
    """Search leads by email, name, or company (LIKE match)."""
    pattern = f"%{_escape_like(query)}%"
    cursor = await db.execute(
        """SELECT * FROM leads
           WHERE email LIKE ? ESCAPE '\\' OR first_name LIKE ? ESCAPE '\\'
                 OR last_name LIKE ? ESCAPE '\\' OR company LIKE ? ESCAPE '\\'
                 OR website LIKE ? ESCAPE '\\'
           ORDER BY id DESC LIMIT ?""",
        (pattern, pattern, pattern, pattern, pattern, limit),
    )
    rows = await cursor.fetchall()
    return [_row_to_struct(r, Lead, _LEAD_COLS) for r in rows]


async def count_leads(db: aiosqlite.Connection, *, email_status: str | None = None) -> int:
    q = "SELECT COUNT(*) FROM leads"
    params: list[str] = []
    if email_status:
        q += " WHERE email_status = ?"
        params.append(email_status)
    cursor = await db.execute(q, params)
    row = await cursor.fetchone()
    return row[0] if row is not None else 0


async def delete_lead(db: aiosqlite.Connection, lead_id: int) -> bool:
    cursor = await db.execute("DELETE FROM leads WHERE id = ?", (lead_id,))
    await db.commit()
    return cursor.rowcount > 0


# ---------------------------------------------------------------------------
# Mailboxes
# ---------------------------------------------------------------------------

_MAILBOX_COLS = [
    "id",
    "email",
    "smtp_host",
    "smtp_port",
    "smtp_user",
    "smtp_pass",
    "imap_host",
    "imap_port",
    "imap_user",
    "imap_pass",
    "daily_limit",
    "warmup_day",
    "is_active",
    "display_name",
    "created_at",
]


async def upsert_mailbox(db: aiosqlite.Connection, mb: Mailbox) -> int:
    smtp_pass = encrypt(mb.smtp_pass)
    imap_pass = encrypt(mb.imap_pass)
    await db.execute(
        """INSERT INTO mailboxes (email, smtp_host, smtp_port, smtp_user, smtp_pass,
                                   imap_host, imap_port, imap_user, imap_pass,
                                   daily_limit, warmup_day, is_active, display_name)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(email) DO UPDATE SET
               smtp_host = excluded.smtp_host, smtp_port = excluded.smtp_port,
               smtp_user = excluded.smtp_user, smtp_pass = excluded.smtp_pass,
               imap_host = excluded.imap_host, imap_port = excluded.imap_port,
               imap_user = excluded.imap_user, imap_pass = excluded.imap_pass,
               daily_limit = excluded.daily_limit, warmup_day = excluded.warmup_day,
               is_active = excluded.is_active, display_name = excluded.display_name
        """,
        (
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
        ),
    )
    await db.commit()
    cursor = await db.execute("SELECT id FROM mailboxes WHERE email = ?", (mb.email,))
    row = await cursor.fetchone()
    if row is None:
        raise RuntimeError(f"Mailbox not found after upsert: {mb.email}")
    return row[0]


async def get_mailboxes(db: aiosqlite.Connection, *, active_only: bool = False) -> list[Mailbox]:
    q = "SELECT * FROM mailboxes"
    if active_only:
        q += " WHERE is_active = 1"
    q += " ORDER BY id"
    cursor = await db.execute(q)
    rows = await cursor.fetchall()
    mailboxes = [_row_to_struct(r, Mailbox, _MAILBOX_COLS) for r in rows]
    for m in mailboxes:
        m.smtp_pass = decrypt(m.smtp_pass)
        m.imap_pass = decrypt(m.imap_pass)
    return mailboxes


async def get_mailbox_by_id(db: aiosqlite.Connection, mailbox_id: int) -> Mailbox | None:
    cursor = await db.execute("SELECT * FROM mailboxes WHERE id = ?", (mailbox_id,))
    row = await cursor.fetchone()
    if row is None:
        return None
    mb = _row_to_struct(row, Mailbox, _MAILBOX_COLS)
    mb.smtp_pass = decrypt(mb.smtp_pass)
    mb.imap_pass = decrypt(mb.imap_pass)
    return mb


async def encrypt_existing_passwords(db: aiosqlite.Connection) -> int:
    """One-time migration: encrypt any plaintext mailbox passwords."""
    cursor = await db.execute("SELECT id, smtp_pass, imap_pass FROM mailboxes")
    rows = await cursor.fetchall()
    count = 0
    for row in rows:
        mid, smtp_pass, imap_pass = row[0], row[1], row[2]
        new_smtp = encrypt(smtp_pass)
        new_imap = encrypt(imap_pass)
        if new_smtp != smtp_pass or new_imap != imap_pass:
            await db.execute(
                "UPDATE mailboxes SET smtp_pass = ?, imap_pass = ? WHERE id = ?",
                (new_smtp, new_imap, mid),
            )
            count += 1
    if count:
        await db.commit()
    return count


# ---------------------------------------------------------------------------
# Campaigns
# ---------------------------------------------------------------------------

_CAMPAIGN_COLS = [
    "id",
    "name",
    "status",
    "mailbox_id",
    "daily_limit",
    "timezone",
    "send_window_start",
    "send_window_end",
    "created_at",
    "updated_at",
]


async def create_campaign(db: aiosqlite.Connection, camp: Campaign) -> int:
    cursor = await db.execute(
        """INSERT INTO campaigns (name, status, mailbox_id, daily_limit, timezone,
                                   send_window_start, send_window_end)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            camp.name,
            camp.status,
            camp.mailbox_id,
            camp.daily_limit,
            camp.timezone,
            camp.send_window_start,
            camp.send_window_end,
        ),
    )
    await db.commit()
    if cursor.lastrowid is None:
        raise RuntimeError("Failed to create campaign: no lastrowid")
    return cursor.lastrowid


async def get_campaigns(db: aiosqlite.Connection, *, status: str | None = None) -> list[Campaign]:
    q = "SELECT * FROM campaigns"
    params: list = []
    if status:
        q += " WHERE status = ?"
        params.append(status)
    q += " ORDER BY id DESC"
    cursor = await db.execute(q, params)
    rows = await cursor.fetchall()
    return [_row_to_struct(r, Campaign, _CAMPAIGN_COLS) for r in rows]


async def get_campaign_by_id(db: aiosqlite.Connection, campaign_id: int) -> Campaign | None:
    cursor = await db.execute("SELECT * FROM campaigns WHERE id = ?", (campaign_id,))
    row = await cursor.fetchone()
    if row is None:
        return None
    return _row_to_struct(row, Campaign, _CAMPAIGN_COLS)


async def update_campaign_status(db: aiosqlite.Connection, campaign_id: int, status: str) -> bool:
    cursor = await db.execute("UPDATE campaigns SET status = ? WHERE id = ?", (status, campaign_id))
    await db.commit()
    return cursor.rowcount > 0


# ---------------------------------------------------------------------------
# Sequence steps
# ---------------------------------------------------------------------------

_STEP_COLS = [
    "id",
    "campaign_id",
    "step_number",
    "template_name",
    "subject",
    "delay_days",
    "is_reply",
]


async def add_sequence_step(db: aiosqlite.Connection, step: SequenceStep) -> int:
    cursor = await db.execute(
        """INSERT INTO sequence_steps (campaign_id, step_number, template_name,
                                        subject, delay_days, is_reply)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            step.campaign_id,
            step.step_number,
            step.template_name,
            step.subject,
            step.delay_days,
            step.is_reply,
        ),
    )
    await db.commit()
    if cursor.lastrowid is None:
        raise RuntimeError("Failed to add sequence step: no lastrowid")
    return cursor.lastrowid


async def get_sequence_steps(db: aiosqlite.Connection, campaign_id: int) -> list[SequenceStep]:
    cursor = await db.execute(
        "SELECT * FROM sequence_steps WHERE campaign_id = ? ORDER BY step_number",
        (campaign_id,),
    )
    rows = await cursor.fetchall()
    return [_row_to_struct(r, SequenceStep, _STEP_COLS) for r in rows]


# ---------------------------------------------------------------------------
# Campaign leads
# ---------------------------------------------------------------------------

_CL_COLS = [
    "id",
    "campaign_id",
    "lead_id",
    "current_step",
    "status",
    "enrolled_at",
    "last_sent_at",
    "next_send_at",
]


async def enroll_lead(db: aiosqlite.Connection, campaign_id: int, lead_id: int) -> int:
    cursor = await db.execute(
        """INSERT INTO campaign_leads (campaign_id, lead_id) VALUES (?, ?)
           ON CONFLICT(campaign_id, lead_id) DO NOTHING""",
        (campaign_id, lead_id),
    )
    await db.commit()
    return cursor.lastrowid or 0


async def get_campaign_leads(
    db: aiosqlite.Connection,
    campaign_id: int,
    *,
    status: str | None = None,
) -> list[CampaignLead]:
    q = "SELECT * FROM campaign_leads WHERE campaign_id = ?"
    params: list = [campaign_id]
    if status:
        q += " AND status = ?"
        params.append(status)
    q += " ORDER BY id"
    cursor = await db.execute(q, params)
    rows = await cursor.fetchall()
    return [_row_to_struct(r, CampaignLead, _CL_COLS) for r in rows]


async def advance_step(
    db: aiosqlite.Connection,
    campaign_lead_id: int,
    *,
    next_send_at: str | None = None,
) -> bool:
    """Increment current_step and update last_sent_at."""
    cursor = await db.execute(
        """UPDATE campaign_leads
           SET current_step = current_step + 1,
               last_sent_at = ?,
               next_send_at = ?
           WHERE id = ?""",
        (_now(), next_send_at, campaign_lead_id),
    )
    await db.commit()
    return cursor.rowcount > 0


async def update_campaign_lead_status(
    db: aiosqlite.Connection, campaign_lead_id: int, status: str
) -> bool:
    cursor = await db.execute(
        "UPDATE campaign_leads SET status = ? WHERE id = ?", (status, campaign_lead_id)
    )
    await db.commit()
    return cursor.rowcount > 0


# ---------------------------------------------------------------------------
# Send queue
# ---------------------------------------------------------------------------


async def get_send_queue(
    db: aiosqlite.Connection,
    campaign_id: int,
    *,
    limit: int = 50,
) -> list[dict]:
    """Get leads ready to receive the next email in a campaign sequence.

    Returns dicts with campaign_lead info + lead details + step template.
    """
    cursor = await db.execute(
        """SELECT cl.id AS cl_id, cl.campaign_id, cl.lead_id, cl.current_step,
                  l.email, l.first_name, l.last_name, l.company, l.website,
                  ss.template_name, ss.subject, ss.delay_days, ss.is_reply
           FROM campaign_leads cl
           JOIN leads l ON l.id = cl.lead_id
           JOIN sequence_steps ss ON ss.campaign_id = cl.campaign_id
                                  AND ss.step_number = cl.current_step
           WHERE cl.campaign_id = ?
             AND cl.status = 'active'
             AND (cl.next_send_at IS NULL OR cl.next_send_at <= strftime('%Y-%m-%dT%H:%M:%SZ','now'))
           ORDER BY cl.id
           LIMIT ?""",
        (campaign_id, limit),
    )
    cols = [d[0] for d in cursor.description]
    rows = await cursor.fetchall()
    return [dict(zip(cols, r, strict=False)) for r in rows]


# ---------------------------------------------------------------------------
# Emails sent
# ---------------------------------------------------------------------------

_ES_COLS = [
    "id",
    "campaign_lead_id",
    "campaign_id",
    "lead_id",
    "mailbox_id",
    "step_number",
    "message_id",
    "subject",
    "to_email",
    "from_email",
    "body_text",
    "status",
    "sent_at",
    "replied_at",
    "bounced_at",
    "bounce_reason",
]


async def log_send(db: aiosqlite.Connection, es: EmailSent) -> int:
    cursor = await db.execute(
        """INSERT INTO emails_sent (campaign_lead_id, campaign_id, lead_id, mailbox_id,
                                     step_number, message_id, subject, to_email,
                                     from_email, body_text, status)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            es.campaign_lead_id,
            es.campaign_id,
            es.lead_id,
            es.mailbox_id,
            es.step_number,
            es.message_id,
            es.subject,
            es.to_email,
            es.from_email,
            es.body_text,
            es.status,
        ),
    )
    await db.commit()
    if cursor.lastrowid is None:
        raise RuntimeError("Failed to log send: no lastrowid")
    return cursor.lastrowid


async def update_email_status(db: aiosqlite.Connection, email_id: int, status: str) -> bool:
    updates = ["status = ?"]
    params: list = [status]
    if status == "replied":
        updates.append("replied_at = ?")
        params.append(_now())
    elif status == "bounced":
        updates.append("bounced_at = ?")
        params.append(_now())
    params.append(email_id)
    cursor = await db.execute(f"UPDATE emails_sent SET {', '.join(updates)} WHERE id = ?", params)
    await db.commit()
    return cursor.rowcount > 0


async def get_emails_for_lead(db: aiosqlite.Connection, lead_id: int) -> list[EmailSent]:
    cursor = await db.execute(
        "SELECT * FROM emails_sent WHERE lead_id = ? ORDER BY sent_at DESC", (lead_id,)
    )
    rows = await cursor.fetchall()
    return [_row_to_struct(r, EmailSent, _ES_COLS) for r in rows]


async def get_emails_sent(
    db: aiosqlite.Connection,
    *,
    limit: int = 50,
    offset: int = 0,
    status: str | None = None,
) -> list[EmailSent]:
    """Fetch sent emails with optional status filter."""
    q = "SELECT * FROM emails_sent WHERE 1=1"
    params: list = []
    if status:
        q += " AND status = ?"
        params.append(status)
    q += " ORDER BY id DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    cursor = await db.execute(q, params)
    rows = await cursor.fetchall()
    return [_row_to_struct(r, EmailSent, _ES_COLS) for r in rows]


async def count_emails_sent(db: aiosqlite.Connection, *, status: str | None = None) -> int:
    """Count sent emails with optional status filter."""
    q = "SELECT COUNT(*) FROM emails_sent"
    params: list[str] = []
    if status:
        q += " WHERE status = ?"
        params.append(status)
    cursor = await db.execute(q, params)
    row = await cursor.fetchone()
    return row[0] if row is not None else 0


async def get_email_by_message_id(db: aiosqlite.Connection, message_id: str) -> EmailSent | None:
    cursor = await db.execute("SELECT * FROM emails_sent WHERE message_id = ?", (message_id,))
    row = await cursor.fetchone()
    if row is None:
        return None
    return _row_to_struct(row, EmailSent, _ES_COLS)


# ---------------------------------------------------------------------------
# Daily send limit
# ---------------------------------------------------------------------------


async def check_daily_limit(db: aiosqlite.Connection, mailbox_id: int) -> tuple[int, int]:
    """Returns (sent_today, daily_limit)."""
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    cursor = await db.execute(
        "SELECT count FROM daily_send_log WHERE mailbox_id = ? AND send_date = ?",
        (mailbox_id, today),
    )
    row = await cursor.fetchone()
    sent = row[0] if row else 0

    cursor2 = await db.execute("SELECT daily_limit FROM mailboxes WHERE id = ?", (mailbox_id,))
    row2 = await cursor2.fetchone()
    limit = row2[0] if row2 else 30
    return sent, limit


async def increment_daily_send(db: aiosqlite.Connection, mailbox_id: int) -> int:
    """Increment today's send count. Returns new count."""
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    await db.execute(
        """INSERT INTO daily_send_log (mailbox_id, send_date, count) VALUES (?, ?, 1)
           ON CONFLICT(mailbox_id, send_date) DO UPDATE SET count = count + 1""",
        (mailbox_id, today),
    )
    await db.commit()
    cursor = await db.execute(
        "SELECT count FROM daily_send_log WHERE mailbox_id = ? AND send_date = ?",
        (mailbox_id, today),
    )
    row = await cursor.fetchone()
    return row[0] if row is not None else 0


# ---------------------------------------------------------------------------
# Deals
# ---------------------------------------------------------------------------

_DEAL_COLS = [
    "id",
    "lead_id",
    "campaign_id",
    "stage",
    "value",
    "close_date",
    "loss_reason",
    "notes",
    "created_at",
    "updated_at",
]


async def upsert_deal(db: aiosqlite.Connection, deal: Deal) -> int:
    if deal.id:
        await db.execute(
            """UPDATE deals SET stage = ?, value = ?, close_date = ?,
                                loss_reason = ?, notes = ? WHERE id = ?""",
            (deal.stage, deal.value, deal.close_date, deal.loss_reason, deal.notes, deal.id),
        )
        await db.commit()
        return deal.id
    cursor = await db.execute(
        """INSERT INTO deals (lead_id, campaign_id, stage, value, close_date, loss_reason, notes)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            deal.lead_id,
            deal.campaign_id,
            deal.stage,
            deal.value,
            deal.close_date,
            deal.loss_reason,
            deal.notes,
        ),
    )
    await db.commit()
    if cursor.lastrowid is None:
        raise RuntimeError("Failed to upsert deal: no lastrowid")
    return cursor.lastrowid


async def get_deals(db: aiosqlite.Connection, *, stage: str | None = None) -> list[Deal]:
    q = "SELECT * FROM deals"
    params: list = []
    if stage:
        q += " WHERE stage = ?"
        params.append(stage)
    q += " ORDER BY id DESC"
    cursor = await db.execute(q, params)
    rows = await cursor.fetchall()
    return [_row_to_struct(r, Deal, _DEAL_COLS) for r in rows]


async def get_deal_by_id(db: aiosqlite.Connection, deal_id: int) -> Deal | None:
    cursor = await db.execute("SELECT * FROM deals WHERE id = ?", (deal_id,))
    row = await cursor.fetchone()
    if row is None:
        return None
    return _row_to_struct(row, Deal, _DEAL_COLS)


# ---------------------------------------------------------------------------
# Tracking events
# ---------------------------------------------------------------------------

_TE_COLS = ["id", "email_sent_id", "event_type", "metadata", "created_at"]


async def log_tracking_event(db: aiosqlite.Connection, evt: TrackingEvent) -> int:
    cursor = await db.execute(
        """INSERT INTO tracking_events (email_sent_id, event_type, metadata)
           VALUES (?, ?, ?)""",
        (evt.email_sent_id, evt.event_type, evt.metadata),
    )
    await db.commit()
    if cursor.lastrowid is None:
        raise RuntimeError("Failed to log tracking event: no lastrowid")
    return cursor.lastrowid


# ---------------------------------------------------------------------------
# Stats / reporting
# ---------------------------------------------------------------------------


async def get_pipeline_stats(db: aiosqlite.Connection) -> dict:
    """Get deal pipeline counts by stage."""
    cursor = await db.execute(
        "SELECT stage, COUNT(*) as cnt FROM deals GROUP BY stage ORDER BY stage"
    )
    rows = await cursor.fetchall()
    return {row[0]: row[1] for row in rows}


async def get_daily_stats(db: aiosqlite.Connection, days: int = 30) -> list[dict]:
    """Get email send stats per day for the last N days."""
    cursor = await db.execute(
        """SELECT DATE(sent_at) as day, status, COUNT(*) as cnt
           FROM emails_sent
           WHERE sent_at >= DATE('now', ?)
           GROUP BY day, status
           ORDER BY day DESC""",
        (f"-{days} days",),
    )
    cols = [d[0] for d in cursor.description]
    rows = await cursor.fetchall()
    return [dict(zip(cols, r, strict=False)) for r in rows]


async def get_lead_stats(db: aiosqlite.Connection) -> dict:
    """Get lead counts grouped by email_status and source."""
    result: dict = {}
    cursor = await db.execute("SELECT email_status, COUNT(*) FROM leads GROUP BY email_status")
    result["by_status"] = {row[0]: row[1] for row in await cursor.fetchall()}

    cursor = await db.execute(
        "SELECT source, COUNT(*) FROM leads GROUP BY source ORDER BY COUNT(*) DESC"
    )
    result["by_source"] = {row[0]: row[1] for row in await cursor.fetchall()}

    cursor = await db.execute(
        "SELECT city, state, COUNT(*) FROM leads WHERE city != '' "
        "GROUP BY city, state ORDER BY COUNT(*) DESC LIMIT 20"
    )
    result["by_city"] = {f"{row[0]}, {row[1]}": row[2] for row in await cursor.fetchall()}

    cursor = await db.execute("SELECT COUNT(*) FROM leads")
    total_row = await cursor.fetchone()
    result["total"] = total_row[0] if total_row is not None else 0
    return result


async def tag_leads(db: aiosqlite.Connection, lead_ids: list[int], tag: str) -> int:
    """Add a tag to multiple leads. Returns count updated."""
    count = 0
    for lid in lead_ids:
        cursor = await db.execute("SELECT tags FROM leads WHERE id = ?", (lid,))
        row = await cursor.fetchone()
        if row is None:
            continue
        existing = row[0] or ""
        tags_list = [t.strip() for t in existing.split(",") if t.strip()]
        if tag not in tags_list:
            tags_list.append(tag)
            await db.execute("UPDATE leads SET tags = ? WHERE id = ?", (",".join(tags_list), lid))
            count += 1
    await db.commit()
    return count


async def deactivate_mailbox(db: aiosqlite.Connection, mailbox_id: int) -> bool:
    cursor = await db.execute("UPDATE mailboxes SET is_active = 0 WHERE id = ?", (mailbox_id,))
    await db.commit()
    return cursor.rowcount > 0


async def delete_campaign(db: aiosqlite.Connection, campaign_id: int) -> bool:
    cursor = await db.execute("DELETE FROM campaigns WHERE id = ?", (campaign_id,))
    await db.commit()
    return cursor.rowcount > 0


async def enroll_leads_by_filter(
    db: aiosqlite.Connection,
    campaign_id: int,
    *,
    city: str | None = None,
    state: str | None = None,
    email_status: str | None = None,
    tag: str | None = None,
) -> int:
    """Enroll leads matching filters into a campaign. Returns count enrolled."""
    q = "SELECT id FROM leads WHERE 1=1"
    params: list = []
    if city:
        q += " AND city = ?"
        params.append(city)
    if state:
        q += " AND state = ?"
        params.append(state)
    if email_status:
        q += " AND email_status = ?"
        params.append(email_status)
    if tag:
        q += " AND tags LIKE ?"
        params.append(f"%{tag}%")
    cursor = await db.execute(q, params)
    rows = await cursor.fetchall()
    count = 0
    for row in rows:
        result = await enroll_lead(db, campaign_id, row[0])
        if result:
            count += 1
    return count


async def get_all_send_queues(db: aiosqlite.Connection, *, limit: int = 50) -> list[dict]:
    """Get send queue across all active campaigns."""
    cursor = await db.execute(
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
             AND (cl.next_send_at IS NULL OR cl.next_send_at <= ?)
           ORDER BY cl.id
           LIMIT ?""",
        (_now(), limit),
    )
    cols = [d[0] for d in cursor.description]
    rows = await cursor.fetchall()
    return [dict(zip(cols, r, strict=False)) for r in rows]


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


async def get_deal_stats(db: aiosqlite.Connection) -> dict:
    """Get deal pipeline stats."""
    cursor = await db.execute(
        "SELECT stage, COUNT(*), SUM(value) FROM deals GROUP BY stage ORDER BY stage"
    )
    rows = await cursor.fetchall()
    stages = {}
    for row in rows:
        stages[row[0]] = {"count": row[1], "value": row[2] or 0.0}
    total_pipeline = sum(s["value"] for s in stages.values())
    total_closed = stages.get("closed_won", {}).get("value", 0.0)
    return {"stages": stages, "pipeline_value": total_pipeline, "closed_value": total_closed}


async def get_today_activity(db: aiosqlite.Connection) -> dict:
    """Get today's email activity summary."""
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    cursor = await db.execute(
        """SELECT
               COUNT(*) as sent,
               SUM(CASE WHEN status = 'replied' THEN 1 ELSE 0 END) as replies,
               SUM(CASE WHEN status = 'bounced' THEN 1 ELSE 0 END) as bounces
           FROM emails_sent WHERE DATE(sent_at) = ?""",
        (today,),
    )
    row = await cursor.fetchone()
    if row is None:
        return {"sent": 0, "replies": 0, "bounces": 0}
    return {"sent": row[0] or 0, "replies": row[1] or 0, "bounces": row[2] or 0}


async def get_email_status_distribution(db: aiosqlite.Connection) -> dict[str, int]:
    """Get count of emails by status."""
    cursor = await db.execute(
        "SELECT status, COUNT(*) FROM emails_sent GROUP BY status"
    )
    rows = await cursor.fetchall()
    return {row[0]: row[1] for row in rows}


async def get_campaign_step_distribution(
    db: aiosqlite.Connection, campaign_id: int
) -> dict[int, dict[str, int]]:
    """Get lead counts by step and status for a campaign."""
    cursor = await db.execute(
        """SELECT current_step, status, COUNT(*)
           FROM campaign_leads
           WHERE campaign_id = ?
           GROUP BY current_step, status""",
        (campaign_id,),
    )
    rows = await cursor.fetchall()
    result: dict[int, dict[str, int]] = {}
    for row in rows:
        step, status, cnt = row[0], row[1], row[2]
        if step not in result:
            result[step] = {}
        result[step][status] = cnt
    return result


async def get_campaign_stats(db: aiosqlite.Connection, campaign_id: int) -> dict:
    """Get stats for a specific campaign."""
    cursor = await db.execute(
        """SELECT
               COUNT(*) as total,
               SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) as active,
               SUM(CASE WHEN status = 'replied' THEN 1 ELSE 0 END) as replied,
               SUM(CASE WHEN status = 'bounced' THEN 1 ELSE 0 END) as bounced,
               SUM(CASE WHEN status = 'unsubscribed' THEN 1 ELSE 0 END) as unsubscribed,
               SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
               SUM(CASE WHEN status = 'paused' THEN 1 ELSE 0 END) as paused
           FROM campaign_leads WHERE campaign_id = ?""",
        (campaign_id,),
    )
    row = await cursor.fetchone()
    if row is None:
        return {}
    cols = [d[0] for d in cursor.description]
    return dict(zip(cols, row, strict=False))


# ---------------------------------------------------------------------------
# Users & Sessions (auth)
# ---------------------------------------------------------------------------

_USER_COLS = [
    "id",
    "username",
    "webauthn_credential_id",
    "webauthn_public_key",
    "webauthn_sign_count",
    "onboarding_completed",
    "created_at",
]

_SESSION_COLS = ["id", "token", "user_id", "created_at", "expires_at"]


async def get_user_count(db: aiosqlite.Connection) -> int:
    cursor = await db.execute("SELECT COUNT(*) FROM users")
    row = await cursor.fetchone()
    return row[0] if row else 0


async def get_user_by_id(db: aiosqlite.Connection, user_id: int) -> User | None:
    cursor = await db.execute("SELECT * FROM users WHERE id = ?", (user_id,))
    row = await cursor.fetchone()
    if row is None:
        return None
    return _row_to_struct(row, User, _USER_COLS)


async def get_user_by_username(db: aiosqlite.Connection, username: str) -> User | None:
    cursor = await db.execute("SELECT * FROM users WHERE username = ?", (username,))
    row = await cursor.fetchone()
    if row is None:
        return None
    return _row_to_struct(row, User, _USER_COLS)


async def create_user(db: aiosqlite.Connection, user: User) -> int:
    cursor = await db.execute(
        """INSERT INTO users (username, webauthn_credential_id, webauthn_public_key,
                              webauthn_sign_count, onboarding_completed)
           VALUES (?, ?, ?, ?, ?)""",
        (
            user.username,
            user.webauthn_credential_id,
            user.webauthn_public_key,
            user.webauthn_sign_count,
            user.onboarding_completed,
        ),
    )
    await db.commit()
    if cursor.lastrowid is None:
        raise RuntimeError("Failed to create user: no lastrowid")
    return cursor.lastrowid


async def update_user_credential(
    db: aiosqlite.Connection,
    user_id: int,
    *,
    credential_id: str,
    public_key: str,
    sign_count: int,
) -> None:
    await db.execute(
        """UPDATE users SET webauthn_credential_id = ?, webauthn_public_key = ?,
                            webauthn_sign_count = ? WHERE id = ?""",
        (credential_id, public_key, sign_count, user_id),
    )
    await db.commit()


async def update_user_sign_count(
    db: aiosqlite.Connection, user_id: int, sign_count: int
) -> None:
    await db.execute(
        "UPDATE users SET webauthn_sign_count = ? WHERE id = ?",
        (sign_count, user_id),
    )
    await db.commit()


async def set_onboarding_completed(db: aiosqlite.Connection, user_id: int) -> None:
    await db.execute(
        "UPDATE users SET onboarding_completed = 1 WHERE id = ?", (user_id,)
    )
    await db.commit()


async def create_session(
    db: aiosqlite.Connection, token: str, user_id: int, expires_at: str
) -> int:
    cursor = await db.execute(
        "INSERT INTO sessions (token, user_id, expires_at) VALUES (?, ?, ?)",
        (token, user_id, expires_at),
    )
    await db.commit()
    if cursor.lastrowid is None:
        raise RuntimeError("Failed to create session: no lastrowid")
    return cursor.lastrowid


async def get_session_by_token(db: aiosqlite.Connection, token: str) -> Session | None:
    cursor = await db.execute("SELECT * FROM sessions WHERE token = ?", (token,))
    row = await cursor.fetchone()
    if row is None:
        return None
    return _row_to_struct(row, Session, _SESSION_COLS)


async def delete_session(db: aiosqlite.Connection, token: str) -> None:
    await db.execute("DELETE FROM sessions WHERE token = ?", (token,))
    await db.commit()


async def cleanup_expired_sessions(db: aiosqlite.Connection) -> int:
    cursor = await db.execute(
        "DELETE FROM sessions WHERE expires_at < ?", (_now(),)
    )
    await db.commit()
    return cursor.rowcount
