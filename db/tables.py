"""Piccolo ORM table definitions for all database entities."""

from __future__ import annotations

import enum
from datetime import UTC, datetime

from piccolo.columns import (
    Integer,
    Real,
    Text,
)
from piccolo.table import Table


def _now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _today() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class EmailStatus(enum.StrEnum):
    unknown = "unknown"
    valid = "valid"
    invalid = "invalid"
    catch_all = "catch_all"
    risky = "risky"
    missing = "missing"


class CampaignStatus(enum.StrEnum):
    draft = "draft"
    active = "active"
    paused = "paused"
    completed = "completed"
    archived = "archived"


class CampaignLeadStatus(enum.StrEnum):
    active = "active"
    replied = "replied"
    bounced = "bounced"
    unsubscribed = "unsubscribed"
    completed = "completed"
    paused = "paused"


class EmailSentStatus(enum.StrEnum):
    sent = "sent"
    delivered = "delivered"
    replied = "replied"
    bounced = "bounced"
    failed = "failed"


class DealStage(enum.StrEnum):
    lead = "lead"
    contacted = "contacted"
    replied = "replied"
    interested = "interested"
    meeting_booked = "meeting_booked"
    proposal_sent = "proposal_sent"
    closed_won = "closed_won"
    closed_lost = "closed_lost"


class EventType(enum.StrEnum):
    reply = "reply"
    bounce = "bounce"
    unsubscribe = "unsubscribe"


# ---------------------------------------------------------------------------
# Tables
# ---------------------------------------------------------------------------


class Lead(Table, tablename="leads"):
    email = Text(unique=True, null=True, default="")
    first_name = Text(default="")
    last_name = Text(default="")
    company = Text(default="")
    job_title = Text(default="")
    website = Text(default="")
    phone = Text(default="")
    address = Text(default="")
    city = Text(default="")
    state = Text(default="")
    zip = Text(default="")
    source = Text(default="")
    source_url = Text(default="")
    email_status = Text(default="unknown", choices=EmailStatus)
    enriched_at = Text(null=True, default=None)
    validated_at = Text(null=True, default=None)
    tags = Text(default="")
    notes = Text(default="")
    created_at = Text(default=_now_iso)
    updated_at = Text(default=_now_iso)
    email_confidence = Real(default=0.0)
    email_source = Text(default="")
    email_provider = Text(default="")


class Mailbox(Table, tablename="mailboxes"):
    email = Text(unique=True)
    smtp_host = Text()
    smtp_port = Integer(default=587)
    smtp_user = Text()
    smtp_pass = Text()
    imap_host = Text(default="")
    imap_port = Integer(default=993)
    imap_user = Text(default="")
    imap_pass = Text(default="")
    daily_limit = Integer(default=30)
    warmup_day = Integer(default=0)
    is_active = Integer(default=1)
    display_name = Text(default="")
    created_at = Text(default=_now_iso)


class Campaign(Table, tablename="campaigns"):
    name = Text()
    status = Text(default="draft", choices=CampaignStatus)
    mailbox_id = Integer(null=True, default=None)
    daily_limit = Integer(default=30)
    timezone = Text(default="America/New_York")
    send_window_start = Text(default="08:00")
    send_window_end = Text(default="17:00")
    created_at = Text(default=_now_iso)
    updated_at = Text(default=_now_iso)


class SequenceStep(Table, tablename="sequence_steps"):
    campaign_id = Integer()
    step_number = Integer()
    template_name = Text(default="")
    subject = Text(default="")
    delay_days = Integer(default=0)
    is_reply = Integer(default=0)


class CampaignLead(Table, tablename="campaign_leads"):
    campaign_id = Integer()
    lead_id = Integer()
    current_step = Integer(default=0)
    status = Text(default="active", choices=CampaignLeadStatus)
    enrolled_at = Text(default=_now_iso)
    last_sent_at = Text(null=True, default=None)
    next_send_at = Text(null=True, default=None)


class EmailSent(Table, tablename="emails_sent"):
    campaign_lead_id = Integer(null=True, default=None)
    campaign_id = Integer(null=True, default=None)
    lead_id = Integer()
    mailbox_id = Integer(null=True, default=None)
    step_number = Integer(default=0)
    message_id = Text(default="")
    subject = Text(default="")
    to_email = Text(default="")
    from_email = Text(default="")
    body_text = Text(default="")
    status = Text(default="sent", choices=EmailSentStatus)
    sent_at = Text(default=_now_iso)
    replied_at = Text(null=True, default=None)
    bounced_at = Text(null=True, default=None)
    bounce_reason = Text(null=True, default=None)


class Deal(Table, tablename="deals"):
    lead_id = Integer()
    campaign_id = Integer(null=True, default=None)
    stage = Text(default="lead", choices=DealStage)
    value = Real(default=0.0)
    close_date = Text(null=True, default=None)
    loss_reason = Text(null=True, default=None)
    notes = Text(default="")
    created_at = Text(default=_now_iso)
    updated_at = Text(default=_now_iso)


class TrackingEvent(Table, tablename="tracking_events"):
    email_sent_id = Integer()
    event_type = Text(choices=EventType)
    metadata = Text(null=True, default=None)
    created_at = Text(default=_now_iso)


class DailySendLog(Table, tablename="daily_send_log"):
    mailbox_id = Integer()
    send_date = Text(default=_today)
    send_count = Integer(default=0, db_column_name="count")


class SchemaVersion(Table, tablename="schema_version"):
    version = Integer()
    applied_at = Text(default=_now_iso)


class User(Table, tablename="users"):
    username = Text(unique=True)
    webauthn_credential_id = Text(default="")
    webauthn_public_key = Text(default="")
    webauthn_sign_count = Integer(default=0)
    onboarding_completed = Integer(default=0)
    created_at = Text(default=_now_iso)


class Session(Table, tablename="sessions"):
    token = Text(unique=True)
    user_id = Integer()
    created_at = Text(default=_now_iso)
    expires_at = Text()


# ---------------------------------------------------------------------------
# Post-creation SQL for indexes and triggers not expressible in Piccolo
# ---------------------------------------------------------------------------

_POST_CREATE_SQL = [
    # Leads indexes
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_leads_company_name ON leads(company, first_name, last_name) WHERE email IS NULL",
    "CREATE INDEX IF NOT EXISTS idx_leads_email_status ON leads(email_status)",
    "CREATE INDEX IF NOT EXISTS idx_leads_source ON leads(source)",
    "CREATE INDEX IF NOT EXISTS idx_leads_city_state ON leads(city, state)",
    # Leads updated_at trigger
    """CREATE TRIGGER IF NOT EXISTS trg_leads_updated_at
       AFTER UPDATE ON leads FOR EACH ROW BEGIN
           UPDATE leads SET updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = OLD.id;
       END""",
    # Campaigns
    "CREATE INDEX IF NOT EXISTS idx_campaigns_status ON campaigns(status)",
    """CREATE TRIGGER IF NOT EXISTS trg_campaigns_updated_at
       AFTER UPDATE ON campaigns FOR EACH ROW BEGIN
           UPDATE campaigns SET updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = OLD.id;
       END""",
    # Sequence steps
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_steps_unique ON sequence_steps(campaign_id, step_number)",
    "CREATE INDEX IF NOT EXISTS idx_steps_campaign ON sequence_steps(campaign_id)",
    # Campaign leads
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_cl_unique ON campaign_leads(campaign_id, lead_id)",
    "CREATE INDEX IF NOT EXISTS idx_cl_campaign ON campaign_leads(campaign_id)",
    "CREATE INDEX IF NOT EXISTS idx_cl_lead ON campaign_leads(lead_id)",
    "CREATE INDEX IF NOT EXISTS idx_cl_next_send ON campaign_leads(next_send_at) WHERE status = 'active'",
    # Emails sent
    "CREATE INDEX IF NOT EXISTS idx_es_campaign ON emails_sent(campaign_id)",
    "CREATE INDEX IF NOT EXISTS idx_es_lead ON emails_sent(lead_id)",
    "CREATE INDEX IF NOT EXISTS idx_es_status ON emails_sent(status)",
    "CREATE INDEX IF NOT EXISTS idx_es_sent_at ON emails_sent(sent_at)",
    "CREATE INDEX IF NOT EXISTS idx_es_msg_id ON emails_sent(message_id)",
    # Deals
    "CREATE INDEX IF NOT EXISTS idx_deals_lead ON deals(lead_id)",
    "CREATE INDEX IF NOT EXISTS idx_deals_stage ON deals(stage)",
    """CREATE TRIGGER IF NOT EXISTS trg_deals_updated_at
       AFTER UPDATE ON deals FOR EACH ROW BEGIN
           UPDATE deals SET updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = OLD.id;
       END""",
    # Tracking events
    "CREATE INDEX IF NOT EXISTS idx_te_email ON tracking_events(email_sent_id)",
    # Daily send log
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_dsl_unique ON daily_send_log(mailbox_id, send_date)",
    "CREATE INDEX IF NOT EXISTS idx_dsl_date ON daily_send_log(mailbox_id, send_date)",
    # Sessions
    "CREATE INDEX IF NOT EXISTS idx_sessions_token ON sessions(token)",
]
