"""msgspec Struct models for all database entities."""

from __future__ import annotations

from typing import Literal

import msgspec


class Lead(msgspec.Struct, kw_only=True):
    id: int = 0
    email: str = ""
    first_name: str = ""
    last_name: str = ""
    company: str = ""
    job_title: str = ""
    website: str = ""
    phone: str = ""
    address: str = ""
    city: str = ""
    state: str = ""
    zip: str = ""
    source: str = ""
    source_url: str = ""
    email_status: Literal["unknown", "valid", "invalid", "catch_all", "risky", "missing"] = (
        "unknown"
    )
    enriched_at: str | None = None
    validated_at: str | None = None
    tags: str = ""
    notes: str = ""
    email_confidence: float = 0.0
    email_source: str = ""
    email_provider: str = ""
    created_at: str = ""
    updated_at: str = ""


class Mailbox(msgspec.Struct, kw_only=True):
    id: int = 0
    email: str = ""
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_pass: str = ""
    imap_host: str = ""
    imap_port: int = 993
    imap_user: str = ""
    imap_pass: str = ""
    daily_limit: int = 30
    warmup_day: int = 0
    is_active: int = 1
    display_name: str = ""
    created_at: str = ""


class Campaign(msgspec.Struct, kw_only=True):
    id: int = 0
    name: str = ""
    status: Literal["draft", "active", "paused", "completed", "archived"] = "draft"
    mailbox_id: int | None = None
    daily_limit: int = 30
    timezone: str = "America/New_York"
    send_window_start: str = "08:00"
    send_window_end: str = "17:00"
    created_at: str = ""
    updated_at: str = ""


class SequenceStep(msgspec.Struct, kw_only=True):
    id: int = 0
    campaign_id: int = 0
    step_number: int = 0
    template_name: str = ""
    subject: str = ""
    delay_days: int = 0
    is_reply: int = 0


class CampaignLead(msgspec.Struct, kw_only=True):
    id: int = 0
    campaign_id: int = 0
    lead_id: int = 0
    current_step: int = 0
    status: Literal["active", "replied", "bounced", "unsubscribed", "completed", "paused"] = (
        "active"
    )
    enrolled_at: str = ""
    last_sent_at: str | None = None
    next_send_at: str | None = None


class EmailSent(msgspec.Struct, kw_only=True):
    id: int = 0
    campaign_lead_id: int | None = None
    campaign_id: int | None = None
    lead_id: int = 0
    mailbox_id: int | None = None
    step_number: int = 0
    message_id: str = ""
    subject: str = ""
    to_email: str = ""
    from_email: str = ""
    body_text: str = ""
    status: Literal["sent", "delivered", "replied", "bounced", "failed"] = "sent"
    sent_at: str = ""
    replied_at: str | None = None
    bounced_at: str | None = None
    bounce_reason: str | None = None


class Deal(msgspec.Struct, kw_only=True):
    id: int = 0
    lead_id: int = 0
    campaign_id: int | None = None
    stage: Literal[
        "lead",
        "contacted",
        "replied",
        "interested",
        "meeting_booked",
        "proposal_sent",
        "closed_won",
        "closed_lost",
    ] = "lead"
    value: float = 0.0
    close_date: str | None = None
    loss_reason: str | None = None
    notes: str = ""
    created_at: str = ""
    updated_at: str = ""


class TrackingEvent(msgspec.Struct, kw_only=True):
    id: int = 0
    email_sent_id: int = 0
    event_type: Literal["reply", "bounce", "unsubscribe"] = "reply"
    metadata: str | None = None
    created_at: str = ""
