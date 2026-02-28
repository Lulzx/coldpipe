"""Tests for the database layer: schema, models, queries, triggers, constraints."""

from __future__ import annotations

import pytest

from db import queries
from db.tables import (
    Campaign,
    Deal,
    EmailSent,
    Lead,
    Mailbox,
    SequenceStep,
    TrackingEvent,
)

# ---------------------------------------------------------------------------
# Schema / init
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_schema_tables_exist(db):
    from db import get_engine

    engine = get_engine()
    rows = await engine.run_ddl("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = {r["name"] for r in rows}
    expected = {
        "leads",
        "mailboxes",
        "campaigns",
        "sequence_steps",
        "campaign_leads",
        "emails_sent",
        "deals",
        "tracking_events",
        "daily_send_log",
        "schema_version",
    }
    assert expected.issubset(tables)


@pytest.mark.asyncio
async def test_schema_version(db):
    from db import get_engine

    engine = get_engine()
    rows = await engine.run_ddl("SELECT MAX(version) as v FROM schema_version")
    assert rows[0]["v"] >= 4


# ---------------------------------------------------------------------------
# Lead CRUD
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upsert_lead_insert(db):
    lead = Lead(email="doc@test.com", first_name="Doc", last_name="Brown", company="DDS")
    lid = await queries.upsert_lead(db, lead)
    assert lid > 0

    fetched = await queries.get_lead_by_id(db, lid)
    assert fetched is not None
    assert fetched.email == "doc@test.com"
    assert fetched.first_name == "Doc"
    assert fetched.company == "DDS"


@pytest.mark.asyncio
async def test_upsert_lead_merge(db):
    """Non-empty fields should overwrite; empty fields should be kept."""
    lead = Lead(email="doc@test.com", first_name="Doc", last_name="Brown", company="DDS")
    await queries.upsert_lead(db, lead)

    update = Lead(
        email="doc@test.com", first_name="", last_name="", company="New Corp", city="Austin"
    )
    lid = await queries.upsert_lead(db, update)

    fetched = await queries.get_lead_by_id(db, lid)
    assert fetched is not None
    assert fetched.first_name == "Doc"  # kept
    assert fetched.company == "New Corp"  # updated
    assert fetched.city == "Austin"  # added


@pytest.mark.asyncio
async def test_get_leads_pagination(db):
    for i in range(10):
        await queries.upsert_lead(db, Lead(email=f"user{i}@test.com"))

    page1 = await queries.get_leads(db, limit=3, offset=0)
    page2 = await queries.get_leads(db, limit=3, offset=3)
    assert len(page1) == 3
    assert len(page2) == 3
    assert page1[0].id != page2[0].id


@pytest.mark.asyncio
async def test_get_leads_filter(db):
    await queries.upsert_lead(db, Lead(email="valid@test.com", email_status="valid"))
    await queries.upsert_lead(db, Lead(email="invalid@test.com", email_status="invalid"))

    valid_leads = await queries.get_leads(db, email_status="valid")
    assert len(valid_leads) == 1
    assert valid_leads[0].email == "valid@test.com"


@pytest.mark.asyncio
async def test_search_leads(db):
    await queries.upsert_lead(db, Lead(email="doc@smile.com", company="Smile Dental"))
    await queries.upsert_lead(db, Lead(email="bob@other.com", company="Other Corp"))

    results = await queries.search_leads(db, "smile")
    assert len(results) == 1
    assert results[0].company == "Smile Dental"


@pytest.mark.asyncio
async def test_count_leads(db):
    assert await queries.count_leads(db) == 0
    await queries.upsert_lead(db, Lead(email="a@b.com"))
    assert await queries.count_leads(db) == 1


@pytest.mark.asyncio
async def test_delete_lead(db):
    lid = await queries.upsert_lead(db, Lead(email="del@test.com"))
    assert await queries.delete_lead(db, lid)
    assert await queries.get_lead_by_id(db, lid) is None


@pytest.mark.asyncio
async def test_upsert_leads_batch(db):
    leads = [Lead(email=f"batch{i}@test.com") for i in range(5)]
    count = await queries.upsert_leads_batch(db, leads)
    assert count == 5
    assert await queries.count_leads(db) == 5


@pytest.mark.asyncio
async def test_lead_email_uniqueness(db):
    """Upserting same email should update, not duplicate."""
    await queries.upsert_lead(db, Lead(email="dup@test.com", first_name="First"))
    await queries.upsert_lead(db, Lead(email="dup@test.com", first_name="Second"))
    assert await queries.count_leads(db) == 1
    lead = await queries.get_lead_by_email(db, "dup@test.com")
    assert lead is not None
    assert lead.first_name == "Second"


# ---------------------------------------------------------------------------
# Triggers
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_updated_at_trigger(db):
    lid = await queries.upsert_lead(db, Lead(email="trigger@test.com"))
    lead1 = await queries.get_lead_by_id(db, lid)
    assert lead1 is not None
    original_updated = lead1.updated_at

    # Update lead to trigger the updated_at trigger
    await Lead.update({Lead.first_name: "Updated"}).where(Lead.id == lid).run()

    lead2 = await queries.get_lead_by_id(db, lid)
    assert lead2 is not None
    assert lead2.updated_at >= original_updated


# ---------------------------------------------------------------------------
# Mailbox CRUD
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upsert_mailbox(db):
    mb = Mailbox(
        email="out@clinic.com",
        smtp_host="smtp.gmail.com",
        smtp_user="user",
        smtp_pass="pass",
    )
    mid = await queries.upsert_mailbox(db, mb)
    assert mid > 0

    fetched = await queries.get_mailbox_by_id(db, mid)
    assert fetched is not None
    assert fetched.email == "out@clinic.com"
    assert fetched.daily_limit == 30
    assert fetched.warmup_day == 0


@pytest.mark.asyncio
async def test_get_mailboxes_active(db):
    await queries.upsert_mailbox(
        db,
        Mailbox(
            email="a@test.com",
            smtp_host="h",
            smtp_user="u",
            smtp_pass="p",
            is_active=1,
        ),
    )
    await queries.upsert_mailbox(
        db,
        Mailbox(
            email="b@test.com",
            smtp_host="h",
            smtp_user="u",
            smtp_pass="p",
            is_active=0,
        ),
    )
    all_mb = await queries.get_mailboxes(db)
    assert len(all_mb) == 2
    active = await queries.get_mailboxes(db, active_only=True)
    assert len(active) == 1
    assert active[0].email == "a@test.com"


# ---------------------------------------------------------------------------
# Campaign CRUD
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_campaign(db):
    camp = Campaign(name="Test Campaign", daily_limit=25, timezone="America/Chicago")
    cid = await queries.create_campaign(db, camp)
    assert cid > 0

    fetched = await queries.get_campaign_by_id(db, cid)
    assert fetched is not None
    assert fetched.name == "Test Campaign"
    assert fetched.daily_limit == 25
    assert fetched.status == "draft"


@pytest.mark.asyncio
async def test_update_campaign_status(db):
    cid = await queries.create_campaign(db, Campaign(name="Camp"))
    ok = await queries.update_campaign_status(db, cid, "active")
    assert ok
    camp = await queries.get_campaign_by_id(db, cid)
    assert camp is not None
    assert camp.status == "active"


@pytest.mark.asyncio
async def test_campaign_status_update(db):
    """Valid status update should work."""
    cid = await queries.create_campaign(db, Campaign(name="Camp"))
    ok = await queries.update_campaign_status(db, cid, "paused")
    assert ok
    camp = await queries.get_campaign_by_id(db, cid)
    assert camp is not None
    assert camp.status == "paused"


# ---------------------------------------------------------------------------
# Sequence steps
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_add_sequence_steps(db):
    cid = await queries.create_campaign(db, Campaign(name="Camp"))
    await queries.add_sequence_step(
        db,
        SequenceStep(
            campaign_id=cid,
            step_number=0,
            template_name="intro",
            subject="Hello",
            delay_days=0,
        ),
    )
    await queries.add_sequence_step(
        db,
        SequenceStep(
            campaign_id=cid,
            step_number=1,
            template_name="followup",
            subject="Just checking in",
            delay_days=3,
            is_reply=1,
        ),
    )

    steps = await queries.get_sequence_steps(db, cid)
    assert len(steps) == 2
    assert steps[0].template_name == "intro"
    assert steps[1].delay_days == 3
    assert steps[1].is_reply == 1


# ---------------------------------------------------------------------------
# Campaign leads & enrollment
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enroll_lead(db):
    lid = await queries.upsert_lead(db, Lead(email="e@test.com"))
    cid = await queries.create_campaign(db, Campaign(name="Camp"))

    clid = await queries.enroll_lead(db, cid, lid)
    assert clid > 0

    cls = await queries.get_campaign_leads(db, cid)
    assert len(cls) == 1
    assert cls[0].status == "active"
    assert cls[0].current_step == 0


@pytest.mark.asyncio
async def test_enroll_lead_duplicate(db):
    """Enrolling same lead twice should not create a duplicate."""
    lid = await queries.upsert_lead(db, Lead(email="e@test.com"))
    cid = await queries.create_campaign(db, Campaign(name="Camp"))

    await queries.enroll_lead(db, cid, lid)
    await queries.enroll_lead(db, cid, lid)

    cls = await queries.get_campaign_leads(db, cid)
    assert len(cls) == 1


@pytest.mark.asyncio
async def test_advance_step(db):
    lid = await queries.upsert_lead(db, Lead(email="e@test.com"))
    cid = await queries.create_campaign(db, Campaign(name="Camp"))
    clid = await queries.enroll_lead(db, cid, lid)

    ok = await queries.advance_step(db, clid, next_send_at="2026-02-20T08:00:00Z")
    assert ok

    cls = await queries.get_campaign_leads(db, cid)
    assert cls[0].current_step == 1
    assert cls[0].last_sent_at is not None
    assert cls[0].next_send_at == "2026-02-20T08:00:00Z"


# ---------------------------------------------------------------------------
# Send queue
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_send_queue(db):
    lid = await queries.upsert_lead(db, Lead(email="doc@test.com", first_name="Doc"))
    cid = await queries.create_campaign(db, Campaign(name="Camp"))
    await queries.add_sequence_step(
        db,
        SequenceStep(
            campaign_id=cid,
            step_number=0,
            template_name="intro",
            subject="Hi",
            delay_days=0,
        ),
    )
    await queries.enroll_lead(db, cid, lid)

    queue = await queries.get_send_queue(db, cid)
    assert len(queue) == 1
    assert queue[0]["email"] == "doc@test.com"
    assert queue[0]["template_name"] == "intro"


# ---------------------------------------------------------------------------
# Email sent
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_log_send_and_fetch(db):
    lid = await queries.upsert_lead(db, Lead(email="e@test.com"))
    es = EmailSent(
        lead_id=lid,
        subject="Hello",
        to_email="e@test.com",
        from_email="me@test.com",
        body_text="Body",
        message_id="msg-123",
    )
    esid = await queries.log_send(db, es)
    assert esid > 0

    fetched = await queries.get_email_by_message_id(db, "msg-123")
    assert fetched is not None
    assert fetched.subject == "Hello"
    assert fetched.to_email == "e@test.com"


@pytest.mark.asyncio
async def test_update_email_status_replied(db):
    lid = await queries.upsert_lead(db, Lead(email="e@test.com"))
    esid = await queries.log_send(
        db,
        EmailSent(
            lead_id=lid,
            message_id="msg-r",
            to_email="e@test.com",
            from_email="me@test.com",
            body_text="Hi",
        ),
    )
    ok = await queries.update_email_status(db, esid, "replied")
    assert ok

    fetched = await queries.get_email_by_message_id(db, "msg-r")
    assert fetched is not None
    assert fetched.status == "replied"
    assert fetched.replied_at is not None


@pytest.mark.asyncio
async def test_update_email_status_bounced(db):
    lid = await queries.upsert_lead(db, Lead(email="e@test.com"))
    esid = await queries.log_send(
        db,
        EmailSent(
            lead_id=lid,
            message_id="msg-b",
            to_email="e@test.com",
            from_email="me@test.com",
            body_text="Hi",
        ),
    )
    ok = await queries.update_email_status(db, esid, "bounced")
    assert ok

    fetched = await queries.get_email_by_message_id(db, "msg-b")
    assert fetched is not None
    assert fetched.status == "bounced"
    assert fetched.bounced_at is not None


# ---------------------------------------------------------------------------
# Daily send limits
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_daily_limit(db):
    mid = await queries.upsert_mailbox(
        db,
        Mailbox(
            email="out@test.com",
            smtp_host="h",
            smtp_user="u",
            smtp_pass="p",
        ),
    )

    sent, limit = await queries.check_daily_limit(db, mid)
    assert sent == 0
    assert limit == 30

    await queries.increment_daily_send(db, mid)
    await queries.increment_daily_send(db, mid)

    sent2, _ = await queries.check_daily_limit(db, mid)
    assert sent2 == 2


# ---------------------------------------------------------------------------
# Deals
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_deal_upsert_and_update(db):
    lid = await queries.upsert_lead(db, Lead(email="e@test.com"))

    deal = Deal(lead_id=lid, stage="contacted", value=3000.0)
    did = await queries.upsert_deal(db, deal)
    assert did > 0

    # Update the deal
    updated = Deal(id=did, lead_id=lid, stage="replied", value=5000.0, notes="Good prospect")
    did2 = await queries.upsert_deal(db, updated)
    assert did2 == did

    fetched = await queries.get_deal_by_id(db, did)
    assert fetched is not None
    assert fetched.stage == "replied"
    assert fetched.value == 5000.0
    assert fetched.notes == "Good prospect"


@pytest.mark.asyncio
async def test_deal_upsert_valid_stage(db):
    """Valid stage should work."""
    lid = await queries.upsert_lead(db, Lead(email="e@test.com"))
    did = await queries.upsert_deal(db, Deal(lead_id=lid, stage="contacted"))
    assert did > 0
    deal = await queries.get_deal_by_id(db, did)
    assert deal is not None
    assert deal.stage == "contacted"


@pytest.mark.asyncio
async def test_pipeline_stats(db):
    lid = await queries.upsert_lead(db, Lead(email="e@test.com"))
    await queries.upsert_deal(db, Deal(lead_id=lid, stage="contacted"))
    await queries.upsert_deal(db, Deal(lead_id=lid, stage="contacted"))

    lid2 = await queries.upsert_lead(db, Lead(email="f@test.com"))
    await queries.upsert_deal(db, Deal(lead_id=lid2, stage="replied"))

    stats = await queries.get_pipeline_stats(db)
    assert stats["contacted"] == 2
    assert stats["replied"] == 1


# ---------------------------------------------------------------------------
# Tracking events
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_log_tracking_event(db):
    lid = await queries.upsert_lead(db, Lead(email="e@test.com"))
    esid = await queries.log_send(
        db,
        EmailSent(
            lead_id=lid,
            message_id="msg-t",
            to_email="e@test.com",
            from_email="me@test.com",
            body_text="Hi",
        ),
    )

    tid = await queries.log_tracking_event(
        db,
        TrackingEvent(
            email_sent_id=esid,
            event_type="reply",
            metadata='{"detail": "test"}',
        ),
    )
    assert tid > 0


# ---------------------------------------------------------------------------
# Stats queries
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_campaign_stats(db):
    lid = await queries.upsert_lead(db, Lead(email="e@test.com"))
    cid = await queries.create_campaign(db, Campaign(name="Camp"))
    await queries.enroll_lead(db, cid, lid)

    stats = await queries.get_campaign_stats(db, cid)
    assert stats["total"] == 1
    assert stats["active"] == 1


@pytest.mark.asyncio
async def test_lead_stats(db):
    await queries.upsert_lead(db, Lead(email="a@test.com", city="Austin", state="TX"))
    await queries.upsert_lead(db, Lead(email="b@test.com", city="Austin", state="TX"))

    stats = await queries.get_lead_stats(db)
    assert stats["total"] == 2
    assert stats["by_city"]["Austin, TX"] == 2


@pytest.mark.asyncio
async def test_tag_leads(db):
    lid1 = await queries.upsert_lead(db, Lead(email="a@test.com"))
    lid2 = await queries.upsert_lead(db, Lead(email="b@test.com"))

    count = await queries.tag_leads(db, [lid1, lid2], "vip")
    assert count == 2

    lead = await queries.get_lead_by_id(db, lid1)
    assert lead is not None
    assert "vip" in lead.tags

    # Tagging again should not duplicate
    count2 = await queries.tag_leads(db, [lid1], "vip")
    assert count2 == 0


@pytest.mark.asyncio
async def test_deactivate_mailbox(db):
    mid = await queries.upsert_mailbox(
        db,
        Mailbox(
            email="m@test.com",
            smtp_host="h",
            smtp_user="u",
            smtp_pass="p",
        ),
    )
    ok = await queries.deactivate_mailbox(db, mid)
    assert ok
    mb = await queries.get_mailbox_by_id(db, mid)
    assert mb is not None
    assert mb.is_active == 0


@pytest.mark.asyncio
async def test_warmup_limit():
    assert queries.get_warmup_limit(1) == 5
    assert queries.get_warmup_limit(3) == 5
    assert queries.get_warmup_limit(5) == 10
    assert queries.get_warmup_limit(10) == 20
    assert queries.get_warmup_limit(15) == 30
    assert queries.get_warmup_limit(21) == 30
    assert queries.get_warmup_limit(30) == 48
    assert queries.get_warmup_limit(50) == 50


# ---------------------------------------------------------------------------
# Migration tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_migrate_idempotent(db):
    """Running init_db twice should not error."""
    from db import get_engine, init_db

    # Re-init on the same DB should be idempotent
    engine = get_engine()
    path = engine.path
    await init_db(path)
    # If we get here without error, migration is idempotent


@pytest.mark.asyncio
async def test_v2_columns_exist(db):
    """v2 migration columns should exist with correct defaults."""
    lid = await queries.upsert_lead(db, Lead(email="v2test@test.com"))
    lead = await queries.get_lead_by_id(db, lid)
    assert lead is not None
    assert lead.email_confidence == 0.0
    assert lead.email_source == ""
    assert lead.email_provider == ""
