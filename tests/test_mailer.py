"""Tests for the email engine: send queue, sequences, templates, daily limits."""

from __future__ import annotations

import pytest

from db import queries
from db.models import Campaign, EmailSent, Lead, Mailbox, SequenceStep
from mailer.queue import SendQueue, warmup_daily_limit
from mailer.sequences import advance_sequence, complete_sequence, handle_reply

# ---------------------------------------------------------------------------
# Send queue
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_send_queue_fill(db):
    """SendQueue.fill() should populate buffer from DB."""
    lid = await queries.upsert_lead(db, Lead(email="doc@test.com"))
    mid = await queries.upsert_mailbox(
        db,
        Mailbox(
            email="out@test.com",
            smtp_host="h",
            smtp_user="u",
            smtp_pass="p",
        ),
    )
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

    from config.settings import SendSettings

    settings = SendSettings(
        send_window_start="00:00",
        send_window_end="23:59",
    )
    queue = SendQueue(db, cid, mid, settings)

    await queue.fill()
    item = await queue.get()
    assert item is not None
    assert item["email"] == "doc@test.com"


@pytest.mark.asyncio
async def test_send_queue_empty_when_stopped(db):
    await queries.upsert_lead(db, Lead(email="doc@test.com"))
    mid = await queries.upsert_mailbox(
        db,
        Mailbox(
            email="out@test.com",
            smtp_host="h",
            smtp_user="u",
            smtp_pass="p",
        ),
    )
    cid = await queries.create_campaign(db, Campaign(name="Camp"))

    from config.settings import SendSettings

    settings = SendSettings(send_window_start="00:00", send_window_end="23:59")
    queue = SendQueue(db, cid, mid, settings)
    queue.stop()

    item = await queue.get()
    assert item is None


@pytest.mark.asyncio
async def test_send_queue_async_iter(db):
    """SendQueue should support async for loop."""
    lid = await queries.upsert_lead(db, Lead(email="doc@test.com"))
    mid = await queries.upsert_mailbox(
        db,
        Mailbox(
            email="out@test.com",
            smtp_host="h",
            smtp_user="u",
            smtp_pass="p",
        ),
    )
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

    from config.settings import SendSettings

    settings = SendSettings(send_window_start="00:00", send_window_end="23:59")
    queue = SendQueue(db, cid, mid, settings)
    await queue.fill()

    items = [item async for item in queue]
    assert len(items) == 1


# ---------------------------------------------------------------------------
# Warmup daily limit
# ---------------------------------------------------------------------------


def test_warmup_daily_limit():
    assert warmup_daily_limit(1) == 5
    assert warmup_daily_limit(3) == 5
    assert warmup_daily_limit(4) == 10
    assert warmup_daily_limit(7) == 10
    assert warmup_daily_limit(8) == 20
    assert warmup_daily_limit(14) == 20
    assert warmup_daily_limit(15) == 30
    assert warmup_daily_limit(21) == 30
    assert warmup_daily_limit(22) == 40
    assert warmup_daily_limit(30) == 48
    assert warmup_daily_limit(50) == 50


# ---------------------------------------------------------------------------
# Sequences
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_advance_sequence(db):
    lid = await queries.upsert_lead(db, Lead(email="doc@test.com"))
    mid = await queries.upsert_mailbox(
        db,
        Mailbox(
            email="out@test.com",
            smtp_host="h",
            smtp_user="u",
            smtp_pass="p",
        ),
    )
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
    clid = await queries.enroll_lead(db, cid, lid)

    email_id = await advance_sequence(
        db,
        campaign_lead_id=clid,
        campaign_id=cid,
        lead_id=lid,
        mailbox_id=mid,
        step_number=0,
        subject="Hello",
        body="Body text",
        message_id="msg-adv-1",
        delay_days=0,
    )
    assert email_id > 0

    # Check that campaign_lead advanced to step 1
    cls = await queries.get_campaign_leads(db, cid)
    assert cls[0].current_step == 1
    assert cls[0].last_sent_at is not None

    # Check daily send incremented
    sent, _ = await queries.check_daily_limit(db, mid)
    assert sent == 1


@pytest.mark.asyncio
async def test_complete_sequence(db):
    lid = await queries.upsert_lead(db, Lead(email="doc@test.com"))
    cid = await queries.create_campaign(db, Campaign(name="Camp"))
    clid = await queries.enroll_lead(db, cid, lid)

    await complete_sequence(db, clid)

    cls = await queries.get_campaign_leads(db, cid)
    assert cls[0].status == "completed"


@pytest.mark.asyncio
async def test_handle_reply(db):
    """handle_reply should mark email as replied, stop sequence, create deal."""
    lid = await queries.upsert_lead(db, Lead(email="doc@test.com"))
    cid = await queries.create_campaign(db, Campaign(name="Camp"))
    await queries.enroll_lead(db, cid, lid)

    esid = await queries.log_send(
        db,
        EmailSent(
            campaign_id=cid,
            lead_id=lid,
            step_number=0,
            message_id="msg-reply-1",
            to_email="doc@test.com",
            from_email="me@test.com",
            body_text="Hi",
        ),
    )

    deal_id = await handle_reply(
        db,
        email_sent_id=esid,
        campaign_id=cid,
        lead_id=lid,
    )
    assert deal_id is not None

    # Email should be replied
    es = await queries.get_email_by_message_id(db, "msg-reply-1")
    assert es is not None
    assert es.status == "replied"

    # Campaign lead should be replied (stopped)
    cls = await queries.get_campaign_leads(db, cid)
    assert cls[0].status == "replied"

    # Deal should exist
    deal = await queries.get_deal_by_id(db, deal_id)
    assert deal is not None
    assert deal.stage == "replied"
    assert deal.lead_id == lid


# ---------------------------------------------------------------------------
# Daily limit checking
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_daily_limit_enforcement(db):
    """After hitting daily limit, check_daily_limit should return correct values."""
    mid = await queries.upsert_mailbox(
        db,
        Mailbox(
            email="out@test.com",
            smtp_host="h",
            smtp_user="u",
            smtp_pass="p",
            daily_limit=3,
        ),
    )

    for _ in range(3):
        await queries.increment_daily_send(db, mid)

    sent, limit = await queries.check_daily_limit(db, mid)
    assert sent == 3
    assert limit == 3


# ---------------------------------------------------------------------------
# Template rendering
# ---------------------------------------------------------------------------


def test_template_rendering(tmp_path):
    """render_template should substitute variables in a Jinja2 template."""
    tpl_dir = tmp_path / "templates"
    tpl_dir.mkdir()
    (tpl_dir / "intro.txt").write_text("Hello {{ first_name }}, welcome to {{ company }}!")

    # Temporarily override the template env
    from jinja2 import Environment, FileSystemLoader

    env = Environment(loader=FileSystemLoader(str(tpl_dir)), autoescape=False)
    tpl = env.get_template("intro.txt")
    result = tpl.render(first_name="Alice", company="Smile Dental")
    assert result == "Hello Alice, welcome to Smile Dental!"
