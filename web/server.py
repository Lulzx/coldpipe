"""Web dashboard — aiohttp + Jinja2 + Tabler UI."""

from __future__ import annotations

import csv
import io
from collections import defaultdict
from pathlib import Path

import aiohttp_jinja2
import jinja2
from aiohttp import web

from config.settings import load_settings
from db import DBPool
from db.models import Deal, Mailbox
from db.queries import (
    check_daily_limit,
    count_emails_sent,
    count_leads,
    deactivate_mailbox,
    delete_campaign,
    get_campaign_by_id,
    get_campaign_leads,
    get_campaign_stats,
    get_campaign_step_distribution,
    get_campaigns,
    get_daily_stats,
    get_deal_by_id,
    get_deal_stats,
    get_deals,
    get_email_status_distribution,
    get_emails_for_lead,
    get_emails_sent,
    get_lead_by_id,
    get_lead_stats,
    get_leads,
    get_mailbox_by_id,
    get_mailboxes,
    get_pipeline_stats,
    get_sequence_steps,
    get_today_activity,
    get_warmup_limit,
    search_leads,
    update_campaign_status,
    upsert_deal,
    upsert_mailbox,
)

TEMPLATES_DIR = Path(__file__).parent / "templates"


# ---------------------------------------------------------------------------
# Lifecycle hooks
# ---------------------------------------------------------------------------


async def on_startup(app: web.Application) -> None:
    pool = DBPool()
    await pool.open()
    app["db"] = pool


async def on_cleanup(app: web.Application) -> None:
    await app["db"].close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PAGE_SIZE = 50
DEAL_STAGES = [
    "lead",
    "contacted",
    "replied",
    "interested",
    "meeting_booked",
    "proposal_sent",
    "closed_won",
    "closed_lost",
]


def _int(val: str | None, default: int = 0) -> int:
    try:
        return int(val) if val else default
    except ValueError:
        return default


def _mask(value: str) -> str:
    if not value:
        return ""
    return value[:2] + "\u2022" * min(len(value) - 2, 20) if len(value) > 4 else "\u2022" * len(value)


# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------


@aiohttp_jinja2.template("dashboard.html")
async def dashboard(request: web.Request) -> dict:
    pool: DBPool = request.app["db"]
    async with pool.acquire() as db:
        lead_stats = await get_lead_stats(db)
        activity = await get_today_activity(db)
        deal_stats = await get_deal_stats(db)
        pipeline = await get_pipeline_stats(db)
        campaigns = await get_campaigns(db, status="active")
        daily = await get_daily_stats(db, 30)
        mailboxes = await get_mailboxes(db, active_only=True)
        mailbox_warmup = []
        for mb in mailboxes:
            sent, limit = await check_daily_limit(db, mb.id)
            warmup_limit = get_warmup_limit(mb.warmup_day)
            mailbox_warmup.append({
                "email": mb.email,
                "warmup_day": mb.warmup_day,
                "warmup_limit": warmup_limit,
                "sent_today": sent,
                "daily_limit": limit,
            })

    # Pivot daily stats into chart series
    days_map: dict[str, dict[str, int]] = defaultdict(lambda: {"sent": 0, "replied": 0, "bounced": 0})
    for row in daily:
        days_map[row["day"]][row["status"]] = row["cnt"]
    sorted_days = sorted(days_map.keys())
    chart_series = {
        "days": sorted_days,
        "sent": [days_map[d]["sent"] for d in sorted_days],
        "replied": [days_map[d]["replied"] for d in sorted_days],
        "bounced": [days_map[d]["bounced"] for d in sorted_days],
    }

    return {
        "active_page": "dashboard",
        "lead_stats": lead_stats,
        "activity": activity,
        "deal_stats": deal_stats,
        "pipeline": pipeline,
        "campaigns": campaigns,
        "daily": daily,
        "chart_series": chart_series,
        "mailbox_warmup": mailbox_warmup,
        "stages": DEAL_STAGES,
    }


@aiohttp_jinja2.template("leads/list.html")
async def leads_list(request: web.Request) -> dict:
    pool: DBPool = request.app["db"]
    page = max(1, _int(request.query.get("page"), 1))
    status = request.query.get("status") or None
    source = request.query.get("source") or None
    offset = (page - 1) * PAGE_SIZE
    async with pool.acquire() as db:
        leads = await get_leads(db, limit=PAGE_SIZE, offset=offset, email_status=status, source=source)
        total = await count_leads(db, email_status=status)
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    return {
        "active_page": "leads",
        "leads": leads,
        "total": total,
        "page": page,
        "total_pages": total_pages,
        "status": status or "",
        "source": source or "",
    }


@aiohttp_jinja2.template("leads/list.html")
async def leads_search(request: web.Request) -> dict:
    pool: DBPool = request.app["db"]
    q = request.query.get("q", "")
    async with pool.acquire() as db:
        leads = await search_leads(db, q, limit=PAGE_SIZE) if q else []
        total = len(leads)
    return {
        "active_page": "leads",
        "leads": leads,
        "total": total,
        "page": 1,
        "total_pages": 1,
        "status": "",
        "source": "",
        "search_query": q,
    }


async def leads_export(request: web.Request) -> web.StreamResponse:
    """Export all leads as CSV."""
    pool: DBPool = request.app["db"]
    async with pool.acquire() as db:
        leads = await get_leads(db, limit=10000, offset=0)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "id", "email", "first_name", "last_name", "company", "job_title",
        "website", "phone", "city", "state", "zip", "source", "email_status",
        "tags", "created_at",
    ])
    for lead in leads:
        writer.writerow([
            lead.id, lead.email, lead.first_name, lead.last_name,
            lead.company, lead.job_title, lead.website, lead.phone,
            lead.city, lead.state, lead.zip, lead.source,
            lead.email_status, lead.tags, lead.created_at,
        ])

    return web.Response(
        body=output.getvalue(),
        content_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=leads.csv"},
    )


@aiohttp_jinja2.template("leads/detail.html")
async def lead_detail(request: web.Request) -> dict:
    pool: DBPool = request.app["db"]
    lead_id = int(request.match_info["id"])
    async with pool.acquire() as db:
        lead = await get_lead_by_id(db, lead_id)
        if lead is None:
            raise web.HTTPNotFound(text="Lead not found")
        emails = await get_emails_for_lead(db, lead_id)
    return {"active_page": "leads", "lead": lead, "emails": emails}


@aiohttp_jinja2.template("campaigns/list.html")
async def campaigns_list(request: web.Request) -> dict:
    pool: DBPool = request.app["db"]
    status = request.query.get("status") or None
    async with pool.acquire() as db:
        campaigns = await get_campaigns(db, status=status)
    return {"active_page": "campaigns", "campaigns": campaigns, "status": status or ""}


@aiohttp_jinja2.template("campaigns/detail.html")
async def campaign_detail(request: web.Request) -> dict:
    pool: DBPool = request.app["db"]
    cid = int(request.match_info["id"])
    async with pool.acquire() as db:
        campaign = await get_campaign_by_id(db, cid)
        if campaign is None:
            raise web.HTTPNotFound(text="Campaign not found")
        stats = await get_campaign_stats(db, cid)
        steps = await get_sequence_steps(db, cid)
        leads = await get_campaign_leads(db, cid)
        step_dist = await get_campaign_step_distribution(db, cid)
    return {
        "active_page": "campaigns",
        "campaign": campaign,
        "stats": stats,
        "steps": steps,
        "leads": leads,
        "step_dist": step_dist,
    }


async def campaign_pause(request: web.Request) -> web.Response:
    pool: DBPool = request.app["db"]
    cid = int(request.match_info["id"])
    async with pool.acquire() as db:
        await update_campaign_status(db, cid, "paused")
    raise web.HTTPFound(f"/campaigns/{cid}")


async def campaign_resume(request: web.Request) -> web.Response:
    pool: DBPool = request.app["db"]
    cid = int(request.match_info["id"])
    async with pool.acquire() as db:
        await update_campaign_status(db, cid, "active")
    raise web.HTTPFound(f"/campaigns/{cid}")


async def campaign_delete(request: web.Request) -> web.Response:
    pool: DBPool = request.app["db"]
    cid = int(request.match_info["id"])
    async with pool.acquire() as db:
        await delete_campaign(db, cid)
    raise web.HTTPFound("/campaigns")


@aiohttp_jinja2.template("mailboxes/list.html")
async def mailboxes_list(request: web.Request) -> dict:
    pool: DBPool = request.app["db"]
    async with pool.acquire() as db:
        mboxes = await get_mailboxes(db)
        usage = {}
        for mb in mboxes:
            sent, limit = await check_daily_limit(db, mb.id)
            usage[mb.id] = {"sent": sent, "limit": limit}
    return {"active_page": "mailboxes", "mailboxes": mboxes, "usage": usage}


@aiohttp_jinja2.template("mailboxes/form.html")
async def mailbox_add_form(request: web.Request) -> dict:
    return {"active_page": "mailboxes", "mailbox": None, "errors": []}


async def mailbox_add_submit(request: web.Request) -> web.Response:
    pool: DBPool = request.app["db"]
    data = await request.post()
    mb = Mailbox(
        email=str(data.get("email", "")),
        smtp_host=str(data.get("smtp_host", "")),
        smtp_port=_int(str(data.get("smtp_port", "587")), 587),
        smtp_user=str(data.get("smtp_user", "")),
        smtp_pass=str(data.get("smtp_pass", "")),
        imap_host=str(data.get("imap_host", "")),
        imap_port=_int(str(data.get("imap_port", "993")), 993),
        imap_user=str(data.get("imap_user", "")),
        imap_pass=str(data.get("imap_pass", "")),
        daily_limit=_int(str(data.get("daily_limit", "30")), 30),
        display_name=str(data.get("display_name", "")),
        is_active=1 if data.get("is_active") else 0,
    )
    async with pool.acquire() as db:
        await upsert_mailbox(db, mb)
    raise web.HTTPFound("/mailboxes")


@aiohttp_jinja2.template("mailboxes/form.html")
async def mailbox_edit_form(request: web.Request) -> dict:
    pool: DBPool = request.app["db"]
    mid = int(request.match_info["id"])
    async with pool.acquire() as db:
        mb = await get_mailbox_by_id(db, mid)
        if mb is None:
            raise web.HTTPNotFound(text="Mailbox not found")
    return {"active_page": "mailboxes", "mailbox": mb, "errors": []}


async def mailbox_edit_submit(request: web.Request) -> web.Response:
    pool: DBPool = request.app["db"]
    mid = int(request.match_info["id"])
    data = await request.post()
    async with pool.acquire() as db:
        existing = await get_mailbox_by_id(db, mid)
        if existing is None:
            raise web.HTTPNotFound(text="Mailbox not found")
        mb = Mailbox(
            id=mid,
            email=existing.email,
            smtp_host=str(data.get("smtp_host", existing.smtp_host)),
            smtp_port=_int(str(data.get("smtp_port", "")), existing.smtp_port),
            smtp_user=str(data.get("smtp_user", existing.smtp_user)),
            smtp_pass=str(data.get("smtp_pass", "")) or existing.smtp_pass,
            imap_host=str(data.get("imap_host", existing.imap_host)),
            imap_port=_int(str(data.get("imap_port", "")), existing.imap_port),
            imap_user=str(data.get("imap_user", existing.imap_user)),
            imap_pass=str(data.get("imap_pass", "")) or existing.imap_pass,
            daily_limit=_int(str(data.get("daily_limit", "")), existing.daily_limit),
            display_name=str(data.get("display_name", existing.display_name)),
            is_active=1 if data.get("is_active") else 0,
        )
        await upsert_mailbox(db, mb)
    raise web.HTTPFound("/mailboxes")


async def mailbox_deactivate(request: web.Request) -> web.Response:
    pool: DBPool = request.app["db"]
    mid = int(request.match_info["id"])
    async with pool.acquire() as db:
        await deactivate_mailbox(db, mid)
    raise web.HTTPFound("/mailboxes")


@aiohttp_jinja2.template("deals/list.html")
async def deals_list(request: web.Request) -> dict:
    pool: DBPool = request.app["db"]
    stage = request.query.get("stage") or None
    async with pool.acquire() as db:
        deals = await get_deals(db, stage=stage)
        stats = await get_deal_stats(db)
    return {
        "active_page": "deals",
        "deals": deals,
        "stats": stats,
        "stage": stage or "",
        "stages": DEAL_STAGES,
    }


@aiohttp_jinja2.template("deals/pipeline.html")
async def deals_pipeline(request: web.Request) -> dict:
    pool: DBPool = request.app["db"]
    async with pool.acquire() as db:
        deals = await get_deals(db)
        stats = await get_deal_stats(db)
    # Group deals by stage
    by_stage: dict[str, list] = {s: [] for s in DEAL_STAGES}
    for d in deals:
        by_stage.setdefault(d.stage, []).append(d)
    return {
        "active_page": "deals",
        "by_stage": by_stage,
        "stats": stats,
        "stages": DEAL_STAGES,
    }


async def deal_create(request: web.Request) -> web.Response:
    pool: DBPool = request.app["db"]
    data = await request.post()
    deal = Deal(
        lead_id=_int(str(data.get("lead_id", "0"))),
        stage=str(data.get("stage", "lead")),
        value=float(data.get("value", 0) or 0),
        notes=str(data.get("notes", "")),
    )
    async with pool.acquire() as db:
        await upsert_deal(db, deal)
    raise web.HTTPFound("/deals")


async def deal_move(request: web.Request) -> web.Response:
    pool: DBPool = request.app["db"]
    did = int(request.match_info["id"])
    data = await request.post()
    new_stage = str(data.get("stage", ""))
    async with pool.acquire() as db:
        deal = await get_deal_by_id(db, did)
        if deal is None:
            raise web.HTTPNotFound(text="Deal not found")
        deal.stage = new_stage
        await upsert_deal(db, deal)
    raise web.HTTPFound("/deals")


@aiohttp_jinja2.template("emails/list.html")
async def emails_list(request: web.Request) -> dict:
    pool: DBPool = request.app["db"]
    page = max(1, _int(request.query.get("page"), 1))
    status = request.query.get("status") or None
    offset = (page - 1) * PAGE_SIZE
    async with pool.acquire() as db:
        emails = await get_emails_sent(db, limit=PAGE_SIZE, offset=offset, status=status)
        total = await count_emails_sent(db, status=status)
        email_dist = await get_email_status_distribution(db)
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    return {
        "active_page": "emails",
        "emails": emails,
        "total": total,
        "page": page,
        "total_pages": total_pages,
        "status": status or "",
        "email_dist": email_dist,
    }


@aiohttp_jinja2.template("settings.html")
async def settings_view(request: web.Request) -> dict:
    settings = load_settings()
    return {"active_page": "settings", "settings": settings, "mask": _mask}


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------


async def create_app() -> web.Application:
    app = web.Application()

    aiohttp_jinja2.setup(
        app,
        loader=jinja2.FileSystemLoader(str(TEMPLATES_DIR)),
    )

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    app.router.add_get("/", dashboard)
    app.router.add_get("/leads", leads_list)
    app.router.add_get("/leads/search", leads_search)
    app.router.add_get("/leads/export", leads_export)
    app.router.add_get("/leads/{id}", lead_detail)
    app.router.add_get("/campaigns", campaigns_list)
    app.router.add_get("/campaigns/{id}", campaign_detail)
    app.router.add_post("/campaigns/{id}/pause", campaign_pause)
    app.router.add_post("/campaigns/{id}/resume", campaign_resume)
    app.router.add_post("/campaigns/{id}/delete", campaign_delete)
    app.router.add_get("/mailboxes", mailboxes_list)
    app.router.add_get("/mailboxes/add", mailbox_add_form)
    app.router.add_post("/mailboxes/add", mailbox_add_submit)
    app.router.add_get("/mailboxes/{id}/edit", mailbox_edit_form)
    app.router.add_post("/mailboxes/{id}/edit", mailbox_edit_submit)
    app.router.add_post("/mailboxes/{id}/deactivate", mailbox_deactivate)
    app.router.add_get("/deals", deals_list)
    app.router.add_get("/deals/pipeline", deals_pipeline)
    app.router.add_post("/deals/create", deal_create)
    app.router.add_post("/deals/{id}/move", deal_move)
    app.router.add_get("/emails", emails_list)
    app.router.add_get("/settings", settings_view)

    return app
