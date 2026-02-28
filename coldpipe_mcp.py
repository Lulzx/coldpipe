"""Coldpipe FastMCP server — exposes all Coldpipe capabilities as MCP tools.

Run with:
    python coldpipe_mcp.py

Claude Code picks this up automatically via .mcp.json in the project root.
"""

from __future__ import annotations

import asyncio
import json
import time

from fastmcp import Context, FastMCP

from db import init_db
from db.queries import (
    count_leads,
    create_campaign,
    enroll_lead,
    get_campaign_stats,
    get_campaigns,
    get_campaign_by_id,
    get_deal_stats,
    get_deals,
    get_lead_by_id,
    get_lead_stats,
    get_leads,
    get_mcp_activity,
    get_mcp_stats,
    get_sequence_steps,
    get_send_queue,
    get_today_activity,
    log_mcp_activity,
    search_leads,
    tag_leads,
    update_campaign_status,
    update_mcp_activity,
    upsert_deal,
    upsert_lead,
    add_sequence_step,
)
from db.tables import Campaign, Deal, Lead, SequenceStep

mcp = FastMCP("coldpipe")


# ---------------------------------------------------------------------------
# Activity logging wrapper
# ---------------------------------------------------------------------------


async def _logged(tool_name: str, params: dict, coro):
    """Wrap a coroutine with MCP activity logging."""
    start = time.monotonic()
    row_id = await log_mcp_activity(tool_name, json.dumps(params), status="running")
    try:
        result = await coro
        summary = str(result)[:200] if result is not None else ""
        await update_mcp_activity(
            row_id,
            "success",
            result_summary=summary,
            duration_ms=int((time.monotonic() - start) * 1000),
        )
        return result
    except Exception as e:
        await update_mcp_activity(
            row_id,
            "error",
            duration_ms=int((time.monotonic() - start) * 1000),
            error=str(e),
        )
        raise


# ---------------------------------------------------------------------------
# Lead Discovery tools
# ---------------------------------------------------------------------------


@mcp.tool
async def scrape_google_maps(city: str, query: str = "businesses", max_results: int = 20) -> str:
    """Scrape business leads from Google Maps for a given city and query."""
    from scrapers.google_maps import GoogleMapsScraper

    async def _work():
        leads = await GoogleMapsScraper().scrape(city=city, max_results=max_results)
        saved = 0
        for lead in leads:
            try:
                await upsert_lead(lead=lead)
                saved += 1
            except Exception:
                pass
        return f"Scraped {len(leads)} leads, saved {saved} to DB"

    return await _logged("scrape_google_maps", {"city": city, "query": query, "max_results": max_results}, _work())


@mcp.tool
async def scrape_exa(query: str, city: str = "", max_results: int = 20) -> str:
    """Search for business websites using Exa.ai and save leads to DB."""
    from scrapers.exa_search import ExaScraper

    async def _work():
        leads = await ExaScraper().scrape(query=query, city=city, max_results=max_results)
        saved = 0
        for lead in leads:
            try:
                await upsert_lead(lead=lead)
                saved += 1
            except Exception:
                pass
        return f"Found {len(leads)} leads via Exa, saved {saved} to DB"

    return await _logged("scrape_exa", {"query": query, "city": city, "max_results": max_results}, _work())


@mcp.tool
async def enrich_websites(limit: int = 50, lead_ids: list[int] | None = None) -> str:
    """Crawl lead websites to extract emails, phones, and contact info."""
    from scrapers.website_enricher import WebsiteEnricher

    async def _work():
        enriched = await WebsiteEnricher().scrape(limit=limit, lead_ids=lead_ids)
        return f"Enriched {len(enriched)} leads"

    return await _logged("enrich_websites", {"limit": limit, "lead_ids": lead_ids}, _work())


# ---------------------------------------------------------------------------
# Lead Management tools
# ---------------------------------------------------------------------------


@mcp.tool
async def get_leads_tool(
    limit: int = 50,
    offset: int = 0,
    email_status: str | None = None,
    source: str | None = None,
) -> str:
    """Fetch leads from the database with optional filters."""
    async def _work():
        leads = await get_leads(limit=limit, offset=offset, email_status=email_status, source=source)
        return json.dumps([
            {
                "id": l.id,
                "email": l.email,
                "first_name": l.first_name,
                "last_name": l.last_name,
                "company": l.company,
                "email_status": l.email_status,
                "source": l.source,
                "city": l.city,
                "website": l.website,
            }
            for l in leads
        ])

    return await _logged("get_leads", {"limit": limit, "offset": offset, "email_status": email_status, "source": source}, _work())


@mcp.tool
async def get_lead(lead_id: int) -> str:
    """Fetch a single lead by ID."""
    async def _work():
        lead = await get_lead_by_id(lead_id=lead_id)
        if not lead:
            return f"Lead {lead_id} not found"
        return json.dumps({
            "id": lead.id,
            "email": lead.email,
            "first_name": lead.first_name,
            "last_name": lead.last_name,
            "company": lead.company,
            "job_title": lead.job_title,
            "website": lead.website,
            "phone": lead.phone,
            "city": lead.city,
            "state": lead.state,
            "email_status": lead.email_status,
            "email_confidence": lead.email_confidence,
            "source": lead.source,
            "tags": lead.tags,
            "notes": lead.notes,
        })

    return await _logged("get_lead", {"lead_id": lead_id}, _work())


@mcp.tool
async def search_leads_tool(query: str, limit: int = 20) -> str:
    """Search leads by email, name, or company."""
    async def _work():
        leads = await search_leads(query=query, limit=limit)
        return json.dumps([
            {"id": l.id, "email": l.email, "name": f"{l.first_name} {l.last_name}".strip(), "company": l.company}
            for l in leads
        ])

    return await _logged("search_leads", {"query": query, "limit": limit}, _work())


@mcp.tool
async def save_lead(
    email: str,
    first_name: str = "",
    last_name: str = "",
    company: str = "",
    job_title: str = "",
    website: str = "",
    phone: str = "",
    city: str = "",
    state: str = "",
    source: str = "manual",
    notes: str = "",
) -> str:
    """Save or update a lead in the database."""
    async def _work():
        lead = Lead(
            email=email,
            first_name=first_name,
            last_name=last_name,
            company=company,
            job_title=job_title,
            website=website,
            phone=phone,
            city=city,
            state=state,
            source=source,
            notes=notes,
        )
        lead_id = await upsert_lead(lead=lead)
        return f"Lead saved with id={lead_id}"

    return await _logged("save_lead", {"email": email, "company": company}, _work())


@mcp.tool
async def tag_leads_tool(lead_ids: list[int], tag: str) -> str:
    """Add a tag to multiple leads."""
    async def _work():
        count = await tag_leads(lead_ids=lead_ids, tag=tag)
        return f"Tagged {count} leads with '{tag}'"

    return await _logged("tag_leads", {"lead_ids": lead_ids, "tag": tag}, _work())


@mcp.tool
async def count_leads_tool(email_status: str | None = None) -> str:
    """Count leads with optional email_status filter."""
    async def _work():
        total = await count_leads(email_status=email_status)
        return f"{total} leads" + (f" with status={email_status}" if email_status else "")

    return await _logged("count_leads", {"email_status": email_status}, _work())


# ---------------------------------------------------------------------------
# Email Validation tools
# ---------------------------------------------------------------------------


@mcp.tool
async def validate_leads(lead_ids: list[int] | None = None, limit: int = 50) -> str:
    """Validate email addresses for leads (batch)."""
    from tools.validate import EmailValidator

    async def _work():
        if lead_ids:
            leads = [l for l in await get_leads(limit=1000) if l.id in set(lead_ids)]
        else:
            leads = await get_leads(limit=limit, email_status="unknown")
        if not leads:
            return "No leads to validate"
        validator = EmailValidator()
        valid_count = 0
        invalid_count = 0
        for lead in leads:
            if not lead.email:
                continue
            result = await validator.validate_email(lead.email)
            status = result.get("status", "unknown")
            if status == "valid":
                valid_count += 1
            elif status == "invalid":
                invalid_count += 1
            lead.email_status = status
            lead.validated_at = __import__("datetime").datetime.now(__import__("datetime").UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            await upsert_lead(lead=lead)
        return f"Validated {len(leads)} leads: {valid_count} valid, {invalid_count} invalid"

    return await _logged("validate_leads", {"lead_ids": lead_ids, "limit": limit}, _work())


# ---------------------------------------------------------------------------
# Campaign Management tools
# ---------------------------------------------------------------------------


@mcp.tool
async def get_campaigns_tool(status: str | None = None) -> str:
    """Fetch all campaigns with optional status filter."""
    async def _work():
        camps = await get_campaigns(status=status)
        return json.dumps([
            {"id": c.id, "name": c.name, "status": c.status, "daily_limit": c.daily_limit, "created_at": c.created_at}
            for c in camps
        ])

    return await _logged("get_campaigns", {"status": status}, _work())


@mcp.tool
async def get_campaign(campaign_id: int) -> str:
    """Fetch a single campaign by ID with its stats."""
    async def _work():
        camp = await get_campaign_by_id(campaign_id=campaign_id)
        if not camp:
            return f"Campaign {campaign_id} not found"
        stats = await get_campaign_stats(campaign_id=campaign_id)
        return json.dumps({
            "id": camp.id,
            "name": camp.name,
            "status": camp.status,
            "daily_limit": camp.daily_limit,
            "timezone": camp.timezone,
            "created_at": camp.created_at,
            "stats": stats,
        })

    return await _logged("get_campaign", {"campaign_id": campaign_id}, _work())


@mcp.tool
async def create_campaign_tool(
    name: str,
    mailbox_id: int,
    daily_limit: int = 30,
    timezone: str = "America/New_York",
) -> str:
    """Create a new email campaign."""
    async def _work():
        camp = Campaign(
            name=name,
            status="draft",
            mailbox_id=mailbox_id,
            daily_limit=daily_limit,
            timezone=timezone,
            send_window_start="08:00",
            send_window_end="17:00",
        )
        camp_id = await create_campaign(camp=camp)
        return f"Campaign created with id={camp_id}"

    return await _logged("create_campaign", {"name": name, "mailbox_id": mailbox_id}, _work())


@mcp.tool
async def update_campaign_status_tool(campaign_id: int, status: str) -> str:
    """Update a campaign's status (draft|active|paused|completed|archived)."""
    async def _work():
        await update_campaign_status(campaign_id=campaign_id, status=status)
        return f"Campaign {campaign_id} status updated to '{status}'"

    return await _logged("update_campaign_status", {"campaign_id": campaign_id, "status": status}, _work())


@mcp.tool
async def enroll_leads_in_campaign(campaign_id: int, lead_ids: list[int]) -> str:
    """Enroll multiple leads into a campaign."""
    async def _work():
        enrolled = 0
        for lead_id in lead_ids:
            result = await enroll_lead(campaign_id=campaign_id, lead_id=lead_id)
            if result:
                enrolled += 1
        return f"Enrolled {enrolled}/{len(lead_ids)} leads into campaign {campaign_id}"

    return await _logged("enroll_leads_in_campaign", {"campaign_id": campaign_id, "count": len(lead_ids)}, _work())


@mcp.tool
async def get_sequence_steps_tool(campaign_id: int) -> str:
    """Fetch sequence steps for a campaign."""
    async def _work():
        steps = await get_sequence_steps(campaign_id=campaign_id)
        return json.dumps([
            {
                "id": s.id,
                "step_number": s.step_number,
                "subject": s.subject,
                "template_name": s.template_name,
                "delay_days": s.delay_days,
                "is_reply": s.is_reply,
            }
            for s in steps
        ])

    return await _logged("get_sequence_steps", {"campaign_id": campaign_id}, _work())


@mcp.tool
async def add_sequence_step_tool(
    campaign_id: int,
    step_number: int,
    subject: str,
    template_name: str,
    delay_days: int = 0,
) -> str:
    """Add a sequence step to a campaign."""
    async def _work():
        step = SequenceStep(
            campaign_id=campaign_id,
            step_number=step_number,
            subject=subject,
            template_name=template_name,
            delay_days=delay_days,
            is_reply=0,
        )
        step_id = await add_sequence_step(step=step)
        return f"Sequence step {step_number} added with id={step_id}"

    return await _logged(
        "add_sequence_step",
        {"campaign_id": campaign_id, "step_number": step_number, "subject": subject},
        _work(),
    )


# ---------------------------------------------------------------------------
# Sending & Replies tools
# ---------------------------------------------------------------------------


@mcp.tool
async def get_send_queue_tool(campaign_id: int, limit: int = 20) -> str:
    """Get leads ready for the next email in a campaign."""
    async def _work():
        queue = await get_send_queue(campaign_id=campaign_id, limit=limit)
        return json.dumps(queue[:limit])

    return await _logged("get_send_queue", {"campaign_id": campaign_id, "limit": limit}, _work())


@mcp.tool
async def check_replies(mailbox_id: int) -> str:
    """Check for email replies in a mailbox using IMAP."""
    from db.queries import get_mailbox_by_id
    from mailer.replies import ReplyWatcher

    async def _work():
        mb = await get_mailbox_by_id(mailbox_id=mailbox_id)
        if not mb:
            return f"Mailbox {mailbox_id} not found"
        watcher = ReplyWatcher(mb)
        count = await watcher.poll_once()
        return f"Processed {count} replies for {mb.email}"

    return await _logged("check_replies", {"mailbox_id": mailbox_id}, _work())


# ---------------------------------------------------------------------------
# Deals tools
# ---------------------------------------------------------------------------


@mcp.tool
async def get_deals_tool(stage: str | None = None) -> str:
    """Fetch deals with optional stage filter."""
    async def _work():
        deals = await get_deals(stage=stage)
        return json.dumps([
            {
                "id": d.id,
                "lead_id": d.lead_id,
                "campaign_id": d.campaign_id,
                "stage": d.stage,
                "value": d.value,
                "notes": d.notes,
                "created_at": d.created_at,
            }
            for d in deals
        ])

    return await _logged("get_deals", {"stage": stage}, _work())


@mcp.tool
async def save_deal(
    lead_id: int,
    campaign_id: int | None = None,
    stage: str = "lead",
    value: float = 0.0,
    notes: str = "",
) -> str:
    """Create or update a deal."""
    async def _work():
        deal = Deal(
            lead_id=lead_id,
            campaign_id=campaign_id,
            stage=stage,
            value=value,
            notes=notes,
        )
        deal_id = await upsert_deal(deal=deal)
        return f"Deal saved with id={deal_id}"

    return await _logged("save_deal", {"lead_id": lead_id, "stage": stage, "value": value}, _work())


# ---------------------------------------------------------------------------
# Analytics tools
# ---------------------------------------------------------------------------


@mcp.tool
async def get_dashboard_stats() -> str:
    """Get summary stats: leads, today's activity, and deals."""
    async def _work():
        lead_stats = await get_lead_stats()
        activity = await get_today_activity()
        deal_stats = await get_deal_stats()
        return json.dumps({
            "leads": lead_stats,
            "today": activity,
            "deals": deal_stats,
        })

    return await _logged("get_dashboard_stats", {}, _work())


@mcp.tool
async def get_campaign_stats_tool(campaign_id: int) -> str:
    """Get detailed stats for a specific campaign."""
    async def _work():
        stats = await get_campaign_stats(campaign_id=campaign_id)
        return json.dumps(stats)

    return await _logged("get_campaign_stats", {"campaign_id": campaign_id}, _work())


@mcp.tool
async def get_mcp_activity_tool(limit: int = 20) -> str:
    """Retrieve recent MCP tool call history (so Claude can see its own activity)."""
    async def _work():
        rows = await get_mcp_activity(limit=limit)
        return json.dumps([
            {
                "id": r.id,
                "tool_name": r.tool_name,
                "status": r.status,
                "result_summary": r.result_summary,
                "duration_ms": r.duration_ms,
                "created_at": r.created_at,
            }
            for r in rows
        ])

    return await _logged("get_mcp_activity", {"limit": limit}, _work())


# ---------------------------------------------------------------------------
# Personalization (MCP sampling — Claude Code generates the text)
# ---------------------------------------------------------------------------

PERSONALIZE_SYSTEM = (
    "Generate a unique 1-2 sentence cold email opener. "
    "Reference ONE specific thing about the business. "
    "Under 30 words. No sycophancy. Return only the opener text."
)


@mcp.tool
async def personalize_opener(lead_json: str, ctx: Context) -> str:
    """Generate a personalized cold email opener for a lead via MCP sampling.

    lead_json: JSON string with lead fields (company, city, job_title, etc.)
    Uses ctx.sample() so Claude Code generates the text — no API key needed.
    Falls back to a template opener if sampling is unavailable.
    """
    from mailer.personalize import _build_user_prompt, _fallback_opener

    async def _work():
        try:
            import json as _json

            lead = _json.loads(lead_json)
        except Exception:
            return "Hi there, "

        user_prompt = _build_user_prompt(lead)

        try:
            result = await ctx.sample(
                user_prompt,
                system_prompt=PERSONALIZE_SYSTEM,
                max_tokens=100,
            )
            text = result.text.strip()
            words = text.split()
            return " ".join(words[:30]) if len(words) > 30 else text
        except Exception:
            return _fallback_opener(lead)

    return await _logged("personalize_opener", {"lead": lead_json[:100]}, _work())


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    asyncio.run(init_db())
    mcp.run()
