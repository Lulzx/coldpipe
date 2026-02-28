"""Dashboard controller."""

from __future__ import annotations

from collections import defaultdict

from litestar import Controller, Request, get
from litestar.response import Template

from db.queries import (
    check_daily_limit,
    get_campaigns,
    get_daily_stats,
    get_deal_stats,
    get_lead_stats,
    get_mailboxes,
    get_mcp_activity,
    get_mcp_stats,
    get_pipeline_stats,
    get_today_activity,
    get_warmup_limit,
)
from web.helpers import template_context

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


class DashboardController(Controller):
    path = "/"
    tags = ["dashboard"]

    @get("/")
    async def index(self, request: Request) -> Template:
        lead_stats = await get_lead_stats()
        activity = await get_today_activity()
        deal_stats = await get_deal_stats()
        pipeline = await get_pipeline_stats()
        campaigns = await get_campaigns(status="active")
        daily = await get_daily_stats(days=30)
        mailboxes = await get_mailboxes(active_only=True)
        mcp_stats = await get_mcp_stats()
        mcp_recent = await get_mcp_activity(limit=5)

        mailbox_warmup = []
        for mb in mailboxes:
            sent, limit = await check_daily_limit(mb.id)
            warmup_limit = get_warmup_limit(mb.warmup_day)
            mailbox_warmup.append(
                {
                    "email": mb.email,
                    "warmup_day": mb.warmup_day,
                    "warmup_limit": warmup_limit,
                    "sent_today": sent,
                    "daily_limit": limit,
                }
            )

        # Pivot daily stats into chart series
        days_map: dict[str, dict[str, int]] = defaultdict(
            lambda: {"sent": 0, "replied": 0, "bounced": 0}
        )
        for row in daily:
            days_map[row["day"]][row["status"]] = row["cnt"]
        sorted_days = sorted(days_map.keys())
        chart_series = {
            "days": sorted_days,
            "sent": [days_map[d]["sent"] for d in sorted_days],
            "replied": [days_map[d]["replied"] for d in sorted_days],
            "bounced": [days_map[d]["bounced"] for d in sorted_days],
        }

        return Template(
            template_name="dashboard.html",
            context=template_context(
                request,
                active_page="dashboard",
                lead_stats=lead_stats,
                activity=activity,
                deal_stats=deal_stats,
                pipeline=pipeline,
                campaigns=campaigns,
                daily=daily,
                chart_series=chart_series,
                mailbox_warmup=mailbox_warmup,
                stages=DEAL_STAGES,
                mcp_stats=mcp_stats,
                mcp_recent=mcp_recent,
            ),
        )
