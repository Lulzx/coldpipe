"""Deals controller."""

from __future__ import annotations

from litestar import Controller, Request, get, post
from litestar.exceptions import NotFoundException
from litestar.response import Redirect, Template

from db.queries import get_deal_by_id, get_deal_stats, get_deals, upsert_deal
from db.tables import Deal
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


def _int(val: str | None, default: int = 0) -> int:
    try:
        return int(val) if val else default
    except ValueError:
        return default


class DealsController(Controller):
    path = "/deals"
    tags = ["deals"]

    @get("/")
    async def list_deals(self, request: Request, stage: str | None = None) -> Template:
        deals = await get_deals(stage=stage or None)
        stats = await get_deal_stats()
        return Template(
            template_name="deals/list.html",
            context=template_context(
                request,
                active_page="deals",
                deals=deals,
                stats=stats,
                stage=stage or "",
                stages=DEAL_STAGES,
            ),
        )

    @get("/pipeline")
    async def pipeline(self, request: Request) -> Template:
        deals = await get_deals()
        stats = await get_deal_stats()
        by_stage: dict[str, list] = {s: [] for s in DEAL_STAGES}
        for d in deals:
            by_stage.setdefault(d.stage, []).append(d)
        return Template(
            template_name="deals/pipeline.html",
            context=template_context(
                request,
                active_page="deals",
                by_stage=by_stage,
                stats=stats,
                stages=DEAL_STAGES,
            ),
        )

    @post("/create")
    async def create(self, request: Request) -> Redirect:
        data = await request.form()
        deal = Deal(
            lead_id=_int(str(data.get("lead_id", "0"))),
            stage=str(data.get("stage", "lead")),
            value=float(data.get("value", 0) or 0),
            notes=str(data.get("notes", "")),
        )
        await upsert_deal(deal)
        return Redirect(path="/deals")

    @post("/{deal_id:int}/move")
    async def move(self, request: Request, deal_id: int) -> Redirect:
        data = await request.form()
        new_stage = str(data.get("stage", ""))
        if new_stage not in DEAL_STAGES:
            raise NotFoundException(detail="Invalid stage")
        deal = await get_deal_by_id(deal_id)
        if deal is None:
            raise NotFoundException(detail="Deal not found")
        deal.stage = new_stage
        await upsert_deal(deal)
        return Redirect(path="/deals")
