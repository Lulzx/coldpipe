"""Campaigns controller."""

from __future__ import annotations

from litestar import Controller, Request, get, post
from litestar.exceptions import NotFoundException
from litestar.response import Redirect, Template

from db.queries import (
    delete_campaign,
    get_campaign_by_id,
    get_campaign_leads,
    get_campaign_stats,
    get_campaign_step_distribution,
    get_campaigns,
    get_sequence_steps,
    update_campaign_status,
)
from web.helpers import template_context


class CampaignsController(Controller):
    path = "/campaigns"
    tags = ["campaigns"]

    @get("/")
    async def list_campaigns(self, request: Request, status: str | None = None) -> Template:
        campaigns = await get_campaigns(status=status or None)
        return Template(
            template_name="campaigns/list.html",
            context=template_context(
                request,
                active_page="campaigns",
                campaigns=campaigns,
                status=status or "",
            ),
        )

    @get("/{campaign_id:int}")
    async def detail(self, request: Request, campaign_id: int) -> Template:
        campaign = await get_campaign_by_id(campaign_id)
        if campaign is None:
            raise NotFoundException(detail="Campaign not found")
        stats = await get_campaign_stats(campaign_id)
        steps = await get_sequence_steps(campaign_id)
        leads = await get_campaign_leads(campaign_id)
        step_dist = await get_campaign_step_distribution(campaign_id)
        return Template(
            template_name="campaigns/detail.html",
            context=template_context(
                request,
                active_page="campaigns",
                campaign=campaign,
                stats=stats,
                steps=steps,
                leads=leads,
                step_dist=step_dist,
            ),
        )

    @post("/{campaign_id:int}/pause")
    async def pause(self, campaign_id: int) -> Redirect:
        await update_campaign_status(campaign_id=campaign_id, status="paused")
        return Redirect(path=f"/campaigns/{campaign_id}")

    @post("/{campaign_id:int}/resume")
    async def resume(self, campaign_id: int) -> Redirect:
        await update_campaign_status(campaign_id=campaign_id, status="active")
        return Redirect(path=f"/campaigns/{campaign_id}")

    @post("/{campaign_id:int}/delete")
    async def delete(self, campaign_id: int) -> Redirect:
        await delete_campaign(campaign_id)
        return Redirect(path="/campaigns")
