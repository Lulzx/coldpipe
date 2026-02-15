"""Leads controller."""

from __future__ import annotations

import csv
import io

from litestar import Controller, Request, Response, get
from litestar.response import Template

from db.queries import (
    count_leads,
    get_emails_for_lead,
    get_lead_by_id,
    get_leads,
    search_leads,
)
from web.helpers import template_context

PAGE_SIZE = 50


class LeadsController(Controller):
    path = "/leads"
    tags = ["leads"]

    @get("/")
    async def list_leads(
        self,
        request: Request,
        page: int = 1,
        status: str | None = None,
        source: str | None = None,
    ) -> Template:
        page = max(1, page)
        offset = (page - 1) * PAGE_SIZE
        leads = await get_leads(
            limit=PAGE_SIZE, offset=offset, email_status=status or None, source=source or None
        )
        total = await count_leads(email_status=status or None)
        total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
        return Template(
            template_name="leads/list.html",
            context=template_context(
                request,
                active_page="leads",
                leads=leads,
                total=total,
                page=page,
                total_pages=total_pages,
                status=status or "",
                source=source or "",
            ),
        )

    @get("/search")
    async def search(self, request: Request, q: str = "") -> Template:
        leads = await search_leads(q, limit=PAGE_SIZE) if q else []
        total = len(leads)
        return Template(
            template_name="leads/list.html",
            context=template_context(
                request,
                active_page="leads",
                leads=leads,
                total=total,
                page=1,
                total_pages=1,
                status="",
                source="",
                search_query=q,
            ),
        )

    @get("/export")
    async def export_csv(self) -> Response:
        leads = await get_leads(limit=10000, offset=0)
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(
            [
                "id",
                "email",
                "first_name",
                "last_name",
                "company",
                "job_title",
                "website",
                "phone",
                "city",
                "state",
                "zip",
                "source",
                "email_status",
                "tags",
                "created_at",
            ]
        )
        for lead in leads:
            writer.writerow(
                [
                    lead.id,
                    lead.email,
                    lead.first_name,
                    lead.last_name,
                    lead.company,
                    lead.job_title,
                    lead.website,
                    lead.phone,
                    lead.city,
                    lead.state,
                    lead.zip,
                    lead.source,
                    lead.email_status,
                    lead.tags,
                    lead.created_at,
                ]
            )
        return Response(
            content=output.getvalue(),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=leads.csv"},
        )

    @get("/{lead_id:int}")
    async def detail(self, request: Request, lead_id: int) -> Template:
        from litestar.exceptions import NotFoundException

        lead = await get_lead_by_id(lead_id)
        if lead is None:
            raise NotFoundException(detail="Lead not found")
        emails = await get_emails_for_lead(lead_id)
        return Template(
            template_name="leads/detail.html",
            context=template_context(request, active_page="leads", lead=lead, emails=emails),
        )
