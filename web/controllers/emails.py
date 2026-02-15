"""Emails controller."""

from __future__ import annotations

from litestar import Controller, Request, get
from litestar.response import Template

from db.queries import count_emails_sent, get_email_status_distribution, get_emails_sent
from web.helpers import template_context

PAGE_SIZE = 50


class EmailsController(Controller):
    path = "/emails"
    tags = ["emails"]

    @get("/")
    async def list_emails(
        self, request: Request, page: int = 1, status: str | None = None
    ) -> Template:
        page = max(1, page)
        offset = (page - 1) * PAGE_SIZE
        emails = await get_emails_sent(limit=PAGE_SIZE, offset=offset, status=status or None)
        total = await count_emails_sent(status=status or None)
        email_dist = await get_email_status_distribution()
        total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
        return Template(
            template_name="emails/list.html",
            context=template_context(
                request,
                active_page="emails",
                emails=emails,
                total=total,
                page=page,
                total_pages=total_pages,
                status=status or "",
                email_dist=email_dist,
            ),
        )
