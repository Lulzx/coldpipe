"""MCP Activity log controller."""

from __future__ import annotations

import asyncio

from litestar import Controller, Request, get
from litestar.response import Template

from db.queries import count_mcp_activity, get_mcp_activity
from web.helpers import template_context

PAGE_SIZE = 50


class ActivityController(Controller):
    path = "/activity"
    tags = ["activity"]

    @get("/")
    async def index(
        self,
        request: Request,
        page: int = 1,
        status: str | None = None,
        tool_name: str | None = None,
    ) -> Template:
        page = max(1, page)
        offset = (page - 1) * PAGE_SIZE
        rows, total = await asyncio.gather(
            get_mcp_activity(limit=PAGE_SIZE, offset=offset, status=status, tool_name=tool_name),
            count_mcp_activity(status=status, tool_name=tool_name),
        )
        total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)

        return Template(
            template_name="activity/list.html",
            context=template_context(
                request,
                active_page="activity",
                rows=rows,
                total=total,
                page=page,
                total_pages=total_pages,
                status=status or "",
                tool_name=tool_name or "",
            ),
        )
