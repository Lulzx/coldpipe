"""MCP Activity log controller."""

from __future__ import annotations

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
        rows = await get_mcp_activity(limit=PAGE_SIZE, offset=offset)
        total = await count_mcp_activity()
        total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)

        # Client-side filter: re-fetch without offset when filtering
        if status or tool_name:
            all_rows = await get_mcp_activity(limit=10000)
            if status:
                all_rows = [r for r in all_rows if r.status == status]
            if tool_name:
                all_rows = [r for r in all_rows if r.tool_name == tool_name]
            total = len(all_rows)
            total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
            rows = all_rows[offset : offset + PAGE_SIZE]

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
