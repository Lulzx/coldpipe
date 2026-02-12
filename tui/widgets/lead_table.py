"""Async-loading DataTable with pagination support for leads."""

from __future__ import annotations

from contextlib import suppress

from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.widget import Widget
from textual.widgets import Button, DataTable, Input, Label, Select

from db import DBPool, queries


class LeadTable(Widget):
    """Paginated leads table with search and filtering."""

    DEFAULT_CSS = """
    LeadTable {
        height: 1fr;
    }
    LeadTable .toolbar {
        height: 3;
        dock: top;
        padding: 0 1;
    }
    LeadTable .toolbar Input {
        width: 30;
    }
    LeadTable .toolbar Select {
        width: 20;
    }
    LeadTable .toolbar Button {
        min-width: 8;
    }
    LeadTable .pagination {
        height: 3;
        dock: bottom;
        align: center middle;
        padding: 0 1;
    }
    LeadTable .pagination Label {
        margin: 0 2;
    }
    LeadTable DataTable {
        height: 1fr;
    }
    """

    PAGE_SIZE = 50

    def __init__(self, pool: DBPool, **kwargs) -> None:
        super().__init__(**kwargs)
        self._pool = pool
        self._page = 0
        self._total = 0
        self._search_query = ""
        self._status_filter: str | None = None
        self._source_filter: str | None = None

    def compose(self) -> ComposeResult:
        with Horizontal(classes="toolbar"):
            yield Input(placeholder="Search leads...", id="lead-search")
            yield Select(
                [
                    ("All Statuses", ""),
                    ("Unknown", "unknown"),
                    ("Valid", "valid"),
                    ("Invalid", "invalid"),
                    ("Catch All", "catch_all"),
                    ("Risky", "risky"),
                    ("Missing", "missing"),
                ],
                value="",
                id="status-filter",
                prompt="Status",
            )
            yield Button("Refresh", id="btn-refresh", variant="primary")

        table = DataTable(id="leads-dt")
        table.cursor_type = "row"
        table.zebra_stripes = True
        yield table

        with Horizontal(classes="pagination"):
            yield Button("<< Prev", id="btn-prev")
            yield Label("Page 1", id="page-label")
            yield Button("Next >>", id="btn-next")

    def on_mount(self) -> None:
        table = self.query_one("#leads-dt", DataTable)
        table.add_columns("ID", "Name", "Company", "Email", "Status", "City", "Source")
        self.load_data()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "lead-search":
            self._search_query = event.value.strip()
            self._page = 0
            self.load_data()

    def on_select_changed(self, event: Select.Changed) -> None:
        if event.select.id == "status-filter":
            self._status_filter = str(event.value) if event.value else None
            self._page = 0
            self.load_data()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-prev" and self._page > 0:
            self._page -= 1
            self.load_data()
        elif event.button.id == "btn-next":
            max_page = max(0, (self._total - 1) // self.PAGE_SIZE)
            if self._page < max_page:
                self._page += 1
                self.load_data()
        elif event.button.id == "btn-refresh":
            self.load_data()

    def load_data(self) -> None:
        self.run_worker(self._fetch_leads(), exclusive=True)

    async def _fetch_leads(self) -> None:
        async with self._pool.acquire() as db:
            if self._search_query:
                leads = await queries.search_leads(db, self._search_query, limit=self.PAGE_SIZE)
                self._total = len(leads)
            else:
                self._total = await queries.count_leads(db)
                leads = await queries.get_leads(
                    db,
                    limit=self.PAGE_SIZE,
                    offset=self._page * self.PAGE_SIZE,
                    email_status=self._status_filter,
                )

        table = self.query_one("#leads-dt", DataTable)
        table.clear()
        for lead in leads:
            name = f"{lead.first_name} {lead.last_name}".strip()
            table.add_row(
                str(lead.id),
                name,
                lead.company,
                lead.email,
                lead.email_status,
                lead.city,
                lead.source,
                key=str(lead.id),
            )

        total_pages = max(1, (self._total + self.PAGE_SIZE - 1) // self.PAGE_SIZE)
        with suppress(Exception):
            self.query_one("#page-label", Label).update(
                f"Page {self._page + 1} / {total_pages}  ({self._total} leads)"
            )
