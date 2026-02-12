"""Campaigns screen: list view + drill-down detail with sequence step progress."""

from __future__ import annotations

from contextlib import suppress

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import (
    Button,
    DataTable,
    Footer,
    Header,
    Label,
    Static,
)

from db import DBPool, queries
from tui.widgets.campaign_progress import CampaignProgress


class CampaignsScreen(Screen):
    """Campaign list with drill-down detail view."""

    BINDINGS = [
        ("d", "app.switch_mode('dashboard')", "Dashboard"),
        ("l", "app.switch_mode('leads')", "Leads"),
        ("c", "app.switch_mode('campaigns')", "Campaigns"),
        ("p", "app.switch_mode('pipeline')", "Pipeline"),
        ("s", "app.switch_mode('settings')", "Settings"),
        ("q", "app.quit", "Quit"),
        ("escape", "back_to_list", "Back"),
    ]

    def __init__(self, pool: DBPool, **kwargs) -> None:
        super().__init__(**kwargs)
        self._pool = pool
        self._selected_campaign_id: int | None = None

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal(id="campaigns-layout"):
            with Vertical(id="campaign-list-panel"):
                yield Static("Campaigns", classes="section-title")
                yield Button("Refresh", id="btn-camp-refresh", variant="primary")
                table = DataTable(id="campaigns-dt")
                table.cursor_type = "row"
                table.zebra_stripes = True
                yield table
            with Vertical(id="campaign-detail-panel"):
                yield Static("Select a campaign", id="detail-title", classes="section-title")
                yield Label("", id="detail-info")
                yield CampaignProgress(id="camp-progress")
                yield Static("", id="step-stats")
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one("#campaigns-dt", DataTable)
        table.add_columns("ID", "Name", "Status", "Mailbox", "Created")
        self._load_campaigns()

    def _load_campaigns(self) -> None:
        self.run_worker(self._fetch_campaigns(), exclusive=True, group="campaigns")

    async def _fetch_campaigns(self) -> None:
        async with self._pool.acquire() as db:
            campaigns = await queries.get_campaigns(db)

        table = self.query_one("#campaigns-dt", DataTable)
        table.clear()
        for camp in campaigns:
            table.add_row(
                str(camp.id),
                camp.name,
                camp.status,
                str(camp.mailbox_id or "--"),
                camp.created_at[:10] if camp.created_at else "",
                key=str(camp.id),
            )

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if event.data_table.id != "campaigns-dt":
            return
        row_key = event.row_key
        if row_key is not None:
            self._selected_campaign_id = int(str(row_key.value))
            self.run_worker(
                self._load_campaign_detail(self._selected_campaign_id),
                exclusive=True,
                group="detail",
            )

    async def _load_campaign_detail(self, campaign_id: int) -> None:
        async with self._pool.acquire() as db:
            camp = await queries.get_campaign_by_id(db, campaign_id)
            if camp is None:
                return
            steps = await queries.get_sequence_steps(db, campaign_id)
            camp_leads = await queries.get_campaign_leads(db, campaign_id)
            stats = await queries.get_campaign_stats(db, campaign_id)

        # Update detail title
        with suppress(Exception):
            self.query_one("#detail-title", Static).update(f"{camp.name}")

        # Detail info
        info_lines = [
            f"Status: {camp.status}",
            f"Total Leads: {stats.get('total', 0)}",
            f"Replied: {stats.get('replied', 0)}",
            f"Bounced: {stats.get('bounced', 0)}",
            f"Completed: {stats.get('completed', 0)}",
        ]
        with suppress(Exception):
            self.query_one("#detail-info", Label).update("\n".join(info_lines))

        # Progress
        lead_counts_by_step: dict[int, int] = {}
        for cl in camp_leads:
            lead_counts_by_step[cl.current_step] = lead_counts_by_step.get(cl.current_step, 0) + 1

        step_dicts = [
            {"step_number": s.step_number, "subject_tpl": s.subject_tpl}
            for s in steps
        ]
        total = len(camp_leads)
        with suppress(Exception):
            progress = self.query_one("#camp-progress", CampaignProgress)
            progress.update_progress(step_dicts, lead_counts_by_step, total)

        # Per-step stats text
        step_lines = []
        for s in steps:
            count = lead_counts_by_step.get(s.step_number, 0)
            step_lines.append(
                f"  Step {s.step_number} (delay: {s.delay_days}d): "
                f"{s.subject_tpl[:40]} -- {count} leads"
            )
        with suppress(Exception):
            self.query_one("#step-stats", Static).update("\n".join(step_lines))

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-camp-refresh":
            self._load_campaigns()

    def action_back_to_list(self) -> None:
        self._selected_campaign_id = None
        try:
            self.query_one("#detail-title", Static).update("Select a campaign")
            self.query_one("#detail-info", Label).update("")
            self.query_one("#step-stats", Static).update("")
        except Exception:
            pass
