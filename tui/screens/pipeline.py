"""Pipeline screen: Kanban board with columns for each deal stage."""

from __future__ import annotations

from datetime import UTC, datetime

from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.screen import Screen
from textual.widgets import Footer, Header

from db import DBPool, queries
from tui.widgets.funnel import STAGE_LABELS, STAGE_ORDER
from tui.widgets.stage_column import StageColumn


class PipelineScreen(Screen):
    """Kanban-style pipeline board."""

    BINDINGS = [
        ("d", "app.switch_mode('dashboard')", "Dashboard"),
        ("l", "app.switch_mode('leads')", "Leads"),
        ("c", "app.switch_mode('campaigns')", "Campaigns"),
        ("p", "app.switch_mode('pipeline')", "Pipeline"),
        ("s", "app.switch_mode('settings')", "Settings"),
        ("q", "app.quit", "Quit"),
        ("r", "refresh", "Refresh"),
    ]

    def __init__(self, pool: DBPool, **kwargs) -> None:
        super().__init__(**kwargs)
        self._pool = pool

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal(id="kanban-board"):
            for stage in STAGE_ORDER:
                label = STAGE_LABELS.get(stage, stage)
                yield StageColumn(stage, label, id=f"stage-{stage}")
        yield Footer()

    def on_mount(self) -> None:
        self._refresh_data()
        self.set_interval(30, self._refresh_data)

    def _refresh_data(self) -> None:
        self.run_worker(self._load_deals(), exclusive=True)

    def action_refresh(self) -> None:
        self._refresh_data()

    async def _load_deals(self) -> None:
        async with self._pool.acquire() as db:
            all_deals = await queries.get_deals(db)
            # Load lead info for company names
            lead_cache: dict[int, str] = {}
            for deal in all_deals:
                if deal.lead_id not in lead_cache:
                    lead = await queries.get_lead_by_id(db, deal.lead_id)
                    lead_cache[deal.lead_id] = lead.company if lead else f"Lead #{deal.lead_id}"

        now = datetime.now(UTC)
        deals_by_stage: dict[str, list[tuple[str, float, int]]] = {s: [] for s in STAGE_ORDER}

        for deal in all_deals:
            company = lead_cache.get(deal.lead_id, "Unknown")
            if deal.updated_at:
                try:
                    updated = datetime.strptime(deal.updated_at, "%Y-%m-%dT%H:%M:%SZ").replace(
                        tzinfo=UTC
                    )
                    days = (now - updated).days
                except ValueError:
                    days = 0
            else:
                days = 0
            if deal.stage in deals_by_stage:
                deals_by_stage[deal.stage].append((company, deal.value, days))

        for stage in STAGE_ORDER:
            try:
                col = self.query_one(f"#stage-{stage}", StageColumn)
                col.update_deals(deals_by_stage.get(stage, []))
            except Exception:
                pass
