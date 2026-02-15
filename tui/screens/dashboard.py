"""Dashboard screen: pipeline funnel, today's activity, revenue stats."""

from __future__ import annotations

from datetime import UTC, datetime

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import Footer, Header, Static

from db import DBPool, queries
from tui.widgets.funnel import PipelineFunnel
from tui.widgets.stat_card import StatCard


class DashboardScreen(Screen):
    """Main dashboard with pipeline funnel and activity stats."""

    BINDINGS = [
        ("d", "app.switch_mode('dashboard')", "Dashboard"),
        ("l", "app.switch_mode('leads')", "Leads"),
        ("c", "app.switch_mode('campaigns')", "Campaigns"),
        ("p", "app.switch_mode('pipeline')", "Pipeline"),
        ("s", "app.switch_mode('settings')", "Settings"),
        ("q", "app.quit", "Quit"),
    ]

    def __init__(self, pool: DBPool, **kwargs) -> None:
        super().__init__(**kwargs)
        self._pool = pool

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal(id="dashboard-main"):
            with Vertical(id="left-panel"):
                yield PipelineFunnel(id="funnel")
            with Vertical(id="right-panel"):
                yield Static("Today's Activity", classes="section-title", id="activity-title")
                yield StatCard("Emails Sent", "-- / --", id="stat-sent")
                yield StatCard("Replies Today", "--", id="stat-replies")
                yield StatCard("Bounces", "--", id="stat-bounces")
                yield StatCard("New Leads Today", "--", id="stat-new-leads")
                yield Static("", classes="spacer")
                yield StatCard("Revenue (pipeline)", "$0", id="stat-rev-pipeline")
                yield StatCard("Revenue (closed)", "$0", id="stat-rev-closed")
        yield Footer()

    def on_mount(self) -> None:
        self._refresh_data()
        self.set_interval(30, self._refresh_data)

    def _refresh_data(self) -> None:
        self.run_worker(self._load_stats(), exclusive=True)

    async def _load_stats(self) -> None:
        async with self._pool.acquire():
            pipeline = await queries.get_pipeline_stats()
            deals = await queries.get_deals()
            today = datetime.now(UTC).strftime("%Y-%m-%d")

            # Today's email stats
            from piccolo.querystring import QueryString

            from db import get_engine

            engine = get_engine()
            email_rows = await engine.run_querystring(
                QueryString(
                    "SELECT status, COUNT(*) as cnt FROM emails_sent"
                    " WHERE DATE(sent_at) = {} GROUP BY status",
                    today,
                )
            )
            email_stats: dict[str, int] = {}
            for row in email_rows:
                email_stats[row["status"]] = row["cnt"]

            total_sent = sum(email_stats.values())
            replies = email_stats.get("replied", 0)
            bounces = email_stats.get("bounced", 0)

            # Daily limit info
            limit_rows = await engine.run_querystring(
                QueryString("SELECT SUM(daily_limit) as total FROM mailboxes WHERE is_active = 1")
            )
            daily_cap = limit_rows[0]["total"] if limit_rows and limit_rows[0]["total"] else 0

            # New leads today
            lead_rows = await engine.run_querystring(
                QueryString(
                    "SELECT COUNT(*) as cnt FROM leads WHERE DATE(created_at) = {}",
                    today,
                )
            )
            new_leads = lead_rows[0]["cnt"] if lead_rows else 0

        # Revenue
        pipeline_rev = sum(d.value for d in deals if d.stage not in ("closed_won", "closed_lost"))
        closed_rev = sum(d.value for d in deals if d.stage == "closed_won")

        # Update widgets
        funnel = self.query_one("#funnel", PipelineFunnel)
        funnel.update_stats(pipeline)

        reply_pct = f"({replies / total_sent * 100:.0f}%)" if total_sent > 0 else ""
        bounce_pct = f"({bounces / total_sent * 100:.0f}%)" if total_sent > 0 else ""

        self.query_one("#stat-sent", StatCard).update_value(f"{total_sent} / {daily_cap}")
        self.query_one("#stat-replies", StatCard).update_value(f"{replies}", reply_pct)
        self.query_one("#stat-bounces", StatCard).update_value(f"{bounces}", bounce_pct)
        self.query_one("#stat-new-leads", StatCard).update_value(f"{new_leads}")
        self.query_one("#stat-rev-pipeline", StatCard).update_value(f"${pipeline_rev:,.0f}")
        self.query_one("#stat-rev-closed", StatCard).update_value(f"${closed_rev:,.0f}")
