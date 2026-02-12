"""Leads screen: DataTable with search, filter, sort, and bulk actions."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.screen import Screen
from textual.widgets import Footer, Header

from db import DBPool
from tui.widgets.lead_table import LeadTable


class LeadsScreen(Screen):
    """Screen displaying the leads table with search and filtering."""

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
        yield LeadTable(self._pool)
        yield Footer()
