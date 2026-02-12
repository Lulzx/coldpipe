"""Main Textual App class for the Coldpipe TUI."""

from __future__ import annotations

from functools import partial
from pathlib import Path

from textual.app import App

from db import DBPool
from tui.screens.campaigns import CampaignsScreen
from tui.screens.dashboard import DashboardScreen
from tui.screens.leads import LeadsScreen
from tui.screens.pipeline import PipelineScreen
from tui.screens.settings import SettingsScreen

CSS_PATH = Path(__file__).parent / "coldpipe.tcss"


def _make_screen(cls, pool):
    return cls(pool)


class ColdpipeApp(App):
    """Coldpipe Customer Acquisition Engine -- TUI."""

    TITLE = "Coldpipe"
    SUB_TITLE = "Customer Acquisition Engine"
    CSS_PATH = CSS_PATH

    BINDINGS = [
        ("d", "switch_mode('dashboard')", "Dashboard"),
        ("l", "switch_mode('leads')", "Leads"),
        ("c", "switch_mode('campaigns')", "Campaigns"),
        ("p", "switch_mode('pipeline')", "Pipeline"),
        ("s", "switch_mode('settings')", "Settings"),
        ("q", "quit", "Quit"),
    ]

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._pool = DBPool()
        # _modes is the working copy read by switch_mode / _init_mode.
        # We set it after super().__init__ which already copied the (empty)
        # class-level MODES dict.
        self._modes = {
            "dashboard": partial(_make_screen, DashboardScreen, self._pool),
            "leads": partial(_make_screen, LeadsScreen, self._pool),
            "campaigns": partial(_make_screen, CampaignsScreen, self._pool),
            "pipeline": partial(_make_screen, PipelineScreen, self._pool),
            "settings": partial(_make_screen, SettingsScreen, self._pool),
        }

    def on_mount(self) -> None:
        self.switch_mode("dashboard")

    async def action_quit(self) -> None:
        await self._pool.close()
        self.exit()
