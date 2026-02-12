"""Visual step-by-step sequence progress for a campaign."""

from __future__ import annotations

from contextlib import suppress

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.widget import Widget
from textual.widgets import Static


class CampaignProgress(Widget):
    """Shows the progress of leads through campaign sequence steps."""

    DEFAULT_CSS = """
    CampaignProgress {
        height: auto;
        min-height: 5;
        padding: 1 2;
        border: solid $primary-background;
        background: $surface;
    }
    CampaignProgress .progress-title {
        text-style: bold;
        margin-bottom: 1;
    }
    CampaignProgress .progress-body {
        height: auto;
    }
    """

    def __init__(self, campaign_name: str = "", **kwargs) -> None:
        super().__init__(**kwargs)
        self._campaign_name = campaign_name

    def compose(self) -> ComposeResult:
        with Vertical():
            yield Static(self._campaign_name or "Campaign Progress", classes="progress-title")
            yield Static("No data", id="progress-body", classes="progress-body")

    def update_progress(
        self,
        steps: list[dict],
        lead_counts_by_step: dict[int, int],
        total_leads: int,
    ) -> None:
        """Update the progress display.

        Args:
            steps: list of step dicts with step_number, subject_tpl
            lead_counts_by_step: mapping of step_number -> count of leads at that step
            total_leads: total leads in the campaign
        """
        if not steps:
            with suppress(Exception):
                self.query_one("#progress-body", Static).update("No steps configured")
            return

        bar_width = 25
        lines = []
        for step in steps:
            snum = step.get("step_number", 0)
            subj = step.get("subject_tpl", f"Step {snum}")[:30]
            count = lead_counts_by_step.get(snum, 0)
            filled = round(count / total_leads * bar_width) if total_leads > 0 else 0
            empty = bar_width - filled
            bar = "\u2588" * filled + "\u2591" * empty
            pct = (count / total_leads * 100) if total_leads > 0 else 0
            lines.append(f"  Step {snum}: {subj:<30s} {bar} {count:>4d} ({pct:.0f}%)")

        with suppress(Exception):
            self.query_one("#progress-body", Static).update("\n".join(lines))
