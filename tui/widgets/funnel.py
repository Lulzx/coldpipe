"""Visual pipeline funnel widget showing counts and bars for each stage."""

from contextlib import suppress

from textual.app import ComposeResult
from textual.widget import Widget
from textual.widgets import Static

STAGE_LABELS = {
    "lead": "Lead",
    "contacted": "Contacted",
    "replied": "Replied",
    "interested": "Interested",
    "meeting_booked": "Mtg Booked",
    "proposal_sent": "Proposal",
    "closed_won": "Closed Won",
    "closed_lost": "Closed Lost",
}

STAGE_ORDER = [
    "lead", "contacted", "replied", "interested",
    "meeting_booked", "proposal_sent", "closed_won", "closed_lost",
]


class PipelineFunnel(Widget):
    """Renders a horizontal bar funnel for the deal pipeline."""

    DEFAULT_CSS = """
    PipelineFunnel {
        height: auto;
        min-height: 12;
        padding: 1 2;
        border: solid $primary-background;
        background: $surface;
    }
    PipelineFunnel .funnel-title {
        text-style: bold;
        color: $text;
        margin-bottom: 1;
    }
    PipelineFunnel .funnel-body {
        height: auto;
    }
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._stats: dict[str, int] = {}

    def compose(self) -> ComposeResult:
        yield Static("Pipeline Funnel", classes="funnel-title")
        yield Static("Loading...", id="funnel-body", classes="funnel-body")

    def update_stats(self, stats: dict[str, int]) -> None:
        self._stats = stats
        self._render_funnel()

    def _render_funnel(self) -> None:
        max_val = max(self._stats.values()) if self._stats else 1
        if max_val == 0:
            max_val = 1
        bar_width = 20
        lines = []
        total_leads = self._stats.get("lead", 0)
        total_won = self._stats.get("closed_won", 0)

        for stage in STAGE_ORDER:
            count = self._stats.get(stage, 0)
            filled = round(count / max_val * bar_width)
            empty = bar_width - filled
            label = STAGE_LABELS.get(stage, stage)
            bar = "\u2588" * filled + "\u2591" * empty
            lines.append(f"  {label:<14s} {bar} {count:>5d}")

        if total_leads > 0:
            pct = total_won / total_leads * 100
            lines.append(f"  Conversion: {pct:.2f}%")
        else:
            lines.append("  Conversion: --")

        with suppress(Exception):
            self.query_one("#funnel-body", Static).update("\n".join(lines))
