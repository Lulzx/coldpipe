"""Single Kanban column widget with scrollable deal cards."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import VerticalScroll
from textual.widget import Widget
from textual.widgets import Label, Static


class DealCard(Static):
    """A single deal card inside a stage column."""

    DEFAULT_CSS = """
    DealCard {
        height: auto;
        min-height: 4;
        margin: 0 0 1 0;
        padding: 1;
        border: round $primary-background;
        background: $surface;
    }
    DealCard .deal-company {
        text-style: bold;
    }
    DealCard .deal-value {
        color: $success;
    }
    DealCard .deal-days {
        color: $text-muted;
    }
    """

    def __init__(self, company: str, value: float, days_in_stage: int, **kwargs) -> None:
        super().__init__(**kwargs)
        self._company = company
        self._value = value
        self._days = days_in_stage

    def compose(self) -> ComposeResult:
        yield Label(self._company, classes="deal-company")
        yield Label(f"${self._value:,.0f}", classes="deal-value")
        yield Label(f"{self._days}d in stage", classes="deal-days")


class StageColumn(Widget):
    """A single Kanban column for a deal stage."""

    DEFAULT_CSS = """
    StageColumn {
        width: 1fr;
        height: 1fr;
        border: solid $primary-background;
        background: $surface-darken-1;
    }
    StageColumn .col-header {
        height: 3;
        padding: 0 1;
        text-style: bold;
        background: $primary-background;
        color: $text;
        text-align: center;
        content-align: center middle;
    }
    StageColumn .col-count {
        height: 1;
        text-align: center;
        color: $text-muted;
    }
    StageColumn VerticalScroll {
        height: 1fr;
        padding: 1;
    }
    """

    def __init__(self, stage: str, label: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self._stage = stage
        self._label = label
        self._deals: list[tuple[str, float, int]] = []

    def compose(self) -> ComposeResult:
        yield Static(self._label, classes="col-header")
        yield Static("0 deals", classes="col-count", id=f"count-{self._stage}")
        yield VerticalScroll(id=f"scroll-{self._stage}")

    def update_deals(self, deals: list[tuple[str, float, int]]) -> None:
        """Update deals. Each tuple is (company, value, days_in_stage)."""
        self._deals = deals
        try:
            count_label = self.query_one(f"#count-{self._stage}", Static)
            count_label.update(f"{len(deals)} deal{'s' if len(deals) != 1 else ''}")
        except Exception:
            pass
        try:
            scroll = self.query_one(f"#scroll-{self._stage}", VerticalScroll)
            scroll.remove_children()
            for company, value, days in deals:
                scroll.mount(DealCard(company, value, days))
        except Exception:
            pass
