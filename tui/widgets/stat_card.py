"""Single metric card widget: label + big number + optional detail."""

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.reactive import reactive
from textual.widget import Widget
from textual.widgets import Label


class StatCard(Widget):
    """Displays a single statistic with a label and value."""

    DEFAULT_CSS = """
    StatCard {
        height: auto;
        min-height: 5;
        padding: 1 2;
        border: solid $primary-background;
        background: $surface;
    }
    StatCard .stat-label {
        color: $text-muted;
        text-style: bold;
    }
    StatCard .stat-value {
        color: $text;
        text-style: bold;
        text-align: center;
    }
    StatCard .stat-detail {
        color: $text-muted;
        text-align: center;
    }
    """

    label_text: reactive[str] = reactive("")
    value_text: reactive[str] = reactive("0")
    detail_text: reactive[str] = reactive("")

    def __init__(
        self,
        label: str = "",
        value: str = "0",
        detail: str = "",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.label_text = label
        self.value_text = value
        self.detail_text = detail

    def compose(self) -> ComposeResult:
        with Vertical():
            yield Label(self.label_text, classes="stat-label")
            yield Label(self.value_text, classes="stat-value", id="stat-val")
            yield Label(self.detail_text, classes="stat-detail", id="stat-detail")

    def update_value(self, value: str, detail: str = "") -> None:
        self.value_text = value
        self.detail_text = detail
        try:
            self.query_one("#stat-val", Label).update(value)
            self.query_one("#stat-detail", Label).update(detail)
        except Exception:
            pass
