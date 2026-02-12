"""Settings screen: mailbox SMTP/IMAP configuration and warmup status."""

from __future__ import annotations

from contextlib import suppress

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import (
    Button,
    Checkbox,
    DataTable,
    Footer,
    Header,
    Input,
    Label,
    Static,
)

from db import DBPool, queries
from db.models import Mailbox


class SettingsScreen(Screen):
    """Mailbox configuration and warmup status."""

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
        self._editing_mailbox_id: int | None = None

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal(id="settings-layout"):
            with Vertical(id="mailbox-list-panel"):
                yield Static("Mailboxes", classes="section-title")
                yield Button("Add New", id="btn-add-mailbox", variant="success")
                yield Button("Refresh", id="btn-refresh-mb", variant="primary")
                table = DataTable(id="mailbox-dt")
                table.cursor_type = "row"
                table.zebra_stripes = True
                yield table
                yield Static("Warmup Status", classes="section-title")
                yield Static("", id="warmup-info")

            with Vertical(id="mailbox-form-panel"):
                yield Static("Mailbox Configuration", classes="section-title")
                yield Label("Email")
                yield Input(placeholder="sender@example.com", id="mb-email")
                yield Label("Display Name")
                yield Input(placeholder="Dr. Smith", id="mb-display-name")
                yield Label("SMTP Host")
                yield Input(placeholder="smtp.gmail.com", id="mb-smtp-host")
                yield Label("SMTP Port")
                yield Input(placeholder="587", id="mb-smtp-port")
                yield Label("SMTP User")
                yield Input(placeholder="user@gmail.com", id="mb-smtp-user")
                yield Label("SMTP Password")
                yield Input(placeholder="app-password", password=True, id="mb-smtp-pass")
                yield Label("IMAP Host")
                yield Input(placeholder="imap.gmail.com", id="mb-imap-host")
                yield Label("IMAP Port")
                yield Input(placeholder="993", id="mb-imap-port")
                yield Label("Daily Limit")
                yield Input(placeholder="50", id="mb-daily-limit")
                yield Checkbox("Active", value=True, id="mb-active")
                with Horizontal(classes="form-buttons"):
                    yield Button("Save", id="btn-save-mb", variant="primary")
                    yield Button("Clear", id="btn-clear-mb", variant="default")
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one("#mailbox-dt", DataTable)
        table.add_columns("ID", "Email", "SMTP Host", "Limit", "Active")
        self._load_mailboxes()

    def _load_mailboxes(self) -> None:
        self.run_worker(self._fetch_mailboxes(), exclusive=True, group="mailboxes")

    async def _fetch_mailboxes(self) -> None:
        async with self._pool.acquire() as db:
            mailboxes = await queries.get_mailboxes(db)
            warmup_lines = []
            for mb in mailboxes:
                sent_today, limit = await queries.check_daily_limit(db, mb.id)
                status = "Active" if mb.is_active else "Inactive"
                pct = sent_today / limit * 100 if limit > 0 else 0
                warmup_lines.append(
                    f"  {mb.email}: {status} -- {sent_today}/{limit} sent today ({pct:.0f}%)"
                )

        table = self.query_one("#mailbox-dt", DataTable)
        table.clear()
        for mb in mailboxes:
            table.add_row(
                str(mb.id),
                mb.email,
                mb.smtp_host,
                str(mb.daily_limit),
                "Yes" if mb.is_active else "No",
                key=str(mb.id),
            )
        with suppress(Exception):
            self.query_one("#warmup-info", Static).update(
                "\n".join(warmup_lines) if warmup_lines else "No mailboxes configured"
            )

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if event.data_table.id != "mailbox-dt":
            return
        row_key = event.row_key
        if row_key is not None:
            mailbox_id = int(str(row_key.value))
            self._editing_mailbox_id = mailbox_id
            self.run_worker(
                self._load_mailbox_form(mailbox_id), exclusive=True, group="form"
            )

    async def _load_mailbox_form(self, mailbox_id: int) -> None:
        async with self._pool.acquire() as db:
            mb = await queries.get_mailbox_by_id(db, mailbox_id)
        if mb is None:
            return
        try:
            self.query_one("#mb-email", Input).value = mb.email
            self.query_one("#mb-display-name", Input).value = mb.display_name
            self.query_one("#mb-smtp-host", Input).value = mb.smtp_host
            self.query_one("#mb-smtp-port", Input).value = str(mb.smtp_port)
            self.query_one("#mb-smtp-user", Input).value = mb.smtp_user
            self.query_one("#mb-smtp-pass", Input).value = mb.smtp_pass
            self.query_one("#mb-imap-host", Input).value = mb.imap_host
            self.query_one("#mb-imap-port", Input).value = str(mb.imap_port)
            self.query_one("#mb-daily-limit", Input).value = str(mb.daily_limit)
            self.query_one("#mb-active", Checkbox).value = bool(mb.is_active)
        except Exception:
            pass

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-save-mb":
            self.run_worker(self._save_mailbox(), exclusive=True, group="save")
        elif event.button.id == "btn-clear-mb":
            self._clear_form()
        elif event.button.id == "btn-add-mailbox":
            self._editing_mailbox_id = None
            self._clear_form()
        elif event.button.id == "btn-refresh-mb":
            self._load_mailboxes()

    async def _save_mailbox(self) -> None:
        try:
            email = self.query_one("#mb-email", Input).value.strip()
            if not email:
                self.notify("Email is required", severity="error")
                return
            mb = Mailbox(
                email=email,
                display_name=self.query_one("#mb-display-name", Input).value.strip(),
                smtp_host=self.query_one("#mb-smtp-host", Input).value.strip(),
                smtp_port=int(self.query_one("#mb-smtp-port", Input).value or "587"),
                smtp_user=self.query_one("#mb-smtp-user", Input).value.strip(),
                smtp_pass=self.query_one("#mb-smtp-pass", Input).value.strip(),
                imap_host=self.query_one("#mb-imap-host", Input).value.strip(),
                imap_port=int(self.query_one("#mb-imap-port", Input).value or "993"),
                daily_limit=int(self.query_one("#mb-daily-limit", Input).value or "50"),
                is_active=1 if self.query_one("#mb-active", Checkbox).value else 0,
            )
        except (ValueError, TypeError) as e:
            self.notify(f"Invalid input: {e}", severity="error")
            return

        async with self._pool.acquire() as db:
            await queries.upsert_mailbox(db, mb)

        self.notify(f"Mailbox {email} saved", severity="information")
        self._load_mailboxes()
        self._clear_form()

    def _clear_form(self) -> None:
        self._editing_mailbox_id = None
        try:
            self.query_one("#mb-email", Input).value = ""
            self.query_one("#mb-display-name", Input).value = ""
            self.query_one("#mb-smtp-host", Input).value = ""
            self.query_one("#mb-smtp-port", Input).value = "587"
            self.query_one("#mb-smtp-user", Input).value = ""
            self.query_one("#mb-smtp-pass", Input).value = ""
            self.query_one("#mb-imap-host", Input).value = ""
            self.query_one("#mb-imap-port", Input).value = "993"
            self.query_one("#mb-daily-limit", Input).value = "50"
            self.query_one("#mb-active", Checkbox).value = True
        except Exception:
            pass
