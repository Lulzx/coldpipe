"""Mailbox management CLI commands."""

from __future__ import annotations

import asyncio

import typer
from rich.console import Console
from rich.table import Table

from config import setup_logging
from db import get_db
from db import queries
from db.models import Mailbox

console = Console()
mailbox_app = typer.Typer(help="Mailbox management commands.")


def _run(coro):
    return asyncio.run(coro)


@mailbox_app.command("add")
def add(
    email: str = typer.Option(..., help="Email address"),
    smtp_host: str = typer.Option(..., "--smtp-host", help="SMTP server hostname"),
    smtp_port: int = typer.Option(587, "--smtp-port"),
    smtp_user: str = typer.Option(..., "--smtp-user", help="SMTP username"),
    smtp_pass: str = typer.Option(..., "--smtp-pass", help="SMTP password"),
    imap_host: str = typer.Option("", "--imap-host", help="IMAP server hostname"),
    imap_port: int = typer.Option(993, "--imap-port"),
    imap_user: str = typer.Option("", "--imap-user", help="IMAP username"),
    imap_pass: str = typer.Option("", "--imap-pass", help="IMAP password"),
    daily_limit: int = typer.Option(30, help="Daily send limit"),
    display_name: str = typer.Option("", "--display-name", help="Sender display name"),
):
    """Add a new mailbox for sending."""
    setup_logging()

    async def _add():
        async with get_db() as db:
            mb = Mailbox(
                email=email,
                smtp_host=smtp_host, smtp_port=smtp_port,
                smtp_user=smtp_user, smtp_pass=smtp_pass,
                imap_host=imap_host or smtp_host.replace("smtp.", "imap."),
                imap_port=imap_port,
                imap_user=imap_user or smtp_user,
                imap_pass=imap_pass or smtp_pass,
                daily_limit=daily_limit,
                display_name=display_name,
            )
            mb_id = await queries.upsert_mailbox(db, mb)
            console.print(f"[green]Added mailbox '{email}' (id={mb_id})[/green]")

    _run(_add())


@mailbox_app.command("list")
def list_mailboxes():
    """List all mailboxes."""
    setup_logging()

    async def _list():
        async with get_db() as db:
            mailboxes = await queries.get_mailboxes(db)
            table = Table(title="Mailboxes")
            table.add_column("ID", style="dim")
            table.add_column("Email")
            table.add_column("SMTP Host")
            table.add_column("Daily Limit", justify="right")
            table.add_column("Warmup Day", justify="right")
            table.add_column("Active")

            for mb in mailboxes:
                table.add_row(
                    str(mb.id), mb.email, mb.smtp_host,
                    str(mb.daily_limit), str(mb.warmup_day),
                    "Yes" if mb.is_active else "No",
                )
            console.print(table)

    _run(_list())


@mailbox_app.command("test")
def test(
    mailbox_id: int = typer.Argument(..., help="Mailbox ID to test"),
    to_email: str = typer.Option(..., "--to", help="Test recipient email"),
):
    """Send a test email from a mailbox."""
    setup_logging()

    async def _test():
        async with get_db() as db:
            mb = await queries.get_mailbox_by_id(db, mailbox_id)
            if not mb:
                console.print(f"[red]Mailbox {mailbox_id} not found[/red]")
                return

            from config.settings import SmtpSettings
            from email_engine.sender import EmailSender

            smtp = SmtpSettings(
                host=mb.smtp_host, port=mb.smtp_port,
                user=mb.smtp_user, password=mb.smtp_pass,
            )
            async with EmailSender(smtp, from_addr=mb.email, display_name=mb.display_name) as sender:
                msg_id = await sender.send(
                    to_email,
                    "Test email from Coldpipe CLI",
                    "This is a test email to verify your mailbox configuration.\n\nIf you received this, your SMTP settings are working correctly.",
                )
                console.print(f"[green]Test email sent! Message-ID: {msg_id}[/green]")

    _run(_test())


@mailbox_app.command("deactivate")
def deactivate(mailbox_id: int = typer.Argument(..., help="Mailbox ID")):
    """Deactivate a mailbox."""
    setup_logging()

    async def _deactivate():
        async with get_db() as db:
            ok = await queries.deactivate_mailbox(db, mailbox_id)
            if ok:
                console.print(f"[yellow]Mailbox {mailbox_id} deactivated[/yellow]")
            else:
                console.print(f"[red]Mailbox {mailbox_id} not found[/red]")

    _run(_deactivate())
