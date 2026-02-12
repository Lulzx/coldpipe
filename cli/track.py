"""Reply and bounce tracking CLI commands."""

from __future__ import annotations

import asyncio

import typer
from rich.console import Console
from rich.table import Table

from config import setup_logging
from db import get_db, queries

console = Console()
track_app = typer.Typer(help="Email tracking commands (replies + bounces).")


def _run(coro):
    return asyncio.run(coro)


@track_app.command("check-replies")
def check_replies():
    """Poll IMAP for replies and update sequences."""
    setup_logging()

    async def _check():
        async with get_db() as db:
            mailboxes = await queries.get_mailboxes(db, active_only=True)
            if not mailboxes:
                console.print("[yellow]No active mailboxes configured[/yellow]")
                return

            from email_engine.replies import check_replies as _check_replies

            total_replies = 0
            for mb in mailboxes:
                if not mb.imap_host or not mb.imap_user:
                    continue
                try:
                    count = await _check_replies(db, mb)
                    total_replies += count
                    if count:
                        console.print(f"  {mb.email}: {count} new replies")
                except Exception as e:
                    console.print(f"  [red]{mb.email}: error - {e}[/red]")

            console.print(f"\n[green]Found {total_replies} new replies[/green]")

    _run(_check())


@track_app.command("check-bounces")
def check_bounces():
    """Check for bounced emails via DSN parsing."""
    setup_logging()

    async def _check():
        async with get_db() as db:
            mailboxes = await queries.get_mailboxes(db, active_only=True)
            if not mailboxes:
                console.print("[yellow]No active mailboxes configured[/yellow]")
                return

            from email_engine.bounces import check_bounces as _check_bounces

            total_bounces = 0
            for mb in mailboxes:
                if not mb.imap_host or not mb.imap_user:
                    continue
                try:
                    count = await _check_bounces(db, mb)
                    total_bounces += count
                    if count:
                        console.print(f"  {mb.email}: {count} bounces")
                except Exception as e:
                    console.print(f"  [red]{mb.email}: error - {e}[/red]")

            console.print(f"\n[yellow]Found {total_bounces} bounces[/yellow]")

    _run(_check())


@track_app.command("stats")
def stats():
    """Show tracking statistics."""
    setup_logging()

    async def _stats():
        async with get_db() as db:
            activity = await queries.get_today_activity(db)
            daily = await queries.get_daily_stats(db, days=7)

            console.print("\n[bold]Today[/bold]")
            console.print(f"  Sent: {activity['sent']}")
            console.print(f"  Replies: {activity['replies']}")
            console.print(f"  Bounces: {activity['bounces']}")
            if activity["sent"] > 0:
                reply_rate = activity["replies"] / activity["sent"] * 100
                console.print(f"  Reply rate: {reply_rate:.1f}%")

            if daily:
                table = Table(title="Last 7 Days")
                table.add_column("Date")
                table.add_column("Status")
                table.add_column("Count", justify="right")
                for row in daily:
                    table.add_row(row["day"], row["status"], str(row["cnt"]))
                console.print(table)

    _run(_stats())
