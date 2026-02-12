"""Campaign management CLI commands."""

from __future__ import annotations

import asyncio

import typer
from rich.console import Console
from rich.table import Table

from config import setup_logging
from db import get_db
from db import queries
from db.models import Campaign, SequenceStep

console = Console()
campaign_app = typer.Typer(help="Campaign management commands.")


def _run(coro):
    return asyncio.run(coro)


@campaign_app.command("create")
def create(
    name: str = typer.Option(..., help="Campaign name"),
    mailbox_id: int = typer.Option(..., "--mailbox-id", help="Mailbox to send from"),
    daily_limit: int = typer.Option(30, help="Daily send limit"),
    timezone: str = typer.Option("America/New_York", help="Timezone for send window"),
):
    """Create a new campaign."""
    setup_logging()

    async def _create():
        async with get_db() as db:
            camp = Campaign(
                name=name,
                mailbox_id=mailbox_id,
                daily_limit=daily_limit,
                timezone=timezone,
            )
            camp_id = await queries.create_campaign(db, camp)

            # Auto-add default 4-step sequence
            steps = [
                SequenceStep(campaign_id=camp_id, step_number=0, template_name="cold_intro.txt",
                             subject="Quick question about {{ company }}", delay_days=0),
                SequenceStep(campaign_id=camp_id, step_number=1, template_name="followup_1.txt",
                             subject="Re: Quick question about {{ company }}", delay_days=3, is_reply=1),
                SequenceStep(campaign_id=camp_id, step_number=2, template_name="followup_2.txt",
                             subject="Re: Quick question about {{ company }}", delay_days=4, is_reply=1),
                SequenceStep(campaign_id=camp_id, step_number=3, template_name="breakup.txt",
                             subject="Re: Quick question about {{ company }}", delay_days=5, is_reply=1),
            ]
            for step in steps:
                await queries.add_sequence_step(db, step)

            console.print(f"[green]Created campaign '{name}' (id={camp_id}) with 4-step sequence[/green]")

    _run(_create())


@campaign_app.command("list")
def list_campaigns(
    status: str = typer.Option("", help="Filter by status"),
):
    """List all campaigns."""
    setup_logging()

    async def _list():
        async with get_db() as db:
            camps = await queries.get_campaigns(db, status=status or None)
            table = Table(title="Campaigns")
            table.add_column("ID", style="dim")
            table.add_column("Name")
            table.add_column("Status")
            table.add_column("Mailbox")
            table.add_column("Daily Limit", justify="right")
            table.add_column("Created")

            for c in camps:
                table.add_row(
                    str(c.id), c.name, c.status,
                    str(c.mailbox_id or "-"), str(c.daily_limit),
                    c.created_at[:10] if c.created_at else "-",
                )
            console.print(table)

    _run(_list())


@campaign_app.command("add-leads")
def add_leads(
    campaign_id: int = typer.Option(..., "--campaign-id", help="Campaign ID"),
    city: str = typer.Option("", help="Filter by city"),
    state: str = typer.Option("", help="Filter by state"),
    email_status: str = typer.Option("valid", help="Filter by email status"),
    tag: str = typer.Option("", help="Filter by tag"),
):
    """Add leads to a campaign by filter."""
    setup_logging()

    async def _add():
        async with get_db() as db:
            count = await queries.enroll_leads_by_filter(
                db,
                campaign_id,
                city=city or None,
                state=state or None,
                email_status=email_status or None,
                tag=tag or None,
            )
            console.print(f"[green]Enrolled {count} leads into campaign {campaign_id}[/green]")

    _run(_add())


@campaign_app.command("pause")
def pause(campaign_id: int = typer.Argument(..., help="Campaign ID")):
    """Pause a campaign."""
    setup_logging()

    async def _pause():
        async with get_db() as db:
            ok = await queries.update_campaign_status(db, campaign_id, "paused")
            if ok:
                console.print(f"[yellow]Campaign {campaign_id} paused[/yellow]")
            else:
                console.print(f"[red]Campaign {campaign_id} not found[/red]")

    _run(_pause())


@campaign_app.command("resume")
def resume(campaign_id: int = typer.Argument(..., help="Campaign ID")):
    """Resume a paused campaign."""
    setup_logging()

    async def _resume():
        async with get_db() as db:
            ok = await queries.update_campaign_status(db, campaign_id, "active")
            if ok:
                console.print(f"[green]Campaign {campaign_id} resumed[/green]")
            else:
                console.print(f"[red]Campaign {campaign_id} not found[/red]")

    _run(_resume())


@campaign_app.command("delete")
def delete(campaign_id: int = typer.Argument(..., help="Campaign ID")):
    """Delete a campaign and its data."""
    setup_logging()

    async def _delete():
        async with get_db() as db:
            ok = await queries.delete_campaign(db, campaign_id)
            if ok:
                console.print(f"[red]Campaign {campaign_id} deleted[/red]")
            else:
                console.print(f"[red]Campaign {campaign_id} not found[/red]")

    _run(_delete())
