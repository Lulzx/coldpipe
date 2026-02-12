"""Deals pipeline CLI commands."""

from __future__ import annotations

import asyncio

import typer
from rich.console import Console
from rich.table import Table

from config import setup_logging
from db import get_db
from db import queries
from db.models import Deal

console = Console()
deals_app = typer.Typer(help="Deal pipeline management.")


def _run(coro):
    return asyncio.run(coro)


@deals_app.command("list")
def list_deals(
    stage: str = typer.Option("", help="Filter by stage"),
):
    """List all deals."""
    setup_logging()

    async def _list():
        async with get_db() as db:
            deals = await queries.get_deals(db, stage=stage or None)
            table = Table(title=f"Deals ({len(deals)})")
            table.add_column("ID", style="dim")
            table.add_column("Lead ID")
            table.add_column("Stage")
            table.add_column("Value", justify="right")
            table.add_column("Notes")
            table.add_column("Created")

            for deal in deals:
                table.add_row(
                    str(deal.id), str(deal.lead_id), deal.stage,
                    f"${deal.value:,.0f}", deal.notes[:40],
                    deal.created_at[:10] if deal.created_at else "-",
                )
            console.print(table)

    _run(_list())


@deals_app.command("create")
def create(
    lead_id: int = typer.Option(..., "--lead-id", help="Lead ID"),
    stage: str = typer.Option("lead", help="Initial stage"),
    value: float = typer.Option(0.0, help="Deal value"),
    notes: str = typer.Option("", help="Notes"),
):
    """Create a new deal."""
    setup_logging()

    async def _create():
        async with get_db() as db:
            deal = Deal(lead_id=lead_id, stage=stage, value=value, notes=notes)
            deal_id = await queries.upsert_deal(db, deal)
            console.print(f"[green]Created deal {deal_id} for lead {lead_id}[/green]")

    _run(_create())


@deals_app.command("move")
def move(
    deal_id: int = typer.Argument(..., help="Deal ID"),
    stage: str = typer.Option(..., "--stage", help="New stage"),
):
    """Move a deal to a new stage."""
    setup_logging()

    async def _move():
        async with get_db() as db:
            deal = await queries.get_deal_by_id(db, deal_id)
            if not deal:
                console.print(f"[red]Deal {deal_id} not found[/red]")
                return
            deal = Deal(
                id=deal.id, lead_id=deal.lead_id, campaign_id=deal.campaign_id,
                stage=stage, value=deal.value, notes=deal.notes,
            )
            await queries.upsert_deal(db, deal)
            console.print(f"[green]Deal {deal_id} moved to '{stage}'[/green]")

    _run(_move())


@deals_app.command("close")
def close(
    deal_id: int = typer.Argument(..., help="Deal ID"),
    won: bool = typer.Option(True, "--won/--lost", help="Won or lost"),
    value: float = typer.Option(0.0, help="Final value"),
    reason: str = typer.Option("", help="Loss reason (if lost)"),
):
    """Close a deal as won or lost."""
    setup_logging()

    async def _close():
        async with get_db() as db:
            deal = await queries.get_deal_by_id(db, deal_id)
            if not deal:
                console.print(f"[red]Deal {deal_id} not found[/red]")
                return
            stage = "closed_won" if won else "closed_lost"
            updated = Deal(
                id=deal.id, lead_id=deal.lead_id, campaign_id=deal.campaign_id,
                stage=stage, value=value if value else deal.value,
                loss_reason=reason if not won else None,
                notes=deal.notes,
            )
            await queries.upsert_deal(db, updated)
            label = "[green]WON[/green]" if won else "[red]LOST[/red]"
            console.print(f"Deal {deal_id} closed as {label}")

    _run(_close())


@deals_app.command("stats")
def stats():
    """Show deal pipeline statistics."""
    setup_logging()

    async def _stats():
        async with get_db() as db:
            s = await queries.get_deal_stats(db)
            stages = s.get("stages", {})

            console.print(f"\n[bold]Deal Pipeline[/bold]")
            console.print(f"  Pipeline value: ${s.get('pipeline_value', 0):,.0f}")
            console.print(f"  Closed value:   ${s.get('closed_value', 0):,.0f}")

            if stages:
                table = Table(title="By Stage")
                table.add_column("Stage")
                table.add_column("Count", justify="right")
                table.add_column("Value", justify="right")
                for stage_name, data in stages.items():
                    table.add_row(stage_name, str(data["count"]), f"${data['value']:,.0f}")
                console.print(table)

    _run(_stats())
