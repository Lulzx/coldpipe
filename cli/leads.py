"""Leads management CLI commands."""

from __future__ import annotations

import asyncio
import csv
import sys

import typer
from rich.console import Console
from rich.table import Table

from config import setup_logging
from db import get_db
from db import queries

console = Console()
leads_app = typer.Typer(help="Lead management commands.")


def _run(coro):
    return asyncio.run(coro)


@leads_app.command("list")
def list_leads(
    limit: int = typer.Option(50, help="Number of leads to show"),
    offset: int = typer.Option(0, help="Offset for pagination"),
    status: str = typer.Option("", help="Filter by email_status"),
    source: str = typer.Option("", help="Filter by source"),
):
    """List leads with optional filters."""
    setup_logging()

    async def _list():
        async with get_db() as db:
            leads = await queries.get_leads(
                db,
                limit=limit,
                offset=offset,
                email_status=status or None,
                source=source or None,
            )
            table = Table(title=f"Leads ({len(leads)} shown)")
            table.add_column("ID", style="dim")
            table.add_column("Name")
            table.add_column("Company")
            table.add_column("Email")
            table.add_column("Status")
            table.add_column("City")
            table.add_column("Source", style="dim")

            for lead in leads:
                name = f"{lead.first_name} {lead.last_name}".strip()
                table.add_row(
                    str(lead.id),
                    name,
                    lead.company,
                    lead.email or "-",
                    lead.email_status,
                    f"{lead.city}, {lead.state}" if lead.city else "-",
                    lead.source[:30] if lead.source else "-",
                )

            console.print(table)

    _run(_list())


@leads_app.command("search")
def search(
    query: str = typer.Argument(..., help="Search term"),
    limit: int = typer.Option(50, help="Max results"),
):
    """Search leads by name, email, or company."""
    setup_logging()

    async def _search():
        async with get_db() as db:
            leads = await queries.search_leads(db, query, limit=limit)
            table = Table(title=f"Search results for '{query}' ({len(leads)} found)")
            table.add_column("ID", style="dim")
            table.add_column("Name")
            table.add_column("Company")
            table.add_column("Email")
            table.add_column("Status")

            for lead in leads:
                name = f"{lead.first_name} {lead.last_name}".strip()
                table.add_row(
                    str(lead.id), name, lead.company,
                    lead.email or "-", lead.email_status,
                )

            console.print(table)

    _run(_search())


@leads_app.command("export")
def export(
    output: str = typer.Option("data/exported_leads.csv", help="Output file path"),
    status: str = typer.Option("", help="Filter by email_status"),
):
    """Export leads to CSV."""
    setup_logging()

    async def _export():
        async with get_db() as db:
            leads = await queries.get_leads(
                db, limit=100000, email_status=status or None,
            )
            fieldnames = [
                "id", "email", "first_name", "last_name", "company",
                "job_title", "website", "phone", "city", "state",
                "email_status", "tags", "source",
            ]
            with open(output, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for lead in leads:
                    writer.writerow({k: getattr(lead, k, "") for k in fieldnames})

            console.print(f"[green]Exported {len(leads)} leads to {output}[/green]")

    _run(_export())


@leads_app.command("tag")
def tag(
    ids: str = typer.Argument(..., help="Comma-separated lead IDs"),
    tag: str = typer.Option(..., "--tag", help="Tag to add"),
):
    """Add a tag to leads."""
    setup_logging()
    lead_ids = [int(x.strip()) for x in ids.split(",")]

    async def _tag():
        async with get_db() as db:
            count = await queries.tag_leads(db, lead_ids, tag)
            console.print(f"[green]Tagged {count} leads with '{tag}'[/green]")

    _run(_tag())


@leads_app.command("dedupe")
def dedupe():
    """Run fuzzy deduplication on leads."""
    setup_logging()

    async def _dedupe():
        from scrapers.dedup import deduplicate_leads

        async with get_db() as db:
            removed = await deduplicate_leads(db)
            if removed:
                console.print(f"[yellow]Removed {removed} duplicate leads[/yellow]")
            else:
                console.print("[green]No duplicates found[/green]")

    _run(_dedupe())


@leads_app.command("stats")
def stats():
    """Show lead statistics."""
    setup_logging()

    async def _stats():
        async with get_db() as db:
            s = await queries.get_lead_stats(db)

            console.print(f"\n[bold]Total leads: {s['total']}[/bold]\n")

            if s.get("by_status"):
                table = Table(title="By Email Status")
                table.add_column("Status")
                table.add_column("Count", justify="right")
                for status, count in sorted(s["by_status"].items()):
                    table.add_row(status, str(count))
                console.print(table)

            if s.get("by_source"):
                table = Table(title="By Source")
                table.add_column("Source")
                table.add_column("Count", justify="right")
                for source, count in list(s["by_source"].items())[:15]:
                    table.add_row(source[:50], str(count))
                console.print(table)

            if s.get("by_city"):
                table = Table(title="By City (top 20)")
                table.add_column("City")
                table.add_column("Count", justify="right")
                for city, count in list(s["by_city"].items())[:20]:
                    table.add_row(city, str(count))
                console.print(table)

    _run(_stats())
