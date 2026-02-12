"""Enrichment CLI commands — scrape websites for missing emails."""

from __future__ import annotations

import asyncio

import typer
from rich.console import Console

from config import setup_logging
from db import get_db, queries

console = Console()
enrich_app = typer.Typer(help="Enrich leads with missing data.")


def _run(coro):
    return asyncio.run(coro)


@enrich_app.command("run")
def run(
    batch_size: int = typer.Option(50, help="Number of leads per batch"),
):
    """Enrich leads by scraping their websites for emails and practice details."""
    setup_logging()

    async def _enrich():
        from scrapers.website_enricher import WebsiteEnricher

        async with get_db() as db:
            # Find leads with website but no email
            leads = await queries.get_leads(db, limit=batch_size, email_status="missing")
            leads += await queries.get_leads(db, limit=batch_size, email_status="unknown")
            # Filter to those with websites
            to_enrich = [lead for lead in leads if lead.website and not lead.email]

            if not to_enrich:
                console.print(
                    "[yellow]No leads to enrich (all have emails or no websites)[/yellow]"
                )
                return

            console.print(f"Enriching {len(to_enrich)} leads...")
            enricher = WebsiteEnricher()

            results = await enricher.scrape(db, limit=len(to_enrich))
            enriched = len(results)

            console.print(f"[green]Enriched {enriched}/{len(to_enrich)} leads[/green]")


@enrich_app.command("status")
def status():
    """Show enrichment status."""
    setup_logging()

    async def _status():
        async with get_db() as db:
            total = await queries.count_leads(db)
            with_email = await queries.count_leads(db, email_status="valid")
            unknown = await queries.count_leads(db, email_status="unknown")
            missing = await queries.count_leads(db, email_status="missing")
            console.print(f"Total leads: {total}")
            console.print(f"  With valid email: {with_email}")
            console.print(f"  Unknown status: {unknown}")
            console.print(f"  Missing email: {missing}")

    _run(_status())
