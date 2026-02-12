"""Scraper CLI commands."""

from __future__ import annotations

import typer
from rich.console import Console

from cli import _run
from config import setup_logging
from db import get_db

console = Console()
scrape_app = typer.Typer(help="Scrape leads from various sources.")


@scrape_app.command("google-maps")
def google_maps(
    city: str = typer.Option(..., help="City/state to search, e.g. 'Phoenix, AZ'"),
    max_results: int = typer.Option(100, "--max", help="Max results to fetch"),
):
    """Scrape leads from Google Maps via Crawl4AI."""
    setup_logging()

    async def _scrape():
        from scrapers.google_maps import GoogleMapsScraper

        async with get_db() as db:
            scraper = GoogleMapsScraper()
            leads = await scraper.scrape(db, city=city, max_results=max_results)
            console.print(f"[green]Imported {len(leads)} leads from Google Maps[/green]")

    _run(_scrape())


@scrape_app.command("yelp")
def yelp(
    city: str = typer.Option(..., help="City/state to search"),
    pages: int = typer.Option(5, help="Number of pages to scrape"),
):
    """Scrape leads from Yelp."""
    setup_logging()

    async def _scrape():
        from scrapers.directories import DirectoryScraper as YelpScraper

        async with get_db() as db:
            scraper = YelpScraper()
            leads = await scraper.scrape(db, city=city, max_results=pages * 10)
            console.print(f"[green]Imported {len(leads)} leads from Yelp[/green]")

    _run(_scrape())


@scrape_app.command("healthgrades")
def healthgrades(
    city: str = typer.Option(..., help="City/state to search"),
):
    """Scrape leads from Healthgrades."""
    setup_logging()

    async def _scrape():
        from scrapers.directories import DirectoryScraper as HealthgradesScraper

        async with get_db() as db:
            scraper = HealthgradesScraper()
            leads = await scraper.scrape(db, city=city)
            console.print(f"[green]Imported {len(leads)} leads from Healthgrades[/green]")

    _run(_scrape())


@scrape_app.command("exa")
def exa(
    query: str = typer.Option(..., help="Search query for Exa.ai"),
    max_results: int = typer.Option(50, "--max", help="Max results"),
):
    """Search for leads via Exa.ai API."""
    setup_logging()

    async def _scrape():
        from scrapers.exa_search import ExaScraper

        async with get_db() as db:
            scraper = ExaScraper()
            leads = await scraper.scrape(db, query=query, max_results=max_results)
            console.print(f"[green]Imported {len(leads)} leads from Exa[/green]")

    _run(_scrape())


@scrape_app.command("import-csv")
def import_csv(
    file: str = typer.Option("", help="Specific CSV file to import (default: all in data/)"),
    source: str = typer.Option("", help="Override source label"),
):
    """Import leads from CSV files into the database."""
    setup_logging()

    async def _import():
        from scrapers.csv_import import CsvImporter

        async with get_db() as db:
            importer = CsvImporter()
            if file:
                from shared.csv_io import load_leads

                rows = load_leads(file)
                from scrapers.csv_import import _dict_to_lead

                leads = []
                for row in rows:
                    if source:
                        row["source_file"] = source
                    lead = _dict_to_lead(row)
                    if lead.email or lead.company or lead.website:
                        from db.queries import upsert_lead

                        await upsert_lead(db, lead)
                        leads.append(lead)
                console.print(f"[green]Imported {len(leads)} leads from {file}[/green]")
            else:
                leads = await importer.scrape(db)
                console.print(f"[green]Imported {len(leads)} leads from all CSVs[/green]")

    _run(_import())
