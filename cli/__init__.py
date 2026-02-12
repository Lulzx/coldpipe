"""CLI root — Typer app with all sub-commands."""

from __future__ import annotations

import asyncio
from pathlib import Path

import typer

from config import setup_logging
from db import get_db
from db.migrate import init_schema

app = typer.Typer(
    name="dentists",
    help="Customer Acquisition Engine for dental practices.",
    no_args_is_help=True,
)


def _run(coro):
    """Run an async coroutine from sync Typer context."""
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# db sub-commands
# ---------------------------------------------------------------------------

db_app = typer.Typer(help="Database management commands.")
app.add_typer(db_app, name="db")


@db_app.command("init")
def db_init():
    """Initialize database schema."""
    setup_logging()

    async def _init():
        async with get_db() as db:
            version = await init_schema(db)
            typer.echo(f"Database initialized at schema version {version}.")

    _run(_init())


@db_app.command("backup")
def db_backup(
    output: str = typer.Option("data/dentists_backup.db", help="Backup file path"),
):
    """Create a backup of the database."""
    import shutil

    from config.settings import DB_PATH

    src = DB_PATH
    dst = Path(output)
    if not src.exists():
        typer.echo("No database found to back up.", err=True)
        raise typer.Exit(1)
    shutil.copy2(src, dst)
    typer.echo(f"Backed up {src} → {dst}")


# ---------------------------------------------------------------------------
# Register sub-apps (lazy to avoid circular imports at module level)
# ---------------------------------------------------------------------------

def _register_subapps():
    from cli.scrape import scrape_app
    from cli.leads import leads_app
    from cli.enrich import enrich_app
    from cli.validate import validate_app
    from cli.campaign import campaign_app
    from cli.send import send_app
    from cli.track import track_app
    from cli.deals import deals_app
    from cli.mailbox import mailbox_app
    from cli.daemon import daemon_app

    app.add_typer(scrape_app, name="scrape")
    app.add_typer(leads_app, name="leads")
    app.add_typer(enrich_app, name="enrich")
    app.add_typer(validate_app, name="validate")
    app.add_typer(campaign_app, name="campaign")
    app.add_typer(send_app, name="send")
    app.add_typer(track_app, name="track")
    app.add_typer(deals_app, name="deals")
    app.add_typer(mailbox_app, name="mailbox")
    app.add_typer(daemon_app, name="daemon")


_register_subapps()
