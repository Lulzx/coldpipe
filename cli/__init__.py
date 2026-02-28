"""CLI root — Typer app with all sub-commands."""

from __future__ import annotations

import asyncio
from pathlib import Path

import typer

from config import setup_logging
from db import close_db, init_db

app = typer.Typer(
    name="coldpipe",
    help="Customer Acquisition Engine.",
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
        await init_db()
        typer.echo("Database initialized.")
        await close_db()

    _run(_init())


@db_app.command("backup")
def db_backup(
    output: str = typer.Option("data/coldpipe_backup.db", help="Backup file path"),
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


@db_app.command("backup-auto")
def db_backup_auto(
    retain: int = typer.Option(7, help="Number of backups to retain"),
    output_dir: str = typer.Option("data/backups", help="Backup output directory"),
):
    """Create timestamped backup with automatic retention."""
    import shutil

    from config.settings import DB_PATH

    dst_dir = Path(output_dir)
    dst_dir.mkdir(parents=True, exist_ok=True)
    src = DB_PATH
    if not src.exists():
        typer.echo("No database found.", err=True)
        raise typer.Exit(1)
    from datetime import UTC, datetime

    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    dst = dst_dir / f"coldpipe_{timestamp}.db"
    shutil.copy2(src, dst)
    # Retention
    backups = sorted(dst_dir.glob("coldpipe_*.db"))
    removed = 0
    for old in backups[:-retain]:
        old.unlink(missing_ok=True)
        removed += 1
    typer.echo(f"Backup created: {dst}" + (f" (removed {removed} old)" if removed else ""))


@db_app.command("health")
def db_health():
    """Run health checks on database and configuration."""
    setup_logging()
    checks_passed = True

    async def _check():
        nonlocal checks_passed
        try:
            from piccolo.querystring import QueryString

            from db import get_engine

            await init_db()
            engine = get_engine()
            rows = await engine.run_querystring(QueryString("PRAGMA integrity_check"))
            if rows and rows[0].get("integrity_check") == "ok":
                typer.echo("[OK] Database integrity check passed")
            else:
                typer.echo("[FAIL] Database integrity check failed", err=True)
                checks_passed = False

            # Check schema version
            try:
                vrows = await engine.run_querystring(
                    QueryString("SELECT MAX(version) as v FROM schema_version")
                )
                version = vrows[0]["v"] if vrows and vrows[0].get("v") else 0
                if version >= 3:
                    typer.echo(f"[OK] Schema version: {version}")
                else:
                    typer.echo(f"[WARN] Schema version: {version} (expected 3)", err=True)
                    checks_passed = False
            except Exception:
                typer.echo("[OK] Schema managed by Piccolo ORM")
            await close_db()
        except Exception as e:
            typer.echo(f"[FAIL] Database connection: {e}", err=True)
            checks_passed = False

        # Check config
        try:
            from config.settings import load_settings

            settings = load_settings()
            typer.echo("[OK] Configuration loaded")

            # Check API key
            if settings.anthropic_api_key:
                typer.echo("[OK] Anthropic API key is set")
            else:
                typer.echo("[WARN] Anthropic API key is not set")
        except Exception as e:
            typer.echo(f"[FAIL] Configuration: {e}", err=True)
            checks_passed = False

        return checks_passed

    result = _run(_check())
    if not result:
        raise typer.Exit(1)


# ---------------------------------------------------------------------------
# Web dashboard
# ---------------------------------------------------------------------------


@app.command("mcp")
def mcp_start():
    """Start the Coldpipe MCP server (for Claude Code integration)."""
    import subprocess
    import sys

    subprocess.run([sys.executable, "coldpipe_mcp.py"])


@app.command("web")
def web_serve(
    host: str = typer.Option("127.0.0.1", help="Host to bind to"),
    port: int = typer.Option(8080, help="Port to listen on"),
):
    """Start the web dashboard."""
    import uvicorn

    from web.app import create_app

    setup_logging()
    typer.echo(f"Starting web dashboard on http://{host}:{port}")
    uvicorn.run(create_app(), host=host, port=port)


# ---------------------------------------------------------------------------
# Register sub-apps (lazy to avoid circular imports at module level)
# ---------------------------------------------------------------------------


def _register_subapps():
    from cli.campaign import campaign_app
    from cli.daemon import daemon_app
    from cli.deals import deals_app
    from cli.enrich import enrich_app
    from cli.leads import leads_app
    from cli.mailbox import mailbox_app
    from cli.scrape import scrape_app
    from cli.send import send_app
    from cli.setup import _setup
    from cli.track import track_app
    from cli.validate import validate_app

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

    @app.command()
    def setup():
        """Interactive setup wizard for first-time configuration."""
        _setup()


_register_subapps()
