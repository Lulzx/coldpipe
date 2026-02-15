"""Interactive CLI setup wizard for first-time configuration."""

from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console
from rich.prompt import Confirm, IntPrompt, Prompt

from cli import _run

setup_app = typer.Typer(help="Interactive setup wizard.")
console = Console()


@setup_app.command("run", hidden=True)
def setup_run():
    """Run the interactive setup wizard."""
    _setup()


def _setup():
    """Entry point called by `coldpipe setup`."""
    console.print("\n[bold blue]Coldpipe Setup Wizard[/bold blue]\n")

    # Step 1: Initialize database
    console.print("[bold]Step 1:[/bold] Initialize database")
    _run(_init_db())

    # Step 2: Configure settings
    console.print("\n[bold]Step 2:[/bold] Configure settings")
    timezone = Prompt.ask("Timezone", default="America/New_York")
    daily_limit = IntPrompt.ask("Daily send limit", default=30)

    _write_config(timezone, daily_limit)
    console.print("[green]Configuration saved to coldpipe.toml[/green]")

    # Step 3: Optionally add mailbox
    if Confirm.ask("\nAdd a mailbox now?", default=True):
        _run(_add_mailbox())

    # Step 4: Optionally import leads
    if Confirm.ask("\nImport leads from CSV?", default=False):
        csv_path = Prompt.ask("Path to CSV file")
        _run(_import_leads(csv_path))

    console.print("\n[bold green]Setup complete![/bold green]")
    console.print("Run [bold]coldpipe web[/bold] to start the dashboard.\n")


async def _init_db():
    from config import setup_logging
    from db import get_db, get_engine
    from db.migrate import migrate_legacy

    setup_logging()
    async with get_db():
        engine = get_engine()
        version = await migrate_legacy(engine)
        console.print(f"  Database initialized at schema version {version}")


def _write_config(timezone: str, daily_limit: int):
    from shared.toml_writer import dumps

    base_dir = Path(__file__).resolve().parent.parent
    toml_path = base_dir / "coldpipe.toml"

    config: dict = {}
    if toml_path.exists():
        import tomllib

        with open(toml_path, "rb") as f:
            config = tomllib.load(f)

    if "send" not in config:
        config["send"] = {}
    config["send"]["timezone"] = timezone
    config["send"]["daily_limit"] = daily_limit

    toml_path.write_text(dumps(config))


async def _add_mailbox():
    from db import get_db
    from db.queries import upsert_mailbox
    from db.tables import Mailbox

    email = Prompt.ask("  Email address")
    smtp_host = Prompt.ask("  SMTP host", default="smtp.gmail.com")
    smtp_port = IntPrompt.ask("  SMTP port", default=587)
    smtp_user = Prompt.ask("  SMTP user", default=email)
    smtp_pass = Prompt.ask("  SMTP password", password=True)
    imap_host = Prompt.ask("  IMAP host", default="imap.gmail.com")
    imap_port = IntPrompt.ask("  IMAP port", default=993)
    imap_user = Prompt.ask("  IMAP user", default=email)
    imap_pass = Prompt.ask("  IMAP password (leave blank to reuse SMTP)", password=True, default="")
    display_name = Prompt.ask("  Display name", default="")
    daily_limit = IntPrompt.ask("  Daily limit", default=30)

    mb = Mailbox(
        email=email,
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        smtp_user=smtp_user,
        smtp_pass=smtp_pass,
        imap_host=imap_host,
        imap_port=imap_port,
        imap_user=imap_user,
        imap_pass=imap_pass or smtp_pass,
        daily_limit=daily_limit,
        display_name=display_name,
        is_active=1,
    )

    async with get_db() as db:
        mid = await upsert_mailbox(db, mb)
        console.print(f"  [green]Mailbox added (id={mid})[/green]")


async def _import_leads(csv_path: str):
    import csv as csv_mod

    from db import get_db
    from db.queries import upsert_lead
    from db.tables import Lead

    path = Path(csv_path)
    if not path.exists():
        console.print(f"  [red]File not found: {csv_path}[/red]")
        return

    count = 0
    async with get_db() as db:
        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv_mod.DictReader(f)
            for row in reader:
                email = row.get("email", "").strip()
                if not email:
                    continue
                lead = Lead(
                    email=email,
                    first_name=row.get("first_name", "").strip(),
                    last_name=row.get("last_name", "").strip(),
                    company=row.get("company", "").strip(),
                    job_title=row.get("job_title", "").strip(),
                    website=row.get("website", "").strip(),
                    phone=row.get("phone", "").strip(),
                    city=row.get("city", "").strip(),
                    state=row.get("state", "").strip(),
                    zip=row.get("zip", "").strip(),
                    source=row.get("source", "csv").strip(),
                    tags=row.get("tags", "").strip(),
                )
                await upsert_lead(db, lead)
                count += 1
    console.print(f"  [green]Imported {count} leads[/green]")
