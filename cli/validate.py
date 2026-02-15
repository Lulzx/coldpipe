"""Email validation CLI commands."""

from __future__ import annotations

from datetime import UTC, datetime

import typer
from rich.console import Console
from rich.progress import Progress

from cli import _run
from config import setup_logging
from db import get_db, queries
from db.tables import Lead

console = Console()
validate_app = typer.Typer(help="Validate lead email addresses.")


@validate_app.command("run")
def run(
    batch_size: int = typer.Option(50, help="Number of emails per batch"),
):
    """Validate email addresses via MX lookup + SMTP probe."""
    setup_logging()

    async def _validate():
        from tools.validate import EmailValidator

        async with get_db() as db:
            # Find leads with unknown email status that have emails
            leads = await queries.get_leads(db, limit=batch_size, email_status="unknown")
            to_validate = [lead for lead in leads if lead.email]

            if not to_validate:
                console.print("[yellow]No emails to validate[/yellow]")
                return

            console.print(f"Validating {len(to_validate)} emails...")
            validator = EmailValidator()

            validated = 0
            with Progress(console=console) as progress:
                task = progress.add_task("Validating...", total=len(to_validate))
                for lead in to_validate:
                    try:
                        result = await validator.validate_email(lead.email)
                        status = result.get("validation_status", "unknown")
                        # Map validator statuses to our schema
                        status_map = {
                            "valid": "valid",
                            "invalid": "invalid",
                            "error": "risky",
                            "no-mx": "invalid",
                            "catch-all": "catch_all",
                        }
                        mapped = status_map.get(status, "unknown")
                        now_iso = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
                        await (
                            Lead.update({Lead.email_status: mapped, Lead.validated_at: now_iso})
                            .where(Lead.id == lead.id)
                            .run()
                        )
                        validated += 1
                    except Exception as e:
                        console.print(f"[red]Error validating {lead.email}: {e}[/red]")
                    progress.advance(task)

            console.print(f"[green]Validated {validated}/{len(to_validate)} emails[/green]")

    _run(_validate())


@validate_app.command("status")
def status():
    """Show validation status summary."""
    setup_logging()

    async def _status():
        async with get_db() as db:
            stats = await queries.get_lead_stats(db)
            console.print("\n[bold]Email Validation Status[/bold]")
            for status, count in sorted(stats.get("by_status", {}).items()):
                console.print(f"  {status}: {count}")

    _run(_status())
