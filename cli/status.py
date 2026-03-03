"""Actionable system status command."""

from __future__ import annotations

from rich.console import Console
from rich.panel import Panel

from cli import _run
from config import setup_logging
from db import get_db, queries
from db.tables import Lead

console = Console()


def status_command():
    """Show actionable system status."""
    setup_logging()

    async def _status():
        async with get_db() as db:
            import asyncio

            stats, activity, campaigns, mailboxes, queue = await asyncio.gather(
                queries.get_lead_stats(db),
                queries.get_today_activity(db),
                queries.get_campaigns(db, status="active"),
                queries.get_mailboxes(db, active_only=True),
                queries.get_all_send_queues(db, limit=500),
            )

            total = stats.get("total", 0)
            by_status = stats.get("by_status", {})
            valid = by_status.get("valid", 0)
            unknown = by_status.get("unknown", 0)
            invalid = by_status.get("invalid", 0)
            missing = by_status.get("missing", 0)

            # Count enrichment/validation needed
            rows = await Lead.raw(
                "SELECT COUNT(*) as cnt FROM leads WHERE (email = '' OR email IS NULL) AND enriched_at IS NULL"
            ).run()
            enrichment_needed = rows[0]["cnt"] if rows else 0

            rows = await Lead.raw(
                "SELECT COUNT(*) as cnt FROM leads WHERE email != '' AND email IS NOT NULL AND email_status = 'unknown'"
            ).run()
            validation_needed = rows[0]["cnt"] if rows else 0

            imap_mailboxes = [mb for mb in mailboxes if mb.imap_host and mb.imap_user]

            # Build status lines
            lines = []
            lines.append(
                f"Leads: {total:,} total ({valid:,} valid, {unknown:,} unknown, {invalid:,} invalid, {missing:,} missing)"
            )
            lines.append(f"Campaigns: {len(campaigns)} active")
            lines.append(f"Send Queue: {len(queue)} emails ready")
            lines.append(
                f"Today: {activity['sent']} sent, {activity['replies']} replies, {activity['bounces']} bounces"
            )

            # Action items
            actions = []
            if enrichment_needed:
                actions.append(f"{enrichment_needed} leads need website enrichment")
            if validation_needed:
                actions.append(f"{validation_needed} leads need email validation")
            if imap_mailboxes:
                actions.append(f"{len(imap_mailboxes)} mailbox(es) to check for replies")
            if not queue and campaigns:
                actions.append("Send queue empty — enroll more leads")

            if actions:
                lines.append("")
                lines.append("[bold]Action needed:[/bold]")
                for action in actions:
                    lines.append(f"  → {action}")
                lines.append("")
                lines.append("Run [bold]coldpipe auto[/bold] to process all.")

            panel = Panel(
                "\n".join(lines),
                title="Coldpipe Status",
                border_style="blue",
                padding=(1, 2),
            )
            console.print(panel)

    _run(_status())
