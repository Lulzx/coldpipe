"""Email sending CLI commands."""

from __future__ import annotations

import typer
from rich.console import Console
from rich.table import Table

from cli import _run
from config import setup_logging
from db import get_db, queries

console = Console()
send_app = typer.Typer(help="Email sending commands.")


@send_app.command("preview")
def preview(
    campaign_id: int = typer.Option(..., "--campaign-id", help="Campaign ID"),
    lead_id: int = typer.Option(0, "--lead-id", help="Specific lead ID (0 = first in queue)"),
):
    """Preview a personalized email without sending."""
    setup_logging()

    async def _preview():
        async with get_db() as db:
            template_name = ""
            subject_template = ""
            if lead_id:
                lead = await queries.get_lead_by_id(db, lead_id)
                if not lead:
                    console.print(f"[red]Lead {lead_id} not found[/red]")
                    return
                steps = await queries.get_sequence_steps(db, campaign_id)
                if not steps:
                    console.print(f"[red]No sequence steps for campaign {campaign_id}[/red]")
                    return
                step = steps[0]
                template_name = step.template_name
                subject_template = step.subject
                lead_dict = {
                    "first_name": lead.first_name,
                    "last_name": lead.last_name,
                    "company": lead.company,
                    "city": lead.city,
                    "state": lead.state,
                    "job_title": lead.job_title,
                    "website": lead.website,
                    "email": lead.email,
                }
            else:
                queue = await queries.get_send_queue(db, campaign_id, limit=1)
                if not queue:
                    console.print("[yellow]No leads in send queue[/yellow]")
                    return
                item = queue[0]
                lead_dict = item
                template_name = item["template_name"]
                subject_template = item["subject"]

            # Generate opener
            from mailer.personalize import personalize_opener

            opener = await personalize_opener(lead_dict, api_key="")

            # Render template
            from mailer.templates import render_template

            context = {
                **lead_dict,
                "opener": opener,
                "sender_name": "Your Name",
                "sender_title": "",
            }
            body = render_template(template_name, context)

            # Render subject
            from jinja2 import Template

            subject = Template(subject_template).render(**context)

            console.print(f"\n[bold]To:[/bold] {lead_dict.get('email', 'N/A')}")
            console.print(f"[bold]Subject:[/bold] {subject}")
            console.print(f"[bold]Opener:[/bold] {opener}")
            console.print(f"\n[dim]--- Body ---[/dim]\n{body}")

    _run(_preview())


@send_app.command("run")
def run(
    campaign_id: int = typer.Option(..., "--campaign-id", help="Campaign ID"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show queue without sending"),
    limit: int = typer.Option(50, help="Max emails to send"),
):
    """Send emails for a specific campaign."""
    setup_logging()

    async def _send():
        async with get_db() as db:
            campaign = await queries.get_campaign_by_id(db, campaign_id)
            if not campaign:
                console.print(f"[red]Campaign {campaign_id} not found[/red]")
                return

            if campaign.status != "active":
                console.print(
                    f"[yellow]Campaign is '{campaign.status}', not 'active'. Activate first.[/yellow]"
                )
                return

            queue = await queries.get_send_queue(db, campaign_id, limit=limit)
            if not queue:
                console.print("[yellow]No leads ready to send[/yellow]")
                return

            console.print(f"Found {len(queue)} leads in send queue")

            if dry_run:
                table = Table(title="Send Queue (dry run)")
                table.add_column("Lead")
                table.add_column("Email")
                table.add_column("Step")
                table.add_column("Template")
                for item in queue:
                    table.add_row(
                        f"{item['first_name']} {item['last_name']}",
                        item["email"],
                        str(item["current_step"]),
                        item["template_name"],
                    )
                console.print(table)
                return

            mailbox = await queries.get_mailbox_by_id(db, campaign.mailbox_id)  # type: ignore[arg-type]
            if not mailbox:
                console.print("[red]No mailbox configured for this campaign[/red]")
                return

            from jinja2 import Template

            from config.settings import SmtpSettings
            from mailer.personalize import personalize_opener
            from mailer.sender import EmailSender
            from mailer.sequences import advance_sequence
            from mailer.templates import render_template

            smtp = SmtpSettings(
                host=mailbox.smtp_host,
                port=mailbox.smtp_port,
                user=mailbox.smtp_user,
                password=mailbox.smtp_pass,
            )

            async with EmailSender(
                smtp, from_addr=mailbox.email, display_name=mailbox.display_name
            ) as sender:
                sent_count = 0
                for item in queue:
                    try:
                        # Check daily limit
                        sent_today, daily_max = await queries.check_daily_limit(db, mailbox.id)
                        if sent_today >= daily_max:
                            console.print("[yellow]Daily limit reached[/yellow]")
                            break

                        # Personalize
                        opener = await personalize_opener(item, api_key="")

                        # Render
                        context = {**item, "opener": opener, "sender_name": mailbox.display_name}
                        body = render_template(item["template_name"], context)
                        subject = Template(item["subject"]).render(**context)

                        # Send
                        message_id = await sender.send_with_delay(
                            item["email"],
                            subject,
                            body,
                        )

                        # Advance sequence
                        await advance_sequence(
                            db,
                            campaign_lead_id=item["cl_id"],
                            campaign_id=campaign_id,
                            lead_id=item["lead_id"],
                            mailbox_id=mailbox.id,
                            step_number=item["current_step"],
                            subject=subject,
                            body=body,
                            message_id=message_id,
                            delay_days=item["delay_days"],
                            to_email=item["email"],
                            from_email=mailbox.email,
                        )
                        sent_count += 1
                        console.print(f"  Sent to {item['email']} (step {item['current_step']})")
                    except Exception as e:
                        console.print(f"  [red]Failed {item['email']}: {e}[/red]")

                console.print(f"\n[green]Sent {sent_count}/{len(queue)} emails[/green]")

    _run(_send())


@send_app.command("run-all")
def run_all(
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview only"),
):
    """Send emails across all active campaigns."""
    setup_logging()

    async def _run_all():
        async with get_db() as db:
            campaigns = await queries.get_campaigns(db, status="active")
            if not campaigns:
                console.print("[yellow]No active campaigns[/yellow]")
                return
            for camp in campaigns:
                console.print(f"\n[bold]Campaign: {camp.name} (id={camp.id})[/bold]")
                queue = await queries.get_send_queue(db, camp.id)
                console.print(f"  Queue size: {len(queue)}")

    _run(_run_all())


@send_app.command("status")
def status():
    """Show sending status across all campaigns."""
    setup_logging()

    async def _status():
        async with get_db() as db:
            activity = await queries.get_today_activity(db)
            console.print("\n[bold]Today's Activity[/bold]")
            console.print(f"  Sent: {activity['sent']}")
            console.print(f"  Replies: {activity['replies']}")
            console.print(f"  Bounces: {activity['bounces']}")

    _run(_status())


@send_app.command("warmup")
def warmup():
    """Show warmup status for all mailboxes."""
    setup_logging()

    async def _warmup():
        async with get_db() as db:
            mailboxes = await queries.get_mailboxes(db, active_only=True)
            table = Table(title="Mailbox Warmup Status")
            table.add_column("ID")
            table.add_column("Email")
            table.add_column("Warmup Day")
            table.add_column("Current Limit")
            for mb in mailboxes:
                limit = queries.get_warmup_limit(mb.warmup_day)
                table.add_row(str(mb.id), mb.email, str(mb.warmup_day), str(limit))
            console.print(table)

    _run(_warmup())
