"""Daemon command — APScheduler-based background processing."""

from __future__ import annotations

import asyncio
import signal

import typer
from rich.console import Console

from config import setup_logging

console = Console()
daemon_app = typer.Typer(help="Background daemon for automated sending and tracking.")


@daemon_app.command("start")
def start():
    """Start the daemon with APScheduler for automated email processing.

    Jobs:
    - Send emails every 15 min (within send window)
    - Check replies every 30 min
    - Check bounces every hour
    - Advance warmup day at midnight
    """
    setup_logging()
    console.print("[bold]Starting daemon...[/bold]")

    async def _run_daemon():
        from apscheduler import AsyncScheduler
        from apscheduler.triggers.interval import IntervalTrigger
        from apscheduler.triggers.cron import CronTrigger

        from db import get_db
        from db import queries

        async def send_job():
            """Send pending emails across all active campaigns."""
            try:
                async with get_db() as db:
                    queue = await queries.get_all_send_queues(db, limit=50)
                    if not queue:
                        return

                    # Group by mailbox
                    by_mailbox: dict[int, list[dict]] = {}
                    for item in queue:
                        mid = item.get("mailbox_id")
                        if mid:
                            by_mailbox.setdefault(mid, []).append(item)

                    from config.settings import SmtpSettings, load_settings
                    from email_engine.sender import EmailSender
                    from email_engine.personalize import personalize_opener
                    from email_engine.templates import render_template
                    from email_engine.sequences import advance_sequence
                    from jinja2 import Template

                    settings = load_settings()

                    for mailbox_id, items in by_mailbox.items():
                        mb = await queries.get_mailbox_by_id(db, mailbox_id)
                        if not mb or not mb.is_active:
                            continue

                        sent_today, daily_max = await queries.check_daily_limit(db, mb.id)
                        warmup_limit = queries.get_warmup_limit(mb.warmup_day)
                        effective_limit = min(daily_max, warmup_limit)
                        remaining = max(0, effective_limit - sent_today)
                        if remaining <= 0:
                            continue

                        smtp = SmtpSettings(
                            host=mb.smtp_host, port=mb.smtp_port,
                            user=mb.smtp_user, password=mb.smtp_pass,
                        )
                        async with EmailSender(smtp, from_addr=mb.email, display_name=mb.display_name) as sender:
                            for item in items[:remaining]:
                                try:
                                    opener = await personalize_opener(
                                        item, api_key=settings.anthropic_api_key
                                    )
                                    context = {**item, "opener": opener, "sender_name": mb.display_name}
                                    body = render_template(item["template_name"], context)
                                    subject = Template(item["subject"]).render(**context)

                                    message_id = await sender.send_with_delay(
                                        item["email"], subject, body,
                                    )

                                    await advance_sequence(
                                        db,
                                        campaign_lead_id=item["cl_id"],
                                        campaign_id=item["campaign_id"],
                                        lead_id=item["lead_id"],
                                        mailbox_id=mailbox_id,
                                        step_number=item["current_step"],
                                        subject=subject,
                                        body=body,
                                        message_id=message_id,
                                        delay_days=item["delay_days"],
                                    )
                                except Exception:
                                    pass  # Per-email error handling, continue batch
            except Exception:
                pass

        async def reply_job():
            """Check for new replies across all mailboxes."""
            try:
                async with get_db() as db:
                    mailboxes = await queries.get_mailboxes(db, active_only=True)
                    from email_engine.replies import check_replies

                    for mb in mailboxes:
                        if mb.imap_host and mb.imap_user:
                            try:
                                await check_replies(db, mb)
                            except Exception:
                                pass
            except Exception:
                pass

        async def bounce_job():
            """Check for bounced emails."""
            try:
                async with get_db() as db:
                    mailboxes = await queries.get_mailboxes(db, active_only=True)
                    from email_engine.bounces import check_bounces

                    for mb in mailboxes:
                        if mb.imap_host and mb.imap_user:
                            try:
                                await check_bounces(db, mb)
                            except Exception:
                                pass
            except Exception:
                pass

        async def warmup_job():
            """Advance warmup day counter for all active mailboxes."""
            try:
                async with get_db() as db:
                    await db.execute(
                        "UPDATE mailboxes SET warmup_day = warmup_day + 1 WHERE is_active = 1"
                    )
                    await db.commit()
            except Exception:
                pass

        async with AsyncScheduler() as scheduler:
            # Send emails every 15 min during business hours
            await scheduler.add_schedule(
                send_job,
                IntervalTrigger(minutes=15),
                id="send_emails",
            )
            # Check replies every 30 min
            await scheduler.add_schedule(
                reply_job,
                IntervalTrigger(minutes=30),
                id="check_replies",
            )
            # Check bounces every hour
            await scheduler.add_schedule(
                bounce_job,
                IntervalTrigger(hours=1),
                id="check_bounces",
            )
            # Advance warmup at midnight
            await scheduler.add_schedule(
                warmup_job,
                CronTrigger(hour=0, minute=0),
                id="advance_warmup",
            )

            console.print("[green]Daemon running. Press Ctrl+C to stop.[/green]")
            console.print("  - Send emails: every 15 min")
            console.print("  - Check replies: every 30 min")
            console.print("  - Check bounces: every hour")
            console.print("  - Advance warmup: midnight")

            stop = asyncio.Event()
            loop = asyncio.get_event_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, stop.set)

            await stop.wait()
            console.print("\n[yellow]Daemon stopped.[/yellow]")

    asyncio.run(_run_daemon())
