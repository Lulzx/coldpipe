"""Daemon command — APScheduler-based background processing."""

from __future__ import annotations

import asyncio
import logging
import signal
from datetime import UTC, datetime

import typer
from rich.console import Console

from config import setup_logging

log = logging.getLogger(__name__)

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
    - Database backup at 00:30
    """
    setup_logging()
    console.print("[bold]Starting daemon...[/bold]")

    async def _run_daemon():
        from apscheduler import AsyncScheduler
        from apscheduler.triggers.cron import CronTrigger
        from apscheduler.triggers.interval import IntervalTrigger

        from db import get_db, queries

        async def send_job():
            """Send pending emails across all active campaigns."""
            sent = 0
            failed = 0
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

                    from jinja2 import Template

                    from config.settings import SmtpSettings, load_settings
                    from mailer.personalize import personalize_opener
                    from mailer.sender import EmailSender
                    from mailer.sequences import advance_sequence
                    from mailer.templates import render_template

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
                            host=mb.smtp_host,
                            port=mb.smtp_port,
                            user=mb.smtp_user,
                            password=mb.smtp_pass,
                        )
                        async with EmailSender(
                            smtp, from_addr=mb.email, display_name=mb.display_name
                        ) as sender:
                            for item in items[:remaining]:
                                try:
                                    opener = await personalize_opener(
                                        item, api_key=""
                                    )
                                    context = {
                                        **item,
                                        "opener": opener,
                                        "sender_name": mb.display_name,
                                    }
                                    body = render_template(item["template_name"], context)
                                    subject = Template(item["subject"]).render(**context)

                                    message_id = await sender.send_with_delay(
                                        item["email"],
                                        subject,
                                        body,
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
                                        to_email=item["email"],
                                        from_email=mb.email,
                                    )
                                    sent += 1
                                except Exception:
                                    failed += 1
                                    log.error(
                                        "send_job: per-email error for %s",
                                        item.get("email", "unknown"),
                                        exc_info=True,
                                    )
            except Exception:
                log.error("send_job error", exc_info=True)
            finally:
                log.info("send_job complete: sent=%d failed=%d", sent, failed)

        async def reply_job():
            """Check for new replies across all mailboxes."""
            checked = 0
            try:
                async with get_db() as db:
                    mailboxes = await queries.get_mailboxes(db, active_only=True)
                    from mailer.replies import check_replies

                    for mb in mailboxes:
                        if mb.imap_host and mb.imap_user:
                            try:
                                await check_replies(db, mb)
                                checked += 1
                            except Exception:
                                log.error(
                                    "reply_job: error checking mailbox %s",
                                    mb.email,
                                    exc_info=True,
                                )
            except Exception:
                log.error("reply_job error", exc_info=True)
            finally:
                log.info("reply_job complete: checked=%d mailboxes", checked)

        async def bounce_job():
            """Check for bounced emails."""
            checked = 0
            try:
                async with get_db() as db:
                    mailboxes = await queries.get_mailboxes(db, active_only=True)
                    from mailer.bounces import check_bounces

                    for mb in mailboxes:
                        if mb.imap_host and mb.imap_user:
                            try:
                                await check_bounces(db, mb)
                                checked += 1
                            except Exception:
                                log.error(
                                    "bounce_job: error checking mailbox %s",
                                    mb.email,
                                    exc_info=True,
                                )
            except Exception:
                log.error("bounce_job error", exc_info=True)
            finally:
                log.info("bounce_job complete: checked=%d mailboxes", checked)

        async def warmup_job():
            """Advance warmup day counter for all active mailboxes."""
            try:
                async with get_db():
                    from db.tables import Mailbox

                    await (
                        Mailbox.update({Mailbox.warmup_day: Mailbox.warmup_day + 1})
                        .where(Mailbox.is_active == 1)
                        .run()
                    )
                    log.info("warmup_job complete")
            except Exception:
                log.error("warmup_job error", exc_info=True)

        async def backup_job():
            """Create timestamped database backup with retention."""
            import shutil

            from config.settings import DATA_DIR

            backup_dir = DATA_DIR / "backups"
            backup_dir.mkdir(exist_ok=True)
            timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
            backup_path = backup_dir / f"coldpipe_{timestamp}.db"
            src = DATA_DIR / "coldpipe.db"
            if not src.exists():
                return
            shutil.copy2(src, backup_path)
            # Verify backup
            import aiosqlite

            async with aiosqlite.connect(str(backup_path)) as bdb:
                cursor = await bdb.execute("PRAGMA integrity_check")
                result = await cursor.fetchone()
                if result and result[0] != "ok":
                    log.error("backup_integrity_failed: %s", str(backup_path))
                    backup_path.unlink(missing_ok=True)
                    return
            # Retention: keep last 7
            backups = sorted(backup_dir.glob("coldpipe_*.db"))
            for old in backups[:-7]:
                old.unlink(missing_ok=True)
            log.info("backup_complete: %s", str(backup_path))

        stop = asyncio.Event()
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)

        scheduler = AsyncScheduler()
        await scheduler.__aenter__()

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
        # Database backup at 00:30
        await scheduler.add_schedule(
            backup_job,
            CronTrigger(hour=0, minute=30),
            id="backup",
        )

        console.print("[green]Daemon running. Press Ctrl+C to stop.[/green]")
        console.print("  - Send emails: every 15 min")
        console.print("  - Check replies: every 30 min")
        console.print("  - Check bounces: every hour")
        console.print("  - Advance warmup: midnight")
        console.print("  - Database backup: 00:30")

        await stop.wait()
        log.info("Shutdown signal received")
        console.print("\n[yellow]Shutting down...[/yellow]")

        # Graceful shutdown with 30s timeout
        try:
            await asyncio.wait_for(scheduler.__aexit__(None, None, None), timeout=30)
        except TimeoutError:
            log.warning("Scheduler shutdown timed out after 30s, forcing exit")

        log.info("Daemon shutdown complete")
        console.print("[yellow]Daemon shutdown complete.[/yellow]")

    asyncio.run(_run_daemon())
