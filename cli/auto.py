"""Full pipeline orchestrator — one command does everything."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import typer
from rich.console import Console
from rich.table import Table

from cli import _run
from config import setup_logging
from db import get_db, queries
from db.tables import Deal, Lead

log = logging.getLogger(__name__)
console = Console()


def auto_command(
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show what would happen without doing it"
    ),
    skip_enrich: bool = typer.Option(False, "--skip-enrich", help="Skip enrichment step"),
    skip_validate: bool = typer.Option(False, "--skip-validate", help="Skip validation step"),
    skip_send: bool = typer.Option(False, "--skip-send", help="Skip sending step"),
    limit: int = typer.Option(50, help="Max items per step"),
):
    """Run the full pipeline: enrich → validate → send → replies → bounces → triage."""
    setup_logging()

    async def _auto():
        summary: dict[str, str | int] = {}

        async with get_db() as db:
            # ── Step 1: Assess ───────────────────────────────────────────
            console.print("\n[bold blue]● Assessing...[/bold blue]")

            queue = await queries.get_all_send_queues(db, limit=500)
            campaigns = await queries.get_campaigns(db, status="active")
            mailboxes = await queries.get_mailboxes(db, active_only=True)
            imap_mailboxes = [mb for mb in mailboxes if mb.imap_host and mb.imap_user]

            rows = await Lead.raw(
                "SELECT COUNT(*) as cnt FROM leads WHERE (email = '' OR email IS NULL) AND enriched_at IS NULL"
            ).run()
            enrichment_needed = rows[0]["cnt"] if rows else 0

            rows = await Lead.raw(
                "SELECT COUNT(*) as cnt FROM leads WHERE email != '' AND email IS NOT NULL AND email_status = 'unknown'"
            ).run()
            validation_needed = rows[0]["cnt"] if rows else 0

            console.print(f"  Send queue: {len(queue)} emails")
            console.print(f"  Enrichment needed: {enrichment_needed} leads")
            console.print(f"  Validation needed: {validation_needed} leads")
            console.print(f"  IMAP mailboxes: {len(imap_mailboxes)}")
            console.print(f"  Active campaigns: {len(campaigns)}")

            if dry_run:
                console.print("\n[yellow]── DRY RUN — no changes will be made ──[/yellow]")

            # ── Step 2: Enrich ───────────────────────────────────────────
            enriched = 0
            if not skip_enrich and enrichment_needed > 0:
                console.print("\n[bold blue]● Enriching leads...[/bold blue]")
                if dry_run:
                    console.print(f"  Would enrich up to {min(limit, enrichment_needed)} leads")
                else:
                    try:
                        from scrapers.website_enricher import WebsiteEnricher

                        leads = await queries.get_leads(db, limit=limit, email_status="missing")
                        leads += await queries.get_leads(db, limit=limit, email_status="unknown")
                        to_enrich = [ld for ld in leads if ld.website and not ld.email][:limit]
                        if to_enrich:
                            enricher = WebsiteEnricher()
                            results = await enricher.scrape(
                                db, limit=len(to_enrich), lead_ids=[ld.id for ld in to_enrich]
                            )
                            enriched = len(results)
                            console.print(f"  [green]Enriched {enriched} leads[/green]")
                        else:
                            console.print("  [dim]No leads with websites to enrich[/dim]")
                    except Exception as e:
                        console.print(f"  [red]Enrichment error: {e}[/red]")
                        log.error("auto: enrich error", exc_info=True)
            summary["enriched"] = enriched

            # ── Step 3: Validate ─────────────────────────────────────────
            validated = 0
            if not skip_validate and validation_needed > 0:
                console.print("\n[bold blue]● Validating emails...[/bold blue]")
                if dry_run:
                    console.print(
                        f"  Would validate up to {min(limit * 2, validation_needed)} emails"
                    )
                else:
                    try:
                        from tools.validate import EmailValidator

                        leads = await queries.get_leads(db, limit=limit * 2, email_status="unknown")
                        to_validate = [ld for ld in leads if ld.email]
                        if to_validate:
                            validator = EmailValidator()
                            status_map = {
                                "valid": "valid",
                                "invalid": "invalid",
                                "error": "risky",
                                "no-mx": "invalid",
                                "catch-all": "catch_all",
                            }
                            for lead in to_validate:
                                try:
                                    result = await validator.validate_email(lead.email)
                                    status = result.get("validation_status", "unknown")
                                    mapped = status_map.get(status, "unknown")
                                    now_iso = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
                                    await (
                                        Lead.update(
                                            {Lead.email_status: mapped, Lead.validated_at: now_iso}
                                        )
                                        .where(Lead.id == lead.id)
                                        .run()
                                    )
                                    validated += 1
                                except Exception as e:
                                    log.warning("auto: validate %s failed: %s", lead.email, e)
                            console.print(
                                f"  [green]Validated {validated}/{len(to_validate)} emails[/green]"
                            )
                        else:
                            console.print("  [dim]No emails to validate[/dim]")
                    except Exception as e:
                        console.print(f"  [red]Validation error: {e}[/red]")
                        log.error("auto: validate error", exc_info=True)
            summary["validated"] = validated

            # ── Step 4: Send ─────────────────────────────────────────────
            total_sent = 0
            if not skip_send and queue:
                console.print("\n[bold blue]● Sending emails...[/bold blue]")
                if dry_run:
                    by_campaign: dict[int, int] = {}
                    for item in queue:
                        cid = item["campaign_id"]
                        by_campaign[cid] = by_campaign.get(cid, 0) + 1
                    for cid, count in by_campaign.items():
                        console.print(f"  Campaign {cid}: would send {count} emails")
                else:
                    try:
                        from jinja2 import Template

                        from config.settings import SmtpSettings
                        from mailer.personalize import personalize_opener
                        from mailer.sender import EmailSender
                        from mailer.sequences import advance_sequence
                        from mailer.templates import render_template

                        # Group by mailbox
                        by_mailbox: dict[int, list[dict]] = {}
                        for item in queue:
                            mid = item.get("mailbox_id")
                            if mid:
                                by_mailbox.setdefault(mid, []).append(item)

                        for mailbox_id, items in by_mailbox.items():
                            mb = await queries.get_mailbox_by_id(db, mailbox_id)
                            if not mb or not mb.is_active:
                                continue

                            sent_today, daily_max = await queries.check_daily_limit(db, mb.id)
                            warmup_limit = queries.get_warmup_limit(mb.warmup_day)
                            effective_limit = min(daily_max, warmup_limit)
                            remaining = max(0, effective_limit - sent_today)
                            if remaining <= 0:
                                console.print(f"  [yellow]{mb.email}: daily limit reached[/yellow]")
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
                                        opener = await personalize_opener(item, api_key="")
                                        context = {
                                            **item,
                                            "opener": opener,
                                            "sender_name": mb.display_name,
                                        }
                                        body = render_template(item["template_name"], context)
                                        subject = Template(item["subject"]).render(**context)

                                        message_id = await sender.send_with_delay(
                                            item["email"], subject, body
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
                                        total_sent += 1
                                    except Exception:
                                        log.error(
                                            "auto: send error for %s",
                                            item.get("email", "unknown"),
                                            exc_info=True,
                                        )
                        console.print(f"  [green]Sent {total_sent} emails[/green]")
                    except Exception as e:
                        console.print(f"  [red]Send error: {e}[/red]")
                        log.error("auto: send error", exc_info=True)
            summary["sent"] = total_sent

            # ── Step 5: Check replies ────────────────────────────────────
            replies_matched = 0
            if imap_mailboxes:
                console.print("\n[bold blue]● Checking replies...[/bold blue]")
                if dry_run:
                    console.print(f"  Would check {len(imap_mailboxes)} mailbox(es)")
                else:
                    from mailer.replies import check_replies

                    for mb in imap_mailboxes:
                        try:
                            matched = await check_replies(db, mb)
                            replies_matched += matched
                        except Exception:
                            log.error("auto: reply check error for %s", mb.email, exc_info=True)
                    console.print(f"  [green]Matched {replies_matched} replies[/green]")
            summary["replies"] = replies_matched

            # ── Step 6: Triage replies ───────────────────────────────────
            triaged = 0
            if not dry_run:
                console.print("\n[bold blue]● Triaging replied deals...[/bold blue]")
                try:
                    from mailer.triage import triage_reply_text

                    # Find deals in 'replied' stage that haven't been triaged yet
                    deals = await Deal.raw(
                        "SELECT id, lead_id, notes FROM deals WHERE stage = 'replied' AND notes NOT LIKE '%auto-triage%'"
                    ).run()
                    for deal_row in deals:
                        notes = deal_row.get("notes", "")
                        if not notes or notes == "Auto-created from email reply":
                            continue
                        result = triage_reply_text(notes)
                        if result["classification"] != "other":
                            action = result["action"]
                            if action == "move_to_deals":
                                await (
                                    Deal.update({Deal.stage: "interested"})
                                    .where(Deal.id == deal_row["id"])
                                    .run()
                                )
                            note = f"\n[auto-triage] {result['classification']} (confidence={result['confidence']})"
                            await Deal.raw(
                                "UPDATE deals SET notes = notes || {} WHERE id = {}",
                                note,
                                deal_row["id"],
                            ).run()
                            triaged += 1
                    if triaged:
                        console.print(f"  [green]Triaged {triaged} deals[/green]")
                    else:
                        console.print("  [dim]No deals to triage[/dim]")
                except Exception:
                    log.error("auto: triage error", exc_info=True)
            summary["triaged"] = triaged

            # ── Step 7: Check bounces ────────────────────────────────────
            bounces_processed = 0
            if imap_mailboxes:
                console.print("\n[bold blue]● Checking bounces...[/bold blue]")
                if dry_run:
                    console.print(f"  Would check {len(imap_mailboxes)} mailbox(es)")
                else:
                    from mailer.bounces import check_bounces

                    for mb in imap_mailboxes:
                        try:
                            processed = await check_bounces(db, mb)
                            bounces_processed += processed
                        except Exception:
                            log.error("auto: bounce check error for %s", mb.email, exc_info=True)
                    console.print(f"  [green]Processed {bounces_processed} bounces[/green]")
            summary["bounces"] = bounces_processed

            # ── Summary ──────────────────────────────────────────────────
            console.print()
            table = Table(title="Pipeline Summary", show_header=True)
            table.add_column("Step", style="bold")
            table.add_column("Result", justify="right")

            if not skip_enrich:
                table.add_row("Enriched", str(summary.get("enriched", 0)))
            if not skip_validate:
                table.add_row("Validated", str(summary.get("validated", 0)))
            if not skip_send:
                table.add_row("Sent", str(summary.get("sent", 0)))
            table.add_row("Replies matched", str(summary.get("replies", 0)))
            table.add_row("Triaged", str(summary.get("triaged", 0)))
            table.add_row("Bounces processed", str(summary.get("bounces", 0)))

            console.print(table)

            if dry_run:
                console.print("\n[yellow]This was a dry run. No changes were made.[/yellow]")

    _run(_auto())
