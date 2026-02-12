#!/usr/bin/env python3
"""Fast email finder for lead CSVs.

Crawls websites concurrently to find email addresses.

Usage:
    python3 scrape_emails.py leads.csv
    python3 scrape_emails.py leads.csv --concurrency 100
"""

import asyncio
import csv
import sys
import time

from shared.http import create_sessions
from shared.patterns import generate_candidates
from shared.scoring import EmailCandidate, pick_best
from shared.scraping import scrape_site_for_emails
from tools.validate import EmailValidator

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[union-attr]

CONCURRENCY = 50


def log(msg: str):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


async def find_email_for(
    sessions,
    company: str,
    website: str,
    stats: dict,
    first_name: str = "",
    last_name: str = "",
) -> str:
    if not website:
        stats["skip"] += 1
        return ""

    website = website.strip()
    if not website.startswith("http"):
        website = "https://" + website

    try:
        emails = await scrape_site_for_emails(sessions, website)
        if emails:
            r = "; ".join(sorted(emails))
            stats["found"] += 1
            log(f"  FOUND[{stats['found']:>3}] {company}: {r[:70]}")
            return r

        # Pattern fallback: generate candidates from name + domain
        if first_name and last_name:
            from urllib.parse import urlparse

            domain = (urlparse(website).hostname or "").replace("www.", "")
            if domain:
                candidates = generate_candidates(first_name, last_name, domain)
                if candidates:
                    validator = EmailValidator(concurrency=10)
                    results = await validator.validate_candidates(candidates, domain)
                    scored = [
                        EmailCandidate(
                            email=r["email"],
                            source="pattern",
                            smtp_status=r["status"],
                            is_catchall=r["is_catchall"],
                            provider=r["provider"],
                            matches_domain=True,
                        )
                        for r in results
                    ]
                    best = pick_best(scored)
                    if best:
                        stats["found"] += 1
                        log(
                            f"  FOUND[{stats['found']:>3}] {company}: {best[0].email} (pattern, score={best[1]:.1f})"
                        )
                        return best[0].email

        stats["miss"] += 1
        log(f"  MISS {company}")
        return ""
    except Exception as e:
        stats["err"] += 1
        log(f"  ERR {company}: {e}")
        return ""


async def main():
    t0 = time.time()
    csv_file = sys.argv[1] if len(sys.argv) > 1 else "leads.csv"

    concurrency = CONCURRENCY
    if "--concurrency" in sys.argv:
        idx = sys.argv.index("--concurrency")
        concurrency = int(sys.argv[idx + 1])

    with open(csv_file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        all_rows = list(reader)

    # Auto-detect column names
    url_col = next(
        (c for c in ("URL", "url", "website", "Website", "Website (Result)") if c in fieldnames),
        None,
    )
    name_col = next(
        (
            c
            for c in ("Title", "company", "Company", "Company Name (Result)", "name")
            if c in fieldnames
        ),
        None,
    )
    first_name_col = next(
        (c for c in ("first_name", "First Name", "firstName") if c in fieldnames), None
    )
    last_name_col = next(
        (c for c in ("last_name", "Last Name", "lastName") if c in fieldnames), None
    )

    if not url_col:
        print(f"ERROR: No URL/website column found in {fieldnames}", file=sys.stderr)
        sys.exit(1)

    # Ensure email column exists
    if "email" not in fieldnames:
        fieldnames.append("email")
        for r in all_rows:
            r["email"] = ""

    # Find rows missing email
    missing_idx = [i for i, r in enumerate(all_rows) if not r.get("email", "").strip()]
    has_email = len(all_rows) - len(missing_idx)

    print("=" * 60, flush=True)
    print("  EMAIL FINDER — crawling websites for missing emails", flush=True)
    print("=" * 60, flush=True)
    log(f"Total: {len(all_rows)} | Has email: {has_email} | Missing: {len(missing_idx)}")
    log(f"URL column: {url_col} | Name column: {name_col or '(none)'}")
    log("-" * 50)

    if not missing_idx:
        print("Nothing to do — all rows have emails.", flush=True)
        return

    stats = {"found": 0, "miss": 0, "skip": 0, "err": 0}
    ssl_session, nossl_session = create_sessions()
    sessions = [ssl_session, nossl_session]
    sem = asyncio.Semaphore(concurrency)

    try:

        async def bounded(idx):
            row = all_rows[idx]
            async with sem:
                email = await find_email_for(
                    sessions,
                    row.get(name_col, "?") if name_col else "?",
                    row.get(url_col, ""),
                    stats,
                    first_name=row.get(first_name_col, "") if first_name_col else "",
                    last_name=row.get(last_name_col, "") if last_name_col else "",
                )
                return idx, email

        tasks = [bounded(i) for i in missing_idx]
        for coro in asyncio.as_completed(tasks):
            idx, email = await coro
            if email:
                all_rows[idx]["email"] = email
    finally:
        await ssl_session.close()
        await nossl_session.close()

    # Write updated CSV
    log("Writing updated CSV...")
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)

    now_has = sum(1 for r in all_rows if r.get("email", "").strip())
    elapsed = time.time() - t0

    print(f"\n{'=' * 60}", flush=True)
    print("  DONE", flush=True)
    print(f"{'=' * 60}", flush=True)
    print(f"  Newly found   : {stats['found']}", flush=True)
    print(f"  Still missing  : {stats['miss']}", flush=True)
    print(f"  No website     : {stats['skip']}", flush=True)
    print(f"  Errors         : {stats['err']}", flush=True)
    print(f"  Total w/ email : {now_has} / {len(all_rows)}", flush=True)
    print(f"  Time           : {elapsed:.1f}s", flush=True)
    print(f"{'=' * 60}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
