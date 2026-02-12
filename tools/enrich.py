"""Tool 2: Find missing emails by scraping company websites."""
import argparse
import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shared.csv_io import load_leads, save_csv, MASTER_FIELDS
from shared.http import create_sessions
from shared.scraping import scrape_site_for_emails

ENRICH_FIELDS = MASTER_FIELDS + ["enriched_email", "email_source"]


async def enrich_leads(leads: list[dict], concurrency: int = 500) -> list[dict]:
    """Scrape websites for leads missing emails."""
    to_scrape = []
    for i, lead in enumerate(leads):
        if not lead.get("email") and lead.get("website"):
            url = lead["website"]
            if not url.startswith("http"):
                url = "https://" + url
            to_scrape.append((i, url))

    print(f"  {len(to_scrape)} leads need enrichment (have website, no email)")

    if not to_scrape:
        return leads

    ssl_session, nossl_session = create_sessions()
    sessions = [ssl_session, nossl_session]
    enriched_count = 0
    sem = asyncio.Semaphore(concurrency)

    async def _scrape(idx: int, url: str):
        nonlocal enriched_count
        async with sem:
            emails = await scrape_site_for_emails(sessions, url)
            if emails:
                best = sorted(emails)[0]  # Pick first alphabetically as deterministic choice
                leads[idx]["enriched_email"] = best
                leads[idx]["email_source"] = "website_scrape"
                enriched_count += 1
                print(f"    [{enriched_count}] {url} -> {best}")

    start = time.time()
    await asyncio.gather(*[_scrape(i, url) for i, url in to_scrape])
    elapsed = time.time() - start

    await ssl_session.close()
    await nossl_session.close()

    print(f"\n  Enriched {enriched_count}/{len(to_scrape)} leads in {elapsed:.1f}s")
    return leads


def main():
    parser = argparse.ArgumentParser(description="Enrich leads with emails from websites")
    parser.add_argument("--input", default="output/master_leads.csv", help="Input CSV")
    parser.add_argument("--output", default="output/enriched_leads.csv", help="Output CSV")
    parser.add_argument("--concurrency", type=int, default=500, help="Max concurrent scrapes")
    args = parser.parse_args()

    print(f"Loading {args.input}...")
    leads = load_leads(args.input)
    print(f"  {len(leads)} leads loaded")

    # Ensure enrichment columns exist
    for lead in leads:
        lead.setdefault("enriched_email", "")
        lead.setdefault("email_source", "")

    leads = asyncio.run(enrich_leads(leads, args.concurrency))

    save_csv(leads, args.output, ENRICH_FIELDS)
    print(f"Saved to {args.output}")


if __name__ == "__main__":
    main()
