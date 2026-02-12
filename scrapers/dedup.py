"""Fuzzy deduplication of leads using rapidfuzz."""

from __future__ import annotations

import aiosqlite
from rapidfuzz import fuzz

from db.models import Lead
from db.queries import delete_lead, get_leads

FUZZY_THRESHOLD = 85


async def deduplicate_leads(db: aiosqlite.Connection) -> int:
    """Remove near-duplicate leads from the database.

    Strategy:
    1. Group by exact email — keep the one with the lowest id.
    2. For leads without email, fuzzy-match on company name using
       token_sort_ratio with threshold 85.

    Returns the number of leads removed.
    """
    removed = 0

    # Load all leads (paginated)
    all_leads: list[Lead] = []
    offset = 0
    while True:
        batch = await get_leads(db, limit=500, offset=offset)
        if not batch:
            break
        all_leads.extend(batch)
        offset += 500

    if not all_leads:
        return 0

    # Phase 1: exact email dedup (DB handles via UNIQUE constraint on upsert,
    # but there may be leads with empty email that share a company).

    # Phase 2: fuzzy company-name dedup
    # Only compare leads that share no email or have empty email
    ids_to_delete: set[int] = set()
    seen: list[Lead] = []

    for lead in all_leads:
        if lead.id in ids_to_delete:
            continue

        is_dup = False
        for prev in seen:
            # Skip if both have emails and they differ (not duplicates)
            if lead.email and prev.email and lead.email != prev.email:
                continue

            # If both have emails and they match, the DB constraint handles it
            if lead.email and prev.email and lead.email == prev.email:
                continue

            # Fuzzy match on company name
            if lead.company and prev.company:
                score = fuzz.token_sort_ratio(lead.company.lower(), prev.company.lower())
                if score >= FUZZY_THRESHOLD:
                    # Keep the one with more data (lower id = earlier, but prefer one with email)
                    if prev.email and not lead.email:
                        ids_to_delete.add(lead.id)
                    elif lead.email and not prev.email:
                        ids_to_delete.add(prev.id)
                        seen.remove(prev)
                        seen.append(lead)
                    else:
                        # Both have or lack email — keep the earlier one
                        ids_to_delete.add(lead.id)
                    is_dup = True
                    break

        if not is_dup:
            seen.append(lead)

    for lid in ids_to_delete:
        await delete_lead(db, lid)
        removed += 1

    return removed
