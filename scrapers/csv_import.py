"""CSV importer — migration bridge from legacy CSV files to the database."""

from __future__ import annotations

import re

import aiosqlite

from db.models import Lead
from db.queries import upsert_lead
from shared.csv_io import load_all_leads

# Common US state abbreviations for location parsing
_STATE_ABBREVS = {
    "alabama": "AL",
    "alaska": "AK",
    "arizona": "AZ",
    "arkansas": "AR",
    "california": "CA",
    "colorado": "CO",
    "connecticut": "CT",
    "delaware": "DE",
    "florida": "FL",
    "georgia": "GA",
    "hawaii": "HI",
    "idaho": "ID",
    "illinois": "IL",
    "indiana": "IN",
    "iowa": "IA",
    "kansas": "KS",
    "kentucky": "KY",
    "louisiana": "LA",
    "maine": "ME",
    "maryland": "MD",
    "massachusetts": "MA",
    "michigan": "MI",
    "minnesota": "MN",
    "mississippi": "MS",
    "missouri": "MO",
    "montana": "MT",
    "nebraska": "NE",
    "nevada": "NV",
    "new hampshire": "NH",
    "new jersey": "NJ",
    "new mexico": "NM",
    "new york": "NY",
    "north carolina": "NC",
    "north dakota": "ND",
    "ohio": "OH",
    "oklahoma": "OK",
    "oregon": "OR",
    "pennsylvania": "PA",
    "rhode island": "RI",
    "south carolina": "SC",
    "south dakota": "SD",
    "tennessee": "TN",
    "texas": "TX",
    "utah": "UT",
    "vermont": "VT",
    "virginia": "VA",
    "washington": "WA",
    "west virginia": "WV",
    "wisconsin": "WI",
    "wyoming": "WY",
}
_STATE_ABBREV_SET = set(_STATE_ABBREVS.values())


def _parse_location(location: str) -> tuple[str, str]:
    """Split a location string into (city, state).

    Handles formats like:
    - "Austin, TX"
    - "New York, New York"
    - "San Francisco, CA 94102"
    - "TX"
    - "California"
    """
    if not location:
        return "", ""

    location = location.strip()

    # Try "City, State" or "City, State ZIP"
    parts = [p.strip() for p in location.split(",", 1)]
    if len(parts) == 2:
        city = parts[0]
        rest = parts[1].strip()
        # Strip trailing zip code
        rest_no_zip = re.sub(r"\s*\d{5}(-\d{4})?\s*$", "", rest).strip()
        # Check if it's a state abbreviation
        if rest_no_zip.upper() in _STATE_ABBREV_SET:
            return city, rest_no_zip.upper()
        # Check if it's a full state name
        if rest_no_zip.lower() in _STATE_ABBREVS:
            return city, _STATE_ABBREVS[rest_no_zip.lower()]
        # Unknown state part — return city and raw state
        return city, rest_no_zip

    # Single token — check if it's a state
    single = location.strip()
    if single.upper() in _STATE_ABBREV_SET:
        return "", single.upper()
    if single.lower() in _STATE_ABBREVS:
        return "", _STATE_ABBREVS[single.lower()]

    # Can't parse — treat the whole thing as city
    return single, ""


def _dict_to_lead(row: dict) -> Lead:
    """Convert a csv_io row dict to a Lead model."""
    city, state = _parse_location(row.get("location", ""))

    return Lead(
        email=row.get("email", ""),
        first_name=row.get("first_name", ""),
        last_name=row.get("last_name", ""),
        company=row.get("company", ""),
        job_title=row.get("job_title", ""),
        website=row.get("website", ""),
        city=city,
        state=state,
        source=row.get("source_file", ""),
    )


class CsvImporter:
    """Import leads from legacy CSV files into the database."""

    async def scrape(
        self,
        db: aiosqlite.Connection,
        *,
        data_dir: str = "data",
    ) -> list[Lead]:
        """Load all CSVs from data_dir, convert to Lead models, upsert into DB.

        Returns the list of Lead objects that were imported.
        """
        raw_rows = load_all_leads(data_dir)
        leads: list[Lead] = []
        skipped = 0

        for row in raw_rows:
            lead = _dict_to_lead(row)

            # Skip rows with no useful identifying info
            if not lead.email and not lead.company and not lead.website:
                skipped += 1
                continue

            await upsert_lead(db, lead)
            leads.append(lead)

        return leads
