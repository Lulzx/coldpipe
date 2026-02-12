"""Tool 4: Generate personalized cold email opening lines."""
import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shared.csv_io import load_leads, save_csv


def _city_from_location(location: str) -> str:
    """Extract city name from location string like 'Phoenix, AZ' or 'Los Angeles, CA'."""
    if not location:
        return ""
    return location.split(",")[0].strip()


def generate_first_line(lead: dict) -> str:
    """Generate a personalized first-line based on available lead data."""
    first_name = lead.get("first_name", "").strip()
    company = lead.get("company", "").strip()
    job_title = lead.get("job_title", "").strip()
    location = lead.get("location", "").strip()
    city = _city_from_location(location)

    # Strategy 1: company + city
    if company and city:
        return f"I noticed {company} serves patients in {city} — "

    # Strategy 2: job title + company
    if job_title and company:
        return f"As a {job_title} at {company}, "

    # Strategy 3: company only
    if company:
        return f"I came across {company} and "

    # Strategy 4: name + job title
    if first_name and job_title:
        return f"Hi {first_name}, as a {job_title}, "

    # Strategy 5: name only
    if first_name:
        return f"Hi {first_name}, "

    return ""


def main():
    parser = argparse.ArgumentParser(description="Generate personalized first-lines for outreach")
    parser.add_argument("--input", default="output/validated_leads.csv", help="Input CSV")
    parser.add_argument("--output", default="output/outreach_ready.csv", help="Output CSV")
    args = parser.parse_args()

    print(f"Loading {args.input}...")
    leads = load_leads(args.input)
    print(f"  {len(leads)} leads loaded")

    # Filter to valid/catch-all emails only
    outreach = []
    for lead in leads:
        status = lead.get("validation_status", "")
        has_email = lead.get("email") or lead.get("enriched_email")
        # Include if: has email and either no validation status (pre-validation) or valid/catch-all
        if has_email and (not status or status in ("valid", "catch-all")):
            outreach.append(lead)

    print(f"  {len(outreach)} leads eligible for outreach")

    generated = 0
    for lead in outreach:
        line = generate_first_line(lead)
        lead["first_line"] = line
        if line:
            generated += 1

    print(f"  Generated {generated} personalized first-lines")

    fieldnames = list(outreach[0].keys()) if outreach else []
    save_csv(outreach, args.output, fieldnames)
    print(f"\nSaved to {args.output}")


if __name__ == "__main__":
    main()
