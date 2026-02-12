"""Tool 1: Merge & deduplicate all CSVs into a master lead list."""
import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shared.csv_io import load_all_leads, save_csv, MASTER_FIELDS


def deduplicate(leads: list[dict]) -> list[dict]:
    """Deduplicate leads by email, then by (first_name + company) for no-email rows."""
    seen_emails = {}  # email -> index in result
    seen_name_company = {}  # (first_name_lower, company_lower) -> index
    result = []

    for lead in leads:
        email = lead["email"]

        if email:
            if email in seen_emails:
                # Merge missing fields from dupe into existing
                idx = seen_emails[email]
                for key in MASTER_FIELDS:
                    if not result[idx].get(key) and lead.get(key):
                        result[idx][key] = lead[key]
                continue
            seen_emails[email] = len(result)
            result.append(lead)
        else:
            # No email — deduplicate by (first_name, company)
            first = (lead.get("first_name") or "").lower().strip()
            company = (lead.get("company") or "").lower().strip()
            if first and company:
                key = (first, company)
                if key in seen_name_company:
                    idx = seen_name_company[key]
                    for k in MASTER_FIELDS:
                        if not result[idx].get(k) and lead.get(k):
                            result[idx][k] = lead[k]
                    continue
                seen_name_company[key] = len(result)
            result.append(lead)

    return result


def main():
    parser = argparse.ArgumentParser(description="Merge & deduplicate all lead CSVs")
    parser.add_argument("--data-dir", default="data/", help="Directory containing input CSVs")
    parser.add_argument("--output", default="output/master_leads.csv", help="Output file path")
    args = parser.parse_args()

    print(f"Loading CSVs from {args.data_dir}...")
    all_leads = load_all_leads(args.data_dir)
    print(f"  Total rows loaded: {len(all_leads)}")

    # Per-file breakdown
    file_counts = {}
    for lead in all_leads:
        src = lead.get("source_file", "unknown")
        file_counts[src] = file_counts.get(src, 0) + 1
    print("\n  Per-file breakdown:")
    for fname, count in sorted(file_counts.items()):
        print(f"    {fname}: {count} rows")

    deduped = deduplicate(all_leads)
    removed = len(all_leads) - len(deduped)
    print(f"\n  Duplicates removed: {removed}")
    print(f"  Final count: {len(deduped)}")

    with_email = sum(1 for r in deduped if r.get("email"))
    without_email = len(deduped) - with_email
    print(f"  With email: {with_email}")
    print(f"  Without email: {without_email}")

    save_csv(deduped, args.output, MASTER_FIELDS)
    print(f"\nSaved to {args.output}")


if __name__ == "__main__":
    main()
