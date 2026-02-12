import csv
import os
from collections.abc import Sequence

# Column detection maps — order matters, first match wins
_EMAIL_COLS = [
    "email",
    "Email (Result)",
    "Work Email (Result)",
    "Email Address (Result)",
    "emails",
]

_FIRST_NAME_COLS = ["First Name", "first_name"]
_LAST_NAME_COLS = ["Last Name", "last_name"]
_COMPANY_COLS = [
    "Company Name (Result)",
    "company",
    "Company",
    "Organization Name (Result)",
]
_WEBSITE_COLS = ["URL", "Website (Result)", "url", "website"]
_JOB_TITLE_COLS = ["Job Title", "job_title"]
_LOCATION_COLS = ["Location (Result)", "location"]


def _find_col(headers: Sequence[str], candidates: list[str]) -> str | None:
    """Return the first header that matches a candidate (case-insensitive)."""
    lower_map = {h.lower().strip(): h for h in headers}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    return None


def _detect_name_and_company(headers: Sequence[str]):
    """Handle the 'Title' column ambiguity.

    If 'Job Title' exists, 'Title' is the person's name.
    If 'Job Title' does not exist, 'Title' is the company/org name.
    """
    has_job_title = _find_col(headers, _JOB_TITLE_COLS) is not None
    title_col = _find_col(headers, ["Title"])

    name_col = None
    company_col = _find_col(headers, _COMPANY_COLS)

    if title_col and has_job_title:
        # Title = person name
        name_col = title_col
    elif title_col and not has_job_title and not company_col:
        # Title = company/org name
        company_col = title_col

    # Override with explicit first_name if present
    explicit_first = _find_col(headers, _FIRST_NAME_COLS)
    if explicit_first:
        name_col = explicit_first

    return name_col, company_col


def _split_name(full_name: str) -> tuple[str, str]:
    """Split 'First Last' into (first, last). Handles single-word names."""
    parts = full_name.strip().split(None, 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    elif len(parts) == 1:
        return parts[0], ""
    return "", ""


def load_leads(filepath: str) -> list[dict]:
    """Load a CSV and return normalized lead dicts."""
    rows = []
    source = os.path.basename(filepath)

    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []

        email_col = _find_col(headers, _EMAIL_COLS)
        name_col, company_col = _detect_name_and_company(headers)
        first_name_col = _find_col(headers, _FIRST_NAME_COLS)
        last_name_col = _find_col(headers, _LAST_NAME_COLS)
        website_col = _find_col(headers, _WEBSITE_COLS)
        job_title_col = _find_col(headers, _JOB_TITLE_COLS)
        location_col = _find_col(headers, _LOCATION_COLS)

        for row in reader:
            email = (row.get(email_col, "") if email_col else "").strip().lower()
            website = (row.get(website_col, "") if website_col else "").strip()
            company = (row.get(company_col, "") if company_col else "").strip()
            job_title = (row.get(job_title_col, "") if job_title_col else "").strip()
            location = (row.get(location_col, "") if location_col else "").strip()

            # Name handling
            if first_name_col:
                first_name = (row.get(first_name_col, "") or "").strip()
                last_name = (row.get(last_name_col, "") if last_name_col else "").strip()
            elif name_col:
                first_name, last_name = _split_name(row.get(name_col, "") or "")
            else:
                first_name, last_name = "", ""

            rows.append(
                {
                    "email": email,
                    "first_name": first_name,
                    "last_name": last_name,
                    "company": company,
                    "website": website,
                    "job_title": job_title,
                    "location": location,
                    "source_file": source,
                }
            )

    return rows


def load_all_leads(data_dir: str) -> list[dict]:
    """Load all CSVs in data_dir, skipping scraper output files."""
    all_rows = []
    for fname in sorted(os.listdir(data_dir)):
        if not fname.endswith(".csv"):
            continue
        if fname.endswith("_emails.csv"):
            continue
        path = os.path.join(data_dir, fname)
        rows = load_leads(path)
        all_rows.extend(rows)
    return all_rows


MASTER_FIELDS = [
    "email",
    "first_name",
    "last_name",
    "company",
    "website",
    "job_title",
    "location",
    "source_file",
]


def save_csv(rows: list[dict], filepath: str, fieldnames: list[str] | None = None):
    """Write a list of dicts to CSV."""
    if not rows:
        return
    if fieldnames is None:
        fieldnames = list(rows[0].keys())
    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
