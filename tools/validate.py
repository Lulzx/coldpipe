"""Tool 3: MX + SMTP email verification."""

import argparse
import asyncio
import os
import random
import string
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shared.csv_io import load_leads, save_csv

try:
    import aiodns
except ImportError:
    sys.exit("aiodns is required: pip install aiodns")

try:
    import aiosmtplib
except ImportError:
    sys.exit("aiosmtplib is required: pip install aiosmtplib")


class EmailValidator:
    def __init__(self, concurrency: int = 50):
        self.concurrency = concurrency
        self.mx_cache: dict[str, list[str]] = {}  # domain -> [mx_hosts]
        self.catchall_cache: dict[str, bool] = {}  # domain -> is_catchall
        self.domain_sems: dict[str, asyncio.Semaphore] = {}
        self.resolver = None

    def _get_domain_sem(self, domain: str) -> asyncio.Semaphore:
        if domain not in self.domain_sems:
            self.domain_sems[domain] = asyncio.Semaphore(3)
        return self.domain_sems[domain]

    async def mx_lookup(self, domain: str) -> list[str]:
        """Resolve MX records for a domain. Returns sorted list of MX hosts."""
        if domain in self.mx_cache:
            return self.mx_cache[domain]

        if self.resolver is None:
            self.resolver = aiodns.DNSResolver()

        try:
            records = await self.resolver.query(domain, "MX")
            hosts = sorted(records, key=lambda r: r.priority)
            result = [r.host for r in hosts]
        except Exception:
            result = []

        self.mx_cache[domain] = result
        return result

    async def smtp_verify(self, email: str, mx_host: str) -> str:
        """Probe SMTP server with RCPT TO. Returns 'valid', 'invalid', or 'error'."""
        domain = email.split("@")[1]
        sem = self._get_domain_sem(domain)

        async with sem:
            try:
                client = aiosmtplib.SMTP(hostname=mx_host, port=25, timeout=10)
                await client.connect()
                await client.ehlo()
                await client.mail("")
                code, _ = await client.rcpt(email)
                await client.quit()

                if 200 <= code < 300:
                    return "valid"
                elif 500 <= code < 600:
                    return "invalid"
                else:
                    return "error"
            except aiosmtplib.SMTPRecipientRefused:
                return "invalid"
            except Exception:
                return "error"
            finally:
                await asyncio.sleep(1)  # Rate limit: 1s delay between probes to same MX

    async def check_catchall(self, domain: str, mx_host: str) -> bool:
        """Probe a random address to detect catch-all domains."""
        if domain in self.catchall_cache:
            return self.catchall_cache[domain]

        random_local = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
        probe = f"{random_local}@{domain}"
        result = await self.smtp_verify(probe, mx_host)
        is_catchall = result == "valid"
        self.catchall_cache[domain] = is_catchall
        return is_catchall

    def detect_provider(self, mx_hosts: list[str]) -> str:
        """Detect email provider from MX hostnames."""
        for host in mx_hosts:
            h = host.lower()
            if "google" in h or "gmail" in h:
                return "gmail"
            if "outlook" in h or "protection.outlook" in h:
                return "microsoft365"
            if "yahoo" in h:
                return "yahoo"
        return "generic"

    async def validate_email(self, email: str) -> dict:
        """Full validation pipeline for a single email."""
        domain = email.split("@")[1] if "@" in email else ""
        if not domain:
            return {"validation_status": "error", "mx_host": "", "provider": "generic"}

        mx_hosts = await self.mx_lookup(domain)
        if not mx_hosts:
            return {"validation_status": "no-mx", "mx_host": "", "provider": "generic"}

        mx_host = mx_hosts[0]
        provider = self.detect_provider(mx_hosts)

        # Check catch-all first (cached per domain)
        is_catchall = await self.check_catchall(domain, mx_host)

        if is_catchall:
            return {"validation_status": "catch-all", "mx_host": mx_host, "provider": provider}

        # SMTP verify the actual email
        result = await self.smtp_verify(email, mx_host)
        return {"validation_status": result, "mx_host": mx_host, "provider": provider}

    async def validate_candidates(self, candidates: list[str], domain: str) -> list[dict]:
        """Batch-validate a list of candidate emails for a single domain.

        Returns a list of dicts with keys: email, status, provider, is_catchall.
        """
        mx_hosts = await self.mx_lookup(domain)
        if not mx_hosts:
            return [
                {"email": c, "status": "no-mx", "provider": "generic", "is_catchall": False}
                for c in candidates
            ]

        mx_host = mx_hosts[0]
        provider = self.detect_provider(mx_hosts)
        is_catchall = await self.check_catchall(domain, mx_host)

        if is_catchall:
            return [
                {"email": c, "status": "catch-all", "provider": provider, "is_catchall": True}
                for c in candidates
            ]

        results = []
        for email in candidates:
            status = await self.smtp_verify(email, mx_host)
            results.append(
                {
                    "email": email,
                    "status": status,
                    "provider": provider,
                    "is_catchall": False,
                }
            )
        return results


async def validate_all(leads: list[dict], concurrency: int = 50) -> list[dict]:
    """Validate all emails in lead list."""
    validator = EmailValidator(concurrency=concurrency)
    sem = asyncio.Semaphore(concurrency)
    validated = 0

    async def _validate(lead: dict):
        nonlocal validated
        email = lead.get("email") or lead.get("enriched_email") or ""
        if not email or "@" not in email:
            lead["validation_status"] = ""
            lead["mx_host"] = ""
            return

        async with sem:
            result = await validator.validate_email(email)
            lead.update(result)
            validated += 1
            if validated % 50 == 0:
                print(f"    Validated {validated} emails...")

    await asyncio.gather(*[_validate(lead) for lead in leads])
    return leads


def main():
    parser = argparse.ArgumentParser(description="Verify email deliverability via MX + SMTP")
    parser.add_argument("--input", default="output/enriched_leads.csv", help="Input CSV")
    parser.add_argument("--output", default="output/validated_leads.csv", help="Output CSV")
    parser.add_argument("--concurrency", type=int, default=50, help="Max concurrent validations")
    args = parser.parse_args()

    print(f"Loading {args.input}...")
    leads = load_leads(args.input)
    print(f"  {len(leads)} leads loaded")

    emails_to_check = sum(1 for row in leads if row.get("email") or row.get("enriched_email"))
    print(f"  {emails_to_check} emails to validate")

    start = time.time()
    leads = asyncio.run(validate_all(leads, args.concurrency))
    elapsed = time.time() - start

    # Stats
    status_counts = {}
    for lead in leads:
        s = lead.get("validation_status", "")
        if s:
            status_counts[s] = status_counts.get(s, 0) + 1
    print(f"\n  Validation complete in {elapsed:.1f}s:")
    for status, count in sorted(status_counts.items()):
        print(f"    {status}: {count}")

    # Determine output fields
    fieldnames = list(leads[0].keys()) if leads else []
    save_csv(leads, args.output, fieldnames)
    print(f"\nSaved to {args.output}")


if __name__ == "__main__":
    main()
