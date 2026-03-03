"""Tool 3: Email verification via email-validator + SMTP probing."""

import argparse
import asyncio
import random
import string
import sys
import time

from shared.csv_io import load_leads, save_csv

try:
    from email_validator import EmailNotValidError, validate_email
except ImportError:
    sys.exit("email-validator is required: pip install email-validator")

try:
    import aiosmtplib
except ImportError:
    sys.exit("aiosmtplib is required: pip install aiosmtplib")

try:
    import dns.asyncresolver
    import dns.exception
    import dns.resolver
except ImportError:
    sys.exit("dnspython is required: pip install dnspython")


class EmailValidator:
    def __init__(self, concurrency: int = 50):
        self.concurrency = concurrency
        self.catchall_cache: dict[str, bool] = {}       # domain -> is_catchall
        self.catchall_locks: dict[str, asyncio.Lock] = {}  # one lock per domain
        self.mx_cache: dict[str, dict] = {}             # domain -> mx info (successes only)
        self.mx_locks: dict[str, asyncio.Lock] = {}     # one lock per domain
        self.domain_sems: dict[str, asyncio.Semaphore] = {}
        self.dns_sem = asyncio.Semaphore(50)            # cap concurrent DNS queries

    def _get_domain_sem(self, domain: str) -> asyncio.Semaphore:
        if domain not in self.domain_sems:
            self.domain_sems[domain] = asyncio.Semaphore(10)
        return self.domain_sems[domain]

    def _get_catchall_lock(self, domain: str) -> asyncio.Lock:
        if domain not in self.catchall_locks:
            self.catchall_locks[domain] = asyncio.Lock()
        return self.catchall_locks[domain]

    def _get_mx_lock(self, domain: str) -> asyncio.Lock:
        if domain not in self.mx_locks:
            self.mx_locks[domain] = asyncio.Lock()
        return self.mx_locks[domain]

    async def validate_syntax_and_mx(self, email: str) -> dict:
        """Validate email syntax (async-safe), then async MX lookup. Cached per domain."""
        domain = email.split("@")[1] if "@" in email else ""

        # Syntax check only (no DNS) — safe to call without a thread
        try:
            validate_email(email, check_deliverability=False)
        except EmailNotValidError as e:
            return {"valid": False, "mx_host": "", "provider": "generic", "error": str(e)}

        if not domain:
            return {"valid": False, "mx_host": "", "provider": "generic", "error": "no domain"}

        # Check MX cache (no lock needed for read)
        if domain in self.mx_cache:
            return self.mx_cache[domain]

        async with self._get_mx_lock(domain):
            if domain in self.mx_cache:
                return self.mx_cache[domain]

            async with self.dns_sem:
                for attempt in range(3):
                    try:
                        answers = await dns.asyncresolver.resolve(domain, "MX")
                        mx_records = sorted((r.preference, str(r.exchange).rstrip(".")) for r in answers)
                        mx_host = mx_records[0][1] if mx_records else ""
                        provider = self._detect_provider_from_mx(mx_records)
                        result = {"valid": True, "mx_host": mx_host, "provider": provider, "error": ""}
                        self.mx_cache[domain] = result  # only cache successes
                        return result
                    except dns.resolver.NXDOMAIN:
                        # Domain doesn't exist — definitive, no retry
                        return {"valid": False, "mx_host": "", "provider": "generic", "error": "no MX (NXDOMAIN)"}
                    except dns.resolver.NoAnswer:
                        # Domain exists but no MX record — definitive
                        return {"valid": False, "mx_host": "", "provider": "generic", "error": "no MX record"}
                    except Exception:
                        if attempt < 2:
                            await asyncio.sleep(0.5 * (attempt + 1))
                        continue

            return {"valid": False, "mx_host": "", "provider": "generic", "error": "no MX (DNS timeout)"}

    @staticmethod
    def _detect_provider_from_mx(mx_records: list) -> str:
        for _priority, host in mx_records:
            h = host.lower()
            if "google" in h or "gmail" in h:
                return "gmail"
            if "outlook" in h or "protection.outlook" in h:
                return "microsoft365"
            if "yahoo" in h:
                return "yahoo"
        return "generic"

    async def smtp_verify(self, email: str, mx_host: str) -> str:
        """Probe SMTP server with RCPT TO. Returns 'valid', 'invalid', or 'error'."""
        domain = email.split("@")[1]
        sem = self._get_domain_sem(domain)

        async with sem:
            for port in (25, 587):
                try:
                    client = aiosmtplib.SMTP(hostname=mx_host, port=port, timeout=5)
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
                    if port == 25:
                        continue  # try 587
                    return "error"
        return "error"

    async def check_catchall(self, domain: str, mx_host: str) -> bool:
        """Probe a random address to detect catch-all domains. Race-safe via per-domain lock."""
        if domain in self.catchall_cache:
            return self.catchall_cache[domain]

        async with self._get_catchall_lock(domain):
            if domain in self.catchall_cache:
                return self.catchall_cache[domain]

            random_local = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
            probe = f"{random_local}@{domain}"
            result = await self.smtp_verify(probe, mx_host)
            is_catchall = result == "valid"
            self.catchall_cache[domain] = is_catchall
            return is_catchall

    async def validate_email(self, email: str) -> dict:
        """Full validation pipeline for a single email."""
        check = await self.validate_syntax_and_mx(email)
        if not check["valid"]:
            status = (
                "no-mx"
                if "dns" in check["error"].lower() or "mx" in check["error"].lower()
                else "invalid"
            )
            return {"validation_status": status, "mx_host": "", "provider": check["provider"]}

        mx_host = check["mx_host"]
        provider = check["provider"]

        if not mx_host:
            return {"validation_status": "no-mx", "mx_host": "", "provider": provider}

        domain = email.split("@")[1]
        # Gmail, M365, Yahoo never accept random addresses — skip catch-all probe
        if provider not in ("gmail", "microsoft365", "yahoo"):
            is_catchall = await self.check_catchall(domain, mx_host)
        else:
            is_catchall = False

        if is_catchall:
            return {"validation_status": "catch-all", "mx_host": mx_host, "provider": provider}

        result = await self.smtp_verify(email, mx_host)
        return {"validation_status": result, "mx_host": mx_host, "provider": provider}

    async def validate_candidates(self, candidates: list[str], domain: str) -> list[dict]:
        """Batch-validate candidate emails for a single domain in parallel."""
        if not candidates:
            return []
        check = await self.validate_syntax_and_mx(candidates[0])
        if not check["valid"] or not check["mx_host"]:
            return [
                {"email": c, "status": "no-mx", "provider": "generic", "is_catchall": False}
                for c in candidates
            ]

        mx_host = check["mx_host"]
        provider = check["provider"]
        is_catchall = await self.check_catchall(domain, mx_host)

        if is_catchall:
            return [
                {"email": c, "status": "catch-all", "provider": provider, "is_catchall": True}
                for c in candidates
            ]

        async def _probe(email: str) -> dict:
            status = await self.smtp_verify(email, mx_host)
            return {"email": email, "status": status, "provider": provider, "is_catchall": False}

        return list(await asyncio.gather(*[_probe(c) for c in candidates]))


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

    status_counts = {}
    for lead in leads:
        s = lead.get("validation_status", "")
        if s:
            status_counts[s] = status_counts.get(s, 0) + 1
    print(f"\n  Validation complete in {elapsed:.1f}s:")
    for status, count in sorted(status_counts.items()):
        print(f"    {status}: {count}")

    fieldnames = list(leads[0].keys()) if leads else []
    save_csv(leads, args.output, fieldnames)
    print(f"\nSaved to {args.output}")


if __name__ == "__main__":
    main()
