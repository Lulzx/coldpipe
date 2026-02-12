import re

from lxml.html import fromstring as parse_html

from .constants import EMAIL_RE, JUNK_DOMAINS, JUNK_PREFIXES


def normalize_email(email: str) -> str:
    return email.lower().strip()


def is_junk(e: str) -> bool:
    e = e.lower()
    local, _, domain = e.partition("@")
    if not domain:
        return True
    if domain in JUNK_DOMAINS or any(domain.endswith("." + j) for j in JUNK_DOMAINS):
        return True
    if e.startswith(JUNK_PREFIXES):
        return True
    if re.search(r"\.(png|jpg|jpeg|gif|svg|webp|css|js|woff2?|ttf|eot)$", e):
        return True
    if len(local) < 2:
        return True
    if len(local) > 20 and all(c in "0123456789abcdef" for c in local):
        return True
    tld = domain.rsplit(".", 1)[-1] if "." in domain else ""
    if len(tld) < 2 or len(domain) < 5:
        return True
    if domain.count(".") >= 3:
        return True
    return False


def extract_emails(html: str) -> set[str]:
    emails = set()
    try:
        doc = parse_html(html)
        for href in doc.xpath("//a[starts-with(@href,'mailto:')]/@href"):
            raw = href[7:].split("?")[0].strip()
            if EMAIL_RE.fullmatch(raw):
                emails.add(raw.lower())
    except Exception:
        pass
    for m in EMAIL_RE.findall(html):
        emails.add(m.lower())
    return {e for e in emails if not is_junk(e)}
