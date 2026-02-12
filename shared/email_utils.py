import re

from lxml.html import fromstring as parse_html

from .constants import CFEMAIL_RE, EMAIL_RE, JS_EMAIL_RE, JUNK_DOMAINS, JUNK_PREFIXES, OBFUSC_RE


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
    if re.search(r"\.(png|jpg|jpeg|gif|svg|webp|css|js|woff2?|ttf|eot|ico)$", e):
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
    if len(e) > 80:
        return True
    return False


def decode_cfemail(encoded: str) -> str:
    """Decode Cloudflare email protection."""
    try:
        key = int(encoded[:2], 16)
        return "".join(chr(int(encoded[i:i+2], 16) ^ key) for i in range(2, len(encoded), 2))
    except Exception:
        return ""


def _extract_from_scripts(html: str) -> set[str]:
    """Extract emails hidden in <script> tags via string concatenation."""
    emails = set()
    try:
        doc = parse_html(html)
        for script in doc.xpath("//script"):
            text = script.text_content()
            if not text:
                continue
            emails.update(EMAIL_RE.findall(text))
            for user, dom in JS_EMAIL_RE.findall(text):
                emails.add(f"{user}@{dom}")
    except Exception:
        pass
    return emails


def extract_emails(html: str, domain: str = "") -> set[str]:
    """Extract emails from HTML using multiple strategies.

    Strategies: mailto links, regex, Cloudflare decoding, [at]/[dot]
    obfuscation, and JS string concatenation in script tags.

    When *domain* is provided, prefers emails matching that domain.
    """
    # Decode Cloudflare-protected emails inline
    for encoded in CFEMAIL_RE.findall(html):
        decoded = decode_cfemail(encoded)
        if "@" in decoded:
            html = html.replace(f"/cdn-cgi/l/email-protection#{encoded}", f"mailto:{decoded}", 1)
            html += f" {decoded} "

    raw: set[str] = set()

    # mailto: links
    try:
        doc = parse_html(html)
        for href in doc.xpath("//a[starts-with(@href,'mailto:')]/@href"):
            addr = href.split("mailto:")[-1].split("?")[0].split("#")[0].strip()
            if EMAIL_RE.fullmatch(addr):
                raw.add(addr)
    except Exception:
        pass

    # Plain regex
    raw.update(EMAIL_RE.findall(html))

    # Obfuscated: user [at] domain [dot] tld
    for user, mid, tld in OBFUSC_RE.findall(html):
        raw.add(f"{user}@{mid}.{tld}")

    # JS script tags
    raw.update(_extract_from_scripts(html))

    # Filter junk
    good = {e.lower().strip().rstrip(".") for e in raw if not is_junk(e.lower().strip().rstrip("."))}

    # Prefer same-domain emails when available
    if domain:
        dc = domain.lower().replace("www.", "")
        matched = {e for e in good if dc in e.split("@")[-1]}
        if matched:
            return matched
    return good
