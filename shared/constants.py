import re

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

JUNK_DOMAINS = {
    "sentry.io", "wixpress.com", "example.com", "domain.com", "yoursite.com",
    "email.com", "yourdomain.com", "test.com", "sentry-next.wixpress.com",
    "change.me", "exa.ai", "myftpupload.com", "googleapis.com", "w3.org",
    "schema.org", "gravatar.com", "wordpress.org", "wordpress.com",
}

JUNK_PREFIXES = ("noreply@", "no-reply@", "webmaster@", "root@", "admin@wordpress")

SKIP_DOMAINS = {"exa.ai"}

HDRS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
}

TIMEOUT = 5
MAX_CONN = 500

PATHS = ["", "/contact", "/contact-us", "/about", "/about-us", "/team", "/our-team"]
