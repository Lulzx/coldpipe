import re

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
OBFUSC_RE = re.compile(
    r"([a-zA-Z0-9._%+\-]+)"
    r"\s*[\[\({\<]?\s*(?:at|AT)\s*[\]\)}\>]?\s*"
    r"([a-zA-Z0-9.\-]+)"
    r"\s*[\[\({\<]?\s*(?:dot|DOT)\s*[\]\)}\>]?\s*"
    r"([a-zA-Z]{2,})"
)
CFEMAIL_RE = re.compile(r'data-cfemail="([0-9a-fA-F]+)"')
JS_EMAIL_RE = re.compile(
    r'["\']([a-zA-Z0-9._%+\-]+)["\']\s*\+\s*["\']@["\']\s*\+\s*'
    r'["\']([a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})["\']'
)

JUNK_DOMAINS = {
    "sentry.io", "wixpress.com", "example.com", "domain.com", "yoursite.com",
    "email.com", "yourdomain.com", "test.com", "sentry-next.wixpress.com",
    "change.me", "exa.ai", "myftpupload.com", "googleapis.com", "w3.org",
    "schema.org", "gravatar.com", "wordpress.org", "wordpress.com",
    "jquery.com", "google.com", "facebook.com", "twitter.com", "instagram.com",
    "youtube.com", "linkedin.com", "cloudflare.com", "gstatic.com",
    "bootstrapcdn.com", "fontawesome.com", "cloudfront.net", "amazonaws.com",
    "googletagmanager.com", "doubleclick.net", "googlesyndication.com",
    "google-analytics.com", "googleadservices.com", "hotjar.com",
    "hubspot.com", "mailchimp.com", "unpkg.com",
}

JUNK_PREFIXES = ("noreply@", "no-reply@", "donotreply@", "mailer-daemon@",
                 "webmaster@", "root@", "admin@wordpress")

SKIP_DOMAINS = {"exa.ai"}

HDRS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
}

TIMEOUT = 8
MAX_CONN = 500

PATHS = [
    "", "/contact", "/contact-us", "/contact.html", "/contactus",
    "/about", "/about-us", "/about.html", "/aboutus",
    "/team", "/our-team", "/leadership", "/people",
    "/company", "/support", "/connect", "/info",
    "/privacy", "/privacy-policy", "/locations", "/offices",
]

CONTACT_KW = re.compile(
    r"contact|about|team|staff|people|connect|reach|get.?in.?touch|"
    r"leadership|email|support|help|info|who.?we.?are|company|meet|directory",
    re.IGNORECASE,
)
