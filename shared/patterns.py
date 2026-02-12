"""Pattern-based email generation and generic prefix detection."""

from __future__ import annotations

import re

GENERIC_PREFIXES = frozenset(
    {
        "info",
        "contact",
        "support",
        "sales",
        "admin",
        "hello",
        "office",
        "reception",
        "team",
        "help",
        "billing",
        "service",
        "inquiries",
        "general",
        "mail",
        "enquiries",
        "marketing",
        "hr",
        "careers",
        "press",
    }
)

_ALPHA_RE = re.compile(r"[^a-z]")


def _clean(name: str) -> str:
    """Lowercase and strip non-alpha characters."""
    return _ALPHA_RE.sub("", name.lower())


def generate_candidates(first_name: str, last_name: str, domain: str) -> list[str]:
    """Generate ~20 common email patterns from a person's name and domain.

    Returns an empty list if any required input is missing.
    """
    f = _clean(first_name)
    l = _clean(last_name)  # noqa: E741
    if not f or not l or not domain:
        return []

    fi = f[0]  # first initial
    li = l[0]  # last initial

    patterns = [
        f"{f}",  # john@
        f"{l}",  # doe@
        f"{f}{l}",  # johndoe@
        f"{f}.{l}",  # john.doe@
        f"{f}_{l}",  # john_doe@
        f"{f}-{l}",  # john-doe@
        f"{fi}{l}",  # jdoe@
        f"{fi}.{l}",  # j.doe@
        f"{fi}_{l}",  # j_doe@
        f"{fi}-{l}",  # j-doe@
        f"{f}{li}",  # johnd@
        f"{f}.{li}",  # john.d@
        f"{l}{f}",  # doejohn@
        f"{l}.{f}",  # doe.john@
        f"{l}_{f}",  # doe_john@
        f"{l}-{f}",  # doe-john@
        f"{l}{fi}",  # doej@
        f"{l}.{fi}",  # doe.j@
        f"{li}{f}",  # djohn@
        f"{li}.{f}",  # d.john@
        f"{fi}{li}",  # jd@
    ]

    seen: set[str] = set()
    candidates: list[str] = []
    for p in patterns:
        email = f"{p}@{domain}"
        if email not in seen:
            seen.add(email)
            candidates.append(email)
    return candidates


def is_generic_prefix(email: str) -> bool:
    """Return True if the local part (before @) is a generic/role prefix."""
    local = email.split("@")[0].lower()
    return local in GENERIC_PREFIXES
