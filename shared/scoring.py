"""Confidence scoring system for email candidates."""

from __future__ import annotations

from dataclasses import dataclass

from .patterns import is_generic_prefix

# Base scores by discovery source
_SOURCE_BASE: dict[str, float] = {
    "mailto": 6.0,
    "regex": 5.0,
    "obfuscated": 4.5,
    "cfemail": 4.5,
    "script": 4.0,
    "pattern": 2.0,
}


@dataclass
class EmailCandidate:
    email: str
    source: str  # "mailto", "regex", "obfuscated", "cfemail", "script", "pattern"
    smtp_status: str = ""  # "valid", "invalid", "catch-all", "error", ""
    is_catchall: bool = False
    provider: str = "generic"
    matches_domain: bool = False
    is_generic: bool = False


def score_email(candidate: EmailCandidate) -> float:
    """Score an email candidate from 0 to 10.

    Base score comes from discovery source, then adjusted by:
      +3   SMTP valid on non-catch-all domain
      +1.5 SMTP valid on catch-all domain
      +1   email domain matches target domain
      -2   generic/role prefix (info@, contact@, etc.)
      -1   SMTP invalid
    """
    base = _SOURCE_BASE.get(candidate.source, 2.0)

    bonus = 0.0
    if candidate.smtp_status == "valid":
        bonus += 1.5 if candidate.is_catchall else 3.0
    elif candidate.smtp_status == "invalid":
        bonus -= 1.0

    if candidate.matches_domain:
        bonus += 1.0

    if candidate.is_generic or is_generic_prefix(candidate.email):
        bonus -= 2.0

    return max(0.0, min(10.0, base + bonus))


def rank_candidates(candidates: list[EmailCandidate]) -> list[tuple[EmailCandidate, float]]:
    """Score and sort candidates by confidence (highest first)."""
    scored = [(c, score_email(c)) for c in candidates]
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored


def pick_best(
    candidates: list[EmailCandidate],
    threshold: float = 3.0,
) -> tuple[EmailCandidate, float] | None:
    """Return the highest-scored candidate above *threshold*, or None."""
    ranked = rank_candidates(candidates)
    if ranked and ranked[0][1] >= threshold:
        return ranked[0]
    return None
