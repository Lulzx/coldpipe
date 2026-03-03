"""Rule-based reply triage — classify replies without an LLM."""

from __future__ import annotations


def triage_reply_text(body: str) -> dict:
    """Classify a reply using keyword matching. No LLM needed.

    Returns: {classification, action, confidence, notes}

    Classifications: interested | not_interested | unsubscribe | out_of_office | other
    Actions: move_to_deals | follow_up_later | mark_unsubscribed | reply_needed | no_action
    """
    body_lower = body.lower()

    if any(
        w in body_lower
        for w in ["unsubscribe", "remove me", "stop emailing", "opt out", "do not contact"]
    ):
        return {
            "classification": "unsubscribe",
            "action": "mark_unsubscribed",
            "confidence": 0.9,
            "notes": "keyword match",
        }

    if any(
        w in body_lower
        for w in ["out of office", "ooo", "on vacation", "away until", "auto-reply", "autoreply"]
    ):
        return {
            "classification": "out_of_office",
            "action": "follow_up_later",
            "confidence": 0.9,
            "notes": "keyword match",
        }

    if any(
        w in body_lower
        for w in [
            "interested",
            "tell me more",
            "schedule",
            "call",
            "meeting",
            "yes",
            "love to chat",
            "let's talk",
        ]
    ):
        return {
            "classification": "interested",
            "action": "move_to_deals",
            "confidence": 0.75,
            "notes": "keyword match",
        }

    if any(
        w in body_lower
        for w in ["not interested", "no thanks", "not looking", "no need", "pass on this"]
    ):
        return {
            "classification": "not_interested",
            "action": "no_action",
            "confidence": 0.8,
            "notes": "keyword match",
        }

    return {
        "classification": "other",
        "action": "reply_needed",
        "confidence": 0.3,
        "notes": "no pattern matched",
    }
