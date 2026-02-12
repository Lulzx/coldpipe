"""Email engine for the dentists outreach system."""

from .bounces import parse_dsn, process_bounce
from .personalize import batch_personalize, personalize_opener
from .queue import SendQueue
from .replies import ReplyWatcher
from .sender import EmailSender
from .sequences import advance_sequence, handle_reply
from .templates import render_template

__all__ = [
    "EmailSender",
    "SendQueue",
    "ReplyWatcher",
    "advance_sequence",
    "batch_personalize",
    "handle_reply",
    "parse_dsn",
    "personalize_opener",
    "process_bounce",
    "render_template",
]
