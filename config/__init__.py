"""Logging and configuration setup."""

from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler

import structlog

_LEVEL_MAP = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


def setup_logging(*, json: bool = False, level: str = "INFO", log_file: str = "") -> None:
    """Configure structlog for the application."""
    shared_processors: list[structlog.typing.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="iso"),
    ]

    log_level = _LEVEL_MAP.get(level.upper(), logging.INFO)

    if log_file:
        # Use stdlib logging as backend so we can attach file + console handlers
        structlog.configure(
            processors=[
                *shared_processors,
                structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
            ],
            wrapper_class=structlog.make_filtering_bound_logger(log_level),
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            cache_logger_on_first_use=True,
        )

        root = logging.getLogger()
        root.setLevel(log_level)

        # Console handler
        console_formatter = structlog.stdlib.ProcessorFormatter(
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                structlog.dev.ConsoleRenderer(),
            ],
        )
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(console_formatter)
        root.addHandler(console_handler)

        # File handler with rotation (10MB, 5 backups)
        if json:
            file_renderer = structlog.processors.JSONRenderer()
        else:
            file_renderer = structlog.dev.ConsoleRenderer(colors=False)
        file_formatter = structlog.stdlib.ProcessorFormatter(
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                file_renderer,
            ],
        )
        file_handler = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5)
        file_handler.setFormatter(file_formatter)
        root.addHandler(file_handler)
    else:
        # Simple structlog-only setup (console only)
        renderer: structlog.typing.Processor = (
            structlog.processors.JSONRenderer() if json else structlog.dev.ConsoleRenderer()
        )

        structlog.configure(
            processors=[*shared_processors, renderer],
            wrapper_class=structlog.make_filtering_bound_logger(log_level),
            context_class=dict,
            logger_factory=structlog.PrintLoggerFactory(),
            cache_logger_on_first_use=True,
        )
