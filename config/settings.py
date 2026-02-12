"""Application settings via msgspec + environment/TOML."""

from __future__ import annotations

import os
from pathlib import Path

import msgspec

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "dentists.db"
TEMPLATES_DIR = DATA_DIR / "templates"
INPUT_DIR = DATA_DIR / "input"


class SmtpSettings(msgspec.Struct, kw_only=True, frozen=True):
    host: str = "smtp.gmail.com"
    port: int = 587
    user: str = ""
    password: str = ""


class ImapSettings(msgspec.Struct, kw_only=True, frozen=True):
    host: str = "imap.gmail.com"
    port: int = 993
    user: str = ""
    password: str = ""


class SendSettings(msgspec.Struct, kw_only=True, frozen=True):
    daily_limit: int = 30
    send_window_start: str = "08:00"
    send_window_end: str = "17:00"
    timezone: str = "America/New_York"
    min_delay_seconds: int = 30
    max_delay_seconds: int = 90


class LlmSettings(msgspec.Struct, kw_only=True, frozen=True):
    model: str = "claude-sonnet-4-20250514"
    max_concurrent: int = 5
    max_opener_words: int = 30


class ScraperSettings(msgspec.Struct, kw_only=True, frozen=True):
    max_concurrent: int = 500
    timeout: int = 5
    dedup_threshold: int = 85


class Settings(msgspec.Struct, kw_only=True):
    db_path: str = str(DB_PATH)
    smtp: SmtpSettings = SmtpSettings()
    imap: ImapSettings = ImapSettings()
    send: SendSettings = SendSettings()
    llm: LlmSettings = LlmSettings()
    scraper: ScraperSettings = ScraperSettings()
    anthropic_api_key: str = ""
    exa_api_key: str = ""
    log_level: str = "INFO"
    log_json: bool = False


def load_settings() -> Settings:
    """Load settings from environment variables and optional TOML."""
    toml_path = BASE_DIR / "dentists.toml"
    data: dict = {}

    if toml_path.exists():
        import tomllib

        with open(toml_path, "rb") as f:
            data = tomllib.load(f)

    # Environment overrides
    env_map = {
        "ANTHROPIC_API_KEY": "anthropic_api_key",
        "EXA_API_KEY": "exa_api_key",
        "DB_PATH": "db_path",
        "LOG_LEVEL": "log_level",
    }
    for env_key, setting_key in env_map.items():
        val = os.environ.get(env_key)
        if val:
            data[setting_key] = val

    return msgspec.convert(data, Settings)
