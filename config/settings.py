"""Application settings via msgspec + environment/TOML."""

from __future__ import annotations

import os
from pathlib import Path
from zoneinfo import ZoneInfo

import msgspec

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "coldpipe.db"
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
    model: str = "claude-haiku-4-5"
    max_concurrent: int = 5
    max_opener_words: int = 30


class ScraperSettings(msgspec.Struct, kw_only=True, frozen=True):
    max_concurrent: int = 500
    timeout: int = 5
    dedup_threshold: int = 85


class WebSettings(msgspec.Struct, kw_only=True, frozen=True):
    rp_id: str = "localhost"
    rp_name: str = "Coldpipe"
    host: str = "127.0.0.1"
    port: int = 8080


class Settings(msgspec.Struct, kw_only=True):
    db_path: str = str(DB_PATH)
    smtp: SmtpSettings = SmtpSettings()
    imap: ImapSettings = ImapSettings()
    send: SendSettings = SendSettings()
    llm: LlmSettings = LlmSettings()
    scraper: ScraperSettings = ScraperSettings()
    web: WebSettings = WebSettings()
    anthropic_api_key: str = ""
    exa_api_key: str = ""
    log_level: str = "INFO"
    log_json: bool = False
    log_file: str = ""

    def validate(self) -> None:
        """Validate settings ranges and constraints. Raises ValueError if invalid."""
        errors: list[str] = []

        if not 1 <= self.send.daily_limit <= 500:
            errors.append(f"send.daily_limit must be 1..500, got {self.send.daily_limit}")

        if self.send.min_delay_seconds >= self.send.max_delay_seconds:
            errors.append(
                f"send.min_delay_seconds ({self.send.min_delay_seconds}) "
                f"must be < send.max_delay_seconds ({self.send.max_delay_seconds})"
            )

        try:
            ZoneInfo(self.send.timezone)
        except KeyError, ValueError:
            errors.append(f"send.timezone is not a valid IANA zone: {self.send.timezone!r}")

        if not 1 <= self.scraper.max_concurrent <= 500:
            errors.append(
                f"scraper.max_concurrent must be 1..500, got {self.scraper.max_concurrent}"
            )

        if not 1 <= self.scraper.timeout <= 60:
            errors.append(f"scraper.timeout must be 1..60, got {self.scraper.timeout}")

        if not 1 <= self.llm.max_concurrent <= 20:
            errors.append(f"llm.max_concurrent must be 1..20, got {self.llm.max_concurrent}")

        if errors:
            raise ValueError("Settings validation failed:\n  " + "\n  ".join(errors))


def load_settings() -> Settings:
    """Load settings from environment variables and optional TOML."""
    toml_path = BASE_DIR / "coldpipe.toml"
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
        "LOG_FILE": "log_file",
    }
    for env_key, setting_key in env_map.items():
        val = os.environ.get(env_key)
        if val:
            data[setting_key] = val

    settings = msgspec.convert(data, Settings)
    settings.validate()
    return settings
