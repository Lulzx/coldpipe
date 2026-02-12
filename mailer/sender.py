"""SMTP sender with retry, reconnect, and inter-send delays."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import uuid
from email.message import EmailMessage

import aiosmtplib

from config.settings import SmtpSettings

log = logging.getLogger(__name__)

MAX_RETRIES = 3
BACKOFF_BASE = 2  # seconds


class EmailSender:
    """Async SMTP sender with connection management and retry logic."""

    def __init__(
        self,
        smtp: SmtpSettings,
        *,
        from_addr: str = "",
        display_name: str = "",
        min_delay: int = 30,
        max_delay: int = 90,
    ):
        self._smtp = smtp
        self._from_addr = from_addr or smtp.user
        self._display_name = display_name
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._client: aiosmtplib.SMTP | None = None

    async def connect(self) -> None:
        """Establish SMTP connection with STARTTLS."""
        self._client = aiosmtplib.SMTP(
            hostname=self._smtp.host,
            port=self._smtp.port,
            start_tls=True,
        )
        await self._client.connect()
        await self._client.login(self._smtp.user, self._smtp.password)
        log.info("SMTP connected to %s:%d", self._smtp.host, self._smtp.port)

    async def disconnect(self) -> None:
        """Close SMTP connection."""
        if self._client:
            with contextlib.suppress(Exception):
                await self._client.quit()
            self._client = None

    async def _ensure_connected(self) -> None:
        """Reconnect if the connection was lost."""
        if self._client is None:
            await self.connect()
            return
        try:
            await self._client.noop()
        except Exception:
            log.warning("SMTP connection lost, reconnecting")
            self._client = None
            await self.connect()

    def _build_message(
        self,
        to_addr: str,
        subject: str,
        body: str,
        *,
        in_reply_to: str | None = None,
    ) -> tuple[EmailMessage, str]:
        """Build a plain-text EmailMessage. Returns (msg, message_id)."""
        msg = EmailMessage()
        msg.set_content(body)

        message_id = f"<{uuid.uuid4()}@{self._smtp.host}>"
        from_header = (
            f"{self._display_name} <{self._from_addr}>" if self._display_name else self._from_addr
        )

        msg["From"] = from_header
        msg["To"] = to_addr
        msg["Subject"] = subject
        msg["Message-ID"] = message_id

        if in_reply_to:
            msg["In-Reply-To"] = in_reply_to
            msg["References"] = in_reply_to

        return msg, message_id

    async def send(
        self,
        to_addr: str,
        subject: str,
        body: str,
        *,
        in_reply_to: str | None = None,
    ) -> str:
        """Send a plain-text email with retry and exponential backoff.

        Returns the generated Message-ID.
        """
        msg, message_id = self._build_message(to_addr, subject, body, in_reply_to=in_reply_to)

        last_exc: Exception | None = None
        for attempt in range(MAX_RETRIES):
            try:
                await self._ensure_connected()
                assert self._client is not None
                await self._client.send_message(msg)
                log.info("Sent email to %s (id=%s)", to_addr, message_id)
                return message_id
            except Exception as exc:
                last_exc = exc
                backoff = BACKOFF_BASE ** (attempt + 1)
                log.warning(
                    "Send attempt %d/%d failed: %s (backoff %ds)",
                    attempt + 1,
                    MAX_RETRIES,
                    exc,
                    backoff,
                )
                self._client = None  # force reconnect
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(backoff)

        raise RuntimeError(
            f"Failed to send to {to_addr} after {MAX_RETRIES} attempts"
        ) from last_exc

    async def send_with_delay(
        self,
        to_addr: str,
        subject: str,
        body: str,
        *,
        in_reply_to: str | None = None,
    ) -> str:
        """Send then sleep a random delay (30-90s) before returning."""
        message_id = await self.send(to_addr, subject, body, in_reply_to=in_reply_to)
        delay = random.randint(self._min_delay, self._max_delay)
        log.debug("Sleeping %ds between sends", delay)
        await asyncio.sleep(delay)
        return message_id

    async def __aenter__(self) -> EmailSender:
        await self.connect()
        return self

    async def __aexit__(self, *exc) -> None:
        await self.disconnect()
