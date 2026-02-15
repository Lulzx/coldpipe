"""Security headers middleware for Litestar."""

from __future__ import annotations

from litestar.middleware import AbstractMiddleware
from litestar.types import Receive, Scope, Send


class SecurityHeadersMiddleware(AbstractMiddleware):
    """Add security headers to all responses."""

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async def send_with_headers(message):
            if message["type"] == "http.response.start":
                extra = [
                    (b"x-frame-options", b"DENY"),
                    (b"x-content-type-options", b"nosniff"),
                    (b"referrer-policy", b"strict-origin-when-cross-origin"),
                    (
                        b"content-security-policy",
                        b"default-src 'self'; "
                        b"script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
                        b"style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
                        b"img-src 'self' data:; "
                        b"font-src 'self' https://cdn.jsdelivr.net",
                    ),
                ]
                existing = list(message.get("headers", []))
                existing.extend(extra)
                message["headers"] = existing
            await send(message)

        await self.app(scope, receive, send_with_headers)
