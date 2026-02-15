"""Simple in-memory per-IP rate limiter middleware for Litestar."""

from __future__ import annotations

import time

from litestar.middleware import AbstractMiddleware
from litestar.response import Response
from litestar.types import Receive, Scope, Send

RATE_LIMIT_REQUESTS = 60
RATE_LIMIT_WINDOW = 60  # seconds


class RateLimitMiddleware(AbstractMiddleware):
    """Per-IP in-memory rate limiter."""

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        # Extract client IP
        client = scope.get("client")
        ip = client[0] if client else "unknown"

        state = scope["app"].state
        if not hasattr(state, "rate_limits"):
            state.rate_limits = {}

        now = time.monotonic()
        limits = state.rate_limits

        if ip in limits:
            cutoff = now - RATE_LIMIT_WINDOW
            limits[ip] = [t for t in limits[ip] if t > cutoff]
            if len(limits[ip]) >= RATE_LIMIT_REQUESTS:
                response = Response(content="Rate limit exceeded", status_code=429)
                await response(scope, receive, send)
                return
        else:
            limits[ip] = []

        limits[ip].append(now)
        await self.app(scope, receive, send)
