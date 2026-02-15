"""CSRF protection middleware for Litestar."""

from __future__ import annotations

from litestar.middleware import AbstractMiddleware
from litestar.response import Response
from litestar.types import Receive, Scope, Send

from web.middleware.auth import SESSION_COOKIE

EXEMPT_PREFIXES = ("/auth/",)


class CSRFMiddleware(AbstractMiddleware):
    """Verify _csrf token on POST requests."""

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        method = scope.get("method", "GET")
        if method != "POST":
            await self.app(scope, receive, send)
            return

        path = scope["path"]
        for prefix in EXEMPT_PREFIXES:
            if path.startswith(prefix):
                await self.app(scope, receive, send)
                return

        # Extract session cookie
        session_token = ""
        for header_name, header_value in scope.get("headers", []):
            if header_name == b"cookie":
                for part in header_value.decode().split(";"):
                    part = part.strip()
                    if part.startswith(f"{SESSION_COOKIE}="):
                        session_token = part[len(SESSION_COOKIE) + 1 :]
                        break
                break

        if not session_token:
            await self.app(scope, receive, send)
            return

        # We need to read the body to check the CSRF token.
        # Collect body chunks.
        body_chunks = []
        got_body = False

        async def receive_wrapper():
            nonlocal got_body
            message = await receive()
            if message["type"] == "http.request":
                body_chunks.append(message.get("body", b""))
                got_body = True
            return message

        # Let the app handle the request — CSRF is checked at the form parsing
        # level. For a raw ASGI middleware, we need a different approach.
        # Instead, we'll check in a simpler way: intercept and replay.

        # Read entire body
        body = b""
        while True:
            message = await receive()
            body += message.get("body", b"")
            if not message.get("more_body", False):
                break

        # Parse form data to find _csrf
        csrf_token = ""
        content_type = ""
        for header_name, header_value in scope.get("headers", []):
            if header_name == b"content-type":
                content_type = header_value.decode()
                break

        if "application/x-www-form-urlencoded" in content_type:
            from urllib.parse import parse_qs

            parsed = parse_qs(body.decode("utf-8", errors="replace"))
            csrf_values = parsed.get("_csrf", [])
            csrf_token = csrf_values[0] if csrf_values else ""

        state = scope["app"].state
        csrf_secrets = getattr(state, "csrf_secrets", {})
        expected = csrf_secrets.get(session_token, "")

        if not csrf_token or not expected or csrf_token != expected:
            response = Response(content="CSRF token invalid", status_code=403)
            await response(scope, receive, send)
            return

        # Replay the body for downstream handlers
        body_sent = False

        async def replayed_receive():
            nonlocal body_sent
            if not body_sent:
                body_sent = True
                return {"type": "http.request", "body": body, "more_body": False}
            return await receive()

        await self.app(scope, replayed_receive, send)
