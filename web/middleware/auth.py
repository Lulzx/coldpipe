"""Authentication middleware for Litestar."""

from __future__ import annotations

import secrets

from litestar.middleware import AbstractMiddleware
from litestar.response import Redirect
from litestar.types import Receive, Scope, Send

from db.queries import (
    delete_session,
    get_session_by_token,
    get_user_by_id,
    get_user_count,
)

SESSION_COOKIE = "coldpipe_session"
SESSION_DURATION_HOURS = 24 * 7

EXEMPT_PREFIXES = ("/auth/",)


def _now_iso() -> str:
    from datetime import UTC, datetime

    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _generate_csrf_token() -> str:
    import base64

    return base64.urlsafe_b64encode(secrets.token_bytes(32)).decode()


class AuthMiddleware(AbstractMiddleware):
    """Session-based auth middleware.

    - Exempt /auth/* paths
    - If no users exist, redirect to registration
    - Validate session cookie, load user into scope
    - Check onboarding completion
    - Generate CSRF tokens
    """

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope["path"]

        # Exempt auth paths
        for prefix in EXEMPT_PREFIXES:
            if path.startswith(prefix):
                await self.app(scope, receive, send)
                return

        user_count = await get_user_count()

        # No users - redirect to registration
        if user_count == 0:
            if path != "/auth/register":
                response = Redirect(path="/auth/register")
                await response(scope, receive, send)
                return
            await self.app(scope, receive, send)
            return

        # Extract session cookie from headers
        session_token = None
        for header_name, header_value in scope.get("headers", []):
            if header_name == b"cookie":
                for part in header_value.decode().split(";"):
                    part = part.strip()
                    if part.startswith(f"{SESSION_COOKIE}="):
                        session_token = part[len(SESSION_COOKIE) + 1 :]
                        break
                break

        if not session_token:
            response = Redirect(path="/auth/login")
            await response(scope, receive, send)
            return

        session = await get_session_by_token(token=session_token)
        if session is None:
            response = Redirect(path="/auth/login")
            await response(scope, receive, send)
            return

        # Check expiry
        if session.expires_at < _now_iso():
            await delete_session(token=session_token)
            response = Redirect(path="/auth/login")
            await response(scope, receive, send)
            return

        user = await get_user_by_id(user_id=session.user_id)
        if user is None:
            response = Redirect(path="/auth/login")
            await response(scope, receive, send)
            return

        # Store user and session in scope
        scope["user"] = user
        scope["session_token"] = session_token

        # Ensure CSRF token
        state = scope["app"].state
        if not hasattr(state, "csrf_secrets"):
            state.csrf_secrets = {}
        if session_token not in state.csrf_secrets:
            state.csrf_secrets[session_token] = _generate_csrf_token()

        # Check onboarding
        if not user.onboarding_completed and not path.startswith("/onboarding"):
            response = Redirect(path="/onboarding")
            await response(scope, receive, send)
            return

        await self.app(scope, receive, send)
