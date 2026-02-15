"""Shared helpers for web controllers."""

from __future__ import annotations

from litestar import Request

from web.middleware.auth import SESSION_COOKIE


def template_context(request: Request, **kwargs) -> dict:
    """Build template context with csrf_token and current_user injected."""
    ctx = dict(kwargs)
    # CSRF token
    if "csrf_token" not in ctx:
        session_token = request.cookies.get(SESSION_COOKIE, "")
        state = request.app.state
        csrf_secrets = getattr(state, "csrf_secrets", {})
        ctx["csrf_token"] = csrf_secrets.get(session_token, "")
    # Current user (set by auth middleware)
    if "current_user" not in ctx:
        ctx["current_user"] = request.scope.get("user")
    return ctx
