"""Settings controller."""

from __future__ import annotations

from litestar import Controller, Request, get
from litestar.response import Template

from config.settings import load_settings
from web.helpers import template_context


def _mask(value: str) -> str:
    if not value:
        return ""
    return (
        value[:2] + "\u2022" * min(len(value) - 2, 20) if len(value) > 4 else "\u2022" * len(value)
    )


class SettingsController(Controller):
    path = "/settings"
    tags = ["settings"]

    @get("/")
    async def view(self, request: Request) -> Template:
        settings = load_settings()
        return Template(
            template_name="settings.html",
            context=template_context(
                request,
                active_page="settings",
                settings=settings,
                mask=_mask,
            ),
        )
