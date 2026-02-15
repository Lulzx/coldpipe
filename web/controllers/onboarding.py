"""Onboarding controller — first-time setup wizard."""

from __future__ import annotations

from pathlib import Path

from litestar import Controller, Request, get, post
from litestar.response import Redirect, Template

from db.queries import set_onboarding_completed, upsert_mailbox
from db.tables import Mailbox, User
from web.helpers import template_context


def _int(val: str | None, default: int = 0) -> int:
    try:
        return int(val) if val else default
    except ValueError:
        return default


class OnboardingController(Controller):
    path = "/onboarding"
    tags = ["onboarding"]

    @get("/")
    async def wizard_page(self, request: Request, step: int = 1) -> Template:
        return Template(
            template_name="onboarding/wizard.html",
            context=template_context(request, step=step),
        )

    @post("/settings")
    async def save_settings(self, request: Request) -> Redirect:
        from shared.toml_writer import dumps

        data = await request.form()
        timezone = str(data.get("timezone", "America/New_York"))
        daily_limit = _int(str(data.get("daily_limit", "30")), 30)

        toml_path = Path(__file__).resolve().parent.parent.parent / "coldpipe.toml"
        config: dict = {}
        if toml_path.exists():
            import tomllib

            with open(toml_path, "rb") as f:
                config = tomllib.load(f)

        if "send" not in config:
            config["send"] = {}
        config["send"]["timezone"] = timezone
        config["send"]["daily_limit"] = daily_limit
        toml_path.write_text(dumps(config))

        return Redirect(path="/onboarding?step=2")

    @post("/mailbox")
    async def add_mailbox(self, request: Request) -> Redirect:
        data = await request.form()
        mb = Mailbox(
            email=str(data.get("email", "")),
            smtp_host=str(data.get("smtp_host", "")),
            smtp_port=_int(str(data.get("smtp_port", "587")), 587),
            smtp_user=str(data.get("smtp_user", "")),
            smtp_pass=str(data.get("smtp_pass", "")),
            imap_host=str(data.get("imap_host", "")),
            imap_port=_int(str(data.get("imap_port", "993")), 993),
            imap_user=str(data.get("imap_user", "")),
            imap_pass=str(data.get("imap_pass", "")),
            daily_limit=_int(str(data.get("daily_limit", "30")), 30),
            display_name=str(data.get("display_name", "")),
            is_active=1,
        )
        if mb.email and mb.smtp_host:
            await upsert_mailbox(mb)
        return Redirect(path="/onboarding?step=3")

    @post("/complete")
    async def complete(self, request: Request) -> Redirect:
        user: User | None = request.scope.get("user")
        if user:
            await set_onboarding_completed(user.id)
        return Redirect(path="/")
