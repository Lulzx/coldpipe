"""Mailboxes controller."""

from __future__ import annotations

from litestar import Controller, Request, get, post
from litestar.exceptions import NotFoundException
from litestar.response import Redirect, Template

from db.queries import (
    check_daily_limit,
    deactivate_mailbox,
    get_mailbox_by_id,
    get_mailboxes,
    upsert_mailbox,
)
from db.tables import Mailbox
from web.helpers import template_context


def _int(val: str | None, default: int = 0) -> int:
    try:
        return int(val) if val else default
    except ValueError:
        return default


class MailboxesController(Controller):
    path = "/mailboxes"
    tags = ["mailboxes"]

    @get("/")
    async def list_mailboxes(self, request: Request) -> Template:
        mboxes = await get_mailboxes()
        usage = {}
        for mb in mboxes:
            sent, limit = await check_daily_limit(mb.id)
            usage[mb.id] = {"sent": sent, "limit": limit}
        return Template(
            template_name="mailboxes/list.html",
            context=template_context(
                request,
                active_page="mailboxes",
                mailboxes=mboxes,
                usage=usage,
            ),
        )

    @get("/add")
    async def add_form(self, request: Request) -> Template:
        return Template(
            template_name="mailboxes/form.html",
            context=template_context(
                request,
                active_page="mailboxes",
                mailbox=None,
                errors=[],
            ),
        )

    @post("/add")
    async def add_submit(self, request: Request) -> Template | Redirect:
        data = await request.form()
        email = str(data.get("email", ""))
        smtp_host = str(data.get("smtp_host", ""))

        errors = []
        if "@" not in email:
            errors.append("Valid email address is required")
        if not smtp_host:
            errors.append("SMTP host is required")
        if errors:
            return Template(
                template_name="mailboxes/form.html",
                context=template_context(
                    request,
                    active_page="mailboxes",
                    mailbox=None,
                    errors=errors,
                ),
            )

        mb = Mailbox(
            email=email,
            smtp_host=smtp_host,
            smtp_port=_int(str(data.get("smtp_port", "587")), 587),
            smtp_user=str(data.get("smtp_user", "")),
            smtp_pass=str(data.get("smtp_pass", "")),
            imap_host=str(data.get("imap_host", "")),
            imap_port=_int(str(data.get("imap_port", "993")), 993),
            imap_user=str(data.get("imap_user", "")),
            imap_pass=str(data.get("imap_pass", "")),
            daily_limit=_int(str(data.get("daily_limit", "30")), 30),
            display_name=str(data.get("display_name", "")),
            is_active=1 if data.get("is_active") else 0,
        )
        await upsert_mailbox(mb)
        return Redirect(path="/mailboxes")

    @get("/{mailbox_id:int}/edit")
    async def edit_form(self, request: Request, mailbox_id: int) -> Template:
        mb = await get_mailbox_by_id(mailbox_id)
        if mb is None:
            raise NotFoundException(detail="Mailbox not found")
        return Template(
            template_name="mailboxes/form.html",
            context=template_context(
                request,
                active_page="mailboxes",
                mailbox=mb,
                errors=[],
            ),
        )

    @post("/{mailbox_id:int}/edit")
    async def edit_submit(self, request: Request, mailbox_id: int) -> Redirect:
        data = await request.form()
        existing = await get_mailbox_by_id(mailbox_id)
        if existing is None:
            raise NotFoundException(detail="Mailbox not found")
        mb = Mailbox(
            id=mailbox_id,
            email=existing.email,
            smtp_host=str(data.get("smtp_host", existing.smtp_host)),
            smtp_port=_int(str(data.get("smtp_port", "")), existing.smtp_port),
            smtp_user=str(data.get("smtp_user", existing.smtp_user)),
            smtp_pass=str(data.get("smtp_pass", "")) or existing.smtp_pass,
            imap_host=str(data.get("imap_host", existing.imap_host)),
            imap_port=_int(str(data.get("imap_port", "")), existing.imap_port),
            imap_user=str(data.get("imap_user", existing.imap_user)),
            imap_pass=str(data.get("imap_pass", "")) or existing.imap_pass,
            daily_limit=_int(str(data.get("daily_limit", "")), existing.daily_limit),
            display_name=str(data.get("display_name", existing.display_name)),
            is_active=1 if data.get("is_active") else 0,
        )
        await upsert_mailbox(mb)
        return Redirect(path="/mailboxes")

    @post("/{mailbox_id:int}/deactivate")
    async def deactivate(self, mailbox_id: int) -> Redirect:
        await deactivate_mailbox(mailbox_id)
        return Redirect(path="/mailboxes")
