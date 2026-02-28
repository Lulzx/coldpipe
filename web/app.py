"""Litestar application factory."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from litestar import Litestar, Request
from litestar.contrib.jinja import JinjaTemplateEngine
from litestar.response import Template
from litestar.template import TemplateConfig

from db import close_db, init_db
from db.queries import cleanup_expired_sessions
from web.controllers.activity import ActivityController
from web.controllers.auth import AuthController
from web.controllers.campaigns import CampaignsController
from web.controllers.dashboard import DashboardController
from web.controllers.deals import DealsController
from web.controllers.emails import EmailsController
from web.controllers.leads import LeadsController
from web.controllers.mailboxes import MailboxesController
from web.controllers.onboarding import OnboardingController
from web.controllers.settings import SettingsController
from web.middleware.auth import AuthMiddleware
from web.middleware.csrf import CSRFMiddleware
from web.middleware.rate_limit import RateLimitMiddleware
from web.middleware.security_headers import SecurityHeadersMiddleware

log = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).parent / "templates"


@asynccontextmanager
async def lifespan(app: Litestar):
    """Start/stop DB and clean up expired sessions."""
    await init_db()
    await cleanup_expired_sessions()
    yield
    await close_db()


def handle_404(request: Request, exc: Exception) -> Template:
    return Template(
        template_name="errors/404.html",
        context={"active_page": "", "csrf_token": "", "current_user": None},
        status_code=404,
    )


def handle_500(request: Request, exc: Exception) -> Template:
    log.exception("Unhandled error for %s %s", request.method, request.url)
    return Template(
        template_name="errors/500.html",
        context={"active_page": "", "csrf_token": "", "current_user": None},
        status_code=500,
    )


def create_app() -> Litestar:
    """Create the Litestar application."""
    from litestar.exceptions import InternalServerException, NotFoundException

    return Litestar(
        route_handlers=[
            ActivityController,
            AuthController,
            OnboardingController,
            DashboardController,
            LeadsController,
            CampaignsController,
            MailboxesController,
            DealsController,
            EmailsController,
            SettingsController,
        ],
        middleware=[
            SecurityHeadersMiddleware,
            RateLimitMiddleware,
            CSRFMiddleware,
            AuthMiddleware,
        ],
        template_config=TemplateConfig(
            engine=JinjaTemplateEngine,
            directory=TEMPLATES_DIR,
        ),
        lifespan=[lifespan],
        exception_handlers={
            NotFoundException: handle_404,  # type: ignore[dict-item]
            InternalServerException: handle_500,  # type: ignore[dict-item]
        },
        debug=False,
    )
