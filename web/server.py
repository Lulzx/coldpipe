"""Web dashboard — aiohttp + Jinja2 + Tabler UI."""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import logging
import secrets
import time
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path

import aiohttp_jinja2
import jinja2
from aiohttp import web

from config.settings import load_settings
from db import DBPool
from db.models import Deal, Mailbox, User
from db.queries import (
    check_daily_limit,
    cleanup_expired_sessions,
    count_emails_sent,
    count_leads,
    create_session,
    create_user,
    deactivate_mailbox,
    delete_campaign,
    delete_session,
    get_campaign_by_id,
    get_campaign_leads,
    get_campaign_stats,
    get_campaign_step_distribution,
    get_campaigns,
    get_daily_stats,
    get_deal_by_id,
    get_deal_stats,
    get_deals,
    get_email_status_distribution,
    get_emails_for_lead,
    get_emails_sent,
    get_lead_by_id,
    get_lead_stats,
    get_leads,
    get_mailbox_by_id,
    get_mailboxes,
    get_pipeline_stats,
    get_sequence_steps,
    get_session_by_token,
    get_today_activity,
    get_user_by_id,
    get_user_by_username,
    get_user_count,
    get_warmup_limit,
    search_leads,
    set_onboarding_completed,
    update_campaign_status,
    update_user_credential,
    update_user_sign_count,
    upsert_deal,
    upsert_mailbox,
)

log = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).parent / "templates"


# ---------------------------------------------------------------------------
# Lifecycle hooks
# ---------------------------------------------------------------------------


async def on_startup(app: web.Application) -> None:
    pool = DBPool()
    await pool.open()
    app["db"] = pool
    app["_webauthn_challenges"] = {}
    app["_rate_limits"] = {}
    app["_csrf_secrets"] = {}
    # Clean up expired sessions on startup
    async with pool.acquire() as db:
        await cleanup_expired_sessions(db)


async def on_cleanup(app: web.Application) -> None:
    await app["db"].close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PAGE_SIZE = 50
DEAL_STAGES = [
    "lead",
    "contacted",
    "replied",
    "interested",
    "meeting_booked",
    "proposal_sent",
    "closed_won",
    "closed_lost",
]

SESSION_COOKIE = "coldpipe_session"
SESSION_DURATION_HOURS = 24 * 7  # 1 week
RATE_LIMIT_REQUESTS = 60
RATE_LIMIT_WINDOW = 60  # seconds


def _int(val: str | None, default: int = 0) -> int:
    try:
        return int(val) if val else default
    except ValueError:
        return default


def _mask(value: str) -> str:
    if not value:
        return ""
    return value[:2] + "\u2022" * min(len(value) - 2, 20) if len(value) > 4 else "\u2022" * len(value)


def _match_id(request: web.Request) -> int:
    """Extract and validate integer 'id' from URL path."""
    try:
        return int(request.match_info["id"])
    except (KeyError, ValueError):
        raise web.HTTPNotFound(text="Invalid ID")


def _generate_csrf_token(session_token: str) -> str:
    """Generate a CSRF token tied to the session."""
    raw = secrets.token_bytes(32)
    token = base64.urlsafe_b64encode(raw).decode()
    return token


def _now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Middlewares
# ---------------------------------------------------------------------------


@web.middleware
async def error_middleware(request: web.Request, handler):
    """Catch exceptions and render styled error pages."""
    try:
        return await handler(request)
    except web.HTTPException:
        raise
    except Exception:
        log.exception("Unhandled error for %s %s", request.method, request.path)
        try:
            return aiohttp_jinja2.render_template(
                "errors/500.html", request, {"active_page": ""}, status=500
            )
        except Exception:
            return web.Response(text="Internal Server Error", status=500)


@web.middleware
async def security_headers_middleware(request: web.Request, handler):
    """Add security headers to all responses."""
    response = await handler(request)
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
        "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
        "img-src 'self' data:; "
        "font-src 'self' https://cdn.jsdelivr.net"
    )
    return response


@web.middleware
async def rate_limit_middleware(request: web.Request, handler):
    """Simple in-memory per-IP rate limiter."""
    ip = request.remote or "unknown"
    limits: dict = request.app["_rate_limits"]
    now = time.monotonic()

    if ip in limits:
        timestamps = limits[ip]
        # Remove timestamps outside the window
        cutoff = now - RATE_LIMIT_WINDOW
        timestamps = [t for t in timestamps if t > cutoff]
        limits[ip] = timestamps

        if len(timestamps) >= RATE_LIMIT_REQUESTS:
            return web.Response(text="Rate limit exceeded", status=429)
    else:
        limits[ip] = []

    limits[ip].append(now)
    return await handler(request)


@web.middleware
async def csrf_middleware(request: web.Request, handler):
    """CSRF protection: verify _csrf token on POST requests."""
    if request.method == "POST":
        # Exempt auth paths (WebAuthn has its own challenge-response)
        if request.path.startswith("/auth/"):
            return await handler(request)

        # Check CSRF token
        session_token = request.cookies.get(SESSION_COOKIE, "")
        if session_token:
            try:
                data = await request.post()
                csrf_token = data.get("_csrf", "")
            except Exception:
                csrf_token = ""

            expected = request.app["_csrf_secrets"].get(session_token, "")
            if not csrf_token or not expected or csrf_token != expected:
                raise web.HTTPForbidden(text="CSRF token invalid")

    return await handler(request)


@web.middleware
async def auth_middleware(request: web.Request, handler):
    """Require authentication for all routes except /auth/* and static."""
    # Exempt paths
    exempt_prefixes = ("/auth/",)
    if request.path.startswith(exempt_prefixes):
        return await handler(request)

    pool: DBPool = request.app["db"]

    async with pool.acquire() as db:
        user_count = await get_user_count(db)

    # If no users exist, redirect to registration
    if user_count == 0:
        if request.path != "/auth/register":
            raise web.HTTPFound("/auth/register")
        return await handler(request)

    # Check session cookie
    session_token = request.cookies.get(SESSION_COOKIE)
    if not session_token:
        raise web.HTTPFound("/auth/login")

    async with pool.acquire() as db:
        session = await get_session_by_token(db, session_token)

    if session is None:
        resp = web.HTTPFound("/auth/login")
        resp.del_cookie(SESSION_COOKIE)
        raise resp

    # Check expiry
    if session.expires_at < _now_iso():
        async with pool.acquire() as db:
            await delete_session(db, session_token)
        resp = web.HTTPFound("/auth/login")
        resp.del_cookie(SESSION_COOKIE)
        raise resp

    # Load user
    async with pool.acquire() as db:
        user = await get_user_by_id(db, session.user_id)

    if user is None:
        resp = web.HTTPFound("/auth/login")
        resp.del_cookie(SESSION_COOKIE)
        raise resp

    # Store user in request for handlers
    request["user"] = user

    # Ensure CSRF token exists for this session
    if session_token not in request.app["_csrf_secrets"]:
        request.app["_csrf_secrets"][session_token] = _generate_csrf_token(session_token)

    # Check onboarding
    if not user.onboarding_completed and not request.path.startswith("/onboarding"):
        raise web.HTTPFound("/onboarding")

    return await handler(request)


# ---------------------------------------------------------------------------
# Jinja2 context processor
# ---------------------------------------------------------------------------


def jinja2_context_processor(request: web.Request) -> dict:
    """Inject CSRF token and user into all templates."""
    ctx: dict = {}
    session_token = request.cookies.get(SESSION_COOKIE, "")
    if session_token and session_token in request.app["_csrf_secrets"]:
        ctx["csrf_token"] = request.app["_csrf_secrets"][session_token]
    else:
        ctx["csrf_token"] = ""
    ctx["current_user"] = request.get("user")
    return ctx


# ---------------------------------------------------------------------------
# Auth route handlers
# ---------------------------------------------------------------------------


@aiohttp_jinja2.template("auth/register.html")
async def auth_register(request: web.Request) -> dict:
    pool: DBPool = request.app["db"]
    async with pool.acquire() as db:
        user_count = await get_user_count(db)
    if user_count > 0:
        raise web.HTTPFound("/auth/login")
    return {}


async def auth_register_begin(request: web.Request) -> web.Response:
    """Generate WebAuthn registration options (JSON)."""
    from webauthn import generate_registration_options, options_to_json
    from webauthn.helpers.structs import (
        AuthenticatorSelectionCriteria,
        ResidentKeyRequirement,
        UserVerificationRequirement,
    )

    pool: DBPool = request.app["db"]
    async with pool.acquire() as db:
        if await get_user_count(db) > 0:
            return web.json_response({"error": "User already exists"}, status=400)

    data = await request.json()
    username = data.get("username", "").strip()
    if not username:
        return web.json_response({"error": "Username required"}, status=400)

    settings = load_settings()
    rp_id = settings.web.rp_id
    rp_name = settings.web.rp_name

    options = generate_registration_options(
        rp_id=rp_id,
        rp_name=rp_name,
        user_name=username,
        user_id=hashlib.sha256(username.encode()).digest(),
        authenticator_selection=AuthenticatorSelectionCriteria(
            resident_key=ResidentKeyRequirement.PREFERRED,
            user_verification=UserVerificationRequirement.PREFERRED,
        ),
    )

    # Store challenge keyed by a nonce cookie
    nonce = secrets.token_urlsafe(32)
    request.app["_webauthn_challenges"][nonce] = {
        "challenge": base64.urlsafe_b64encode(options.challenge).decode(),
        "username": username,
    }

    response = web.Response(
        text=options_to_json(options),
        content_type="application/json",
    )
    response.set_cookie("webauthn_nonce", nonce, httponly=True, samesite="Strict", max_age=300)
    return response


async def auth_register_complete(request: web.Request) -> web.Response:
    """Verify attestation, create user + session."""
    import webauthn
    from webauthn.helpers import base64url_to_bytes

    nonce = request.cookies.get("webauthn_nonce", "")
    challenge_data = request.app["_webauthn_challenges"].pop(nonce, None)
    if not challenge_data:
        return web.json_response({"error": "Challenge expired"}, status=400)

    settings = load_settings()
    body = await request.json()

    try:
        verification = webauthn.verify_registration_response(
            credential=body,
            expected_challenge=base64url_to_bytes(challenge_data["challenge"]),
            expected_rp_id=settings.web.rp_id,
            expected_origin=f"http://{settings.web.host}:{settings.web.port}",
        )
    except Exception as exc:
        log.warning("WebAuthn registration failed: %s", exc)
        return web.json_response({"error": "Registration failed"}, status=400)

    # Create user
    pool: DBPool = request.app["db"]
    credential_id = base64.urlsafe_b64encode(verification.credential_id).decode()
    public_key = base64.urlsafe_b64encode(verification.credential_public_key).decode()

    async with pool.acquire() as db:
        user_id = await create_user(
            db,
            User(
                username=challenge_data["username"],
                webauthn_credential_id=credential_id,
                webauthn_public_key=public_key,
                webauthn_sign_count=verification.sign_count,
            ),
        )

    # Create session
    token = secrets.token_urlsafe(48)
    expires = (datetime.now(UTC) + timedelta(hours=SESSION_DURATION_HOURS)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    async with pool.acquire() as db:
        await create_session(db, token, user_id, expires)

    response = web.json_response({"ok": True, "redirect": "/onboarding"})
    response.set_cookie(
        SESSION_COOKIE, token, httponly=True, samesite="Strict", max_age=SESSION_DURATION_HOURS * 3600
    )
    response.del_cookie("webauthn_nonce")
    return response


@aiohttp_jinja2.template("auth/login.html")
async def auth_login(request: web.Request) -> dict:
    return {}


async def auth_login_begin(request: web.Request) -> web.Response:
    """Generate WebAuthn authentication options (JSON)."""
    from webauthn import generate_authentication_options, options_to_json
    from webauthn.helpers.structs import PublicKeyCredentialDescriptor, UserVerificationRequirement

    pool: DBPool = request.app["db"]
    settings = load_settings()

    # Get all credential IDs
    allow_credentials = []
    async with pool.acquire() as db:
        cursor = await db.execute("SELECT webauthn_credential_id FROM users WHERE webauthn_credential_id != ''")
        rows = await cursor.fetchall()
        for row in rows:
            try:
                cred_bytes = base64.urlsafe_b64decode(row[0] + "==")
                allow_credentials.append(PublicKeyCredentialDescriptor(id=cred_bytes))
            except Exception:
                pass

    options = generate_authentication_options(
        rp_id=settings.web.rp_id,
        allow_credentials=allow_credentials,
        user_verification=UserVerificationRequirement.PREFERRED,
    )

    nonce = secrets.token_urlsafe(32)
    request.app["_webauthn_challenges"][nonce] = {
        "challenge": base64.urlsafe_b64encode(options.challenge).decode(),
    }

    response = web.Response(
        text=options_to_json(options),
        content_type="application/json",
    )
    response.set_cookie("webauthn_nonce", nonce, httponly=True, samesite="Strict", max_age=300)
    return response


async def auth_login_complete(request: web.Request) -> web.Response:
    """Verify assertion, create session."""
    import webauthn
    from webauthn.helpers import base64url_to_bytes

    nonce = request.cookies.get("webauthn_nonce", "")
    challenge_data = request.app["_webauthn_challenges"].pop(nonce, None)
    if not challenge_data:
        return web.json_response({"error": "Challenge expired"}, status=400)

    settings = load_settings()
    body = await request.json()

    # Find the user by credential ID
    raw_id = body.get("rawId", body.get("id", ""))

    pool: DBPool = request.app["db"]
    async with pool.acquire() as db:
        cursor = await db.execute(
            "SELECT id, webauthn_credential_id, webauthn_public_key, webauthn_sign_count FROM users"
        )
        rows = await cursor.fetchall()

    user_row = None
    for row in rows:
        if row[1] == raw_id or row[1].rstrip("=") == raw_id.rstrip("="):
            user_row = row
            break

    if user_row is None:
        return web.json_response({"error": "Unknown credential"}, status=400)

    user_id, _, public_key_b64, sign_count = user_row

    try:
        verification = webauthn.verify_authentication_response(
            credential=body,
            expected_challenge=base64url_to_bytes(challenge_data["challenge"]),
            expected_rp_id=settings.web.rp_id,
            expected_origin=f"http://{settings.web.host}:{settings.web.port}",
            credential_public_key=base64.urlsafe_b64decode(public_key_b64 + "=="),
            credential_current_sign_count=sign_count,
        )
    except Exception as exc:
        log.warning("WebAuthn login failed: %s", exc)
        return web.json_response({"error": "Authentication failed"}, status=400)

    # Update sign count
    async with pool.acquire() as db:
        await update_user_sign_count(db, user_id, verification.new_sign_count)

    # Create session
    token = secrets.token_urlsafe(48)
    expires = (datetime.now(UTC) + timedelta(hours=SESSION_DURATION_HOURS)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    async with pool.acquire() as db:
        await create_session(db, token, user_id, expires)

    response = web.json_response({"ok": True, "redirect": "/"})
    response.set_cookie(
        SESSION_COOKIE, token, httponly=True, samesite="Strict", max_age=SESSION_DURATION_HOURS * 3600
    )
    response.del_cookie("webauthn_nonce")
    return response


async def auth_logout(request: web.Request) -> web.Response:
    """Delete session, clear cookie."""
    token = request.cookies.get(SESSION_COOKIE, "")
    if token:
        pool: DBPool = request.app["db"]
        async with pool.acquire() as db:
            await delete_session(db, token)
        # Clean up CSRF secret
        request.app["_csrf_secrets"].pop(token, None)
    resp = web.HTTPFound("/auth/login")
    resp.del_cookie(SESSION_COOKIE)
    raise resp


# ---------------------------------------------------------------------------
# Onboarding route handlers
# ---------------------------------------------------------------------------


@aiohttp_jinja2.template("onboarding/wizard.html")
async def onboarding_page(request: web.Request) -> dict:
    step = _int(request.query.get("step"), 1)
    return {"step": step}


async def onboarding_settings(request: web.Request) -> web.Response:
    """Save timezone + daily limit during onboarding."""
    data = await request.post()
    timezone = str(data.get("timezone", "America/New_York"))
    daily_limit = _int(str(data.get("daily_limit", "30")), 30)

    # Write/update coldpipe.toml
    from shared.toml_writer import dumps

    toml_path = Path(__file__).resolve().parent.parent / "coldpipe.toml"
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

    raise web.HTTPFound("/onboarding?step=2")


async def onboarding_mailbox(request: web.Request) -> web.Response:
    """Add first mailbox during onboarding."""
    pool: DBPool = request.app["db"]
    data = await request.post()
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
        async with pool.acquire() as db:
            await upsert_mailbox(db, mb)
    raise web.HTTPFound("/onboarding?step=3")


async def onboarding_complete(request: web.Request) -> web.Response:
    """Mark onboarding as completed."""
    user: User = request["user"]
    pool: DBPool = request.app["db"]
    async with pool.acquire() as db:
        await set_onboarding_completed(db, user.id)
    raise web.HTTPFound("/")


# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------


@aiohttp_jinja2.template("dashboard.html")
async def dashboard(request: web.Request) -> dict:
    pool: DBPool = request.app["db"]
    async with pool.acquire() as db:
        lead_stats = await get_lead_stats(db)
        activity = await get_today_activity(db)
        deal_stats = await get_deal_stats(db)
        pipeline = await get_pipeline_stats(db)
        campaigns = await get_campaigns(db, status="active")
        daily = await get_daily_stats(db, 30)
        mailboxes = await get_mailboxes(db, active_only=True)
        mailbox_warmup = []
        for mb in mailboxes:
            sent, limit = await check_daily_limit(db, mb.id)
            warmup_limit = get_warmup_limit(mb.warmup_day)
            mailbox_warmup.append({
                "email": mb.email,
                "warmup_day": mb.warmup_day,
                "warmup_limit": warmup_limit,
                "sent_today": sent,
                "daily_limit": limit,
            })

    # Pivot daily stats into chart series
    days_map: dict[str, dict[str, int]] = defaultdict(lambda: {"sent": 0, "replied": 0, "bounced": 0})
    for row in daily:
        days_map[row["day"]][row["status"]] = row["cnt"]
    sorted_days = sorted(days_map.keys())
    chart_series = {
        "days": sorted_days,
        "sent": [days_map[d]["sent"] for d in sorted_days],
        "replied": [days_map[d]["replied"] for d in sorted_days],
        "bounced": [days_map[d]["bounced"] for d in sorted_days],
    }

    return {
        "active_page": "dashboard",
        "lead_stats": lead_stats,
        "activity": activity,
        "deal_stats": deal_stats,
        "pipeline": pipeline,
        "campaigns": campaigns,
        "daily": daily,
        "chart_series": chart_series,
        "mailbox_warmup": mailbox_warmup,
        "stages": DEAL_STAGES,
    }


@aiohttp_jinja2.template("leads/list.html")
async def leads_list(request: web.Request) -> dict:
    pool: DBPool = request.app["db"]
    page = max(1, _int(request.query.get("page"), 1))
    status = request.query.get("status") or None
    source = request.query.get("source") or None
    offset = (page - 1) * PAGE_SIZE
    async with pool.acquire() as db:
        leads = await get_leads(db, limit=PAGE_SIZE, offset=offset, email_status=status, source=source)
        total = await count_leads(db, email_status=status)
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    return {
        "active_page": "leads",
        "leads": leads,
        "total": total,
        "page": page,
        "total_pages": total_pages,
        "status": status or "",
        "source": source or "",
    }


@aiohttp_jinja2.template("leads/list.html")
async def leads_search(request: web.Request) -> dict:
    pool: DBPool = request.app["db"]
    q = request.query.get("q", "")
    async with pool.acquire() as db:
        leads = await search_leads(db, q, limit=PAGE_SIZE) if q else []
        total = len(leads)
    return {
        "active_page": "leads",
        "leads": leads,
        "total": total,
        "page": 1,
        "total_pages": 1,
        "status": "",
        "source": "",
        "search_query": q,
    }


async def leads_export(request: web.Request) -> web.StreamResponse:
    """Export all leads as CSV."""
    pool: DBPool = request.app["db"]
    async with pool.acquire() as db:
        leads = await get_leads(db, limit=10000, offset=0)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "id", "email", "first_name", "last_name", "company", "job_title",
        "website", "phone", "city", "state", "zip", "source", "email_status",
        "tags", "created_at",
    ])
    for lead in leads:
        writer.writerow([
            lead.id, lead.email, lead.first_name, lead.last_name,
            lead.company, lead.job_title, lead.website, lead.phone,
            lead.city, lead.state, lead.zip, lead.source,
            lead.email_status, lead.tags, lead.created_at,
        ])

    return web.Response(
        body=output.getvalue(),
        content_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=leads.csv"},
    )


@aiohttp_jinja2.template("leads/detail.html")
async def lead_detail(request: web.Request) -> dict:
    pool: DBPool = request.app["db"]
    lead_id = _match_id(request)
    async with pool.acquire() as db:
        lead = await get_lead_by_id(db, lead_id)
        if lead is None:
            raise web.HTTPNotFound(text="Lead not found")
        emails = await get_emails_for_lead(db, lead_id)
    return {"active_page": "leads", "lead": lead, "emails": emails}


@aiohttp_jinja2.template("campaigns/list.html")
async def campaigns_list(request: web.Request) -> dict:
    pool: DBPool = request.app["db"]
    status = request.query.get("status") or None
    async with pool.acquire() as db:
        campaigns = await get_campaigns(db, status=status)
    return {"active_page": "campaigns", "campaigns": campaigns, "status": status or ""}


@aiohttp_jinja2.template("campaigns/detail.html")
async def campaign_detail(request: web.Request) -> dict:
    pool: DBPool = request.app["db"]
    cid = _match_id(request)
    async with pool.acquire() as db:
        campaign = await get_campaign_by_id(db, cid)
        if campaign is None:
            raise web.HTTPNotFound(text="Campaign not found")
        stats = await get_campaign_stats(db, cid)
        steps = await get_sequence_steps(db, cid)
        leads = await get_campaign_leads(db, cid)
        step_dist = await get_campaign_step_distribution(db, cid)
    return {
        "active_page": "campaigns",
        "campaign": campaign,
        "stats": stats,
        "steps": steps,
        "leads": leads,
        "step_dist": step_dist,
    }


async def campaign_pause(request: web.Request) -> web.Response:
    pool: DBPool = request.app["db"]
    cid = _match_id(request)
    async with pool.acquire() as db:
        await update_campaign_status(db, cid, "paused")
    raise web.HTTPFound(f"/campaigns/{cid}")


async def campaign_resume(request: web.Request) -> web.Response:
    pool: DBPool = request.app["db"]
    cid = _match_id(request)
    async with pool.acquire() as db:
        await update_campaign_status(db, cid, "active")
    raise web.HTTPFound(f"/campaigns/{cid}")


async def campaign_delete(request: web.Request) -> web.Response:
    pool: DBPool = request.app["db"]
    cid = _match_id(request)
    async with pool.acquire() as db:
        await delete_campaign(db, cid)
    raise web.HTTPFound("/campaigns")


@aiohttp_jinja2.template("mailboxes/list.html")
async def mailboxes_list(request: web.Request) -> dict:
    pool: DBPool = request.app["db"]
    async with pool.acquire() as db:
        mboxes = await get_mailboxes(db)
        usage = {}
        for mb in mboxes:
            sent, limit = await check_daily_limit(db, mb.id)
            usage[mb.id] = {"sent": sent, "limit": limit}
    return {"active_page": "mailboxes", "mailboxes": mboxes, "usage": usage}


@aiohttp_jinja2.template("mailboxes/form.html")
async def mailbox_add_form(request: web.Request) -> dict:
    return {"active_page": "mailboxes", "mailbox": None, "errors": []}


async def mailbox_add_submit(request: web.Request) -> web.Response:
    pool: DBPool = request.app["db"]
    data = await request.post()
    email = str(data.get("email", ""))
    smtp_host = str(data.get("smtp_host", ""))

    # Validate
    errors = []
    if "@" not in email:
        errors.append("Valid email address is required")
    if not smtp_host:
        errors.append("SMTP host is required")
    if errors:
        return aiohttp_jinja2.render_template(
            "mailboxes/form.html",
            request,
            {"active_page": "mailboxes", "mailbox": None, "errors": errors},
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
    async with pool.acquire() as db:
        await upsert_mailbox(db, mb)
    raise web.HTTPFound("/mailboxes")


@aiohttp_jinja2.template("mailboxes/form.html")
async def mailbox_edit_form(request: web.Request) -> dict:
    pool: DBPool = request.app["db"]
    mid = _match_id(request)
    async with pool.acquire() as db:
        mb = await get_mailbox_by_id(db, mid)
        if mb is None:
            raise web.HTTPNotFound(text="Mailbox not found")
    return {"active_page": "mailboxes", "mailbox": mb, "errors": []}


async def mailbox_edit_submit(request: web.Request) -> web.Response:
    pool: DBPool = request.app["db"]
    mid = _match_id(request)
    data = await request.post()
    async with pool.acquire() as db:
        existing = await get_mailbox_by_id(db, mid)
        if existing is None:
            raise web.HTTPNotFound(text="Mailbox not found")
        mb = Mailbox(
            id=mid,
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
        await upsert_mailbox(db, mb)
    raise web.HTTPFound("/mailboxes")


async def mailbox_deactivate(request: web.Request) -> web.Response:
    pool: DBPool = request.app["db"]
    mid = _match_id(request)
    async with pool.acquire() as db:
        await deactivate_mailbox(db, mid)
    raise web.HTTPFound("/mailboxes")


@aiohttp_jinja2.template("deals/list.html")
async def deals_list(request: web.Request) -> dict:
    pool: DBPool = request.app["db"]
    stage = request.query.get("stage") or None
    async with pool.acquire() as db:
        deals = await get_deals(db, stage=stage)
        stats = await get_deal_stats(db)
    return {
        "active_page": "deals",
        "deals": deals,
        "stats": stats,
        "stage": stage or "",
        "stages": DEAL_STAGES,
    }


@aiohttp_jinja2.template("deals/pipeline.html")
async def deals_pipeline(request: web.Request) -> dict:
    pool: DBPool = request.app["db"]
    async with pool.acquire() as db:
        deals = await get_deals(db)
        stats = await get_deal_stats(db)
    # Group deals by stage
    by_stage: dict[str, list] = {s: [] for s in DEAL_STAGES}
    for d in deals:
        by_stage.setdefault(d.stage, []).append(d)
    return {
        "active_page": "deals",
        "by_stage": by_stage,
        "stats": stats,
        "stages": DEAL_STAGES,
    }


async def deal_create(request: web.Request) -> web.Response:
    pool: DBPool = request.app["db"]
    data = await request.post()
    deal = Deal(
        lead_id=_int(str(data.get("lead_id", "0"))),
        stage=str(data.get("stage", "lead")),
        value=float(data.get("value", 0) or 0),
        notes=str(data.get("notes", "")),
    )
    async with pool.acquire() as db:
        await upsert_deal(db, deal)
    raise web.HTTPFound("/deals")


async def deal_move(request: web.Request) -> web.Response:
    pool: DBPool = request.app["db"]
    did = _match_id(request)
    data = await request.post()
    new_stage = str(data.get("stage", ""))
    if new_stage not in DEAL_STAGES:
        raise web.HTTPNotFound(text="Invalid stage")
    async with pool.acquire() as db:
        deal = await get_deal_by_id(db, did)
        if deal is None:
            raise web.HTTPNotFound(text="Deal not found")
        deal.stage = new_stage
        await upsert_deal(db, deal)
    raise web.HTTPFound("/deals")


@aiohttp_jinja2.template("emails/list.html")
async def emails_list(request: web.Request) -> dict:
    pool: DBPool = request.app["db"]
    page = max(1, _int(request.query.get("page"), 1))
    status = request.query.get("status") or None
    offset = (page - 1) * PAGE_SIZE
    async with pool.acquire() as db:
        emails = await get_emails_sent(db, limit=PAGE_SIZE, offset=offset, status=status)
        total = await count_emails_sent(db, status=status)
        email_dist = await get_email_status_distribution(db)
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    return {
        "active_page": "emails",
        "emails": emails,
        "total": total,
        "page": page,
        "total_pages": total_pages,
        "status": status or "",
        "email_dist": email_dist,
    }


@aiohttp_jinja2.template("settings.html")
async def settings_view(request: web.Request) -> dict:
    settings = load_settings()
    return {"active_page": "settings", "settings": settings, "mask": _mask}


# ---------------------------------------------------------------------------
# 404 handler
# ---------------------------------------------------------------------------


async def handle_404(request: web.Request) -> web.Response:
    return aiohttp_jinja2.render_template(
        "errors/404.html", request, {"active_page": ""}, status=404
    )


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------


async def create_app() -> web.Application:
    app = web.Application(
        middlewares=[
            error_middleware,
            security_headers_middleware,
            rate_limit_middleware,
            csrf_middleware,
            auth_middleware,
        ]
    )

    env = aiohttp_jinja2.setup(
        app,
        loader=jinja2.FileSystemLoader(str(TEMPLATES_DIR)),
        context_processors=[jinja2_context_processor, aiohttp_jinja2.request_processor],
    )

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    # Auth routes
    app.router.add_get("/auth/register", auth_register)
    app.router.add_post("/auth/register/begin", auth_register_begin)
    app.router.add_post("/auth/register/complete", auth_register_complete)
    app.router.add_get("/auth/login", auth_login)
    app.router.add_post("/auth/login/begin", auth_login_begin)
    app.router.add_post("/auth/login/complete", auth_login_complete)
    app.router.add_post("/auth/logout", auth_logout)

    # Onboarding routes
    app.router.add_get("/onboarding", onboarding_page)
    app.router.add_post("/onboarding/settings", onboarding_settings)
    app.router.add_post("/onboarding/mailbox", onboarding_mailbox)
    app.router.add_post("/onboarding/complete", onboarding_complete)

    # App routes
    app.router.add_get("/", dashboard)
    app.router.add_get("/leads", leads_list)
    app.router.add_get("/leads/search", leads_search)
    app.router.add_get("/leads/export", leads_export)
    app.router.add_get("/leads/{id}", lead_detail)
    app.router.add_get("/campaigns", campaigns_list)
    app.router.add_get("/campaigns/{id}", campaign_detail)
    app.router.add_post("/campaigns/{id}/pause", campaign_pause)
    app.router.add_post("/campaigns/{id}/resume", campaign_resume)
    app.router.add_post("/campaigns/{id}/delete", campaign_delete)
    app.router.add_get("/mailboxes", mailboxes_list)
    app.router.add_get("/mailboxes/add", mailbox_add_form)
    app.router.add_post("/mailboxes/add", mailbox_add_submit)
    app.router.add_get("/mailboxes/{id}/edit", mailbox_edit_form)
    app.router.add_post("/mailboxes/{id}/edit", mailbox_edit_submit)
    app.router.add_post("/mailboxes/{id}/deactivate", mailbox_deactivate)
    app.router.add_get("/deals", deals_list)
    app.router.add_get("/deals/pipeline", deals_pipeline)
    app.router.add_post("/deals/create", deal_create)
    app.router.add_post("/deals/{id}/move", deal_move)
    app.router.add_get("/emails", emails_list)
    app.router.add_get("/settings", settings_view)

    return app
