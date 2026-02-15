"""Auth controller — WebAuthn passkey registration and login."""

from __future__ import annotations

import base64
import hashlib
import logging
import secrets
from datetime import UTC, datetime, timedelta

from litestar import Controller, Request, Response, get, post
from litestar.response import Redirect, Template

from config.settings import load_settings
from db.queries import (
    create_session,
    create_user,
    delete_session,
    get_user_count,
    update_user_sign_count,
)
from db.tables import User
from web.middleware.auth import SESSION_COOKIE, SESSION_DURATION_HOURS

log = logging.getLogger(__name__)


class AuthController(Controller):
    path = "/auth"
    tags = ["auth"]

    @get("/register")
    async def register_page(self) -> Template | Redirect:
        user_count = await get_user_count()
        if user_count > 0:
            return Redirect(path="/auth/login")
        return Template(template_name="auth/register.html")

    @post("/register/begin")
    async def register_begin(self, request: Request) -> Response:
        from webauthn import generate_registration_options, options_to_json
        from webauthn.helpers.structs import (
            AuthenticatorSelectionCriteria,
            ResidentKeyRequirement,
            UserVerificationRequirement,
        )

        if await get_user_count() > 0:
            return Response(content={"error": "User already exists"}, status_code=400)

        data = await request.json()
        username = data.get("username", "").strip()
        if not username:
            return Response(content={"error": "Username required"}, status_code=400)

        settings = load_settings()
        options = generate_registration_options(
            rp_id=settings.web.rp_id,
            rp_name=settings.web.rp_name,
            user_name=username,
            user_id=hashlib.sha256(username.encode()).digest(),
            authenticator_selection=AuthenticatorSelectionCriteria(
                resident_key=ResidentKeyRequirement.PREFERRED,
                user_verification=UserVerificationRequirement.PREFERRED,
            ),
        )

        nonce = secrets.token_urlsafe(32)
        state = request.app.state
        if not hasattr(state, "webauthn_challenges"):
            state.webauthn_challenges = {}
        state.webauthn_challenges[nonce] = {
            "challenge": base64.urlsafe_b64encode(options.challenge).decode(),
            "username": username,
        }

        response = Response(
            content=options_to_json(options),
            media_type="application/json",
            status_code=200,
        )
        response.set_cookie("webauthn_nonce", nonce, httponly=True, samesite="strict", max_age=300)
        return response

    @post("/register/complete")
    async def register_complete(self, request: Request) -> Response:
        import webauthn
        from webauthn.helpers import base64url_to_bytes

        nonce = request.cookies.get("webauthn_nonce", "")
        state = request.app.state
        challenges = getattr(state, "webauthn_challenges", {})
        challenge_data = challenges.pop(nonce, None)
        if not challenge_data:
            return Response(content={"error": "Challenge expired"}, status_code=400)

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
            return Response(content={"error": "Registration failed"}, status_code=400)

        credential_id = base64.urlsafe_b64encode(verification.credential_id).decode()
        public_key = base64.urlsafe_b64encode(verification.credential_public_key).decode()

        user_id = await create_user(
            User(
                username=challenge_data["username"],
                webauthn_credential_id=credential_id,
                webauthn_public_key=public_key,
                webauthn_sign_count=verification.sign_count,
            ),
        )

        token = secrets.token_urlsafe(48)
        expires = (datetime.now(UTC) + timedelta(hours=SESSION_DURATION_HOURS)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        await create_session(token=token, user_id=user_id, expires_at=expires)

        response = Response(content={"ok": True, "redirect": "/onboarding"}, status_code=200)
        response.set_cookie(
            SESSION_COOKIE,
            token,
            httponly=True,
            samesite="strict",
            max_age=SESSION_DURATION_HOURS * 3600,
        )
        response.delete_cookie("webauthn_nonce")
        return response

    @get("/login")
    async def login_page(self) -> Template:
        return Template(template_name="auth/login.html")

    @post("/login/begin")
    async def login_begin(self, request: Request) -> Response:
        from webauthn import generate_authentication_options, options_to_json
        from webauthn.helpers.structs import (
            PublicKeyCredentialDescriptor,
            UserVerificationRequirement,
        )

        settings = load_settings()

        cred_rows = (
            await User.select(User.webauthn_credential_id)
            .where(User.webauthn_credential_id != "")
            .run()
        )
        allow_credentials = []
        for row in cred_rows:
            try:
                cred_bytes = base64.urlsafe_b64decode(row["webauthn_credential_id"] + "==")
                allow_credentials.append(PublicKeyCredentialDescriptor(id=cred_bytes))
            except Exception:
                pass

        options = generate_authentication_options(
            rp_id=settings.web.rp_id,
            allow_credentials=allow_credentials,
            user_verification=UserVerificationRequirement.PREFERRED,
        )

        nonce = secrets.token_urlsafe(32)
        state = request.app.state
        if not hasattr(state, "webauthn_challenges"):
            state.webauthn_challenges = {}
        state.webauthn_challenges[nonce] = {
            "challenge": base64.urlsafe_b64encode(options.challenge).decode(),
        }

        response = Response(
            content=options_to_json(options),
            media_type="application/json",
            status_code=200,
        )
        response.set_cookie("webauthn_nonce", nonce, httponly=True, samesite="strict", max_age=300)
        return response

    @post("/login/complete")
    async def login_complete(self, request: Request) -> Response:
        import webauthn
        from webauthn.helpers import base64url_to_bytes

        nonce = request.cookies.get("webauthn_nonce", "")
        state = request.app.state
        challenges = getattr(state, "webauthn_challenges", {})
        challenge_data = challenges.pop(nonce, None)
        if not challenge_data:
            return Response(content={"error": "Challenge expired"}, status_code=400)

        settings = load_settings()
        body = await request.json()

        raw_id = body.get("rawId", body.get("id", ""))

        rows = await User.select(
            User.id,
            User.webauthn_credential_id,
            User.webauthn_public_key,
            User.webauthn_sign_count,
        ).run()

        user_row = None
        for row in rows:
            if row["webauthn_credential_id"] == raw_id or row["webauthn_credential_id"].rstrip(
                "="
            ) == raw_id.rstrip("="):
                user_row = row
                break

        if user_row is None:
            return Response(content={"error": "Unknown credential"}, status_code=400)

        user_id = user_row["id"]
        public_key_b64 = user_row["webauthn_public_key"]
        sign_count = user_row["webauthn_sign_count"]

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
            return Response(content={"error": "Authentication failed"}, status_code=400)

        await update_user_sign_count(user_id, verification.new_sign_count)

        token = secrets.token_urlsafe(48)
        expires = (datetime.now(UTC) + timedelta(hours=SESSION_DURATION_HOURS)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        await create_session(token=token, user_id=user_id, expires_at=expires)

        response = Response(content={"ok": True, "redirect": "/"}, status_code=200)
        response.set_cookie(
            SESSION_COOKIE,
            token,
            httponly=True,
            samesite="strict",
            max_age=SESSION_DURATION_HOURS * 3600,
        )
        response.delete_cookie("webauthn_nonce")
        return response

    @post("/logout")
    async def logout(self, request: Request) -> Redirect:
        token = request.cookies.get(SESSION_COOKIE, "")
        if token:
            await delete_session(token)
            state = request.app.state
            csrf_secrets = getattr(state, "csrf_secrets", {})
            csrf_secrets.pop(token, None)
        response = Redirect(path="/auth/login")
        response.delete_cookie(SESSION_COOKIE)
        return response
