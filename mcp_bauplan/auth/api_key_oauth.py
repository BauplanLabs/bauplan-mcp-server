from __future__ import annotations

import asyncio
import html
import logging
import secrets
import time
from collections.abc import Awaitable, Callable
from typing import Any

from authlib.jose import JsonWebToken
from authlib.jose.errors import JoseError
from cryptography.fernet import Fernet, InvalidToken
from fastmcp.server.auth.auth import AccessToken, ClientRegistrationOptions, OAuthProvider
from fastmcp.server.auth.jwt_issuer import JWTIssuer, derive_jwt_key
from mcp.server.auth.provider import (
    AuthorizationCode,
    AuthorizationParams,
    AuthorizeError,
    RefreshToken,
    TokenError,
    construct_redirect_uri,
)
from mcp.shared.auth import OAuthClientInformationFull, OAuthToken
from pydantic import AnyUrl, TypeAdapter, ValidationError
from starlette.requests import Request
from starlette.responses import HTMLResponse, RedirectResponse, Response
from starlette.routing import Route

from .config import BAUPLAN_API_KEY_CLAIM, OAuthConfig

logger = logging.getLogger(__name__)

_KEY_ENTRY_PATH = "/authorize/key"
_ENCRYPTED_API_KEY_CLAIM = "bauplan_api_key_enc"
_TOKEN_USE_CLAIM = "token_use"
_CLIENT_REGISTRATION_USE = "client-registration"

_SIGNING_SALT = "bauplan-mcp-oauth-signing"
_STORAGE_SALT = "bauplan-mcp-api-key-storage"
_AUTH_CODE_TTL_SECONDS = 60
_TXN_TTL_SECONDS = 15 * 60
_PUBLIC_CLIENT_AUTH_METHOD = "none"

_ANY_URL = TypeAdapter(AnyUrl)
_NO_STORE_HEADERS = {
    "Cache-Control": "no-store",
    "Pragma": "no-cache",
    "Referrer-Policy": "no-referrer",
    "X-Content-Type-Options": "nosniff",
}
_HTML_SECURITY_HEADERS = {
    **_NO_STORE_HEADERS,
    "Content-Security-Policy": "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; frame-ancestors 'none'",
}

ValidateApiKey = Callable[[str], bool | Awaitable[bool]]


class APIKeyOAuthProvider(OAuthProvider):
    """OAuth provider whose consent page collects a Bauplan API key.

    OAuth clients, authorization codes, and tokens are stateless signed
    containers. The Bauplan API key is encrypted into opaque JWT claims and is
    validated only when the user authorizes or refreshes the MCP token.
    """

    def __init__(
        self,
        *,
        config: OAuthConfig,
        validate_api_key: ValidateApiKey,
    ) -> None:
        super().__init__(
            base_url=config.base_url,
            client_registration_options=ClientRegistrationOptions(
                enabled=True,
                valid_scopes=[],
                default_scopes=[],
            ),
        )
        self._validate_api_key = validate_api_key
        self._access_token_ttl_seconds = config.access_token_ttl_seconds
        self._refresh_token_ttl_seconds = config.refresh_token_ttl_seconds
        self._client_registration_ttl_seconds = config.client_registration_ttl_seconds
        self._base_url = config.base_url
        self._audience = f"{config.base_url.rstrip('/')}/mcp"
        self._jwt = JsonWebToken(["HS256"])
        self._signing_key = derive_jwt_key(
            low_entropy_material=config.secret,
            salt=_SIGNING_SALT,
        )
        self._fernet = Fernet(
            derive_jwt_key(
                low_entropy_material=config.secret,
                salt=_STORAGE_SALT,
            )
        )
        self._issuer = JWTIssuer(
            issuer=config.base_url,
            audience=self._audience,
            signing_key=self._signing_key,
        )
        self._key_entry_url = f"{config.base_url.rstrip('/')}{_KEY_ENTRY_PATH}"

    async def get_client(self, client_id: str) -> OAuthClientInformationFull | None:
        rec = self._load_container_token(client_id, expected_use=_CLIENT_REGISTRATION_USE)
        if rec is None:
            return None
        return _client_from_claims(client_id, rec)

    async def register_client(self, client_info: OAuthClientInformationFull) -> None:
        issued_at = int(time.time())
        client_info.token_endpoint_auth_method = _PUBLIC_CLIENT_AUTH_METHOD
        client_info.client_secret = None
        client_info.client_secret_expires_at = None
        client_info.client_id_issued_at = issued_at
        client_info.client_id = self._issue_container_token(
            container_use=_CLIENT_REGISTRATION_USE,
            expires_in=self._client_registration_ttl_seconds,
            claims=_client_to_claims(client_info, issued_at),
        )

    async def authorize(self, client: OAuthClientInformationFull, params: AuthorizationParams) -> str:
        if client.client_id is None:
            raise AuthorizeError("invalid_request", "client_id is required")
        if params.resource and params.resource.rstrip("/") != self._audience:
            raise AuthorizeError("invalid_request", "resource must match the MCP protected resource")

        txn_token = self._issue_container_token(
            container_use="auth-txn",
            expires_in=_TXN_TTL_SECONDS,
            claims={
                "client_id": client.client_id,
                "client_name": client.client_name or "MCP client",
                "redirect_uri": str(params.redirect_uri),
                "redirect_uri_provided_explicitly": params.redirect_uri_provided_explicitly,
                "state": params.state or "",
                "code_challenge": params.code_challenge,
                "scopes": params.scopes or [],
                "resource": params.resource or "",
            },
        )
        return f"{self._key_entry_url}?txn_id={txn_token}"

    async def _render_form(self, request: Request) -> Response:
        txn_id = request.query_params.get("txn_id", "")
        txn = self._load_container_token(txn_id, expected_use="auth-txn")
        if txn is None:
            return _html_response(
                "Authorization request expired. Please reconnect from your MCP client.", 400
            )

        client_name = html.escape(str(txn.get("client_name") or "MCP client"))
        escaped_txn_id = html.escape(txn_id)
        page = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Bauplan MCP Authorization</title>
</head>
<body>
  <h1>Authorize Bauplan MCP</h1>
  <p><strong>{client_name}</strong> wants to connect to your Bauplan account.</p>
  <form method="post" action="{_KEY_ENTRY_PATH}">
    <input type="hidden" name="txn_id" value="{escaped_txn_id}">
    <label for="api_key">Bauplan API key</label>
    <input id="api_key" name="api_key" type="password" autocomplete="off" required autofocus>
    <button type="submit">Authorize</button>
  </form>
</body>
</html>"""
        return HTMLResponse(page, headers=_HTML_SECURITY_HEADERS)

    async def _handle_submit(self, request: Request) -> Response:
        sec_fetch_site = request.headers.get("sec-fetch-site")
        if sec_fetch_site not in (None, "same-origin", "none"):
            return _html_response("Cross-site authorization blocked.", 403)

        form = await request.form()
        txn_id = str(form.get("txn_id") or "")
        api_key = str(form.get("api_key") or "").strip()
        txn = self._load_container_token(txn_id, expected_use="auth-txn")
        if txn is None:
            return _html_response(
                "Authorization request expired. Please reconnect from your MCP client.", 400
            )
        if not api_key:
            return _html_response("A Bauplan API key is required.", 400)

        if not await self._call_validate_api_key(api_key):
            logger.info("Rejected Bauplan API key during OAuth authorization")
            return _html_response("The provided Bauplan API key could not be validated.", 401)

        code = self._issue_container_token(
            container_use="auth-code",
            expires_in=_AUTH_CODE_TTL_SECONDS,
            claims={
                **txn,
                "encrypted_api_key": self._encrypt_api_key(api_key),
            },
        )
        location = construct_redirect_uri(
            str(txn["redirect_uri"]), code=code, state=str(txn.get("state") or "")
        )
        return RedirectResponse(location, status_code=303, headers=_NO_STORE_HEADERS)

    async def _call_validate_api_key(self, api_key: str) -> bool:
        result = self._validate_api_key(api_key)
        if asyncio.iscoroutine(result) or isinstance(result, Awaitable):
            result = await result
        return bool(result)

    async def load_authorization_code(
        self,
        client: OAuthClientInformationFull,
        authorization_code: str,
    ) -> AuthorizationCode | None:
        rec = self._load_container_token(authorization_code, expected_use="auth-code")
        if rec is None or rec.get("client_id") != client.client_id:
            return None

        return AuthorizationCode(
            code=authorization_code,
            client_id=str(rec["client_id"]),
            redirect_uri=rec["redirect_uri"],
            redirect_uri_provided_explicitly=bool(rec["redirect_uri_provided_explicitly"]),
            scopes=list(rec.get("scopes") or []),
            expires_at=float(rec["exp"]),
            code_challenge=str(rec["code_challenge"]),
            resource=str(rec["resource"]) or None,
        )

    async def exchange_authorization_code(
        self,
        client: OAuthClientInformationFull,
        authorization_code: AuthorizationCode,
    ) -> OAuthToken:
        rec = self._load_container_token(authorization_code.code, expected_use="auth-code")
        if rec is None or rec.get("client_id") != client.client_id:
            raise TokenError("invalid_grant", "Authorization code not found or used.")

        return await self._issue_tokens(
            encrypted_api_key=str(rec["encrypted_api_key"]),
            client_id=authorization_code.client_id,
            scopes=authorization_code.scopes,
        )

    async def load_refresh_token(
        self,
        client: OAuthClientInformationFull,
        refresh_token: str,
    ) -> RefreshToken | None:
        try:
            payload = self._issuer.verify_token(refresh_token)
        except JoseError:
            return None
        if payload.get(_TOKEN_USE_CLAIM) != "refresh" or payload.get("client_id") != client.client_id:
            return None
        if self._decrypt_api_key(payload) is None:
            return None
        return RefreshToken(
            token=refresh_token,
            client_id=str(payload["client_id"]),
            scopes=str(payload.get("scope") or "").split(),
            expires_at=int(payload["exp"]),
        )

    async def exchange_refresh_token(
        self,
        client: OAuthClientInformationFull,
        refresh_token: RefreshToken,
        scopes: list[str],
    ) -> OAuthToken:
        if not set(scopes).issubset(set(refresh_token.scopes)):
            raise TokenError("invalid_scope", "Requested scopes exceed the original authorization.")

        try:
            old_payload = self._issuer.verify_token(refresh_token.token)
        except JoseError as e:
            raise TokenError("invalid_grant", "Refresh token invalid.") from e
        api_key = self._decrypt_api_key(old_payload)
        if api_key is None or old_payload.get("client_id") != client.client_id:
            raise TokenError("invalid_grant", "Refresh token invalid.")
        if not await self._call_validate_api_key(api_key):
            raise TokenError("invalid_grant", "Bauplan API key is no longer valid.")

        return await self._issue_tokens(
            encrypted_api_key=self._encrypt_api_key(api_key),
            client_id=client.client_id or refresh_token.client_id,
            scopes=scopes,
        )

    async def load_access_token(self, token: str) -> AccessToken | None:
        try:
            payload = self._issuer.verify_token(token)
        except JoseError:
            return None
        if payload.get(_TOKEN_USE_CLAIM) != "access":
            return None

        api_key = self._decrypt_api_key(payload)
        if api_key is None:
            return None

        return AccessToken(
            token=token,
            client_id=str(payload["client_id"]),
            scopes=str(payload.get("scope") or "").split(),
            expires_at=int(payload["exp"]),
            claims={BAUPLAN_API_KEY_CLAIM: api_key},
        )

    async def revoke_token(self, token: AccessToken | RefreshToken) -> None:
        return None

    def get_routes(self, mcp_path: str | None = None) -> list[Route]:
        routes = super().get_routes(mcp_path)
        routes.append(Route(_KEY_ENTRY_PATH, self._render_form, methods=["GET"]))
        routes.append(Route(_KEY_ENTRY_PATH, self._handle_submit, methods=["POST"]))
        return routes

    async def _issue_tokens(self, *, encrypted_api_key: str, client_id: str, scopes: list[str]) -> OAuthToken:
        access_token = self._issue_token(
            client_id=client_id,
            scopes=scopes,
            encrypted_api_key=encrypted_api_key,
            token_use="access",
            expires_in=self._access_token_ttl_seconds,
        )
        refresh_token = self._issue_token(
            client_id=client_id,
            scopes=scopes,
            encrypted_api_key=encrypted_api_key,
            token_use="refresh",
            expires_in=self._refresh_token_ttl_seconds,
        )
        return OAuthToken(
            access_token=access_token,
            refresh_token=refresh_token,
            token_type="Bearer",
            expires_in=self._access_token_ttl_seconds,
            scope=" ".join(scopes),
        )

    def _issue_token(
        self,
        *,
        client_id: str,
        scopes: list[str],
        encrypted_api_key: str,
        token_use: str,
        expires_in: int,
    ) -> str:
        now = int(time.time())
        payload = {
            "iss": self._base_url,
            "aud": self._audience,
            "client_id": client_id,
            "scope": " ".join(scopes),
            "exp": now + expires_in,
            "iat": now,
            "jti": secrets.token_urlsafe(24),
            _TOKEN_USE_CLAIM: token_use,
            _ENCRYPTED_API_KEY_CLAIM: encrypted_api_key,
        }
        return self._jwt.encode({"alg": "HS256", "typ": "JWT"}, payload, self._signing_key).decode("utf-8")

    def _encrypt_api_key(self, api_key: str) -> str:
        return self._fernet.encrypt(api_key.encode("utf-8")).decode("ascii")

    def _decrypt_api_key(self, payload: dict[str, Any]) -> str | None:
        encrypted_api_key = payload.get(_ENCRYPTED_API_KEY_CLAIM)
        if not isinstance(encrypted_api_key, str):
            return None
        try:
            return self._fernet.decrypt(encrypted_api_key.encode("ascii")).decode("utf-8")
        except (InvalidToken, UnicodeDecodeError, UnicodeEncodeError):
            return None

    def _issue_container_token(self, *, container_use: str, expires_in: int, claims: dict[str, Any]) -> str:
        now = int(time.time())
        payload = {
            **claims,
            "iss": self._base_url,
            "aud": self._audience,
            "exp": now + expires_in,
            "iat": now,
            "jti": secrets.token_urlsafe(24),
            "container_use": container_use,
        }
        return self._jwt.encode({"alg": "HS256", "typ": "JWT"}, payload, self._signing_key).decode("utf-8")

    def _load_container_token(self, token: str, *, expected_use: str) -> dict[str, Any] | None:
        try:
            payload = self._issuer.verify_token(token)
        except JoseError:
            return None
        if payload.get("container_use") != expected_use:
            return None
        return dict(payload)


async def validate_bauplan_api_key(api_key: str) -> bool:
    """Validate a Bauplan API key without leaking exception details to clients."""
    try:
        import bauplan

        await asyncio.to_thread(bauplan.Client(api_key=api_key).info)
    except Exception:
        return False
    return True


def create_api_key_oauth_provider(config: OAuthConfig) -> APIKeyOAuthProvider:
    return APIKeyOAuthProvider(config=config, validate_api_key=validate_bauplan_api_key)


def _html_response(content: str, status_code: int) -> HTMLResponse:
    return HTMLResponse(content, status_code, headers=_HTML_SECURITY_HEADERS)


def _client_to_claims(client_info: OAuthClientInformationFull, issued_at: int) -> dict[str, Any]:
    return {
        "client_id_issued_at": issued_at,
        "redirect_uris": [str(uri) for uri in client_info.redirect_uris or []],
        "grant_types": list(client_info.grant_types),
        "response_types": list(client_info.response_types),
        "scope": client_info.scope or "",
        "client_name": client_info.client_name or "",
    }


def _client_from_claims(client_id: str, claims: dict[str, Any]) -> OAuthClientInformationFull | None:
    try:
        return OAuthClientInformationFull(
            client_id=client_id,
            client_id_issued_at=int(claims["client_id_issued_at"]),
            client_secret=None,
            client_secret_expires_at=None,
            redirect_uris=[_ANY_URL.validate_python(uri) for uri in claims.get("redirect_uris") or []],
            token_endpoint_auth_method=_PUBLIC_CLIENT_AUTH_METHOD,
            grant_types=list(claims.get("grant_types") or ["authorization_code", "refresh_token"]),
            response_types=list(claims.get("response_types") or ["code"]),
            scope=str(claims.get("scope") or "") or None,
            client_name=str(claims.get("client_name") or "") or None,
        )
    except (KeyError, TypeError, ValueError, ValidationError):
        return None
