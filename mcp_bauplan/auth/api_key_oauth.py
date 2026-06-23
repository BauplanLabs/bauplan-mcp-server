from __future__ import annotations

import asyncio
import html
import logging
import secrets
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from functools import lru_cache
from importlib.resources import files
from string import Template
from typing import Any, cast
from urllib.parse import SplitResult, urlsplit

from cryptography.fernet import Fernet, InvalidToken
from fastmcp.server.auth.auth import AccessToken, ClientRegistrationOptions, OAuthProvider
from fastmcp.server.auth.jwt_issuer import derive_jwt_key
from joserfc import jwk, jwt
from joserfc.errors import JoseError
from mcp.server.auth.provider import (
    AuthorizationCode,
    AuthorizationParams,
    AuthorizeError,
    RefreshToken,
    RegistrationError,
    TokenError,
    construct_redirect_uri,
)
from mcp.shared.auth import OAuthClientInformationFull, OAuthToken
from pydantic import AnyUrl, TypeAdapter, ValidationError
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, Response
from starlette.routing import Route

from .config import BAUPLAN_API_KEY_CLAIM, OAuthConfig

logger = logging.getLogger(__name__)

_KEY_ENTRY_PATH = "/authorize/key"
_ENCRYPTED_API_KEY_CLAIM = "bauplan_api_key_enc"
_TOKEN_USE_CLAIM = "token_use"
_CLIENT_REGISTRATION_USE = "client-registration"

_SIGNING_SALT = "bauplan-mcp-oauth-signing"
_STORAGE_SALT = "bauplan-mcp-api-key-storage"
_AUTH_CODE_TTL_SECONDS = 5 * 60
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
    "Content-Security-Policy": (
        "default-src 'none'; style-src 'unsafe-inline'; img-src 'self'; "
        "form-action 'self'; frame-ancestors 'none'"
    ),
}


@dataclass(frozen=True)
class BauplanUserInfo:
    username: str | None
    full_name: str | None


ValidateApiKey = Callable[[str], BauplanUserInfo | None | Awaitable[BauplanUserInfo | None]]


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
        self._trusted_redirects = config.trusted_redirects
        self._base_url = config.base_url
        self._audience = f"{config.base_url.rstrip('/')}/mcp"
        self._signing_key = derive_jwt_key(
            low_entropy_material=config.secret,
            salt=_SIGNING_SALT,
        )
        self._jwk = jwk.import_key(self._signing_key, "oct")
        self._fernet = Fernet(
            derive_jwt_key(
                low_entropy_material=config.secret,
                salt=_STORAGE_SALT,
            )
        )
        self._key_entry_url = f"{config.base_url.rstrip('/')}{_KEY_ENTRY_PATH}"

    async def get_client(self, client_id: str) -> OAuthClientInformationFull | None:
        rec = self._load_container_token(client_id, expected_use=_CLIENT_REGISTRATION_USE)
        if rec is None:
            return None
        client = _client_from_claims(client_id, rec)
        if client is None or not _redirect_uris_valid(client.redirect_uris or []):
            return None
        return client

    async def register_client(self, client_info: OAuthClientInformationFull) -> None:
        if not _redirect_uris_valid(client_info.redirect_uris or []):
            raise RegistrationError("invalid_redirect_uri", "redirect_uri is not allowed")

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
        if not _redirect_uri_registered(str(params.redirect_uri), client.redirect_uris or []):
            raise AuthorizeError("invalid_request", "redirect_uri is not registered")
        if not _redirect_uri_valid(str(params.redirect_uri)):
            raise AuthorizeError("invalid_request", "redirect_uri is not allowed")

        txn_token = self._issue_container_token(
            container_use="auth-txn",
            expires_in=_TXN_TTL_SECONDS,
            claims={
                "client_id": client.client_id,
                "client_name": client.client_name or "MCP client",
                "redirect_uri": str(params.redirect_uri),
                "redirect_uri_trusted": _redirect_uri_trusted(
                    str(params.redirect_uri), self._trusted_redirects
                ),
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
        redirect_uri = str(txn.get("redirect_uri") or "")
        redirect_host = html.escape(_redirect_host(redirect_uri) or "unknown destination")
        warning_section = ""
        if not txn.get("redirect_uri_trusted"):
            warning_section = (
                '      <section class="warning" aria-label="Unverified callback destination">\n'
                "        <h2>Unverified callback destination</h2>\n"
                "        <p>This client destination is not verified by Bauplan. "
                "Continue only if you trust it.</p>\n"
                "      </section>\n"
            )
        escaped_txn_id = html.escape(txn_id)
        page = _render_html_template(
            "authorize_form.html",
            client_name=client_name,
            redirect_host=redirect_host,
            warning_section=warning_section,
            key_entry_path=_KEY_ENTRY_PATH,
            escaped_txn_id=escaped_txn_id,
        )
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

        user_info = await self._call_validate_api_key(api_key)
        if user_info is None:
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
        escaped_location = html.escape(location, quote=True)
        redirect_uri = str(txn["redirect_uri"])
        redirect_host = html.escape(_redirect_host(redirect_uri) or "unknown destination")
        client_name = html.escape(str(txn.get("client_name") or "MCP client"))
        if txn.get("redirect_uri_trusted"):
            continue_copy = f"Continue to complete the {client_name} connection."
            button_copy = f"Return to {client_name}"
        else:
            continue_copy = (
                f"This client destination is not verified by Bauplan. "
                f"Only continue if you trust {redirect_host}."
            )
            button_copy = f"Continue to {redirect_host}"
        escaped_username = html.escape(user_info.username) if user_info.username else None
        escaped_full_name = html.escape(user_info.full_name) if user_info.full_name else None
        user_details = ""
        if escaped_username:
            user_details += (
                '          <div class="meta-item">\n'
                '            <p class="meta-label">Username</p>\n'
                f'            <p class="meta-value">{escaped_username}</p>\n'
                "          </div>\n"
            )
        if escaped_full_name:
            user_details += (
                '          <div class="meta-item">\n'
                '            <p class="meta-label">Name</p>\n'
                f'            <p class="meta-value">{escaped_full_name}</p>\n'
                "          </div>\n"
            )
        page = _render_html_template(
            "authorize_complete.html",
            user_details=user_details,
            redirect_host=redirect_host,
            continue_copy=continue_copy,
            escaped_location=escaped_location,
            button_copy=button_copy,
        )
        return HTMLResponse(page, headers=_HTML_SECURITY_HEADERS)

    async def _call_validate_api_key(self, api_key: str) -> BauplanUserInfo | None:
        result = self._validate_api_key(api_key)
        if asyncio.iscoroutine(result) or isinstance(result, Awaitable):
            result = await result
        return cast(BauplanUserInfo | None, result)

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
            payload = self._verify_token(refresh_token)
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
            old_payload = self._verify_token(refresh_token.token)
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
            payload = self._verify_token(token)
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
        routes = [self._public_client_metadata_route(route) for route in routes]
        routes.append(Route("/authorize/favicon.ico", self._favicon, methods=["GET"]))
        routes.append(Route(_KEY_ENTRY_PATH, self._render_form, methods=["GET"]))
        routes.append(Route(_KEY_ENTRY_PATH, self._handle_submit, methods=["POST"]))
        return routes

    async def _favicon(self, _: Request) -> Response:
        return Response(
            _load_binary_asset("favicon.ico"),
            media_type="image/x-icon",
            headers=_NO_STORE_HEADERS,
        )

    def _public_client_metadata_route(self, route: Route) -> Route:
        if route.path != "/.well-known/oauth-authorization-server":
            return route
        return Route(
            route.path,
            self._authorization_server_metadata,
            methods=["GET", "OPTIONS"],
        )

    async def _authorization_server_metadata(self, _: Request) -> Response:
        base_url = self._base_url.rstrip("/")
        metadata: dict[str, Any] = {
            "issuer": base_url,
            "authorization_endpoint": f"{base_url}/authorize",
            "token_endpoint": f"{base_url}/token",
            "registration_endpoint": f"{base_url}/register",
            "scopes_supported": [],
            "response_types_supported": ["code"],
            "grant_types_supported": ["authorization_code", "refresh_token"],
            "token_endpoint_auth_methods_supported": [_PUBLIC_CLIENT_AUTH_METHOD],
            "code_challenge_methods_supported": ["S256"],
        }
        return JSONResponse(
            metadata,
            headers={"Cache-Control": "public, max-age=3600"},
        )

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
        return jwt.encode({"alg": "HS256", "typ": "JWT"}, payload, self._jwk)

    def _verify_token(self, token: str) -> dict[str, Any]:
        claims = jwt.decode(token, self._jwk, algorithms=["HS256"]).claims
        exp = claims.get("exp")
        if not isinstance(exp, int | float) or exp < time.time():
            raise JoseError("Token has expired")
        if claims.get("iss") != self._base_url:
            raise JoseError("Invalid token issuer")
        if claims.get("aud") != self._audience:
            raise JoseError("Invalid token audience")
        return claims

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
        return jwt.encode({"alg": "HS256", "typ": "JWT"}, payload, self._jwk)

    def _load_container_token(self, token: str, *, expected_use: str) -> dict[str, Any] | None:
        try:
            payload = self._verify_token(token)
        except JoseError:
            return None
        if payload.get("container_use") != expected_use:
            return None
        return dict(payload)


async def validate_bauplan_api_key(api_key: str) -> BauplanUserInfo | None:
    """Validate a Bauplan API key without leaking exception details to clients."""
    try:
        import bauplan

        info = await asyncio.to_thread(bauplan.Client(api_key=api_key).info)
        user = info.user
    except Exception:
        return None
    return BauplanUserInfo(
        username=getattr(user, "username", None),
        full_name=getattr(user, "full_name", None),
    )


def create_api_key_oauth_provider(config: OAuthConfig) -> APIKeyOAuthProvider:
    return APIKeyOAuthProvider(config=config, validate_api_key=validate_bauplan_api_key)


def _html_response(content: str, status_code: int) -> HTMLResponse:
    return HTMLResponse(content, status_code, headers=_HTML_SECURITY_HEADERS)


def _render_html_template(template_name: str, **context: str) -> str:
    template = _load_html_template(template_name)
    return Template(template).substitute(
        {
            **context,
            "css_styles": _load_html_template("_styles.css"),
            "bauplan_logo": _load_html_template("_bauplan_logo.svg"),
        }
    )


@lru_cache(maxsize=None)
def _load_html_template(template_name: str) -> str:
    return files("mcp_bauplan.auth").joinpath("templates", template_name).read_text(encoding="utf-8")


@lru_cache(maxsize=None)
def _load_binary_asset(asset_name: str) -> bytes:
    return files("mcp_bauplan.auth").joinpath("templates", asset_name).read_bytes()


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


def _redirect_uris_valid(redirect_uris: list[AnyUrl]) -> bool:
    return bool(redirect_uris) and all(_redirect_uri_valid(str(uri)) for uri in redirect_uris)


def _redirect_uri_valid(redirect_uri: str) -> bool:
    try:
        target = urlsplit(redirect_uri)
        _normalized_port(target)
    except ValueError:
        return False
    if not target.scheme or not target.hostname or target.fragment or target.username or target.password:
        return False

    if target.scheme == "https":
        return True
    return target.scheme == "http" and target.hostname in ("localhost", "127.0.0.1", "::1")


def _redirect_uri_registered(redirect_uri: str, registered_redirect_uris: list[AnyUrl]) -> bool:
    return any(redirect_uri == str(registered_uri) for registered_uri in registered_redirect_uris)


def _redirect_uri_trusted(redirect_uri: str, trusted_redirects: tuple[str, ...]) -> bool:
    try:
        target = urlsplit(redirect_uri)
        _normalized_port(target)
    except ValueError:
        return False
    if not _redirect_uri_valid(redirect_uri):
        return False

    return any(_redirect_rule_matches(target, rule) for rule in trusted_redirects)


def _redirect_rule_matches(target: SplitResult, rule: str) -> bool:
    is_prefix = rule.endswith("*")
    rule_value = rule[:-1] if is_prefix else rule
    try:
        allowed = urlsplit(rule_value)
        target_port = _normalized_port(target)
        allowed_port = _normalized_port(allowed)
    except ValueError:
        return False

    if not allowed.scheme or not allowed.hostname or allowed.fragment or allowed.username or allowed.password:
        return False
    if not target.hostname:
        return False
    if target.scheme != allowed.scheme:
        return False
    if target.hostname.lower() != allowed.hostname.lower():
        return False
    if target_port != allowed_port:
        return False
    if is_prefix:
        return (
            not target.query and target.path.startswith(allowed.path) and len(target.path) > len(allowed.path)
        )
    return target.path == allowed.path and target.query == allowed.query


def _normalized_port(parts: SplitResult) -> int | None:
    port = parts.port
    if (parts.scheme, port) in (("https", 443), ("http", 80)):
        return None
    return port


def _redirect_host(redirect_uri: str) -> str | None:
    try:
        parts = urlsplit(redirect_uri)
        host = parts.hostname
        port = _normalized_port(parts)
    except ValueError:
        return None
    if host is None:
        return None
    return host if port is None else f"{host}:{port}"
