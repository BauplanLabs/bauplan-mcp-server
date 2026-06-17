from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Literal
from urllib.parse import urlsplit, urlunsplit

AuthMode = Literal["none", "api-key-oauth"]

MIN_SECRET_LENGTH = 32
API_KEY_OAUTH_MODE: AuthMode = "api-key-oauth"
BAUPLAN_API_KEY_CLAIM = "bauplan_api_key"
DEFAULT_TRUSTED_REDIRECTS = (
    "https://claude.ai/api/mcp/auth_callback",
    "https://chatgpt.com/connector/oauth/*",
    "https://chatgpt.com/connector_platform_oauth_redirect",
)


@dataclass(frozen=True)
class OAuthConfig:
    base_url: str
    secret: str
    access_token_ttl_seconds: int = 15 * 60
    refresh_token_ttl_seconds: int = 60 * 60 * 24 * 7
    client_registration_ttl_seconds: int = 60 * 60 * 24 * 30
    trusted_redirects: tuple[str, ...] = DEFAULT_TRUSTED_REDIRECTS


def get_auth_mode() -> AuthMode:
    mode = os.getenv("MCP_AUTH_MODE", "none").strip().lower()
    if mode in ("", "none"):
        if os.getenv("MCP_PUBLIC_BASE_URL", "").strip():
            raise ValueError("MCP_AUTH_MODE must be 'api-key-oauth' when MCP_PUBLIC_BASE_URL is set")
        return "none"
    if mode == API_KEY_OAUTH_MODE:
        return API_KEY_OAUTH_MODE
    raise ValueError("MCP_AUTH_MODE must be either 'none' or 'api-key-oauth'")


def load_oauth_config() -> OAuthConfig:
    base_url = _normalize_base_url(_required_env("MCP_PUBLIC_BASE_URL"))
    secret = _required_env("MCP_OAUTH_SECRET")
    if len(secret) < MIN_SECRET_LENGTH:
        raise ValueError(f"MCP_OAUTH_SECRET must be at least {MIN_SECRET_LENGTH} characters")

    return OAuthConfig(
        base_url=base_url,
        secret=secret,
        access_token_ttl_seconds=_positive_int_env("MCP_OAUTH_ACCESS_TOKEN_TTL_SECONDS", 15 * 60),
        refresh_token_ttl_seconds=_positive_int_env(
            "MCP_OAUTH_REFRESH_TOKEN_TTL_SECONDS",
            60 * 60 * 24 * 7,
        ),
        client_registration_ttl_seconds=_positive_int_env(
            "MCP_OAUTH_CLIENT_REGISTRATION_TTL_SECONDS",
            60 * 60 * 24 * 30,
        ),
        trusted_redirects=_csv_env("MCP_OAUTH_TRUSTED_REDIRECTS", DEFAULT_TRUSTED_REDIRECTS),
    )


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or not value.strip():
        raise ValueError(f"{name} is required when MCP_AUTH_MODE=api-key-oauth")
    return value.strip()


def _normalize_base_url(value: str) -> str:
    parts = urlsplit(value.strip().rstrip("/"))
    netloc = parts.netloc
    if (parts.scheme, parts.port) in (("https", 443), ("http", 80)):
        netloc = parts.hostname or netloc
    return urlunsplit((parts.scheme, netloc, parts.path, "", ""))


def _positive_int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    try:
        parsed = int(value)
    except ValueError as e:
        raise ValueError(f"{name} must be a positive integer") from e
    if parsed <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return parsed


def _csv_env(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return tuple(item.strip() for item in value.split(",") if item.strip())
