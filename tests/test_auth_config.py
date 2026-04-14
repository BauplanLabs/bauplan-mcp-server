import pytest

from mcp_bauplan.auth.config import get_auth_mode, load_oauth_config


def test_auth_mode_defaults_to_none(monkeypatch):
    monkeypatch.delenv("MCP_AUTH_MODE", raising=False)

    assert get_auth_mode() == "none"


def test_unknown_auth_mode_fails(monkeypatch):
    monkeypatch.setenv("MCP_AUTH_MODE", "unknown")

    with pytest.raises(ValueError, match="MCP_AUTH_MODE"):
        get_auth_mode()


def test_oauth_config_requires_strong_secret(monkeypatch):
    monkeypatch.setenv("MCP_PUBLIC_BASE_URL", "https://mcp.example.com/")
    monkeypatch.setenv("MCP_OAUTH_SECRET", "short")

    with pytest.raises(ValueError, match="MCP_OAUTH_SECRET"):
        load_oauth_config()


def test_oauth_config_normalizes_base_url(monkeypatch, tmp_path):
    monkeypatch.setenv("MCP_PUBLIC_BASE_URL", "https://mcp.example.com/")
    monkeypatch.setenv("MCP_OAUTH_SECRET", "x" * 32)
    monkeypatch.setenv("MCP_OAUTH_CLIENT_REGISTRATION_TTL_SECONDS", "86400")

    config = load_oauth_config()

    assert config.base_url == "https://mcp.example.com"
    assert config.secret == "x" * 32
    assert config.client_registration_ttl_seconds == 86400
