from types import SimpleNamespace
from typing import ClassVar

import pytest

import mcp_bauplan.tools.create_client as create_client
from mcp_bauplan.auth.config import BAUPLAN_API_KEY_CLAIM


class FakeBauplanClient:
    api_keys: ClassVar[list[str | None]] = []

    def __init__(self, api_key=None):
        self.api_key = api_key
        self.api_keys.append(api_key)


def test_get_bauplan_client_uses_bauplan_header(monkeypatch):
    FakeBauplanClient.api_keys = []
    monkeypatch.setattr(create_client.bauplan, "Client", FakeBauplanClient)
    monkeypatch.setattr(
        create_client,
        "get_http_request",
        lambda: SimpleNamespace(headers={"Bauplan": "header-key", "Authorization": "Bearer auth-key"}),
    )

    create_client.get_bauplan_client()

    assert FakeBauplanClient.api_keys[-1] == "header-key"


def test_get_bauplan_client_uses_oauth_claim_in_oauth_mode(monkeypatch):
    FakeBauplanClient.api_keys = []
    monkeypatch.setenv("MCP_AUTH_MODE", "api-key-oauth")
    monkeypatch.setattr(create_client.bauplan, "Client", FakeBauplanClient)
    monkeypatch.setattr(
        create_client,
        "get_http_request",
        lambda: SimpleNamespace(headers={"Authorization": "Bearer oauth-jwt"}),
    )
    monkeypatch.setattr(
        create_client,
        "get_access_token",
        lambda: SimpleNamespace(token="oauth-token", claims={BAUPLAN_API_KEY_CLAIM: "oauth-key"}),
    )

    create_client.get_bauplan_client()

    assert FakeBauplanClient.api_keys[-1] == "oauth-key"


def test_get_bauplan_client_does_not_use_authorization_header_in_oauth_mode(monkeypatch):
    FakeBauplanClient.api_keys = []
    monkeypatch.setenv("MCP_AUTH_MODE", "api-key-oauth")
    monkeypatch.setattr(create_client.bauplan, "Client", FakeBauplanClient)
    monkeypatch.setattr(
        create_client,
        "get_http_request",
        lambda: SimpleNamespace(headers={"Authorization": "Bearer oauth-jwt"}),
    )
    monkeypatch.setattr(
        create_client,
        "get_access_token",
        lambda: SimpleNamespace(token="oauth-jwt", claims={}),
    )

    with pytest.raises(RuntimeError, match="User is not authenticated"):
        create_client.get_bauplan_client()

    assert FakeBauplanClient.api_keys == []


def test_get_bauplan_client_reports_unauthenticated_when_oauth_context_is_missing(monkeypatch):
    FakeBauplanClient.api_keys = []
    monkeypatch.setenv("MCP_AUTH_MODE", "api-key-oauth")
    monkeypatch.setattr(create_client.bauplan, "Client", FakeBauplanClient)
    monkeypatch.setattr(
        create_client,
        "get_http_request",
        lambda: SimpleNamespace(headers={"Authorization": "Bearer oauth-jwt"}),
    )
    monkeypatch.setattr(
        create_client,
        "get_access_token",
        lambda: (_ for _ in ()).throw(RuntimeError("missing auth context")),
    )

    with pytest.raises(RuntimeError, match="User is not authenticated"):
        create_client.get_bauplan_client()

    assert FakeBauplanClient.api_keys == []


def test_get_bauplan_client_uses_default_credentials_for_http_without_headers(monkeypatch):
    FakeBauplanClient.api_keys = []
    monkeypatch.delenv("MCP_AUTH_MODE", raising=False)
    monkeypatch.setattr(create_client.bauplan, "Client", FakeBauplanClient)
    monkeypatch.setattr(
        create_client,
        "get_http_request",
        lambda: SimpleNamespace(headers={}),
    )
    monkeypatch.setattr(create_client, "get_access_token", lambda: None)

    create_client.get_bauplan_client()

    assert FakeBauplanClient.api_keys[-1] is None


def test_get_bauplan_client_uses_default_credentials_outside_http_context(monkeypatch):
    FakeBauplanClient.api_keys = []
    monkeypatch.setattr(create_client.bauplan, "Client", FakeBauplanClient)
    monkeypatch.setattr(
        create_client, "get_http_request", lambda: (_ for _ in ()).throw(RuntimeError("no http"))
    )

    create_client.get_bauplan_client()

    assert FakeBauplanClient.api_keys[-1] is None
