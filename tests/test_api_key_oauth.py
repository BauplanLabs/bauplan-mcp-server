import asyncio
import base64
import json
from collections.abc import MutableMapping
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest
from joserfc import jwt
from mcp.server.auth.provider import (
    AuthorizationParams,
    AuthorizeError,
    RefreshToken,
    RegistrationError,
    TokenError,
)
from mcp.shared.auth import OAuthClientInformationFull
from pydantic import AnyUrl, TypeAdapter
from starlette.requests import Request

from mcp_bauplan.auth.api_key_oauth import APIKeyOAuthProvider, BauplanUserInfo
from mcp_bauplan.auth.config import BAUPLAN_API_KEY_CLAIM, OAuthConfig

_ANY_URL = TypeAdapter(AnyUrl)


def _decode_jwt_payload(token: str) -> dict:
    payload = token.split(".")[1]
    padded = payload + "=" * (-len(payload) % 4)
    return json.loads(base64.urlsafe_b64decode(padded))


def _url(value: str) -> AnyUrl:
    return _ANY_URL.validate_python(value)


def _user_info() -> BauplanUserInfo:
    return BauplanUserInfo(username="test-user", full_name="Test User")


def _localhost_config() -> OAuthConfig:
    return OAuthConfig(
        base_url="https://mcp.example.com",
        secret="x" * 32,
        trusted_redirects=("http://localhost/callback",),
    )


def _form_request(body: bytes) -> Request:
    async def receive() -> MutableMapping[str, Any]:
        return {
            "type": "http.request",
            "body": body,
            "more_body": False,
        }

    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/authorize/key",
            "headers": [
                (b"content-type", b"application/x-www-form-urlencoded"),
                (b"content-length", str(len(body)).encode()),
            ],
        },
        receive=receive,
    )


def _get_request(path: str, query_string: str) -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "query_string": query_string.encode(),
            "headers": [],
        }
    )


def test_api_key_oauth_issues_stateless_encrypted_claim_tokens():
    async def run():
        api_key = "bp_secret_test_key"
        validation_calls = []

        def validate(key: str) -> BauplanUserInfo | None:
            validation_calls.append(key)
            return _user_info() if key == api_key else None

        provider = APIKeyOAuthProvider(
            config=_localhost_config(),
            validate_api_key=validate,
        )
        client = OAuthClientInformationFull(
            client_id="client-1",
            client_name="Claude Desktop",
            redirect_uris=[_url("http://localhost/callback")],
        )
        await provider.register_client(client)

        auth_url = await provider.authorize(
            client,
            AuthorizationParams(
                state="state-1",
                scopes=[],
                code_challenge="challenge-1",
                redirect_uri=_url("http://localhost/callback"),
                redirect_uri_provided_explicitly=True,
            ),
        )
        txn_id = parse_qs(urlparse(auth_url).query)["txn_id"][0]
        txn = provider._load_container_token(txn_id, expected_use="auth-txn")
        assert txn is not None
        assert "api_key" not in txn

        code = provider._issue_container_token(
            container_use="auth-code",
            expires_in=300,
            claims={**txn, "encrypted_api_key": provider._encrypt_api_key(api_key)},
        )
        auth_code = await provider.load_authorization_code(client, code)
        assert auth_code is not None

        token = await provider.exchange_authorization_code(client, auth_code)
        payload = _decode_jwt_payload(token.access_token)
        assert payload["jti"]
        assert payload["bauplan_api_key_enc"]
        assert api_key not in json.dumps(payload)

        access_token = await provider.load_access_token(token.access_token)
        assert access_token is not None
        assert access_token.claims[BAUPLAN_API_KEY_CLAIM] == api_key
        assert validation_calls == []

        assert await provider.load_authorization_code(client, code) is not None

    asyncio.run(run())


def test_api_key_oauth_registration_is_stateless_across_provider_instances():
    async def run():
        config = _localhost_config()
        provider = APIKeyOAuthProvider(config=config, validate_api_key=lambda _: _user_info())
        client = OAuthClientInformationFull(
            client_id="original-client-id",
            client_secret="original-client-secret",
            token_endpoint_auth_method="client_secret_post",
            client_name="Claude Desktop",
            redirect_uris=[_url("http://localhost/callback")],
        )

        await provider.register_client(client)

        assert client.client_id is not None
        assert client.client_id != "original-client-id"
        assert client.client_secret is None
        assert client.token_endpoint_auth_method == "none"

        fresh_provider = APIKeyOAuthProvider(config=config, validate_api_key=lambda _: _user_info())
        decoded_client = await fresh_provider.get_client(client.client_id)

        assert decoded_client is not None
        assert decoded_client.client_id == client.client_id
        assert decoded_client.client_secret is None
        assert decoded_client.token_endpoint_auth_method == "none"
        assert decoded_client.client_name == "Claude Desktop"
        assert [str(uri) for uri in decoded_client.redirect_uris or []] == ["http://localhost/callback"]

    asyncio.run(run())


def test_api_key_oauth_marks_known_redirects_as_trusted_by_default():
    async def run():
        provider = APIKeyOAuthProvider(
            config=OAuthConfig(
                base_url="https://mcp.example.com",
                secret="x" * 32,
            ),
            validate_api_key=lambda _: _user_info(),
        )
        clients = [
            OAuthClientInformationFull(
                client_id="claude-client",
                redirect_uris=[_url("https://claude.ai/api/mcp/auth_callback")],
            ),
            OAuthClientInformationFull(
                client_id="chatgpt-client",
                redirect_uris=[_url("https://chatgpt.com/connector/oauth/callback-123")],
            ),
        ]

        for client in clients:
            await provider.register_client(client)
            assert client.redirect_uris is not None
            auth_url = await provider.authorize(
                client,
                AuthorizationParams(
                    state="state-1",
                    scopes=[],
                    code_challenge="challenge-1",
                    redirect_uri=client.redirect_uris[0],
                    redirect_uri_provided_explicitly=True,
                ),
            )
            txn_id = parse_qs(urlparse(auth_url).query)["txn_id"][0]
            txn = provider._load_container_token(txn_id, expected_use="auth-txn")
            assert txn is not None
            assert txn["redirect_uri_trusted"] is True

        assert all(client.client_id for client in clients)

    asyncio.run(run())


def test_api_key_oauth_allows_unknown_https_redirect_registration():
    async def run():
        provider = APIKeyOAuthProvider(
            config=OAuthConfig(
                base_url="https://mcp.example.com",
                secret="x" * 32,
            ),
            validate_api_key=lambda _: _user_info(),
        )
        client = OAuthClientInformationFull(
            client_id="client-1",
            client_name="Hermes",
            redirect_uris=[_url("https://hermes.example.com/oauth/callback")],
        )

        await provider.register_client(client)

        assert client.client_id is not None

    asyncio.run(run())


def test_api_key_oauth_rejects_invalid_registration_redirect():
    async def run():
        provider = APIKeyOAuthProvider(
            config=OAuthConfig(
                base_url="https://mcp.example.com",
                secret="x" * 32,
            ),
            validate_api_key=lambda _: _user_info(),
        )
        client = OAuthClientInformationFull(
            client_id="client-1",
            client_name="Bad Client",
            redirect_uris=[_url("https://client.example.com/callback#fragment")],
        )

        with pytest.raises(RegistrationError, match="redirect_uri"):
            await provider.register_client(client)

    asyncio.run(run())


def test_api_key_oauth_rejects_unregistered_authorize_redirect():
    async def run():
        provider = APIKeyOAuthProvider(
            config=OAuthConfig(
                base_url="https://mcp.example.com",
                secret="x" * 32,
            ),
            validate_api_key=lambda _: _user_info(),
        )
        client = OAuthClientInformationFull(
            client_id="client-1",
            client_name="Claude",
            redirect_uris=[_url("https://claude.ai/api/mcp/auth_callback")],
        )

        with pytest.raises(AuthorizeError, match="registered"):
            await provider.authorize(
                client,
                AuthorizationParams(
                    state="state-1",
                    scopes=[],
                    code_challenge="challenge-1",
                    redirect_uri=_url("https://chatgpt.com/other/callback"),
                    redirect_uri_provided_explicitly=True,
                ),
            )

    asyncio.run(run())


def test_api_key_oauth_rejects_stale_client_with_invalid_redirect():
    async def run():
        provider = APIKeyOAuthProvider(
            config=OAuthConfig(
                base_url="https://mcp.example.com",
                secret="x" * 32,
            ),
            validate_api_key=lambda _: _user_info(),
        )
        client_id = provider._issue_container_token(
            container_use="client-registration",
            expires_in=300,
            claims={
                "client_id_issued_at": 1,
                "redirect_uris": ["https://attacker.example/callback#code"],
                "grant_types": ["authorization_code", "refresh_token"],
                "response_types": ["code"],
                "scope": "",
                "client_name": "Claude",
            },
        )

        assert await provider.get_client(client_id) is None

    asyncio.run(run())


def test_api_key_oauth_rejects_mismatched_resource():
    async def run():
        provider = APIKeyOAuthProvider(
            config=_localhost_config(),
            validate_api_key=lambda _: _user_info(),
        )
        client = OAuthClientInformationFull(
            client_id="client-1",
            client_name="Claude Desktop",
            redirect_uris=[_url("http://localhost/callback")],
        )

        with pytest.raises(AuthorizeError, match="resource"):
            await provider.authorize(
                client,
                AuthorizationParams(
                    state="state-1",
                    scopes=[],
                    code_challenge="challenge-1",
                    redirect_uri=_url("http://localhost/callback"),
                    redirect_uri_provided_explicitly=True,
                    resource="https://other.example.com/mcp",
                ),
            )

    asyncio.run(run())


def test_api_key_oauth_submit_returns_completion_page():
    async def run():
        api_key = "bp_secret_test_key"
        provider = APIKeyOAuthProvider(
            config=OAuthConfig(
                base_url="https://mcp.example.com",
                secret="x" * 32,
            ),
            validate_api_key=lambda key: _user_info() if key == api_key else None,
        )
        txn_id = provider._issue_container_token(
            container_use="auth-txn",
            expires_in=300,
            claims={
                "client_id": "client-1",
                "client_name": "Claude",
                "redirect_uri": "https://claude.ai/api/mcp/auth_callback",
                "redirect_uri_trusted": True,
                "redirect_uri_provided_explicitly": True,
                "state": "state-1",
                "code_challenge": "challenge-1",
                "scopes": [],
                "resource": "https://mcp.example.com/mcp",
            },
        )

        body = f"txn_id={txn_id}&api_key={api_key}".encode()
        request = _form_request(body)
        response = await provider._handle_submit(request)

        body = bytes(response.body).decode()
        assert response.status_code == 200
        assert "Return to Claude" in body
        assert "Your Bauplan API key was validated." in body
        assert "test-user" in body
        assert "Test User" in body
        assert "https://claude.ai/api/mcp/auth_callback?code=" in body
        assert "state=state-1" in body

    asyncio.run(run())


def test_api_key_oauth_form_warns_for_unknown_redirect_before_key_entry():
    async def run():
        provider = APIKeyOAuthProvider(
            config=OAuthConfig(
                base_url="https://mcp.example.com",
                secret="x" * 32,
            ),
            validate_api_key=lambda _: _user_info(),
        )
        txn_id = provider._issue_container_token(
            container_use="auth-txn",
            expires_in=300,
            claims={
                "client_id": "client-1",
                "client_name": "Hermes",
                "redirect_uri": "https://hermes.example.com/oauth/callback",
                "redirect_uri_trusted": False,
                "redirect_uri_provided_explicitly": True,
                "state": "state-1",
                "code_challenge": "challenge-1",
                "scopes": [],
                "resource": "https://mcp.example.com/mcp",
            },
        )

        response = await provider._render_form(_get_request("/authorize/key", f"txn_id={txn_id}"))
        body = bytes(response.body).decode()

        assert response.status_code == 200
        assert "Callback destination: <strong>hermes.example.com</strong>" in body
        assert "not verified by Bauplan" in body

    asyncio.run(run())


def test_api_key_oauth_submit_warns_for_unknown_redirect():
    async def run():
        api_key = "bp_secret_test_key"
        provider = APIKeyOAuthProvider(
            config=OAuthConfig(
                base_url="https://mcp.example.com",
                secret="x" * 32,
            ),
            validate_api_key=lambda key: _user_info() if key == api_key else None,
        )
        txn_id = provider._issue_container_token(
            container_use="auth-txn",
            expires_in=300,
            claims={
                "client_id": "client-1",
                "client_name": "Hermes",
                "redirect_uri": "https://hermes.example.com/oauth/callback",
                "redirect_uri_trusted": False,
                "redirect_uri_provided_explicitly": True,
                "state": "state-1",
                "code_challenge": "challenge-1",
                "scopes": [],
                "resource": "https://mcp.example.com/mcp",
            },
        )

        body = f"txn_id={txn_id}&api_key={api_key}".encode()
        request = _form_request(body)
        response = await provider._handle_submit(request)

        body = bytes(response.body).decode()
        assert response.status_code == 200
        assert "Callback destination: <strong>hermes.example.com</strong>" in body
        assert "not verified by Bauplan" in body
        assert "Continue to hermes.example.com" in body
        assert "https://hermes.example.com/oauth/callback?code=" in body

    asyncio.run(run())


def test_api_key_oauth_submit_issues_code_with_human_click_ttl(monkeypatch):
    async def run():
        api_key = "bp_secret_test_key"
        provider = APIKeyOAuthProvider(
            config=OAuthConfig(
                base_url="https://mcp.example.com",
                secret="x" * 32,
            ),
            validate_api_key=lambda key: _user_info() if key == api_key else None,
        )
        txn_id = provider._issue_container_token(
            container_use="auth-txn",
            expires_in=300,
            claims={
                "client_id": "client-1",
                "client_name": "Claude",
                "redirect_uri": "https://claude.ai/api/mcp/auth_callback",
                "redirect_uri_trusted": True,
                "redirect_uri_provided_explicitly": True,
                "state": "state-1",
                "code_challenge": "challenge-1",
                "scopes": [],
                "resource": "https://mcp.example.com/mcp",
            },
        )

        monkeypatch.setattr("mcp_bauplan.auth.api_key_oauth.time.time", lambda: 1_000)

        body = f"txn_id={txn_id}&api_key={api_key}".encode()
        request = _form_request(body)
        response = await provider._handle_submit(request)
        callback_url = urlparse(bytes(response.body).decode().split('href="', 1)[1].split('"', 1)[0])
        code = parse_qs(callback_url.query)["code"][0]
        payload = _decode_jwt_payload(code)

        assert payload["exp"] - payload["iat"] == 5 * 60

    asyncio.run(run())


def test_api_key_oauth_submit_escapes_user_info():
    async def run():
        api_key = "bp_secret_test_key"
        provider = APIKeyOAuthProvider(
            config=OAuthConfig(
                base_url="https://mcp.example.com",
                secret="x" * 32,
            ),
            validate_api_key=lambda _: BauplanUserInfo(
                username='<script>alert("x")</script>',
                full_name="Alice & Bob <img src=x onerror=alert(1)>",
            ),
        )
        txn_id = provider._issue_container_token(
            container_use="auth-txn",
            expires_in=300,
            claims={
                "client_id": "client-1",
                "client_name": "Claude",
                "redirect_uri": "https://claude.ai/api/mcp/auth_callback",
                "redirect_uri_trusted": True,
                "redirect_uri_provided_explicitly": True,
                "state": "state-1",
                "code_challenge": "challenge-1",
                "scopes": [],
                "resource": "https://mcp.example.com/mcp",
            },
        )

        body = f"txn_id={txn_id}&api_key={api_key}".encode()
        request = _form_request(body)
        response = await provider._handle_submit(request)
        response_body = bytes(response.body).decode()

        assert "<script>" not in response_body
        assert "<img" not in response_body
        assert "&lt;script&gt;alert(&quot;x&quot;)&lt;/script&gt;" in response_body
        assert "Alice &amp; Bob &lt;img src=x onerror=alert(1)&gt;" in response_body

    asyncio.run(run())


def test_api_key_oauth_loads_access_token_without_revalidating_key():
    async def run():
        api_key = "bp_secret_test_key"
        validation_calls = 0

        def validate(_: str) -> BauplanUserInfo | None:
            nonlocal validation_calls
            validation_calls += 1
            return None

        provider = APIKeyOAuthProvider(
            config=OAuthConfig(
                base_url="https://mcp.example.com",
                secret="x" * 32,
            ),
            validate_api_key=validate,
        )
        token = await provider._issue_tokens(
            encrypted_api_key=provider._encrypt_api_key(api_key),
            client_id="client-1",
            scopes=[],
        )
        access_token = await provider.load_access_token(token.access_token)

        assert access_token is not None
        assert access_token.claims[BAUPLAN_API_KEY_CLAIM] == api_key
        assert validation_calls == 0

    asyncio.run(run())


def test_api_key_oauth_rejects_non_access_token_on_access_load():
    async def run():
        api_key = "bp_secret_test_key"
        provider = APIKeyOAuthProvider(
            config=OAuthConfig(
                base_url="https://mcp.example.com",
                secret="x" * 32,
            ),
            validate_api_key=lambda _: _user_info(),
        )
        code = provider._issue_container_token(
            container_use="auth-code",
            expires_in=300,
            claims={
                "client_id": "client-1",
                "encrypted_api_key": provider._encrypt_api_key(api_key),
            },
        )

        assert await provider.load_access_token(code) is None

    asyncio.run(run())


def test_api_key_oauth_returns_none_for_non_ascii_encrypted_claim():
    async def run():
        provider = APIKeyOAuthProvider(
            config=OAuthConfig(
                base_url="https://mcp.example.com",
                secret="x" * 32,
            ),
            validate_api_key=lambda _: _user_info(),
        )
        payload = {
            "iss": "https://mcp.example.com",
            "aud": "https://mcp.example.com/mcp",
            "client_id": "client-1",
            "scope": "",
            "exp": 9_999_999_999,
            "iat": 1,
            "jti": "jti-1",
            "token_use": "access",
            "bauplan_api_key_enc": "é",
        }
        token = jwt.encode({"alg": "HS256", "typ": "JWT"}, payload, provider._jwk)

        assert await provider.load_access_token(token) is None

    asyncio.run(run())


def test_api_key_oauth_rejects_token_signed_with_different_secret():
    async def run():
        api_key = "bp_secret_test_key"
        provider = APIKeyOAuthProvider(
            config=OAuthConfig(
                base_url="https://mcp.example.com",
                secret="x" * 32,
            ),
            validate_api_key=lambda _: _user_info(),
        )
        other_provider = APIKeyOAuthProvider(
            config=OAuthConfig(
                base_url="https://mcp.example.com",
                secret="y" * 32,
            ),
            validate_api_key=lambda _: _user_info(),
        )
        token = other_provider._issue_token(
            client_id="client-1",
            scopes=[],
            encrypted_api_key=other_provider._encrypt_api_key(api_key),
            token_use="access",
            expires_in=300,
        )

        assert await provider.load_access_token(token) is None

    asyncio.run(run())


def test_api_key_oauth_rejects_unexpected_jwt_algorithm():
    async def run():
        api_key = "bp_secret_test_key"
        provider = APIKeyOAuthProvider(
            config=OAuthConfig(
                base_url="https://mcp.example.com",
                secret="x" * 32,
            ),
            validate_api_key=lambda _: _user_info(),
        )
        payload = {
            "iss": "https://mcp.example.com",
            "aud": "https://mcp.example.com/mcp",
            "client_id": "client-1",
            "scope": "",
            "exp": 9_999_999_999,
            "iat": 1,
            "jti": "jti-1",
            "token_use": "access",
            "bauplan_api_key_enc": provider._encrypt_api_key(api_key),
        }
        token = jwt.encode({"alg": "HS384", "typ": "JWT"}, payload, provider._jwk, algorithms=["HS384"])

        assert await provider.load_access_token(token) is None

    asyncio.run(run())


def test_api_key_oauth_rejects_expired_token():
    async def run():
        provider = APIKeyOAuthProvider(
            config=OAuthConfig(
                base_url="https://mcp.example.com",
                secret="x" * 32,
            ),
            validate_api_key=lambda _: _user_info(),
        )
        token = provider._issue_token(
            client_id="client-1",
            scopes=[],
            encrypted_api_key=provider._encrypt_api_key("bp_secret_test_key"),
            token_use="access",
            expires_in=-1,
        )

        assert await provider.load_access_token(token) is None

    asyncio.run(run())


def test_api_key_oauth_rejects_wrong_issuer_or_audience():
    async def run():
        provider = APIKeyOAuthProvider(
            config=OAuthConfig(
                base_url="https://mcp.example.com",
                secret="x" * 32,
            ),
            validate_api_key=lambda _: _user_info(),
        )
        payload = {
            "iss": "https://other.example.com",
            "aud": "https://mcp.example.com/mcp",
            "client_id": "client-1",
            "scope": "",
            "exp": 9_999_999_999,
            "iat": 1,
            "jti": "jti-1",
            "token_use": "access",
            "bauplan_api_key_enc": provider._encrypt_api_key("bp_secret_test_key"),
        }
        wrong_issuer = jwt.encode({"alg": "HS256", "typ": "JWT"}, payload, provider._jwk)
        payload["iss"] = "https://mcp.example.com"
        payload["aud"] = "https://other.example.com/mcp"
        wrong_audience = jwt.encode({"alg": "HS256", "typ": "JWT"}, payload, provider._jwk)

        assert await provider.load_access_token(wrong_issuer) is None
        assert await provider.load_access_token(wrong_audience) is None

    asyncio.run(run())


def test_api_key_oauth_advertises_public_client_token_auth():
    async def run():
        provider = APIKeyOAuthProvider(
            config=OAuthConfig(
                base_url="https://mcp.example.com",
                secret="x" * 32,
            ),
            validate_api_key=lambda _: _user_info(),
        )
        metadata_route = next(
            route
            for route in provider.get_routes("/mcp")
            if route.path == "/.well-known/oauth-authorization-server"
        )
        request = Request({"type": "http", "method": "GET", "path": metadata_route.path})

        response = await metadata_route.endpoint(request)
        metadata = json.loads(response.body)

        assert metadata["issuer"] == "https://mcp.example.com"
        assert metadata["token_endpoint_auth_methods_supported"] == ["none"]
        assert metadata["registration_endpoint"] == "https://mcp.example.com/register"

    asyncio.run(run())


def test_api_key_oauth_refresh_token_is_stateless_container():
    async def run():
        api_key = "bp_secret_test_key"
        provider = APIKeyOAuthProvider(
            config=OAuthConfig(
                base_url="https://mcp.example.com",
                secret="x" * 32,
            ),
            validate_api_key=lambda _: _user_info(),
        )
        token = await provider._issue_tokens(
            encrypted_api_key=provider._encrypt_api_key(api_key),
            client_id="client-1",
            scopes=[],
        )
        client = OAuthClientInformationFull(
            client_id="client-1", redirect_uris=[_url("http://localhost/callback")]
        )
        assert token.refresh_token is not None

        refresh_token = await provider.load_refresh_token(client, token.refresh_token)
        assert refresh_token is not None

        new_token = await provider.exchange_refresh_token(client, refresh_token, scopes=[])
        assert new_token.refresh_token != token.refresh_token
        assert await provider.load_refresh_token(client, token.refresh_token) is not None

        access_token = await provider.load_access_token(new_token.access_token)
        assert access_token is not None
        assert access_token.claims[BAUPLAN_API_KEY_CLAIM] == api_key

    asyncio.run(run())


def test_api_key_oauth_refresh_exchange_wraps_invalid_token_as_token_error():
    async def run():
        provider = APIKeyOAuthProvider(
            config=OAuthConfig(
                base_url="https://mcp.example.com",
                secret="x" * 32,
            ),
            validate_api_key=lambda _: _user_info(),
        )
        client = OAuthClientInformationFull(
            client_id="client-1", redirect_uris=[_url("http://localhost/callback")]
        )

        with pytest.raises(TokenError, match="Refresh token invalid"):
            await provider.exchange_refresh_token(
                client,
                RefreshToken(token="not-a-jwt", client_id="client-1", scopes=[]),
                scopes=[],
            )

    asyncio.run(run())
