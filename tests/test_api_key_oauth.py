import asyncio
import base64
import json
from urllib.parse import parse_qs, urlparse

import pytest
from mcp.server.auth.provider import AuthorizationParams, AuthorizeError, RefreshToken, TokenError
from mcp.shared.auth import OAuthClientInformationFull
from pydantic import AnyUrl, TypeAdapter

from mcp_bauplan.auth.api_key_oauth import APIKeyOAuthProvider
from mcp_bauplan.auth.config import BAUPLAN_API_KEY_CLAIM, OAuthConfig

_ANY_URL = TypeAdapter(AnyUrl)


def _decode_jwt_payload(token: str) -> dict:
    payload = token.split(".")[1]
    padded = payload + "=" * (-len(payload) % 4)
    return json.loads(base64.urlsafe_b64decode(padded))


def _url(value: str) -> AnyUrl:
    return _ANY_URL.validate_python(value)


def test_api_key_oauth_issues_stateless_encrypted_claim_tokens():
    async def run():
        api_key = "bp_secret_test_key"
        validation_calls = []

        def validate(key: str) -> bool:
            validation_calls.append(key)
            return key == api_key

        provider = APIKeyOAuthProvider(
            config=OAuthConfig(
                base_url="https://mcp.example.com",
                secret="x" * 32,
            ),
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
        config = OAuthConfig(
            base_url="https://mcp.example.com",
            secret="x" * 32,
        )
        provider = APIKeyOAuthProvider(config=config, validate_api_key=lambda _: True)
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

        fresh_provider = APIKeyOAuthProvider(config=config, validate_api_key=lambda _: True)
        decoded_client = await fresh_provider.get_client(client.client_id)

        assert decoded_client is not None
        assert decoded_client.client_id == client.client_id
        assert decoded_client.client_secret is None
        assert decoded_client.token_endpoint_auth_method == "none"
        assert decoded_client.client_name == "Claude Desktop"
        assert [str(uri) for uri in decoded_client.redirect_uris or []] == ["http://localhost/callback"]

    asyncio.run(run())


def test_api_key_oauth_rejects_mismatched_resource():
    async def run():
        provider = APIKeyOAuthProvider(
            config=OAuthConfig(
                base_url="https://mcp.example.com",
                secret="x" * 32,
            ),
            validate_api_key=lambda _: True,
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


def test_api_key_oauth_loads_access_token_without_revalidating_key():
    async def run():
        api_key = "bp_secret_test_key"
        validation_calls = 0

        def validate(_: str) -> bool:
            nonlocal validation_calls
            validation_calls += 1
            return False

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
            validate_api_key=lambda _: True,
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
            validate_api_key=lambda _: True,
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
        token = provider._jwt.encode({"alg": "HS256", "typ": "JWT"}, payload, provider._signing_key).decode(
            "utf-8"
        )

        assert await provider.load_access_token(token) is None

    asyncio.run(run())


def test_api_key_oauth_refresh_token_is_stateless_container():
    async def run():
        api_key = "bp_secret_test_key"
        provider = APIKeyOAuthProvider(
            config=OAuthConfig(
                base_url="https://mcp.example.com",
                secret="x" * 32,
            ),
            validate_api_key=lambda _: True,
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
            validate_api_key=lambda _: True,
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
