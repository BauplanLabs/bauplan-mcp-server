import asyncio

import pytest

from mcp_bauplan.app import create_http_app, create_mcp
from mcp_bauplan.auth.config import API_KEY_OAUTH_MODE


def test_create_mcp_registers_local_file_tools_without_oauth(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("MCP_AUTH_MODE", raising=False)

    async def run():
        tools = {tool.name for tool in await create_mcp().list_tools()}

        assert "project_run" in tools
        assert "run_query_to_csv" in tools

    asyncio.run(run())


def test_create_mcp_hides_local_file_tools_in_oauth_mode(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("MCP_AUTH_MODE", API_KEY_OAUTH_MODE)
    monkeypatch.setenv("MCP_PUBLIC_BASE_URL", "https://mcp.example.com")
    monkeypatch.setenv("MCP_OAUTH_SECRET", "x" * 32)

    async def run():
        tools = {tool.name for tool in await create_mcp().list_tools()}

        assert "project_run" not in tools
        assert "run_query_to_csv" not in tools

    asyncio.run(run())


def test_create_http_app_rejects_stdio_transport(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("MCP_TRANSPORT", "stdio")

    with pytest.raises(ValueError, match="HTTP app transport"):
        create_http_app()
