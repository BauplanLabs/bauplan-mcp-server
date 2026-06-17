import asyncio

import pytest

from mcp_bauplan.app import _log_tool_args_enabled, create_http_app, create_mcp
from mcp_bauplan.auth.config import API_KEY_OAUTH_MODE


def test_create_mcp_registers_local_file_tools_without_oauth(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("MCP_AUTH_MODE", raising=False)
    monkeypatch.delenv("MCP_PUBLIC_BASE_URL", raising=False)

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
    monkeypatch.delenv("MCP_PUBLIC_BASE_URL", raising=False)
    monkeypatch.setenv("MCP_TRANSPORT", "stdio")

    with pytest.raises(ValueError, match="HTTP app transport"):
        create_http_app()


def test_log_tool_args_defaults_to_false(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("MCP_LOG_TOOL_ARGS", raising=False)

    assert not _log_tool_args_enabled()


def test_log_tool_args_accepts_local_opt_in(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("MCP_LOG_TOOL_ARGS", "true")
    monkeypatch.delenv("MCP_PUBLIC_BASE_URL", raising=False)

    assert _log_tool_args_enabled()


def test_log_tool_args_allows_public_server_opt_in_with_warning(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    monkeypatch.setenv("MCP_LOG_TOOL_ARGS", "true")
    monkeypatch.setenv("MCP_PUBLIC_BASE_URL", "https://mcp.example.com")

    assert _log_tool_args_enabled()
    assert "tool arguments may contain sensitive data" in caplog.text
