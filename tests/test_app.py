import asyncio

import pytest
from mcp.types import ToolAnnotations

from mcp_bauplan.app import _log_tool_args_enabled, create_http_app, create_mcp
from mcp_bauplan.auth.config import API_KEY_OAUTH_MODE


def test_create_mcp_registers_local_file_tools_without_oauth(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("MCP_AUTH_MODE", raising=False)
    monkeypatch.delenv("MCP_PUBLIC_BASE_URL", raising=False)
    monkeypatch.delenv("MCP_VISIBLE_TOOL_TAGS", raising=False)

    async def run():
        tools = {tool.name: tool for tool in await create_mcp().list_tools()}

        assert "project_run" in tools
        assert "run_query_to_csv" in tools
        assert tools["project_run"].tags == {"local", "write", "destructive"}
        assert tools["run_query_to_csv"].tags == {"local", "write"}

    asyncio.run(run())


def test_create_mcp_hides_local_file_tools_in_oauth_mode(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("MCP_AUTH_MODE", API_KEY_OAUTH_MODE)
    monkeypatch.setenv("MCP_PUBLIC_BASE_URL", "https://mcp.example.com")
    monkeypatch.setenv("MCP_OAUTH_SECRET", "x" * 32)
    monkeypatch.delenv("MCP_VISIBLE_TOOL_TAGS", raising=False)

    async def run():
        tools = {tool.name for tool in await create_mcp().list_tools()}

        assert "get_table" in tools
        assert "create_branch" in tools
        assert "project_run" not in tools
        assert "run_query_to_csv" not in tools
        assert await create_mcp().get_tool("project_run") is None
        assert await create_mcp().get_tool("run_query_to_csv") is None

    asyncio.run(run())


def test_create_mcp_filters_visible_tool_tags_from_env(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("MCP_AUTH_MODE", raising=False)
    monkeypatch.delenv("MCP_PUBLIC_BASE_URL", raising=False)
    monkeypatch.setenv("MCP_VISIBLE_TOOL_TAGS", "read")

    async def run():
        tools = {tool.name for tool in await create_mcp().list_tools()}

        assert "get_table" in tools
        assert "create_branch" not in tools
        assert "project_run" not in tools
        assert await create_mcp().get_tool("create_branch") is None

    asyncio.run(run())


def test_create_mcp_rejects_unknown_visible_tool_tags(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("MCP_AUTH_MODE", raising=False)
    monkeypatch.delenv("MCP_PUBLIC_BASE_URL", raising=False)
    monkeypatch.setenv("MCP_VISIBLE_TOOL_TAGS", "remtoe")

    with pytest.raises(ValueError, match="unknown tags: remtoe"):
        create_mcp()


def test_create_mcp_exposes_tool_annotations(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("MCP_AUTH_MODE", raising=False)
    monkeypatch.delenv("MCP_PUBLIC_BASE_URL", raising=False)
    monkeypatch.delenv("MCP_VISIBLE_TOOL_TAGS", raising=False)

    async def run():
        tools = {tool.name: tool for tool in await create_mcp().list_tools()}

        assert all(tool.annotations is not None for tool in tools.values())

        def annotations_for(tool_name: str) -> ToolAnnotations:
            annotations = tools[tool_name].annotations
            assert annotations is not None
            return annotations

        get_table = annotations_for("get_table")
        assert get_table.title == "Get table"
        assert get_table.readOnlyHint is True
        assert get_table.destructiveHint is False
        assert get_table.idempotentHint is True
        assert get_table.openWorldHint is True
        assert tools["get_table"].tags == {"remote", "read"}

        get_instructions = annotations_for("get_instructions")
        assert get_instructions.readOnlyHint is True
        assert get_instructions.openWorldHint is False

        create_branch = annotations_for("create_branch")
        assert create_branch.readOnlyHint is False
        assert create_branch.destructiveHint is False
        assert tools["create_branch"].tags == {"remote", "write"}

        delete_table = annotations_for("delete_table")
        assert delete_table.readOnlyHint is False
        assert delete_table.destructiveHint is True
        assert tools["delete_table"].tags == {"remote", "write", "destructive"}

        run_query_to_csv = annotations_for("run_query_to_csv")
        assert run_query_to_csv.readOnlyHint is False
        assert run_query_to_csv.destructiveHint is False

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
