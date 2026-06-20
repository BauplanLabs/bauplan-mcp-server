import logging
import os
import warnings
from typing import Any, Literal

import uvicorn
from fastmcp import FastMCP
from fastmcp.server.middleware import Middleware, MiddlewareContext
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import PlainTextResponse

from .auth.config import API_KEY_OAUTH_MODE, get_auth_mode, load_oauth_config
from .tools._schema import TOOL_TAG_REMOTE, TOOL_TAGS
from .tools.apply_table_creation_plan import register_apply_table_creation_plan_tool
from .tools.cancel_job import register_cancel_job_tool
from .tools.code_run import register_code_run_tool
from .tools.create_branch import register_create_branch_tool
from .tools.create_namespace import register_create_namespace_tool
from .tools.create_table import register_create_table_tool
from .tools.create_tag import register_create_tag_tool
from .tools.delete_branch import register_delete_branch_tool
from .tools.delete_namespace import register_delete_namespace_tool
from .tools.delete_table import register_delete_table_tool
from .tools.delete_tag import register_delete_tag_tool
from .tools.get_branch import register_get_branch_tool
from .tools.get_branches import register_get_branches_tool
from .tools.get_commits import register_get_commits_tool
from .tools.get_instructions import register_get_instructions_tool
from .tools.get_job import register_get_job_tool
from .tools.get_jobs import register_get_jobs_tool
from .tools.get_namespace import register_get_namespace_tool
from .tools.get_namespaces import register_get_namespaces_tool
from .tools.get_table import register_get_table_tool
from .tools.get_tables import register_get_tables_tool
from .tools.get_tag import register_get_tag_tool
from .tools.get_tags import register_get_tags_tool
from .tools.get_user_info import register_get_user_info_tool
from .tools.import_data import register_import_data_tool
from .tools.merge_branch import register_merge_branch_tool
from .tools.plan_table_creation import register_plan_table_creation_tool
from .tools.project_run import register_project_run_tool
from .tools.revert_table import register_revert_table_tool
from .tools.run_query import register_run_query_tool
from .tools.run_query_to_csv import register_run_query_to_csv_tool

# Suppress known deprecation warnings from uvicorn/websockets compatibility issue
# These warnings are harmless and will be fixed in future uvicorn releases
warnings.filterwarnings("ignore", category=DeprecationWarning, module="websockets.legacy")
warnings.filterwarnings("ignore", category=DeprecationWarning, module="uvicorn.protocols.websockets")

# Get the global MCP server name and instructions
MCP_SERVER_NAME = "mcp-bauplan"
MCP_PATH = "/mcp"
LOG_TOOL_ARGS_ENV = "MCP_LOG_TOOL_ARGS"
VISIBLE_TOOL_TAGS_ENV = "MCP_VISIBLE_TOOL_TAGS"
INSTRUCTIONS = (
    "This is the MCP server for Bauplan, an AI-first data lakehouse entirely "
    "programmable as code. Bauplan is built on two fundamental abstractions: "
    "Git-for-data branching, which lets you develop and test on isolated data "
    "branches before merging into main, and serverless multi-language pipelines "
    "that run SQL and Python transformations without managing infrastructure.\n\n"
    "Through this MCP server you can manage branches, run queries, execute "
    "pipelines, inspect tables and schemas, import data, and perform all core "
    "lakehouse operations programmatically. Every write operation follows the "
    "branch-based workflow: create a branch, make changes, review, and merge — "
    "ensuring safe, auditable data development."
)

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


class LoggingMiddleware(Middleware):
    """FastMCP middleware that logs tool calls with name and duration."""

    def __init__(self) -> None:
        self._log_tool_args = _log_tool_args_enabled()

    async def on_call_tool(self, context: MiddlewareContext, call_next):
        import time

        tool_name = context.message.name
        if self._log_tool_args:
            logger.info("Calling tool '%s' with args: %s", tool_name, context.message.arguments)
        else:
            logger.info("Calling tool '%s'", tool_name)
        t0 = time.perf_counter()
        try:
            result = await call_next(context)
            elapsed = time.perf_counter() - t0
            logger.info("Tool '%s' completed in %.2fs", tool_name, elapsed)
            return result
        except Exception:
            elapsed = time.perf_counter() - t0
            logger.exception(f"Tool '{tool_name}' failed after {elapsed:.2f}s")
            raise


def _log_tool_args_enabled() -> bool:
    value = os.getenv(LOG_TOOL_ARGS_ENV, "").strip().lower()
    if value in ("", "0", "false", "no", "off"):
        return False
    if value in ("1", "true", "yes", "on"):
        if os.getenv("MCP_PUBLIC_BASE_URL", "").strip():
            logger.warning(
                "%s is enabled on a public MCP server; tool arguments may contain sensitive data",
                LOG_TOOL_ARGS_ENV,
            )
        return True
    raise ValueError(f"{LOG_TOOL_ARGS_ENV} must be true or false")


def _visible_tool_tags(auth_mode: str) -> set[str] | None:
    value = os.getenv(VISIBLE_TOOL_TAGS_ENV)
    if value is None:
        return {TOOL_TAG_REMOTE} if auth_mode == API_KEY_OAUTH_MODE else None

    tags = {tag.strip() for tag in value.split(",") if tag.strip()}
    if not tags:
        raise ValueError(f"{VISIBLE_TOOL_TAGS_ENV} must contain at least one tag")
    unknown_tags = tags - TOOL_TAGS
    if unknown_tags:
        allowed_tags = ", ".join(sorted(TOOL_TAGS))
        raise ValueError(
            f"{VISIBLE_TOOL_TAGS_ENV} contains unknown tags: {', '.join(sorted(unknown_tags))}. "
            f"Allowed tags are: {allowed_tags}"
        )
    return tags


def _set_profile(profile: str | None) -> None:
    if profile:
        os.environ["BAUPLAN_PROFILE"] = profile


def _auth_provider(auth_mode: str) -> Any | None:
    if auth_mode != API_KEY_OAUTH_MODE:
        return None

    from .auth.api_key_oauth import create_api_key_oauth_provider

    logger.info("Enabled API-key-backed OAuth authentication")
    return create_api_key_oauth_provider(load_oauth_config())


def _register_tools(mcp: FastMCP, auth_mode: str) -> None:
    register_get_tables_tool(mcp)
    register_get_table_tool(mcp)
    register_run_query_tool(mcp)
    register_run_query_to_csv_tool(mcp)
    register_get_branches_tool(mcp)
    register_get_branch_tool(mcp)
    register_get_commits_tool(mcp)
    register_get_namespaces_tool(mcp)
    register_get_namespace_tool(mcp)
    register_create_branch_tool(mcp)
    register_create_namespace_tool(mcp)
    register_create_table_tool(mcp)
    register_plan_table_creation_tool(mcp)
    register_apply_table_creation_plan_tool(mcp)
    register_delete_table_tool(mcp)
    register_import_data_tool(mcp)
    register_revert_table_tool(mcp)
    register_project_run_tool(mcp)
    register_code_run_tool(mcp)
    register_get_jobs_tool(mcp)
    register_get_job_tool(mcp)
    register_cancel_job_tool(mcp)
    register_merge_branch_tool(mcp)
    register_delete_branch_tool(mcp)
    register_delete_namespace_tool(mcp)
    register_get_tags_tool(mcp)
    register_get_tag_tool(mcp)
    register_create_tag_tool(mcp)
    register_delete_tag_tool(mcp)
    register_get_user_info_tool(mcp)
    register_get_instructions_tool(mcp)

    visible_tool_tags = _visible_tool_tags(auth_mode)
    if visible_tool_tags:
        mcp.enable(tags=visible_tool_tags, components={"tool"}, only=True)


def create_mcp(profile: str | None = None) -> FastMCP:
    _set_profile(profile)
    auth_mode = get_auth_mode()
    mcp = FastMCP(
        MCP_SERVER_NAME,
        instructions=INSTRUCTIONS,
        auth=_auth_provider(auth_mode),
    )
    _register_tools(mcp, auth_mode)
    mcp.add_middleware(LoggingMiddleware())
    return mcp


def create_http_app(
    transport: Literal["sse", "streamable-http"] | None = None,
    profile: str | None = None,
) -> Any:
    resolved_transport = transport or os.getenv("MCP_TRANSPORT", "streamable-http")
    if resolved_transport not in ("sse", "streamable-http"):
        raise ValueError("HTTP app transport must be 'sse' or 'streamable-http'.")

    mcp = create_mcp(profile=profile)

    # Health check endpoint (must be registered before http_app()).
    @mcp.custom_route("/healthz", methods=["GET"])
    async def health(_: Request) -> PlainTextResponse:
        return PlainTextResponse("ok")

    if resolved_transport == "sse":
        app = mcp.http_app(transport="sse")
    else:
        app = mcp.http_app(path=MCP_PATH, stateless_http=True)

    app.add_middleware(
        CORSMiddleware,  # type: ignore[arg-type]
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    return app


def main(
    transport: Literal["stdio", "sse", "streamable-http"] = "stdio",
    host: str = "0.0.0.0",
    port: int = 8000,
    profile: str | None = None,
) -> None:
    """
    Main entry point for the MCP Bauplan server.
    """
    if transport == "stdio":
        create_mcp(profile=profile).run(transport=transport)
        return

    app = create_http_app(transport=transport, profile=profile)
    uvicorn.run(app, host=host, port=port)
