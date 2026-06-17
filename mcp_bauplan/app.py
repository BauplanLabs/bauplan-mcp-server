import logging
import os
import warnings
from typing import Literal

import uvicorn
from fastmcp import FastMCP
from fastmcp.server.middleware import Middleware, MiddlewareContext
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import PlainTextResponse

from .auth.config import API_KEY_OAUTH_MODE, get_auth_mode, load_oauth_config
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

    async def on_call_tool(self, context: MiddlewareContext, call_next):
        import time

        tool_name = context.message.name
        args = context.message.arguments
        logger.info(f"Calling tool '{tool_name}' with args: {args}")
        t0 = time.perf_counter()
        try:
            result = await call_next(context)
            elapsed = time.perf_counter() - t0
            logger.info(f"Tool '{tool_name}' completed in {elapsed:.2f}s")
            return result
        except Exception:
            elapsed = time.perf_counter() - t0
            logger.exception(f"Tool '{tool_name}' failed after {elapsed:.2f}s")
            raise


def main(
    transport: Literal["stdio", "sse", "streamable-http"] = "stdio",
    host: str = "0.0.0.0",
    port: int = 8000,
    profile: str | None = None,
) -> None:
    """
    Main entry point for the MCP Bauplan server.
    """
    # if the profile is set, add it to the envs
    if profile:
        os.environ["BAUPLAN_PROFILE"] = profile

    auth_mode = get_auth_mode()

    auth_provider = None
    if auth_mode == API_KEY_OAUTH_MODE:
        from .auth.api_key_oauth import create_api_key_oauth_provider

        auth_provider = create_api_key_oauth_provider(load_oauth_config())
        logger.info("Enabled API-key-backed OAuth authentication")

    mcp = FastMCP(
        MCP_SERVER_NAME,
        instructions=INSTRUCTIONS,
        auth=auth_provider,
    )

    # Register tools
    register_get_tables_tool(mcp)
    register_get_table_tool(mcp)
    register_run_query_tool(mcp)
    # OAuth remote clients cannot read server-local CSV files.
    if auth_mode != API_KEY_OAUTH_MODE:
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
    # OAuth remote clients cannot use client-local project paths.
    if auth_mode != API_KEY_OAUTH_MODE:
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

    mcp.add_middleware(LoggingMiddleware())

    if transport != "stdio":
        # Health check endpoint (must be registered before http_app())
        @mcp.custom_route("/healthz", methods=["GET"])
        async def health(_: Request) -> PlainTextResponse:
            return PlainTextResponse("ok")

        # Create the app based on transport type
        if transport == "sse":
            app = mcp.http_app(transport="sse")
        else:
            # For HTTP/streamable-http
            app = mcp.http_app(path=MCP_PATH, stateless_http=True)

        # Add CORS middleware
        app.add_middleware(
            CORSMiddleware,  # type: ignore[arg-type]
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        # Run server
        uvicorn.run(app, host=host, port=port)
    else:
        mcp.run(transport=transport)

    return
