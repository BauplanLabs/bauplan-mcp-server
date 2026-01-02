# mcp_bauplan/app.py
from pathlib import Path
from fastmcp import FastMCP
from starlette.responses import PlainTextResponse
from starlette.requests import Request
from fastmcp.server.middleware import Middleware, MiddlewareContext
from fastmcp.server.dependencies import get_http_request
from starlette.middleware.cors import CORSMiddleware
import uvicorn
from starlette.middleware.base import BaseHTTPMiddleware
import logging
import os
import warnings

MCP_SERVER_NAME = "mcp-bauplan"
INSTRUCTIONS = (Path(__file__).parent / "CLAUDE.md").read_text()
from .tools.list_tables import register_list_tables_tool
from .tools.get_schema import register_get_schema_tool
from .tools.get_table import register_get_table_tool
from .tools.run_query import register_run_query_tool
from .tools.run_query_to_csv import register_run_query_to_csv_tool
from .tools.get_branches import register_get_branches_tool
from .tools.get_commits import register_get_commits_tool
from .tools.get_namespaces import register_get_namespaces_tool
from .tools.create_branch import register_create_branch_tool
from .tools.has_branch import register_has_branch_tool
from .tools.create_namespace import register_create_namespace_tool
from .tools.has_namespace import register_has_namespace_tool
from .tools.create_table import register_create_table_tool
from .tools.plan_table_creation import register_plan_table_creation_tool
from .tools.apply_table_creation_plan import register_apply_table_creation_plan_tool
from .tools.has_table import register_has_table_tool
from .tools.delete_table import register_delete_table_tool
from .tools.import_data import register_import_data_tool
from .tools.revert_table import register_revert_table_tool
from .tools.project_run import register_project_run_tool
from .tools.code_run import register_code_run_tool
from .tools.list_jobs import register_list_jobs_tool
from .tools.get_job import register_get_job_tool
from .tools.cancel_job import register_cancel_job_tool
from .tools.merge_branch import register_merge_branch_tool
from .tools.delete_branch import register_delete_branch_tool
from .tools.delete_namespace import register_delete_namespace_tool
from .tools.get_tags import register_get_tags_tool
from .tools.create_tag import register_create_tag_tool
from .tools.has_tag import register_has_tag_tool
from .tools.delete_tag import register_delete_tag_tool
from .tools.get_instructions import register_get_instructions_tool
from .tools.get_user_info import register_get_user_info_tool

# Suppress known deprecation warnings from uvicorn/websockets compatibility issue
# These warnings are harmless and will be fixed in future uvicorn releases
warnings.filterwarnings(
    "ignore", category=DeprecationWarning, module="websockets.legacy"
)
warnings.filterwarnings(
    "ignore", category=DeprecationWarning, module="uvicorn.protocols.websockets"
)


# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


class SimpleLoggingMiddleware(Middleware):
    """
    FastMCP middleware to capture the header of a call_tool request.
    """

    async def on_call_tool(self, context: MiddlewareContext, call_next):
        request: Request = get_http_request()
        headers = request.headers

        # If 'bauplan' or 'Bauplan' is explicitly in the headers, use it as the API key
        # This allows the model to pass a custom API key for Bauplan operations instead of
        # relying on the default one in the config file.
        if "bauplan" in headers or "Bauplan" in headers:
            api_key = headers.get("bauplan") or headers.get("Bauplan")
            if api_key and api_key.lower().startswith("bearer "):
                api_key = api_key[7:].strip()
            context.message.arguments["api_key"] = api_key

        try:
            result = await call_next(context)
            return result
        except Exception as e:
            print(f"Failed {context.method}: {e}", flush=True)
            raise


class HTTPLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        return response


def main(
    transport: str = "stdio",
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

    mcp = FastMCP(
        MCP_SERVER_NAME,
        instructions=INSTRUCTIONS,
    )

    # Register tools
    register_list_tables_tool(mcp)
    register_get_schema_tool(mcp)
    register_get_table_tool(mcp)
    register_run_query_tool(mcp)
    register_run_query_to_csv_tool(mcp)
    register_get_branches_tool(mcp)
    register_get_commits_tool(mcp)
    register_get_namespaces_tool(mcp)
    register_create_branch_tool(mcp)
    register_has_branch_tool(mcp)
    register_create_namespace_tool(mcp)
    register_has_namespace_tool(mcp)
    register_create_table_tool(mcp)
    register_plan_table_creation_tool(mcp)
    register_apply_table_creation_plan_tool(mcp)
    register_has_table_tool(mcp)
    register_delete_table_tool(mcp)
    register_import_data_tool(mcp)
    register_revert_table_tool(mcp)
    register_project_run_tool(mcp)
    register_code_run_tool(mcp)
    register_list_jobs_tool(mcp)
    register_get_job_tool(mcp)
    register_cancel_job_tool(mcp)
    register_merge_branch_tool(mcp)
    register_delete_branch_tool(mcp)
    register_delete_namespace_tool(mcp)
    register_get_tags_tool(mcp)
    register_create_tag_tool(mcp)
    register_has_tag_tool(mcp)
    register_delete_tag_tool(mcp)
    register_get_user_info_tool(mcp)
    register_get_instructions_tool(mcp)

    if transport != "stdio":
        ## add middleware to add the Bauplan api_key to all requests
        mcp.add_middleware(SimpleLoggingMiddleware())
        # Create the app based on transport type
        if transport == "sse":
            app = mcp.http_app(transport="sse")
        else:
            # For HTTP/streamable-http
            app = mcp.http_app()

        # Add CORS middleware
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        # Health check endpoint
        @mcp.custom_route("/healthz", methods=["GET"])
        async def health(_: Request) -> PlainTextResponse:
            return PlainTextResponse("ok")

        # Run server
        uvicorn.run(app, host=host, port=port)
    else:
        mcp.run(transport=transport)

    return
