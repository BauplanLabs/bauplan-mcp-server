# mcp_bauplan/app.py
from fastmcp import FastMCP
import argparse
from starlette.responses import PlainTextResponse
from starlette.requests import Request
from fastmcp.server.middleware import Middleware, MiddlewareContext
from fastmcp.server.dependencies import get_http_request
from starlette.requests import Request
from starlette.middleware.cors import CORSMiddleware
import uvicorn
from starlette.middleware.base import BaseHTTPMiddleware
import logging
import warnings
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
from .tools.list_jobs import register_list_jobs_tool
from .tools.get_job import register_get_job_tool
from .tools.get_job_logs import register_get_job_logs_tool
from .tools.cancel_job import register_cancel_job_tool
from .tools.merge_branch import register_merge_branch_tool
from .tools.delete_branch import register_delete_branch_tool
from .tools.delete_namespace import register_delete_namespace_tool
from .tools.get_tags import register_get_tags_tool
from .tools.create_tag import register_create_tag_tool
from .tools.has_tag import register_has_tag_tool
from .tools.delete_tag import register_delete_tag_tool

# Suppress known deprecation warnings from uvicorn/websockets compatibility issue
# These warnings are harmless and will be fixed in future uvicorn releases
warnings.filterwarnings("ignore", category=DeprecationWarning, module="websockets.legacy")
warnings.filterwarnings("ignore", category=DeprecationWarning, module="uvicorn.protocols.websockets")

MCP_SERVER_NAME = "mcp-bauplan"
logger = logging.getLogger(__name__)


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
        if 'bauplan' in headers or 'Bauplan' in headers:
            api_key = headers.get('bauplan') or headers.get('Bauplan')
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
    

def main() -> None:
    """
    Main entry point for the MCP Bauplan server.
    """
    ap = argparse.ArgumentParser()
    ap.add_argument("--transport", default="stdio",
                    choices=["stdio", "sse", "streamable-http"])
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=8000)
    args = ap.parse_args()

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    mcp = FastMCP(
        MCP_SERVER_NAME,
        instructions="""
The Bauplan MCP Server exposes structured operations for interacting with a Bauplan data lakehouse, including actions related to querying data at any point in time, running data pipelines as DAGs of SQL and Python functions, data versioning in a Git-style fashion, and auditing and inspecting lineage and table history.
Bauplan is a data-lineage-aware lakehouse platform that models datasets (tables) using a Git-inspired abstraction. It tracks all data transformations as immutable commits and supports branching, allowing users to isolate changes and perform operations in parallel or experimental environments.
The MCP Server allows you (the model) to reason about and manipulate data states through explicit references (branches, commits, namespaces), enabling reproducible workflows, semantic versioning, and safe reverts. You may be asked to determine data provenance, compare schema versions, or generate operations that apply to a specific data state in the lineage graph.

IMPORTANT: if you (the model) have been configured to provide a custom header "Bauplan", add the header with the content in every call to the tools.

IMPORTANT: for SQL generation use the DuckDB SQL dialect. Exception: do not use DESCRIBE or CREATE. 
Use get_table to get the schema of a table. Use get_schema to get the schema of all the tables in a branch. Use list_tables to list all the tables in a branch.

DATA LINEAGE CONCEPTS:
1. Branch
A branch is a mutable pointer to the most recent commit in a lineage.
It represents the HEAD, like in Git: after successful write or pipeline operations, the branch pointer is updated to reference the newly created commit.
Branches enable isolated development, experimentation, and progression of data pipelines.
A branch name has always the format <username.name>, with the exception of the special branch main, which typically represents the production version of all tables.
2. Commit
A commit (also called a ref) is an immutable snapshot of the data lake’s state at a specific moment in time.
Metadata includes:
creation timestamp
author or job identifier
list of tables and changes introduced
user comment
Once created, a commit never changes and is tracked by its unique 64-character hexadecimal hash.
A commit is referred to as @<hash> that is a 64 hexadecimal string that starts with "@".
3. Ref
A ref is a reference to a commit: it can either be a hash that starts with "@"  and has 64 additional characters or a branch name in the form <username.name>. If it is a branch, it is implicitly resolved to the HEAD, the last commit on that branch.
4. Namespace
A namespace is a logical container that groups related tables, under a common prefix.
It defines ownership, isolation, and scoping within the data lake, typically aligned with teams, projects, or environments.
5. Tag
A tag is a label that points to a commit. 
It allows to tag versions of the lakehouse with a user-friendly string (e.g. v1.0-passed-qa), which then can be retrieved by passing it to the query method for example.
6. Table
A table in Bauplan is a versioned dataset that represents structured data stored in a namespace within a given branch or commit.
A table name always has the format <namespace.name>. When a namespace is an optional parameter, the default namespace is "bauplan".
Tables are immutable at the commit level: once written in a commit, their contents do not change. They support schema evolution, branching, and reproducibility.
IMPORTANT: branch, commit, ref, namespace, tag, table appear as parameters in the MCP server tools. ALWAYS FOLLOW THEIR DEFINITIONS ABOVE WHEN USING THEM.
TYPICAL PIPELINE  WORKFLOW:
Create a branch
client.create_branch('feature_xyz', from_ref='main')

Commit a change through a pipeline 
run = client.run('pipeline', ref='feature_xyz')

Reference a specific commit as a version
client.query('select …. from …;', ref='@c5f1a7d9e3...')  # full 64-hex commit hash prefixed with '@'

Tag a commit 
client.create_tag('v1.0-qa', commit_ref)
WORKFLOW FOR DATA TABLE CREATION:
Option 1: single step. Use the single shot tool 'create_table' when there is only one table in the S3 bucket or when the objects in the bucket are certain to be homogeneous and have the same schema. PREFERRED.
Option 2: two steps. Used when schema conflicts exist after plan creation and need manual resolution. First step:  `plan_table_creation `- Create a table import plan from S3 location (generates YAML schema plan with job tracking. Second step:  `apply_table_creation_plan `- Apply a table creation plan to resolve schema conflicts as described in the YAML schema plan returned by the first step (returns job_id for tracking). TO USE WHEN OPTION 1 FAILS.
"""
    )

    ## add middleware to add the Bauplan api_key to all requests
    mcp.add_middleware(SimpleLoggingMiddleware()) 
    
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
    register_list_jobs_tool(mcp)
    register_get_job_tool(mcp)
    register_get_job_logs_tool(mcp)
    register_cancel_job_tool(mcp)
    register_merge_branch_tool(mcp)
    register_delete_branch_tool(mcp)
    register_delete_namespace_tool(mcp)
    register_get_tags_tool(mcp)
    register_create_tag_tool(mcp)
    register_has_tag_tool(mcp)
    register_delete_tag_tool(mcp)
    
    if args.transport != "stdio":
        # Create the app based on transport type
        if args.transport == "sse":
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
        
        # Handle /mcp -> /mcp/ redirect issue with middleware
        class TrailingSlashMiddleware(BaseHTTPMiddleware):
            async def dispatch(self, request: Request, call_next):
                # If path is /mcp (without trailing slash), modify it
                if request.url.path == "/mcp":
                    request.scope["path"] = "/mcp/"
                    
                response = await call_next(request)
                return response
        
        # Add the trailing slash middleware
        app.add_middleware(TrailingSlashMiddleware)
        
        # Health check endpoint
        @mcp.custom_route("/healthz", methods=["GET"])           
        async def health(_: Request) -> PlainTextResponse:      
            return PlainTextResponse("ok")
        
        # Run server
        uvicorn.run(app, host=args.host, port=args.port)
    else:
        mcp.run(transport=args.transport)


if __name__ == "__main__":
    main()
