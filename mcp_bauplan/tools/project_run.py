"""
Run a Bauplan project from a specified directory.
"""

import logging
from typing import Annotated

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import Field

from .create_client import get_bauplan_client
from .get_job import JobOut
from .run_bauplan_project import run_project

logger = logging.getLogger(__name__)


def register_project_run_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="project_run")
    async def project_run(
        project_dir: Annotated[
            str,
            Field(
                description="Server-local project directory to run.",
            ),
        ],
        ref: Annotated[
            str,
            Field(
                description="Non-main branch to run the project against.",
            ),
        ],
        namespace: Annotated[
            str | None,
            Field(
                description="Namespace to materialize models into, or null for the default namespace.",
            ),
        ] = None,
        parameters: Annotated[
            dict[str, str | int | float | bool | None] | None,
            Field(
                description="Optional parameters for templating SQL or Python models.",
            ),
        ] = None,
        dry_run: Annotated[
            bool,
            Field(
                description="Whether to run without materializing models. Defaults to false.",
            ),
        ] = False,
        client_timeout: Annotated[
            int,
            Field(
                description="Client timeout in seconds. Defaults to 30.",
                ge=1,
                le=30,
            ),
        ] = 30,
        detach: Annotated[
            bool,
            Field(
                description="Whether to return after job submission instead of waiting for completion. Use get_job to check status. Defaults to true.",
            ),
        ] = True,
        strict: Annotated[
            bool,
            Field(
                description="Whether to enable strict mode for the run. Defaults to true.",
            ),
        ] = True,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> JobOut:
        """
        Run a Bauplan project already present on the server filesystem.
        Use this for local or trusted server-side workflows where the project path is accessible to the MCP server.
        """

        try:
            if ctx:
                await ctx.info(f"Running project from '{project_dir}' with ref '{ref}'")

            return await run_project(
                project_dir=project_dir,
                ref=ref,
                namespace=namespace,
                parameters=parameters,
                dry_run=dry_run,
                client_timeout=client_timeout,
                detach=detach,
                strict=strict,
                logger=logger,
                bauplan_client=bauplan_client,
            )
        except Exception as e:
            raise ToolError(f"Error executing project_run '{project_dir}': {e!s}") from e
