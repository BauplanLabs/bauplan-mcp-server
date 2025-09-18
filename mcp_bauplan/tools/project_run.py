"""
Run a Bauplan project from a specified directory.
"""

from fastmcp import FastMCP

from typing import Optional, Dict, Union
from fastmcp.exceptions import ToolError

from .create_client import with_bauplan_client
import bauplan
import logging
from fastmcp import Context
from .run_bauplan_project import RunState, run_project

logger = logging.getLogger(__name__)


def register_project_run_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="project_run", exclude_args=["bauplan_client"])
    @with_bauplan_client
    async def project_run(
        project_dir: str,
        ref: str,
        namespace: Optional[str] = None,
        parameters: Optional[Dict[str, Optional[Union[str, int, float, bool]]]] = None,
        dry_run: Optional[bool] = False,
        client_timeout: Optional[int] = 120,
        ctx: Optional[Context] | None = None,
        bauplan_client: bauplan.Client = None,
    ) -> RunState:
        """
        Launch a job for a Bauplan pipeline from a specified directory and reference in the Bauplan catalog, returning a job ID to poll for the job status.
        Run asynchronously a Bauplan pipeline from a specified project directory and reference.
        The method will return a job ID that can be used to poll for the job status.

        Args:
            project_dir: The directory of the project (where the bauplan_project.yml file is located).
            ref: The ref or branch name from which to run the project.
            namespace: The Namespace to run the job in. If not set, the job will be run in the default namespace.
            parameters: Parameters for templating DAGs. Keys are parameter names, values must be simple types (str, int, float, bool).
            dry_run: Whether to enable or disable dry-run mode for the run; models are not materialized (defaults to False).
            client_timeout: Seconds to timeout (defaults to 120).

        Returns:
            ProjectRun: Object indicating success/failure with job details
        """
        try:
            if ctx:
                await ctx.info(f"Running project from '{project_dir}' with ref '{ref}'")

            return run_project(
                project_dir=project_dir,
                ref=ref,
                namespace=namespace,
                parameters=parameters,
                dry_run=dry_run,
                client_timeout=client_timeout,
                logger=logger,
                bauplan_client=bauplan_client,
            )
        except Exception as e:
            logger.error(f"Error running project from {project_dir}: {str(e)}")
            raise ToolError(f"Failed to run project from {project_dir}: {str(e)}")
