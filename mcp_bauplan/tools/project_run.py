"""
Run a Bauplan project.
"""

from fastmcp import FastMCP
from pydantic import BaseModel
from typing import Optional, Dict, Union
from fastmcp.exceptions import ToolError

from .create_client import with_fresh_client
import logging
from fastmcp import Context

logger = logging.getLogger(__name__)


class ProjectRun(BaseModel):
    success: bool
    message: str
    project_dir: str
    ref: str
    namespace: Optional[str]


def register_project_run_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="project_run",
        description="Execute a Bauplan project from a specified directory and reference in the user's Bauplan data catalog, returning a job ID.",
    )
    @with_fresh_client
    async def project_run(
        project_dir: str,
        ref: str,
        bauplan_client,
        namespace: Optional[str] = None,
        parameters: Optional[Dict[str, Union[str, int, float, bool]]] = None,
        dry_run: bool = False,
        client_timeout: int = 120,
        detach: bool = True,
        ctx: Context = None,
    ) -> ProjectRun:
        """
        Run a Bauplan project from a specified directory and reference.

        Args:
            project_dir: The directory of the project (where the bauplan_project.yml file is located).
            ref: The ref or branch name from which to run the project.
            namespace: The Namespace to run the job in. If not set, the job will be run in the default namespace.
            parameters: Parameters for templating into Python models. Must be simple types (str, int, float, bool).
            dry_run: Whether to enable or disable dry-run mode for the run; models are not materialized (defaults to False).
            client_timeout: Seconds to timeout (defaults to 120).
            detach: Whether to detach the run and return immediately instead of blocking on log streaming (defaults to True).

        Returns:
            ProjectRun: Object indicating success/failure with run details
        """
        try:
            if ctx:
                await ctx.info(f"Running project from '{project_dir}' with ref '{ref}'")

            # Process parameters to ensure they are primitive types
            processed_parameters = None
            if parameters:
                processed_parameters = {}
                for key, value in parameters.items():
                    if isinstance(value, (str, int, float, bool)):
                        processed_parameters[key] = value
                    else:
                        # Convert complex types to string representation
                        processed_parameters[key] = str(value)
                        logger.warning(
                            f"Parameter '{key}' converted from {type(value)} to string: {processed_parameters[key]}"
                        )

                logger.info(f"Processed parameters: {processed_parameters}")

            # Call run function
            bauplan_client.run(
                project_dir=project_dir,
                ref=ref,
                namespace=namespace,
                parameters=processed_parameters,
                dry_run=dry_run,
                client_timeout=client_timeout,
                detach=detach,
            )

            # Log successful run
            logger.info(
                f"Successfully executed project run from '{project_dir}' with ref '{ref}'"
            )

            return ProjectRun(
                success=True,
                message=f"Project run successfully executed from '{project_dir}' with ref '{ref}'",
                project_dir=project_dir,
                ref=ref,
                namespace=namespace,
            )

        except Exception as e:
            logger.error(f"Error running project from {project_dir}: {str(e)}")
            raise ToolError(f"Failed to run project from {project_dir}: {str(e)}")
