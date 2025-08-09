"""
Run a Bauplan project.
"""

from fastmcp import FastMCP
from pydantic import BaseModel
from typing import Optional, Dict, Union
from fastmcp.exceptions import ToolError

from .create_client import create_bauplan_client
import logging
from fastmcp import Context

logger = logging.getLogger(__name__)


class ProjectRun(BaseModel):
    success: bool
    message: str
    project_dir: str
    ref: str
    job_id: str
    namespace: Optional[str]


def register_project_run_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="project_run",
        description="Launch a job for a Bauplan pipeline from a specified directory and reference in the Bauplan catalog, returning a job ID to poll for the job status.",
    )
    async def project_run(
        project_dir: str,
        ref: str,
        namespace: Optional[str] = None,
        parameters: Optional[Dict[str, Union[str, int, float, bool]]] = None,
        dry_run: bool = False,
        client_timeout: int = 120,
        api_key: Optional[str] = None,
        ctx: Context = None,
    ) -> ProjectRun:
        """
        Run asynchronously a Bauplan pipeline from a specified project directory and reference.
        The method will return a job ID that can be used to poll for the job status.

        Args:
            project_dir: The directory of the project (where the bauplan_project.yml file is located).
            ref: The ref or branch name from which to run the project.
            namespace: The Namespace to run the job in. If not set, the job will be run in the default namespace.
            parameters: Parameters for templating into Python models. Must be simple types (str, int, float, bool).
            dry_run: Whether to enable or disable dry-run mode for the run; models are not materialized (defaults to False).
            client_timeout: Seconds to timeout (defaults to 120).
            api_key: The Bauplan API key for authentication.
            
        Returns:
            ProjectRun: Object indicating success/failure with job details
        """
        try:
            # Create a fresh Bauplan client
            bauplan_client = create_bauplan_client(api_key)
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

            # We dry-run everywhere, but no non-dry-run can be done with ref = 'main'
            assert dry_run or ref != "main", "Runs not allowed with ref='main', unless dry_run=True"

            # Call run function
            run_state = bauplan_client.run(
                project_dir=project_dir,
                ref=ref,
                namespace=namespace,
                parameters=processed_parameters,
                dry_run=dry_run,
                client_timeout=client_timeout,
                detach=True,  # Detach to run in background as default
            )

            # Log successful run
            logger.info(
                f"Successfully project run from '{project_dir}' with ref '{ref}', job ID: {run_state.job_id}"
            )

            return ProjectRun(
                success=True,
                message=f"Project run successfully executed from '{project_dir}' with ref '{ref}', job ID: {run_state.job_id}",
                project_dir=project_dir,
                ref=ref,
                job_id=run_state.job_id,
                namespace=namespace,
            )

        except Exception as e:
            logger.error(f"Error running project from {project_dir}: {str(e)}")
            raise ToolError(f"Failed to run project from {project_dir}: {str(e)}")
