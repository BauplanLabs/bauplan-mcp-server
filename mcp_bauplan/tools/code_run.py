"""
Run a Bauplan project from code files provided as a dictionary.
"""

from fastmcp import FastMCP
from typing import Optional, Dict, Union
from fastmcp.exceptions import ToolError
import tempfile
import os
import shutil
from pathlib import Path

from .create_client import with_bauplan_client
from .run_bauplan_project import RunState, run_project
import bauplan
import logging
from fastmcp import Context

logger = logging.getLogger(__name__)


def register_code_run_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="code_run", exclude_args=["bauplan_client"])
    @with_bauplan_client
    async def code_run(
        project_files: Dict[str, str],
        ref: str,
        namespace: Optional[str] = None,
        parameters: Optional[Dict[str, Union[str, int, float, bool]]] = None,
        dry_run: Optional[bool] = False,
        client_timeout: Optional[int] = 120,
        ctx: Optional[Context] | None = None,
        bauplan_client: Optional[bauplan.Client] | None = None,
    ) -> RunState:
        """
        Launch a job for a Bauplan pipeline from code files provided as a dictionary, returning a job ID to poll for the job status.
        Run asynchronously a Bauplan pipeline from provided project files.
        The method will return a job ID that can be used to poll for the job status.

        Args:
            project_files: Dictionary mapping file names to file content as strings. Must contain bauplan_project.yml and .sql/.py files.
            ref: The ref or branch name from which to run the project.
            namespace: The Namespace to run the job in. If not set, the job will be run in the default namespace.
            parameters: Parameters for templating DAGs. Keys are parameter names, values must be simple types (str, int, float, bool).
            dry_run: Whether to enable or disable dry-run mode for the run; models are not materialized (defaults to False).
            client_timeout: Seconds to timeout (defaults to 120).

        Returns:
            CodeRun: Object indicating success/failure with job details
        """
        temp_dir = None
        try:
            # Validate that bauplan_project.yml exists
            if "bauplan_project.yml" not in project_files:
                raise ToolError("project_files must contain 'bauplan_project.yml'")

            # Validate file extensions
            for filename in project_files.keys():
                if filename != "bauplan_project.yml":
                    if not (filename.endswith(".sql") or filename.endswith(".py")):
                        raise ToolError(
                            f"Invalid file extension for '{filename}'. Only .sql and .py files are allowed (besides bauplan_project.yml)"
                        )

            if ctx:
                await ctx.info(
                    f"Running project from {len(project_files)} provided files with ref '{ref}'"
                )

            # Create temporary directory
            temp_dir = tempfile.mkdtemp(prefix="bauplan_code_run_")
            logger.info(f"Created temporary directory: {temp_dir}")

            # Write all files to temporary directory
            for filename, content in project_files.items():
                file_path = Path(temp_dir) / filename

                # Create subdirectories if filename contains path separators
                file_path.parent.mkdir(parents=True, exist_ok=True)

                # Write the file
                file_path.write_text(content)
                logger.debug(f"Written file: {file_path}")

            return run_project(
                project_dir=temp_dir,
                ref=ref,
                namespace=namespace,
                parameters=parameters,
                dry_run=dry_run,
                client_timeout=client_timeout,
                logger=logger,
                bauplan_client=bauplan_client,
            )

        except Exception as e:
            logger.error(f"Error running code project: {str(e)}")
            raise ToolError(f"Failed to run code project: {str(e)}")

        finally:
            # Clean up temporary directory if it was created
            if temp_dir and os.path.exists(temp_dir):
                try:
                    shutil.rmtree(temp_dir)
                    logger.debug(f"Cleaned up temporary directory: {temp_dir}")
                except Exception as cleanup_error:
                    logger.warning(
                        f"Failed to clean up temporary directory {temp_dir}: {cleanup_error}"
                    )
