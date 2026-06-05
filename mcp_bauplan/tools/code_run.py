"""
Run a Bauplan project from code files provided as a dictionary.
"""

import logging
import os
import shutil
import tempfile
from pathlib import Path

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError

from .create_client import get_bauplan_client
from .run_bauplan_project import RunState, run_project

logger = logging.getLogger(__name__)


def _project_file_path(root: Path, filename: str) -> Path:
    relative_path = Path(filename)
    if not filename or relative_path.is_absolute() or ".." in relative_path.parts:
        raise ToolError(f"Invalid project file path '{filename}'")
    return root / relative_path


def register_code_run_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="code_run")
    async def code_run(
        project_files: dict[str, str],
        ref: str | None = None,
        namespace: str | None = None,
        parameters: dict[str, str | int | float | bool | None] | None = None,
        dry_run: bool = False,
        client_timeout: int = 30,
        detach: bool = True,
        strict: bool = True,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> RunState:
        """
        Run a pipeline from provided source code files as a dictionary and a data ref,
        returning a job ID and success/failure to the caller.

        Args:
            project_files: Dictionary mapping file names to source code as strings. Must contain bauplan_project.yml and .sql/.py files.
            ref: The ref or branch name from which to run the project. Required unless dry_run is True.
            namespace: The Namespace to run the job in. If not set, the job will be run in the default namespace.
            parameters: Parameters for templating DAGs. Keys are parameter names, values must be simple types (str, int, float, bool). Default: None.
            dry_run: Whether to enable or disable dry-run mode for the run; models are not materialized (defaults to False).
            client_timeout: Seconds to timeout (defaults to 30).
            detach: Whether to return after job submission instead of waiting for completion; use get_job to check status (defaults to True).
            strict: Whether to enable strict mode for the run (defaults to True).

        Returns:
            RunState: Object indicating success/failure with job Id.
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
                await ctx.info(f"Running project from {len(project_files)} provided files with ref '{ref}'")

            # Create temporary directory
            temp_dir = tempfile.mkdtemp(prefix="bauplan_code_run_")
            logger.info(f"Created temporary directory: {temp_dir}")

            # Write all files to temporary directory
            for filename, content in project_files.items():
                file_path = _project_file_path(Path(temp_dir), filename)

                # Create subdirectories if filename contains path separators
                file_path.parent.mkdir(parents=True, exist_ok=True)

                # Write the file
                file_path.write_text(content)
                logger.debug(f"Written file: {file_path}")

            return await run_project(
                project_dir=temp_dir,
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
            logger.error(f"Error running code project: {e!s}")
            raise ToolError(f"Error executing code_run: {e!s}") from e

        finally:
            # Clean up temporary directory if it was created
            if temp_dir and os.path.exists(temp_dir):
                try:
                    shutil.rmtree(temp_dir)
                    logger.debug(f"Cleaned up temporary directory: {temp_dir}")
                except Exception as cleanup_error:
                    logger.warning(f"Failed to clean up temporary directory {temp_dir}: {cleanup_error}")
