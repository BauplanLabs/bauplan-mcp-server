"""
Run a Bauplan project from code files provided as a dictionary.
"""

import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import Annotated

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import Field

from ._schema import mutating_tool_annotations
from .create_client import get_bauplan_client
from .get_job import JobOut
from .run_bauplan_project import run_project

logger = logging.getLogger(__name__)

PROJECT_CONFIG_FILES = ("bauplan_project.yml", "bauplan_project.yaml")


def _project_file_path(root: Path, filename: str) -> Path:
    relative_path = Path(filename)
    if not filename or relative_path.is_absolute() or ".." in relative_path.parts:
        raise ToolError(f"Invalid project file path '{filename}'")
    return root / relative_path


def register_code_run_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="code_run", annotations=mutating_tool_annotations("Run code", destructive=True))
    async def code_run(
        project_files: Annotated[
            dict[str, str],
            Field(
                description=(
                    "Project files keyed by Unix-style path relative to the Bauplan project root. "
                    "Must include bauplan_project.yml or bauplan_project.yaml and SQL or Python model files."
                ),
            ),
        ],
        ref: Annotated[
            str,
            Field(
                description=(
                    "Explicit branch or ref to run the project against. Non-dry-run executions "
                    "must target a non-main branch."
                ),
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
        Run a Bauplan project from source files provided directly by the client.
        Use this for remote workflows where project_run cannot access client-local files.
        """

        temp_dir = None
        try:
            # Validate that a Bauplan project config exists.
            if not any(config_file in project_files for config_file in PROJECT_CONFIG_FILES):
                raise ToolError("project_files must contain 'bauplan_project.yml' or 'bauplan_project.yaml'")

            # Validate file extensions
            for filename in project_files.keys():
                if filename not in PROJECT_CONFIG_FILES:
                    if not (filename.endswith(".sql") or filename.endswith(".py")):
                        raise ToolError(
                            f"Invalid file extension for '{filename}'. Only .sql and .py files are allowed besides Bauplan project config files"
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
