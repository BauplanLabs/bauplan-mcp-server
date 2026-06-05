"""
Create a table import plan from an S3 location.
"""

import asyncio
import logging

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel

from ._guards import require_writable_branch
from .create_client import get_bauplan_client

logger = logging.getLogger(__name__)


class TablePlanCreated(BaseModel):
    job_id: str | None = None
    job_status: str | None = None
    table_name: str
    search_uri: str
    success: bool
    message: str
    namespace: str | None
    branch: str
    error: str | None = None
    plan: str | None = None
    can_auto_apply: bool
    files_to_be_imported: list[str]


def register_plan_table_creation_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="plan_table_creation")
    async def plan_table_creation(
        table: str,
        search_uri: str,
        branch: str,
        namespace: str | None = None,
        partitioned_by: str | None = None,
        replace: bool | None = None,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> TablePlanCreated:
        """
        Generate a YAML schema plan for importing a table from an S3 URI in the user's Bauplan data catalog returning a job ID for tracking).
        Create a table import plan from an S3 location.

        This operation will attempt to create a table based of schemas of N parquet files found by a given search uri.
        A YAML file containing the schema and plan is returned and if there are no conflicts, it is automatically applied.

        Args:
            table: Name of the table to plan creation for.
            search_uri: S3 URI to search for parquet files.
            namespace: Optional namespace. If omitted, resolution uses the default namespace.
            branch: Writable non-main branch name.
            partitioned_by: Optional partitioning column.
            replace: Optional flag to replace existing table.

        Returns:
            TablePlanCreated: Object indicating success/failure with job tracking details
        """

        try:
            branch = require_writable_branch(branch, "plan_table_creation")

            if ctx:
                await ctx.info(f"Creating table plan for '{table}' from search URI '{search_uri}'")

            result = await asyncio.to_thread(
                lambda: bauplan_client.plan_table_creation(
                    table=table,
                    search_uri=search_uri,
                    namespace=namespace,
                    branch=branch,
                    partitioned_by=partitioned_by,
                    replace=replace,
                )
            )

            # Extract job_id from TableCreatePlanState object
            job_id = result.job_id

            # Log successful plan creation with job_id
            logger.info(f"Successfully created table plan for '{table}' with job_id: {job_id}")
            success = result.error is None

            return TablePlanCreated(
                job_id=job_id,
                job_status=result.job_status,
                table_name=table,
                search_uri=search_uri,
                success=success,
                message=(
                    f"Table plan created successfully for '{table}' with job_id: {job_id}"
                    if success
                    else f"Table plan for '{table}' needs attention with job_id: {job_id}: {result.error}"
                ),
                namespace=namespace,
                branch=branch,
                error=result.error,
                plan=result.plan,
                can_auto_apply=result.can_auto_apply,
                files_to_be_imported=result.files_to_be_imported,
            )

        except Exception as e:
            logger.error(f"Error creating table plan for {table}: {e!s}")
            raise ToolError(f"Error executing plan_table_creation '{table}': {e!s}") from e
