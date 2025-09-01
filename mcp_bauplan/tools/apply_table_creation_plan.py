"""
Apply a table creation plan to resolve schema conflicts.
"""

from fastmcp import FastMCP
from pydantic import BaseModel
from typing import Optional, Dict, Any
from fastmcp.exceptions import ToolError

from .create_client import with_bauplan_client
import bauplan
import logging
from fastmcp import Context

logger = logging.getLogger(__name__)


class TablePlanApplied(BaseModel):
    job_id: str
    success: bool
    message: str


def register_apply_table_creation_plan_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="apply_table_creation_plan", exclude_args=["bauplan_client"])
    @with_bauplan_client
    async def apply_table_creation_plan(
        plan: Dict[str, Any],
        args: Optional[Dict[str, str]] = None,
        client_timeout: int = 120,
        ctx: Context = None,
        bauplan_client: bauplan.Client = None,
    ) -> TablePlanApplied:
        """
        Apply a provided table creation plan to resolve schema conflicts and create a new table in the system.
        Returns a job_id for tracking the asynchronous operation.

        This function is used when schema conflicts exist after plan creation and need manual resolution.
        Most common schema conflict is two parquet files with the same column name but different datatype.
        Note: This is done automatically during table plan creation if no schema conflicts exist.

        Args:
            plan: The plan dictionary or TableCreatePlanState to apply.
            args: Additional arguments for plan application (optional).
            client_timeout: Timeout in seconds (defaults to 120).

        Returns:
            TablePlanApplied: Object indicating success/failure with job tracking details
        """
        try:
            if ctx:
                await ctx.info("Applying table creation plan")

            # Call apply_table_creation_plan function
            result = bauplan_client.apply_table_creation_plan(
                plan=plan, args=args, client_timeout=client_timeout
            )

            # Extract job_id from TableCreatePlanApplyState object
            job_id = result.job_id

            # Log successful plan application with job_id
            logger.info(
                f"Successfully applied table creation plan with job_id: {job_id}"
            )

            return TablePlanApplied(
                job_id=job_id,
                success=True,
                message=f"Table creation plan applied successfully with job_id: {job_id}",
            )

        except Exception as e:
            logger.error(f"Error applying table creation plan: {str(e)}")
            raise ToolError(f"Failed to apply table creation plan: {str(e)}")
