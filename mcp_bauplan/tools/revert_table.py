"""
Revert a table from a source reference to a target branch.
"""

from fastmcp import FastMCP
from pydantic import BaseModel
from typing import Optional
from fastmcp.exceptions import ToolError

from .create_client import with_bauplan_client
import bauplan
import logging
from fastmcp import Context

logger = logging.getLogger(__name__)


class TableReverted(BaseModel):
    table_name: str
    source_ref: str
    into_branch: str
    success: bool
    message: str


def register_revert_table_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="revert_table", exclude_args=["bauplan_client"])
    @with_bauplan_client
    async def revert_table(
        table: str,
        source_ref: str,
        into_branch: str,
        replace: Optional[bool] = None,
        ctx: Context = None,
        bauplan_client: bauplan.Client = None,
    ) -> TableReverted:
        """
        Revert a specified table from a source reference to a target branch in the user's Bauplan data catalog using a table name, source reference, and target branch.
        Revert a table from a source reference to a target branch.

        Args:
            table: The table to revert.
            source_ref: The name of the source ref.
            into_branch: The name of the target branch where the table will be reverted.
            replace: Optional, whether to replace the table if it already exists.

        Returns:
            TableReverted: Object indicating success/failure with revert details
        """
        try:
            if ctx:
                await ctx.info(
                    f"Reverting table '{table}' from source ref '{source_ref}' into branch '{into_branch}'"
                )

            # Call revert_table function
            bauplan_client.revert_table(
                table=table,
                source_ref=source_ref,
                into_branch=into_branch,
                replace=replace,
            )

            # Log successful revert
            logger.info(
                f"Successfully reverted table '{table}' from '{source_ref}' into branch '{into_branch}'"
            )

            return TableReverted(
                table_name=table,
                source_ref=source_ref,
                into_branch=into_branch,
                success=True,
                message=f"Table '{table}' successfully reverted from '{source_ref}' into branch '{into_branch}'",
            )

        except Exception as e:
            logger.error(f"Error reverting table {table}: {str(e)}")
            raise ToolError(f"Failed to revert table {table}: {str(e)}")
