"""
Delete a table.
"""

import asyncio
import logging

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError

from ._guards import require_writable_branch
from .create_client import get_bauplan_client
from .get_branch import BranchInfo, BranchOut

logger = logging.getLogger(__name__)


def register_delete_table_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="delete_table")
    async def delete_table(
        table: str,
        branch: str,
        namespace: str | None = None,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> BranchOut:
        """
        Delete a specified table from the user's Bauplan data catalog using a table name.
        Delete a table from the user's Bauplan data lake.

        Args:
            table: Name of the table to delete.
            branch: Branch name where the table will be deleted. Must follow the format <username.branch_name>.
            namespace: Optional namespace. If omitted, resolution uses the default namespace.

        Returns:
            BranchOut: Object containing the updated branch name and head commit hash.
        """

        try:
            branch = require_writable_branch(branch, "delete_table")

            if ctx:
                await ctx.info(f"Deleting table '{table}' from branch '{branch}'")

            result = await asyncio.to_thread(
                lambda: bauplan_client.delete_table(
                    table=table,
                    branch=branch,
                    namespace=namespace or None,
                )
            )

            # Log the result
            logger.info(f"Successfully deleted table '{table}' from branch '{branch}'")

            return BranchOut(branch=BranchInfo(name=result.name, hash=result.hash))

        except Exception as e:
            logger.error(f"Error deleting table {table}: {e!s}")
            raise ToolError(f"Error executing delete_table '{table}' in branch '{branch}': {e!s}") from e
