"""
Delete a table.
"""

from fastmcp import FastMCP
from pydantic import BaseModel
from fastmcp.exceptions import ToolError

from .create_client import with_bauplan_client
import bauplan
import logging
from fastmcp import Context

logger = logging.getLogger(__name__)


class TableDeleted(BaseModel):
    table_name: str
    deleted: bool
    message: str


def register_delete_table_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="delete_table", exclude_args=["bauplan_client"])
    @with_bauplan_client
    async def delete_table(
        table: str,
        branch: str,
        ctx: Context = None,
        bauplan_client: bauplan.Client = None,
    ) -> TableDeleted:
        """
        Delete a specified table from the user's Bauplan data catalog using a table name.
        Delete a table from the user's Bauplan data lake.

        Args:
            table: Name of the table to delete.
            branch: Branch name where the table will be deleted. Must follow the format <username.branch_name>.

        Returns:
            TableDeleted: Object indicating whether the table was deleted with details
        """
        try:
            if ctx:
                await ctx.info(f"Deleting table '{table}' from branch '{branch}")

            # Call delete_table function
            bauplan_client.delete_table(table=table, branch=branch)

            # Log the result
            logger.info(f"Successfully deleted table '{table}' from branch '{branch}'")

            return TableDeleted(
                table_name=table,
                deleted=True,
                message=f"Table '{table}' successfully deleted from branch '{branch}'",
            )

        except Exception as e:
            logger.error(f"Error deleting table {table}: {str(e)}")
            raise ToolError(f"Failed to delete table {table}: {str(e)}")
