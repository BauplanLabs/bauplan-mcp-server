"""
Check if a table exists.
"""

from fastmcp import FastMCP
from pydantic import BaseModel
from fastmcp.exceptions import ToolError

from .create_client import with_bauplan_client
import bauplan
import logging
from fastmcp import Context

logger = logging.getLogger(__name__)


class TableExists(BaseModel):
    table_name: str
    exists: bool
    message: str


def register_has_table_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="has_table", exclude_args=["bauplan_client"])
    @with_bauplan_client
    async def has_table(
        table: str, ref: str, ctx: Context = None, bauplan_client: bauplan.Client = None
    ) -> TableExists:
        """
        Check if a specified table exists in a given branch of the user's Bauplan data catalog using a table name and branch name.

        Args:
            table: Name of the table to check for existence.
            ref: A reference to a commit that is a state of the user data lake: can be either a hash that starts with "@" and has 64 additional characters or a branch name, that is a mnemonic reference to the last commit that follows the "username.name" format.

        Returns:
            TableExists: Object indicating whether the table exists with details
        """
        try:
            if ctx:
                await ctx.info(f"Checking if table '{table}' exists in ref '{ref}'")

            # Call has_table function
            exists = bauplan_client.has_table(table=table, ref=ref)

            # Log the result
            logger.info(f"Table '{table}' exists: {exists}")

            return TableExists(
                table_name=table,
                exists=exists,
                message=f"Table '{table}' {'exists' if exists else 'does not exist'} in ref '{ref}'",
            )

        except Exception as e:
            logger.error(f"Error checking table {table}: {str(e)}")
            raise ToolError(f"Failed to check table {table}: {str(e)}")
