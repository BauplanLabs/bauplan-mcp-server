"""
Check if a table exists.
"""

import asyncio
import logging

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel

from .create_client import get_bauplan_client

logger = logging.getLogger(__name__)


class TableExists(BaseModel):
    table_name: str
    exists: bool
    message: str


def register_has_table_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="has_table")
    async def has_table(
        table: str,
        ref: str,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
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
            exists = await asyncio.to_thread(
                bauplan_client.has_table,
                table=table,
                ref=ref,
            )

            # Log the result
            logger.info(f"Table '{table}' exists: {exists}")

            return TableExists(
                table_name=table,
                exists=exists,
                message=f"Table '{table}' {'exists' if exists else 'does not exist'} in ref '{ref}'",
            )

        except Exception as e:
            logger.error(f"Error checking table {table}: {e!s}")
            raise ToolError(f"Failed to check table {table}: {e!s}") from e
