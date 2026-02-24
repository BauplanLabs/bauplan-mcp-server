"""
Check if a branch exists.
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


class BranchExists(BaseModel):
    branch_name: str
    exists: bool
    message: str


def register_has_branch_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="has_branch")
    async def has_branch(
        branch: str,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> BranchExists:
        """
        Check if a specified branch exists in the user's Bauplan data catalog using a branch name.
        Check if a specific branch exists in the Bauplan catalog.

        Args:
            branch: Name of the branch to check for existence.

        Returns:
            BranchExists: Object indicating whether the branch exists with details
        """
        try:
            if ctx:
                await ctx.info(f"Checking if branch '{branch}' exists")

            # Call has_branch function
            exists = await asyncio.to_thread(
                bauplan_client.has_branch,
                branch=branch,
            )

            # Log the result
            logger.info(f"Branch '{branch}' exists: {exists}")

            return BranchExists(
                branch_name=branch,
                exists=exists,
                message=f"Branch '{branch}' {'exists' if exists else 'does not exist'} in the catalog",
            )

        except Exception as e:
            logger.error(f"Error checking branch {branch}: {e!s}")
            raise ToolError(f"Failed to check branch {branch}: {e!s}") from e
