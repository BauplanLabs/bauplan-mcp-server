"""
Check if a branch exists.
"""

from fastmcp import FastMCP
from pydantic import BaseModel
from typing import Optional
from fastmcp.exceptions import ToolError

from .create_client import create_bauplan_client
import logging
from fastmcp import Context

logger = logging.getLogger(__name__)


class BranchExists(BaseModel):
    branch_name: str
    exists: bool
    message: str


def register_has_branch_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="has_branch",
        description="Check if a specified branch exists in the user's Bauplan data catalog using a branch name.",
    )
    async def has_branch(
        branch: str, api_key: Optional[str] = None, ctx: Context = None
    ) -> BranchExists:
        """
        Check if a specific branch exists in the Bauplan catalog.

        Args:
            branch: Name of the branch to check for existence.
            api_key: The Bauplan API key for authentication.

        Returns:
            BranchExists: Object indicating whether the branch exists with details
        """
        try:
            # Create a fresh Bauplan client
            bauplan_client = create_bauplan_client(api_key)
            if ctx:
                await ctx.info(f"Checking if branch '{branch}' exists")

            # Call has_branch function
            exists = bauplan_client.has_branch(branch=branch)

            # Log the result
            logger.info(f"Branch '{branch}' exists: {exists}")

            return BranchExists(
                branch_name=branch,
                exists=exists,
                message=f"Branch '{branch}' {'exists' if exists else 'does not exist'} in the catalog",
            )

        except Exception as e:
            logger.error(f"Error checking branch {branch}: {str(e)}")
            raise ToolError(f"Failed to check branch {branch}: {str(e)}")
