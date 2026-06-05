"""
Get a branch by name.
"""

import asyncio

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel

from .create_client import get_bauplan_client


class BranchInfo(BaseModel):
    name: str
    hash: str


class BranchOut(BaseModel):
    branch: BranchInfo


def register_get_branch_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="get_branch")
    async def get_branch(
        branch: str,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> BranchOut:
        """
        Retrieve a single Bauplan branch by name.

        Args:
            branch: Name of the branch to retrieve.

        Returns:
            BranchOut: Object containing the branch name and head commit hash.
        """

        try:
            if ctx:
                await ctx.info(f"Getting branch '{branch}'")

            branch_info = await asyncio.to_thread(
                lambda: bauplan_client.get_branch(
                    branch=branch,
                )
            )

            return BranchOut(branch=BranchInfo(name=branch_info.name, hash=branch_info.hash))

        except Exception as e:
            raise ToolError(f"Error executing get_branch '{branch}': {e}") from e
