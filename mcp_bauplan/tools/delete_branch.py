import asyncio

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel

from ._guards import require_truthy_result, require_writable_branch
from .create_client import get_bauplan_client


class BranchDeleted(BaseModel):
    deleted: bool


def register_delete_branch_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="delete_branch")
    async def delete_branch(
        branch: str,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> BranchDeleted:
        """
        Delete a specified branch from the user's Bauplan data catalog using a branch name.

        Args:
            branch: Name of the branch to delete. Must follow the format <username.branch_name>.

        Returns:
            BranchDeleted: Object indicating whether the branch was deleted.
        """

        try:
            branch = require_writable_branch(branch, "delete_branch")

            if ctx:
                await ctx.info(f"Deleting branch '{branch}'")

            result = await asyncio.to_thread(
                lambda: bauplan_client.delete_branch(
                    branch=branch,
                )
            )
            require_truthy_result(result, "delete_branch")

            return BranchDeleted(deleted=True)

        except Exception as e:
            raise ToolError(f"Error executing delete_branch '{branch}': {e}") from e
