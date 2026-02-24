import asyncio

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel

from .create_client import get_bauplan_client


class BranchDeleted(BaseModel):
    deleted: bool
    branch: str
    message: str | None = None


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
            BranchDeleted: Object indicating success/failure of the deletion
        """
        try:
            if ctx:
                await ctx.info(f"Deleting branch '{branch}")

            # Delete the branch
            assert await asyncio.to_thread(
                bauplan_client.delete_branch,
                branch=branch,
            )

            return BranchDeleted(
                deleted=True,
                branch=branch,
                message=f"Successfully deleted branch '{branch}'",
            )

        except Exception as e:
            raise ToolError(f"Error deleting branch: {e}") from e
