from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError

from pydantic import BaseModel
from typing import Optional

from .create_client import create_bauplan_client


class BranchDeleted(BaseModel):
    deleted: bool
    branch: str
    message: Optional[str] = None


def register_delete_branch_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="delete_branch",
        description="Delete a specified branch from the user's Bauplan data catalog using a branch name.",
    )
    async def delete_branch(
        api_key: str, branch: str, ctx: Context = None
    ) -> BranchDeleted:
        """
        Delete a branch from the user's Bauplan catalog.

        Args:
            api_key: The Bauplan API key for authentication.
            branch: Name of the new branch to create. Must follow the format <username.branch_name>.
            from_ref: Reference (branch/commit) to create the branch from. Can be either a branch name or a hash that starts with "@" and
            has 64 additional characters.

        Returns:
            BranchDeleted: Object indicating success/failure of the deletion
        """
        try:
            # Create a fresh Bauplan client
            bauplan_client = create_bauplan_client(api_key)
            if ctx:
                await ctx.info(f"Deleting branch '{branch}")

            # Delete the branch
            assert bauplan_client.delete_branch(branch)

            return BranchDeleted(
                deleted=True,
                branch=branch,
                message=f"Successfully deleted branch '{branch}'",
            )

        except Exception as err:
            raise ToolError(f"Error creating branch: {err}")
