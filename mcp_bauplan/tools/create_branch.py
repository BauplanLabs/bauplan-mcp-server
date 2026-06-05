import asyncio

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError

from ._guards import require_writable_branch
from .create_client import get_bauplan_client
from .get_branch import BranchInfo, BranchOut


def register_create_branch_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="create_branch")
    async def create_branch(
        branch: str,
        from_ref: str,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> BranchOut:
        """
        Create a new branch in the user's Bauplan data catalog using a branch name, returning a confirmation.

        Args:
            branch: Name of the new branch to create. Must follow the format <username.branch_name>.
            from_ref: Reference (branch/commit) to create the branch from. Can be either a branch name or a hash that starts with "@" and
            has 64 additional characters.

        Returns:
            BranchOut: Object containing the created branch name and head commit hash.
        """

        try:
            branch = require_writable_branch(branch, "create_branch")

            if ctx:
                await ctx.info(f"Creating branch '{branch}' from ref '{from_ref}'")

            result = await asyncio.to_thread(
                lambda: bauplan_client.create_branch(
                    branch=branch,
                    from_ref=from_ref,
                )
            )
            return BranchOut(branch=BranchInfo(name=result.name, hash=result.hash))

        except Exception as e:
            raise ToolError(f"Error executing create_branch '{branch}': {e}") from e
