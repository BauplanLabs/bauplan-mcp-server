import asyncio

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError

from ._guards import require_writable_branch
from .create_client import get_bauplan_client
from .get_branch import BranchInfo, BranchOut


def register_delete_namespace_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="delete_namespace")
    async def delete_namespace(
        namespace: str,
        branch: str,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> BranchOut:
        """
        Delete a specified namespace from a given branch in the user's Bauplan data catalog using a namespace name and branch name.
        Delete a namespace from a specific branch of the user's Bauplan catalog.

        Args:
            namespace: Name of the namespace to delete.
            branch: Branch name containing the namespace to delete. Must follow the format <username.branch_name>.

        Returns:
            BranchOut: Object containing the updated branch name and head commit hash.
        """

        try:
            branch = require_writable_branch(branch, "delete_namespace")

            if ctx:
                await ctx.info(f"Deleting namespace '{namespace}' from branch '{branch}'")

            result = await asyncio.to_thread(
                lambda: bauplan_client.delete_namespace(
                    namespace=namespace,
                    branch=branch,
                )
            )

            return BranchOut(branch=BranchInfo(name=result.name, hash=result.hash))

        except Exception as e:
            raise ToolError(f"Error executing delete_namespace '{namespace}' in branch '{branch}': {e}") from e
