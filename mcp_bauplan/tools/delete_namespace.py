import asyncio
from typing import Annotated

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import Field

from ._guards import require_writable_branch
from .create_client import get_bauplan_client
from .get_branch import BranchInfo, BranchOut


def register_delete_namespace_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="delete_namespace")
    async def delete_namespace(
        namespace: Annotated[
            str,
            Field(
                description="Name of the namespace to delete.",
            ),
        ],
        branch: Annotated[
            str,
            Field(
                description="Writable branch containing the namespace to delete.",
            ),
        ],
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> BranchOut:
        """
        Delete a namespace from a writable Bauplan branch.
        Use this only after the namespace is empty and no longer needed on that branch.
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
            raise ToolError(
                f"Error executing delete_namespace '{namespace}' in branch '{branch}': {e}"
            ) from e
