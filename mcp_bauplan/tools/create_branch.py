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


def register_create_branch_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="create_branch")
    async def create_branch(
        branch: Annotated[
            str,
            Field(
                description="Name of the branch to create.",
            ),
        ],
        from_ref: Annotated[
            str,
            Field(
                description="Branch, tag, or commit ref to create the branch from.",
            ),
        ],
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> BranchOut:
        """
        Create a writable, zero-copy development branch from an existing branch, tag, or commit ref.
        Use this before catalog changes so writes happen in isolation before review and merge.
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
