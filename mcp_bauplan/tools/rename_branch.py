import asyncio
from typing import Annotated

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import Field

from ._guards import require_writable_branch
from ._schema import mutating_tool_annotations, remote_write_tags
from .create_client import get_bauplan_client
from .get_branch import BranchInfo, BranchOut


def register_rename_branch_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="rename_branch",
        annotations=mutating_tool_annotations("Rename branch", destructive=True),
        tags=remote_write_tags(destructive=True),
    )
    async def rename_branch(
        branch: Annotated[
            str,
            Field(
                description="Writable branch to rename.",
            ),
        ],
        new_branch: Annotated[
            str,
            Field(
                description="New branch name. It must start with your username, e.g. alice.new_name.",
            ),
        ],
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> BranchOut:
        """
        Rename a writable branch. The old name is gone, so jobs and references that use it will fail.
        Use this only when the user asks to rename a branch.
        """

        try:
            branch = require_writable_branch(branch, "rename_branch")
            new_branch = require_writable_branch(new_branch, "rename_branch")

            if ctx:
                await ctx.info(f"Renaming branch '{branch}' to '{new_branch}'")

            result = await asyncio.to_thread(
                lambda: bauplan_client.rename_branch(
                    branch=branch,
                    new_branch=new_branch,
                )
            )
            return BranchOut(branch=BranchInfo(name=result.name, hash=result.hash))

        except Exception as e:
            raise ToolError(f"Error executing rename_branch '{branch}': {e}") from e
