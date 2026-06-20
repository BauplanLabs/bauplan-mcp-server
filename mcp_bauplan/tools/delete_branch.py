import asyncio
from typing import Annotated

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel, Field

from ._guards import require_truthy_result, require_writable_branch
from ._schema import mutating_tool_annotations, remote_write_tags
from .create_client import get_bauplan_client


class BranchDeleted(BaseModel):
    deleted: Annotated[
        bool,
        Field(
            description="Whether the branch was deleted.",
        ),
    ]


def register_delete_branch_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="delete_branch",
        annotations=mutating_tool_annotations("Delete branch", destructive=True),
        tags=remote_write_tags(destructive=True),
    )
    async def delete_branch(
        branch: Annotated[
            str,
            Field(
                description="Writable branch name to delete.",
            ),
        ],
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> BranchDeleted:
        """
        Delete a writable Bauplan branch.
        Use this only after confirming the branch is no longer needed.
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
