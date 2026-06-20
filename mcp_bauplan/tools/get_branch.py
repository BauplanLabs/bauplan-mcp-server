import asyncio
from typing import Annotated

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel, Field

from ._schema import read_only_tool_annotations, remote_read_tags
from .create_client import get_bauplan_client


class BranchInfo(BaseModel):
    name: Annotated[
        str,
        Field(
            description="Branch name.",
        ),
    ]
    hash: Annotated[
        str,
        Field(
            description="Commit hash currently referenced by the branch.",
        ),
    ]


class BranchOut(BaseModel):
    branch: Annotated[
        BranchInfo,
        Field(
            description="Requested branch.",
        ),
    ]


def register_get_branch_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="get_branch", annotations=read_only_tool_annotations("Get branch"), tags=remote_read_tags()
    )
    async def get_branch(
        branch: Annotated[
            str,
            Field(
                description="Branch name to retrieve.",
            ),
        ],
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> BranchOut:
        """
        Get one catalog branch by name.
        Use this when the branch name is known and the model needs the current commit hash.
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
