import asyncio
from typing import Annotated

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel, Field

from ._schema import EXACT_OR_REGEX_FILTER_DESCRIPTION, read_only_tool_annotations, remote_read_tags
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


class BranchesOut(BaseModel):
    branches: Annotated[
        list[BranchInfo],
        Field(
            description="Branches matching the requested filters.",
        ),
    ]


def register_get_branches_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="get_branches",
        annotations=read_only_tool_annotations("Get branches"),
        tags=remote_read_tags(),
    )
    async def get_branches(
        name: Annotated[
            str | None,
            Field(
                description=f"Optional branch name filter. {EXACT_OR_REGEX_FILTER_DESCRIPTION}",
            ),
        ] = None,
        user: Annotated[
            str | None,
            Field(
                description=(
                    f"Optional branch owner filter. {EXACT_OR_REGEX_FILTER_DESCRIPTION} "
                    "Use ~ for the current user."
                ),
            ),
        ] = None,
        limit: Annotated[
            int,
            Field(
                description="Maximum number of branches to return. Defaults to 25.",
                ge=1,
                le=250,
            ),
        ] = 25,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> BranchesOut:
        """
        List catalog branches, optionally filtered by name or user.
        Use this to find development branches or validate available branch names before branch operations.
        """

        try:
            branches_list = []

            all_branches = await asyncio.to_thread(
                lambda: list(
                    bauplan_client.get_branches(
                        name=name or None,
                        user=user or None,
                        limit=limit,
                    )
                )
            )
            for branch in all_branches:
                if branch.hash is None:
                    raise ToolError(f"Branch '{branch.name}' does not have a valid hash.")

                branch_info = BranchInfo(name=branch.name, hash=branch.hash)
                branches_list.append(branch_info)

            return BranchesOut(branches=branches_list)

        except Exception as e:
            raise ToolError(f"Error executing get_branches: {e}") from e
