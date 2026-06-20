import asyncio
from typing import Annotated

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import Field

from ._guards import require_writable_branch
from ._schema import mutating_tool_annotations
from .create_client import get_bauplan_client
from .get_branch import BranchInfo, BranchOut


def register_merge_branch_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="merge_branch", annotations=mutating_tool_annotations("Merge branch", destructive=True))
    async def merge_branch(
        source_ref: Annotated[
            str,
            Field(
                description="Source branch, tag, or commit ref to merge.",
            ),
        ],
        into_branch: Annotated[
            str,
            Field(
                description="Target branch that will receive the merge.",
            ),
        ],
        commit_message: Annotated[
            str | None,
            Field(
                description="Optional merge commit message.",
            ),
        ] = None,
        commit_body: Annotated[
            str | None,
            Field(
                description="Optional merge commit body.",
            ),
        ] = None,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> BranchOut:
        """
        Merge a source ref into a target Bauplan branch.
        Use this only after validating the source ref and confirming the target branch is the intended writable destination.
        """

        try:
            into_branch = require_writable_branch(into_branch, "merge_branch")

            if ctx:
                await ctx.info(f"Merging '{source_ref}' into '{into_branch}'")

            result = await asyncio.to_thread(
                lambda: bauplan_client.merge_branch(
                    source_ref=source_ref,
                    into_branch=into_branch,
                    commit_message=commit_message or None,
                    commit_body=commit_body or None,
                )
            )

            return BranchOut(branch=BranchInfo(name=result.name, hash=result.hash))

        except Exception as e:
            raise ToolError(f"Error executing merge_branch '{source_ref}' into '{into_branch}': {e!s}") from e
