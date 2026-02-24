import asyncio

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel

from .create_client import get_bauplan_client


class MergeResult(BaseModel):
    merged: bool
    source_ref: str
    target_branch: str
    message: str | None = None


def register_merge_branch_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="merge_branch")
    async def merge_branch(
        source_ref: str,
        into_branch: str,
        commit_message: str | None = None,
        commit_body: str | None = None,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> MergeResult:
        """
        Merge a source branch into a target branch in the user's Bauplan data catalog using source and target branch names.
        Branch names must follow the format <username.branch_name>.

        Args:
            source_ref: The branch to merge from. The name must follow the format <username.branch_name>.
            into_branch: The target branch to merge into. The name must follow the format <username.branch_name>.
            commit_message: Optional custom commit message for the merge
            commit_body: Optional additional commit body/description

        Returns:
            MergeResult: Object indicating success/failure with merge details
        """
        try:
            if ctx:
                await ctx.info(f"Merging '{source_ref}' into '{into_branch}'")

            # Perform the merge
            assert await asyncio.to_thread(
                bauplan_client.merge_branch,
                source_ref=source_ref,
                into_branch=into_branch,
                commit_message=commit_message,
                commit_body=commit_body,
            )

            return MergeResult(
                merged=True,
                source_ref=source_ref,
                target_branch=into_branch,
                message=f"Successfully merged '{source_ref}' into '{into_branch}'",
            )

        except Exception as e:
            raise ToolError(f"Error merging branch: {e!s}") from e
