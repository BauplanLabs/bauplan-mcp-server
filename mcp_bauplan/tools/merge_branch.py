from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError

from pydantic import BaseModel
from typing import Optional

from .create_client import create_bauplan_client


class MergeResult(BaseModel):
    merged: bool
    source_ref: str
    target_branch: str
    message: Optional[str] = None


def register_merge_branch_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="merge_branch",
        description="Merge a source branch into a target branch in the user's Bauplan data catalog using source and target branch names.",
    )
    async def merge_branch(
        source_ref: str,
        into_branch: str,
        commit_message: Optional[str] = None,
        commit_body: Optional[str] = None,
        api_key: Optional[str] = None,
        ctx: Context = None,
    ) -> MergeResult:
        """
        Merge a source branch into a target branch. Branch names must follow the format <username.branch_name>.

        Args:
            source_ref: The branch to merge from. The name must follow the format <username.branch_name>.
            into_branch: The target branch to merge into. The name must follow the format <username.branch_name>.
            commit_message: Optional custom commit message for the merge
            commit_body: Optional additional commit body/description
            api_key: The Bauplan API key for authentication.

        Returns:
            MergeResult: Object indicating success/failure with merge details
        """
        try:
            # Create a fresh Bauplan client
            bauplan_client = create_bauplan_client(api_key)
            # Build kwargs for the API call
            kwargs = {"source_ref": source_ref, "into_branch": into_branch}

            if commit_message:
                kwargs["commit_message"] = commit_message
            if commit_body:
                kwargs["commit_body"] = commit_body

            if ctx:
                await ctx.info(f"Merging '{source_ref}' into '{into_branch}'")

            # Perform the merge
            assert bauplan_client.merge_branch(
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

        except Exception as err:
            raise ToolError(f"Error merging branch: {err}")
