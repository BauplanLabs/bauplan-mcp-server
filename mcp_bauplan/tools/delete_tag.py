from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError

from pydantic import BaseModel
from typing import Optional

from .create_client import with_bauplan_client
import bauplan


class TagDeleted(BaseModel):
    deleted: bool
    tag: str
    message: Optional[str] = None


def register_delete_tag_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="delete_tag", exclude_args=["bauplan_client"])
    @with_bauplan_client
    async def delete_tag(
        tag: str, ctx: Context = None, bauplan_client: bauplan.Client = None
    ) -> TagDeleted:
        """
        Delete a specified tag from a given branch in the user's Bauplan data catalog using a tag name and branch name.

        Args:
            tag: Name of the tag to delete.

        Returns:
            TagDeleted: Object indicating success/failure of the deletion
        """
        try:
            if ctx:
                await ctx.info(f"Deleting tag '{tag}")

            # Delete the tag
            assert bauplan_client.delete_tag(tag=tag)

            return TagDeleted(
                deleted=True, tag=tag, message=f"Successfully deleted tag '{tag}'"
            )

        except Exception as err:
            raise ToolError(f"Error executing delete_tag: {err}")
