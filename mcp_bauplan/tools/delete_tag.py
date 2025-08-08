from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError

from pydantic import BaseModel
from typing import Optional

from .create_client import create_bauplan_client


class TagDeleted(BaseModel):
    deleted: bool
    tag: str
    message: Optional[str] = None


def register_delete_tag_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="delete_tag",
        description="Delete a specified tag from a given branch in the user's Bauplan data catalog using a tag name and branch name.",
    )
    async def delete_tag(api_key: str, tag: str, ctx: Context = None) -> TagDeleted:
        """
        Delete a tag from a specific branch of the user's Bauplan catalog.

        Args:
            api_key: The Bauplan API key for authentication.
            tag: Name of the tag to delete.

        Returns:
            TagDeleted: Object indicating success/failure of the deletion
        """
        try:
            # Create a fresh Bauplan client
            bauplan_client = create_bauplan_client(api_key)
            if ctx:
                await ctx.info(f"Deleting tag '{tag}")

            # Delete the tag
            assert bauplan_client.delete_tag(tag=tag)

            return TagDeleted(
                deleted=True, tag=tag, message=f"Successfully deleted tag '{tag}'"
            )

        except Exception as err:
            raise ToolError(f"Error executing delete_tag: {err}")
