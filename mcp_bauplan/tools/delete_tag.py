import asyncio

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel

from .create_client import get_bauplan_client


class TagDeleted(BaseModel):
    deleted: bool
    tag: str
    message: str | None = None


def register_delete_tag_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="delete_tag")
    async def delete_tag(
        tag: str,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> TagDeleted:
        """
        Delete a specified tag from the user's Bauplan data catalog using a tag name.

        Args:
            tag: Name of the tag to delete.

        Returns:
            TagDeleted: Object indicating success/failure of the deletion
        """
        try:
            if ctx:
                await ctx.info(f"Deleting tag '{tag}")

            # Delete the tag
            assert await asyncio.to_thread(
                bauplan_client.delete_tag,
                tag=tag,
            )

            return TagDeleted(deleted=True, tag=tag, message=f"Successfully deleted tag '{tag}'")

        except Exception as e:
            raise ToolError(f"Error executing delete_tag: {e}") from e
