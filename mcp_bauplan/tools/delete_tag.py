import asyncio

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel

from ._guards import require_truthy_result
from .create_client import get_bauplan_client


class TagDeleted(BaseModel):
    deleted: bool


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
            TagDeleted: Object indicating whether the tag was deleted.
        """

        try:
            if ctx:
                await ctx.info(f"Deleting tag '{tag}'")

            result = await asyncio.to_thread(
                lambda: bauplan_client.delete_tag(
                    tag=tag,
                )
            )
            require_truthy_result(result, "delete_tag")

            return TagDeleted(deleted=True)

        except Exception as e:
            raise ToolError(f"Error executing delete_tag '{tag}': {e}") from e
