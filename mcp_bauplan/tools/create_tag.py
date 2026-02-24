import asyncio

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel

from .create_client import get_bauplan_client


class TagCreated(BaseModel):
    created: bool
    tag: str
    from_ref: str
    message: str | None = None


def register_create_tag_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="create_tag")
    async def create_tag(
        tag: str,
        from_ref: str,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> TagCreated:
        """
        Create a new tag in a specified branch of the user's Bauplan data catalog using a tag name.
        Create a new tag in a specific branch of the user's Bauplan catalog.

        Args:
            tag: Name of the tag to create.
            from_ref: Reference (branch or commit) from which to create the tag.

        Returns:
            TagCreated: Object indicating success/failure with tag details
        """
        try:
            if ctx:
                await ctx.info(f"Creating tag '{tag}' from reference '{from_ref}")

            # Create the tag
            assert await asyncio.to_thread(
                bauplan_client.create_tag,
                tag=tag,
                from_ref=from_ref,
            )

            return TagCreated(
                created=True,
                tag=tag,
                from_ref=from_ref,
                message=f"Successfully created tag '{tag}' from reference '{from_ref}'",
            )

        except Exception as e:
            raise ToolError(f"Error executing create_tag: {e}") from e
