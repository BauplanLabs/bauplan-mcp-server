import asyncio

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError

from .create_client import get_bauplan_client
from .get_tag import TagInfo, TagOut


def register_create_tag_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="create_tag")
    async def create_tag(
        tag: str,
        from_ref: str,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> TagOut:
        """
        Create a new tag in a specified branch of the user's Bauplan data catalog using a tag name.
        Create a new tag in a specific branch of the user's Bauplan catalog.

        Args:
            tag: Name of the tag to create.
            from_ref: Reference (branch or commit) from which to create the tag.

        Returns:
            TagOut: Object containing the created tag name and target commit hash.
        """

        try:
            if ctx:
                await ctx.info(f"Creating tag '{tag}' from reference '{from_ref}'")

            result = await asyncio.to_thread(
                lambda: bauplan_client.create_tag(
                    tag=tag,
                    from_ref=from_ref,
                )
            )

            return TagOut(tag=TagInfo(name=result.name, hash=result.hash))

        except Exception as e:
            raise ToolError(f"Error executing create_tag '{tag}': {e}") from e
