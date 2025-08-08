from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError

from pydantic import BaseModel
from typing import Optional

from .create_client import with_fresh_client


class TagCreated(BaseModel):
    created: bool
    tag: str
    from_ref: str
    message: Optional[str] = None


def register_create_tag_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="create_tag",
        description="Create a new tag in a specified branch of the user's Bauplan data catalog using a tag name.",
    )
    @with_fresh_client
    async def create_tag(
        tag: str, from_ref: str, bauplan_client, ctx: Context = None
    ) -> TagCreated:
        """
        Create a new tag in a specific branch of the user's Bauplan catalog.

        Args:
            tag: Name of the tag to create.
            from_ref: Reference (branch or commit) from which to create the tag.

        Returns:
            TagCreated: Object indicating success/failure with tag details
        """
        try:
            if ctx:
                await ctx.info(f"Creating tag '{tag}' from reference '{from_ref}'")

            # Create the tag
            assert bauplan_client.create_tag(tag=tag, from_ref=from_ref)

            return TagCreated(
                created=True,
                tag=tag,
                from_ref=from_ref,
                message=f"Successfully created tag '{tag}' from reference '{from_ref}'",
            )

        except Exception as err:
            raise ToolError(f"Error executing create_tag: {err}")
