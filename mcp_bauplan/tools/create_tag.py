import asyncio
from typing import Annotated

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import Field

from ._schema import mutating_tool_annotations, remote_write_tags
from .create_client import get_bauplan_client
from .get_tag import TagInfo, TagOut


def register_create_tag_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="create_tag", annotations=mutating_tool_annotations("Create tag"), tags=remote_write_tags()
    )
    async def create_tag(
        tag: Annotated[
            str,
            Field(
                description="Name of the tag to create.",
            ),
        ],
        from_ref: Annotated[
            str,
            Field(
                description="Branch, tag, or commit ref the new tag will point to.",
            ),
        ],
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> TagOut:
        """
        Create a stable catalog tag pointing to a branch, tag, or commit ref.
        Use this to mark a known catalog snapshot for later inspection or reuse.
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
