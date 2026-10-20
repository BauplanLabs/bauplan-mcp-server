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


def register_rename_tag_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="rename_tag",
        annotations=mutating_tool_annotations("Rename tag", destructive=True),
        tags=remote_write_tags(destructive=True),
    )
    async def rename_tag(
        tag: Annotated[
            str,
            Field(
                description="Tag to rename.",
            ),
        ],
        new_tag: Annotated[
            str,
            Field(
                description="New tag name.",
            ),
        ],
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> TagOut:
        """
        Rename a catalog tag. The old name is gone, so references that use it will fail.
        Use this only when the user asks to rename a tag.
        """

        try:
            if ctx:
                await ctx.info(f"Renaming tag '{tag}' to '{new_tag}'")

            result = await asyncio.to_thread(
                lambda: bauplan_client.rename_tag(
                    tag=tag,
                    new_tag=new_tag,
                )
            )
            return TagOut(tag=TagInfo(name=result.name, hash=result.hash))

        except Exception as e:
            raise ToolError(f"Error executing rename_tag '{tag}': {e}") from e
