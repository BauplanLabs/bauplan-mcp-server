"""
Get a tag by name.
"""

import asyncio

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel

from .create_client import get_bauplan_client


class TagInfo(BaseModel):
    name: str
    hash: str


class TagOut(BaseModel):
    tag: TagInfo


def register_get_tag_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="get_tag")
    async def get_tag(
        tag: str,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> TagOut:
        """
        Retrieve a single Bauplan tag by name.

        Args:
            tag: Name of the tag to retrieve.

        Returns:
            TagOut: Object containing the tag name and target commit hash.
        """

        try:
            if ctx:
                await ctx.info(f"Getting tag '{tag}'")

            tag_info = await asyncio.to_thread(
                lambda: bauplan_client.get_tag(
                    tag=tag,
                )
            )

            return TagOut(tag=TagInfo(name=tag_info.name, hash=tag_info.hash))

        except Exception as e:
            raise ToolError(f"Error executing get_tag '{tag}': {e}") from e
