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


class TagsOut(BaseModel):
    tags: list[TagInfo]


def register_get_tags_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="get_tags")
    async def get_tags(
        filter_by_name: str | None = None,
        limit: int = 10,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> TagsOut:
        """
        Retrieve tags from the user's Bauplan data catalog as a list, with optional filter_by_name and limit (integer) to reduce response size.
        Get the tags using optional filters.

        Args:
            filter_by_name: Optional filter for tag names (substring match)
            limit: Optional maximum number of tags to return

        Returns:
            TagsOut: Object containing list of tags with their names and hashes
        """

        try:
            if ctx:
                await ctx.debug(f"Calling get_tags with filter_by_name='{filter_by_name}', limit={limit}")

            all_tags = await asyncio.to_thread(
                lambda: list(
                    bauplan_client.get_tags(
                        filter_by_name=filter_by_name or None,
                        limit=limit or 10,
                    )
                )
            )
            tags_list = [TagInfo(name=tag.name, hash=tag.hash) for tag in all_tags]

            return TagsOut(tags=tags_list)

        except Exception as e:
            raise ToolError(f"Error executing get_tags: {e}") from e
