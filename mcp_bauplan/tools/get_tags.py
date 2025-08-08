from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError

from pydantic import BaseModel
from typing import List, Optional


from .create_client import create_bauplan_client


class TagInfo(BaseModel):
    name: str


class TagsOut(BaseModel):
    tags: List[TagInfo]
    total_count: int


def register_get_tags_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="get_tags",
        description="Retrieve tags for a specified branch in the user's Bauplan data catalog as a list, using a branch name with optional filter_by_name and limit (integer) to reduce response size.",
    )
    async def get_tags(
        api_key: str,
        filter_by_name: Optional[str] = None,
        limit: Optional[int] = 10,
        ctx: Context = None,
    ) -> TagsOut:
        """
        Get the tags of a branch using optional filters.

        Args:
            api_key: The Bauplan API key for authentication.
            filter_by_name: Optional filter for tag names (substring match)
            limit: Optional maximum number of tags to return

        Returns:
            TagsOut: Object containing list of tags and total count
        """

        try:
            # Create a fresh Bauplan client
            bauplan_client = create_bauplan_client(api_key)
            # Debug logging
            if ctx:
                await ctx.debug(
                    f"Calling get_tags with filter_by_name='{filter_by_name}', limit={limit}"
                )

            # Get tags with filters
            tags_list = []
            tag_count = 0

            try:
                # Call with direct parameters instead of kwargs
                for tag in bauplan_client.get_tags(
                    filter_by_name=filter_by_name, limit=limit
                ):
                    try:
                        # Extract tag information
                        tag_info = TagInfo(name=getattr(tag, "name", str(tag)))
                        tags_list.append(tag_info)
                        tag_count += 1

                        # If we have a limit and reached it, break
                        if limit and tag_count >= limit:
                            break

                    except Exception as e:
                        if ctx:
                            await ctx.debug(f"Error processing tag: {str(e)}")
                        continue

            except Exception as e:
                if ctx:
                    await ctx.error(f"Error iterating tags: {str(e)}")

            return TagsOut(tags=tags_list, total_count=len(tags_list))

        except Exception as err:
            raise ToolError(f"Error executing get_tags: {err}")
