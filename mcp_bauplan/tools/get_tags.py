import asyncio
from typing import Annotated

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel, Field

from ._schema import EXACT_OR_REGEX_FILTER_DESCRIPTION, read_only_tool_annotations
from .create_client import get_bauplan_client


class TagInfo(BaseModel):
    name: Annotated[
        str,
        Field(
            description="Tag name.",
        ),
    ]
    hash: Annotated[
        str,
        Field(
            description="Commit hash referenced by the tag.",
        ),
    ]


class TagsOut(BaseModel):
    tags: Annotated[
        list[TagInfo],
        Field(
            description="Tags matching the requested filters.",
        ),
    ]


def register_get_tags_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="get_tags", annotations=read_only_tool_annotations("Get tags"))
    async def get_tags(
        filter_by_name: Annotated[
            str | None,
            Field(
                description=f"Optional tag name filter. {EXACT_OR_REGEX_FILTER_DESCRIPTION}",
            ),
        ] = None,
        limit: Annotated[
            int,
            Field(
                description="Maximum number of tags to return. Defaults to 25.",
                ge=1,
                le=250,
            ),
        ] = 25,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> TagsOut:
        """
        List tags in the catalog, optionally filtered by name.
        Use this when the user asks for stable refs or wants to inspect named catalog snapshots.
        """

        try:
            if ctx:
                await ctx.debug(f"Calling get_tags with filter_by_name='{filter_by_name}', limit={limit}")

            all_tags = await asyncio.to_thread(
                lambda: list(
                    bauplan_client.get_tags(
                        filter_by_name=filter_by_name or None,
                        limit=limit,
                    )
                )
            )
            tags_list = [TagInfo(name=tag.name, hash=tag.hash) for tag in all_tags]

            return TagsOut(tags=tags_list)

        except Exception as e:
            raise ToolError(f"Error executing get_tags: {e}") from e
