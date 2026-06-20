"""
Get a tag by name.
"""

import asyncio
from typing import Annotated

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel, Field

from ._schema import read_only_tool_annotations
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


class TagOut(BaseModel):
    tag: Annotated[
        TagInfo,
        Field(
            description="Requested tag.",
        ),
    ]


def register_get_tag_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="get_tag", annotations=read_only_tool_annotations("Get tag"))
    async def get_tag(
        tag: Annotated[
            str,
            Field(
                description="Tag name to retrieve.",
            ),
        ],
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> TagOut:
        """
        Retrieve a single Bauplan tag by name.
        Use this when the tag name is known and the model needs the referenced commit hash.
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
