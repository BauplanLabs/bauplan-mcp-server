import asyncio
from typing import Annotated

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel, Field

from ._guards import require_truthy_result
from .create_client import get_bauplan_client


class TagDeleted(BaseModel):
    deleted: Annotated[
        bool,
        Field(
            description="Whether the tag was deleted.",
        ),
    ]


def register_delete_tag_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="delete_tag")
    async def delete_tag(
        tag: Annotated[
            str,
            Field(
                description="Name of the tag to delete.",
            ),
        ],
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> TagDeleted:
        """
        Delete a Bauplan catalog tag by name.
        Use this only when the user explicitly wants to remove a named catalog snapshot.
        """

        try:
            if ctx:
                await ctx.info(f"Deleting tag '{tag}'")

            result = await asyncio.to_thread(
                lambda: bauplan_client.delete_tag(
                    tag=tag,
                )
            )
            require_truthy_result(result, "delete_tag")

            return TagDeleted(deleted=True)

        except Exception as e:
            raise ToolError(f"Error executing delete_tag '{tag}': {e}") from e
