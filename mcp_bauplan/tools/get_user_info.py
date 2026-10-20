import asyncio
from typing import Annotated

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel, Field

from ._schema import read_only_tool_annotations, remote_read_tags
from .create_client import get_bauplan_client


class UserInfo(BaseModel):
    username: Annotated[
        str,
        Field(
            description="Authenticated Bauplan username.",
        ),
    ]
    full_name: Annotated[
        str,
        Field(
            description="Authenticated user's full name.",
        ),
    ]


def register_get_user_info_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="get_user_info",
        annotations=read_only_tool_annotations("Get user info"),
        tags=remote_read_tags(),
    )
    async def get_user_info(
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> UserInfo:
        """
        Get the authenticated Bauplan user's username and display name.
        Use this to confirm which credentials the MCP server is using before user-scoped operations.
        """

        try:
            info = await asyncio.to_thread(lambda: bauplan_client.info())
            return UserInfo(
                username=info.user.username,
                full_name=info.user.full_name,
            )

        except Exception as e:
            raise ToolError(f"Error executing get_user_info: {e!s}") from e
