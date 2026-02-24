import asyncio

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel

from .create_client import get_bauplan_client


class UserInfo(BaseModel):
    username: str | None
    full_name: str | None


def register_get_user_info_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="get_user_info")
    async def get_user_info(
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> UserInfo:
        """
        Retrieve user information about the current authenticated Bauplan user.
        Get information about the current authenticated user.

        Args:

        Returns:
            UserInfo: Object containing username and full name of the authenticated user
        """

        try:
            # Get user info from the client
            info = await asyncio.to_thread(bauplan_client.info)
            user = info.user

            if user is None:
                raise ToolError("No user information available for the authenticated user.")

            return UserInfo(
                username=user.username,
                full_name=user.full_name,
            )

        except Exception as e:
            raise ToolError(f"Failed to get user info: {e!s}") from e
