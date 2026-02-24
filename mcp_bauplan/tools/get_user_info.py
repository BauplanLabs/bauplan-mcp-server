import asyncio

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError
from pydantic import BaseModel

from .create_client import with_bauplan_client


class UserInfo(BaseModel):
    username: str | None
    full_name: str | None


def register_get_user_info_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="get_user_info", exclude_args=["bauplan_client"])
    @with_bauplan_client
    async def get_user_info(
        bauplan_client: bauplan.Client,
        ctx: Context | None = None,
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
