from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError

from pydantic import BaseModel
from typing import Optional

from .create_client import create_bauplan_client


class UserInfo(BaseModel):
    username: str
    full_name: str


def register_get_user_info_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="get_user_info",
        description="Retrieve user information about the current authenticated Bauplan user.",
    )
    async def get_user_info(
        api_key: Optional[str] = None,
        ctx: Context = None,
    ) -> UserInfo:
        """
        Get information about the current authenticated user.

        Args:
            api_key: The Bauplan API key for authentication.

        Returns:
            UserInfo: Object containing username and full name of the authenticated user
        """

        try:
            # Create a fresh Bauplan client
            bauplan_client = create_bauplan_client(api_key)
            
            # Get user info from the client
            info = bauplan_client.info()
            user = info.user
            
            # Debug logging
            if ctx:
                await ctx.debug(
                    f"Retrieved user info for username: {user.username}"
                )

            return UserInfo(
                username=user.username,
                full_name=user.full_name
            )

        except ConnectionError as e:
            raise ToolError(f"Connection error: {str(e)}")
        except Exception as e:
            raise ToolError(f"Failed to get user info: {str(e)}")