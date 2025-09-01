from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError

from pydantic import BaseModel

from .create_client import with_bauplan_client
import bauplan


class UserInfo(BaseModel):
    username: str
    full_name: str | None


def register_get_user_info_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="get_user_info", exclude_args=["bauplan_client"])
    @with_bauplan_client
    async def get_user_info(
        ctx: Context = None,
        bauplan_client: bauplan.Client = None,
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
            user = bauplan_client.info().user
            username = user.username
            full_name = user.full_name

            return UserInfo(username=username, full_name=full_name)

        except Exception as e:
            raise ToolError(f"Failed to get user info: {str(e)}")
