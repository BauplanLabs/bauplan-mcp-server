import asyncio

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel

from .create_client import get_bauplan_client


class NamespaceDeleted(BaseModel):
    deleted: bool
    namespace: str
    branch: str
    message: str | None = None


def register_delete_namespace_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="delete_namespace")
    async def delete_namespace(
        namespace: str,
        branch: str,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> NamespaceDeleted:
        """
        Delete a specified namespace from a given branch in the user's Bauplan data catalog using a namespace name and branch name.
        Delete a namespace from a specific branch of the user's Bauplan catalog.

        Args:
            namespace: Name of the namespace to delete.
            branch: Branch name containing the namespace to delete. Must follow the format <username.branch_name>.

        Returns:
            NamespaceDeleted: Object indicating success/failure of the deletion
        """
        try:
            if ctx:
                await ctx.info(f"Deleting namespace '{namespace}' from branch '{branch}'")

            # Delete the namespace
            assert await asyncio.to_thread(
                bauplan_client.delete_namespace,
                namespace=namespace,
                branch=branch,
            )

            return NamespaceDeleted(
                deleted=True,
                namespace=namespace,
                branch=branch,
                message=f"Successfully deleted namespace '{namespace}' from branch '{branch}'",
            )

        except Exception as e:
            raise ToolError(f"Error executing delete_namespace: {e}") from e
