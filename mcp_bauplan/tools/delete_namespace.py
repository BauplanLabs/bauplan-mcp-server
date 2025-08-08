from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError

from pydantic import BaseModel
from typing import Optional

from .create_client import create_bauplan_client


class NamespaceDeleted(BaseModel):
    deleted: bool
    namespace: str
    branch: str
    message: Optional[str] = None


def register_delete_namespace_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="delete_namespace",
        description="Delete a specified namespace from a given branch in the user's Bauplan data catalog using a namespace name and branch name.",
    )
    async def delete_namespace(
        api_key: str, namespace: str, branch: str, ctx: Context = None
    ) -> NamespaceDeleted:
        """
        Delete a namespace from a specific branch of the user's Bauplan catalog.

        Args:
            api_key: The Bauplan API key for authentication.
            namespace: Name of the namespace to delete.
            branch: Branch name containing the namespace to delete. Must follow the format <username.branch_name>.

        Returns:
            NamespaceDeleted: Object indicating success/failure of the deletion
        """
        try:
            # Create a fresh Bauplan client
            bauplan_client = create_bauplan_client(api_key)
            if ctx:
                await ctx.info(
                    f"Deleting namespace '{namespace}' from branch '{branch}'"
                )

            # Delete the namespace
            assert bauplan_client.delete_namespace(namespace=namespace, branch=branch)

            return NamespaceDeleted(
                deleted=True,
                namespace=namespace,
                branch=branch,
                message=f"Successfully deleted namespace '{namespace}' from branch '{branch}'",
            )

        except Exception as err:
            raise ToolError(f"Error executing get_table: {err}")
