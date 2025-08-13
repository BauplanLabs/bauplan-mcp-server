from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError

from pydantic import BaseModel
from typing import Optional

from .create_client import create_bauplan_client


class NamespaceCreated(BaseModel):
    created: bool
    namespace: str
    branch: str
    message: Optional[str] = None


def register_create_namespace_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="create_namespace",
        description="Create a new namespace in a specified branch of the user's Bauplan data catalog using a namespace name.",
    )
    async def create_namespace(
        namespace: str, branch: str, api_key: Optional[str] = None, ctx: Context = None
    ) -> NamespaceCreated:
        """
        Create a new namespace in a specific branch of the user's Bauplan catalog.

        Args:
            namespace: Name of the namespace to create.
            branch: Branch name where the namespace will be created. Must follow the format <username.branch_name>.
            api_key: The Bauplan API key for authentication.

        Returns:
            NamespaceCreated: Object indicating success/failure with namespace details
        """
        try:
            # Create a fresh Bauplan client
            bauplan_client = create_bauplan_client(api_key)
            if ctx:
                await ctx.info(f"Creating namespace '{namespace}' in branch '{branch}")

            # Create the namespace
            assert bauplan_client.create_namespace(namespace=namespace, branch=branch)

            return NamespaceCreated(
                created=True,
                namespace=namespace,
                branch=branch,
                message=f"Successfully created namespace '{namespace}' in branch '{branch}'",
            )

        except Exception as err:
            raise ToolError(f"Error executing create_namespace: {err}")
