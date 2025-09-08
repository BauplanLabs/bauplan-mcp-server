from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError

from pydantic import BaseModel
from typing import Optional

from .create_client import with_bauplan_client
import bauplan


class NamespaceCreated(BaseModel):
    created: bool
    namespace: str
    branch: str
    message: Optional[str] = None


def register_create_namespace_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="create_namespace", exclude_args=["bauplan_client"])
    @with_bauplan_client
    async def create_namespace(
        namespace: str,
        branch: str,
        ctx: Context = None,
        bauplan_client: bauplan.Client = None,
    ) -> NamespaceCreated:
        """
        Create a new namespace in a specified branch of the user's Bauplan data catalog using a namespace name.

        Args:
            namespace: Name of the namespace to create.
            branch: Branch name where the namespace will be created. Must follow the format <username.branch_name>.

        Returns:
            NamespaceCreated: Object indicating success/failure with namespace details
        """
        try:
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
