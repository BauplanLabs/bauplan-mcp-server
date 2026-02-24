import asyncio

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError
from pydantic import BaseModel

from .create_client import with_bauplan_client


class NamespaceCreated(BaseModel):
    created: bool
    namespace: str
    branch: str
    message: str | None = None


def register_create_namespace_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="create_namespace", exclude_args=["bauplan_client"])
    @with_bauplan_client
    async def create_namespace(
        bauplan_client: bauplan.Client,
        namespace: str,
        branch: str,
        ctx: Context | None = None,
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
            assert await asyncio.to_thread(
                bauplan_client.create_namespace,
                namespace=namespace,
                branch=branch,
            )

            return NamespaceCreated(
                created=True,
                namespace=namespace,
                branch=branch,
                message=f"Successfully created namespace '{namespace}' in branch '{branch}'",
            )

        except Exception as e:
            raise ToolError(f"Error executing create_namespace: {e}") from e
