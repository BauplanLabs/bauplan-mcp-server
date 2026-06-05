import asyncio

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError

from ._guards import require_writable_branch
from .create_client import get_bauplan_client
from .get_namespace import NamespaceInfo, NamespaceOut


def register_create_namespace_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="create_namespace")
    async def create_namespace(
        namespace: str,
        branch: str,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> NamespaceOut:
        """
        Create a new namespace in a specified branch of the user's Bauplan data catalog using a namespace name.

        Args:
            namespace: Name of the namespace to create.
            branch: Branch name where the namespace will be created. Must follow the format <username.branch_name>.

        Returns:
            NamespaceOut: Object containing the created namespace name.
        """

        try:
            branch = require_writable_branch(branch, "create_namespace")

            if ctx:
                await ctx.info(f"Creating namespace '{namespace}' in branch '{branch}'")

            result = await asyncio.to_thread(
                lambda: bauplan_client.create_namespace(
                    namespace=namespace,
                    branch=branch,
                )
            )

            return NamespaceOut(namespace=NamespaceInfo(name=result.name))

        except Exception as e:
            raise ToolError(f"Error executing create_namespace '{namespace}' in branch '{branch}': {e}") from e
