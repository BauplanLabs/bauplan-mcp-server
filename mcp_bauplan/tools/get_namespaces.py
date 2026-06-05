import asyncio

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel

from .create_client import get_bauplan_client


class NamespaceInfo(BaseModel):
    name: str


class NamespacesOut(BaseModel):
    namespaces: list[NamespaceInfo]


def register_get_namespaces_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="get_namespaces")
    async def get_namespaces(
        ref: str,
        namespace: str | None = None,
        limit: int = 10,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> NamespacesOut:
        """
        Retrieve namespaces for a branch from the user's Bauplan data catalog as a list. Use 'limit' (integer) to reduce response size.
        Get the namespaces of a branch using optional filters.

        Args:
            ref: branch or commit hash to get namespaces from. Can be either a hash that starts with "@" and
                has 64 additional characters or a branch name, that is a mnemonic reference to the last commit that follows the "username.name" format.
            namespace: Optional filter for namespace names (substring match)
            limit: Optional maximum number of namespaces to return (default: 10)

        Returns:
            NamespacesOut: Object containing namespaces matching the optional filter.
        """

        try:
            if ctx:
                await ctx.debug(
                    f"Calling get_namespaces with ref='{ref}', filter_by_name='{namespace}', limit={limit}"
                )

            all_namespaces = await asyncio.to_thread(
                lambda: list(
                    bauplan_client.get_namespaces(
                        ref=ref,
                        filter_by_name=namespace or None,
                        limit=limit or 10,
                    )
                )
            )
            namespaces_list = [NamespaceInfo(name=ns.name) for ns in all_namespaces]

            return NamespacesOut(namespaces=namespaces_list)

        except Exception as e:
            raise ToolError(f"Error executing get_namespaces: {e}") from e
