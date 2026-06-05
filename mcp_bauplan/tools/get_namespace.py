"""
Get a namespace by name.
"""

import asyncio

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel

from .create_client import get_bauplan_client


class NamespaceInfo(BaseModel):
    name: str


class NamespaceOut(BaseModel):
    namespace: NamespaceInfo


def register_get_namespace_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="get_namespace")
    async def get_namespace(
        namespace: str,
        ref: str,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> NamespaceOut:
        """
        Retrieve a single Bauplan namespace by name at a ref.

        Args:
            namespace: Name of the namespace to retrieve.
            ref: Branch, tag, or commit ref to read from.

        Returns:
            NamespaceOut: Object containing the namespace name.
        """

        try:
            if ctx:
                await ctx.info(f"Getting namespace '{namespace}' in ref '{ref}'")

            namespace_info = await asyncio.to_thread(
                lambda: bauplan_client.get_namespace(
                    namespace=namespace,
                    ref=ref,
                )
            )

            return NamespaceOut(namespace=NamespaceInfo(name=namespace_info.name))

        except Exception as e:
            raise ToolError(f"Error executing get_namespace '{namespace}' in ref '{ref}': {e}") from e
