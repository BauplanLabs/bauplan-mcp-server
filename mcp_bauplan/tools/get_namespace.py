"""
Get a namespace by name.
"""

import asyncio
from typing import Annotated

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel, Field

from .create_client import get_bauplan_client


class NamespaceInfo(BaseModel):
    name: Annotated[
        str,
        Field(
            description="Namespace name.",
        ),
    ]


class NamespaceOut(BaseModel):
    namespace: Annotated[
        NamespaceInfo,
        Field(
            description="Requested namespace.",
        ),
    ]


def register_get_namespace_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="get_namespace")
    async def get_namespace(
        namespace: Annotated[
            str,
            Field(
                description="Namespace name to retrieve.",
            ),
        ],
        ref: Annotated[
            str,
            Field(
                description="Branch, tag, or commit ref to inspect.",
            ),
        ],
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> NamespaceOut:
        """
        Retrieve a single Bauplan namespace by name at a ref.
        Use this when the namespace name is known and the model needs to verify it exists on a ref.
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
