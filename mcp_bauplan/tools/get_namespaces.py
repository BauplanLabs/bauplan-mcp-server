import asyncio
from typing import Annotated

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel, Field

from ._schema import read_only_tool_annotations, remote_read_tags
from .create_client import get_bauplan_client


class NamespaceInfo(BaseModel):
    name: Annotated[
        str,
        Field(
            description="Namespace name.",
        ),
    ]


class NamespacesOut(BaseModel):
    namespaces: Annotated[
        list[NamespaceInfo],
        Field(
            description="Namespaces available on the requested ref.",
        ),
    ]


def register_get_namespaces_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="get_namespaces",
        annotations=read_only_tool_annotations("Get namespaces"),
        tags=remote_read_tags(),
    )
    async def get_namespaces(
        ref: Annotated[
            str,
            Field(
                description="Branch, tag, or commit ref to inspect.",
            ),
        ],
        namespace: Annotated[
            str | None,
            Field(
                description="Optional namespace name filter.",
            ),
        ] = None,
        limit: Annotated[
            int,
            Field(
                description="Maximum number of namespaces to return. Defaults to 25.",
                ge=1,
                le=250,
            ),
        ] = 25,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> NamespacesOut:
        """
        List namespaces on a branch, tag, or commit ref.
        Use this to discover namespace names before table lookup, import, or namespace cleanup.
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
                        limit=limit,
                    )
                )
            )
            namespaces_list = [NamespaceInfo(name=ns.name) for ns in all_namespaces]

            return NamespacesOut(namespaces=namespaces_list)

        except Exception as e:
            raise ToolError(f"Error executing get_namespaces: {e}") from e
