"""
Delete a table.
"""

import asyncio
import logging
from typing import Annotated

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import Field

from ._guards import require_writable_branch
from ._schema import mutating_tool_annotations, remote_write_tags
from .create_client import get_bauplan_client
from .get_branch import BranchInfo, BranchOut

logger = logging.getLogger(__name__)


def register_delete_table_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="delete_table",
        annotations=mutating_tool_annotations("Delete table", destructive=True),
        tags=remote_write_tags(destructive=True),
    )
    async def delete_table(
        table: Annotated[
            str,
            Field(
                description="Table to delete. Use namespace.table for non-default namespaces.",
            ),
        ],
        branch: Annotated[
            str,
            Field(
                description="Writable branch containing the table to delete.",
            ),
        ],
        namespace: Annotated[
            str | None,
            Field(
                description=(
                    "Namespace for a bare table name. Leave null when the table name is fully "
                    "qualified or should resolve through the default namespace."
                ),
            ),
        ] = None,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> BranchOut:
        """
        Delete a table from a writable Bauplan branch.
        Use this when the user wants to remove a table from an isolated branch before review or merge.
        """

        try:
            branch = require_writable_branch(branch, "delete_table")

            if ctx:
                await ctx.info(f"Deleting table '{table}' from branch '{branch}'")

            result = await asyncio.to_thread(
                lambda: bauplan_client.delete_table(
                    table=table,
                    branch=branch,
                    namespace=namespace or None,
                )
            )

            # Log the result
            logger.info(f"Successfully deleted table '{table}' from branch '{branch}'")

            return BranchOut(branch=BranchInfo(name=result.name, hash=result.hash))

        except Exception as e:
            logger.error(f"Error deleting table {table}: {e!s}")
            raise ToolError(f"Error executing delete_table '{table}' in branch '{branch}': {e!s}") from e
