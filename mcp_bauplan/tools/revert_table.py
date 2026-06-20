"""
Revert a table from a source reference to a target branch.
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


def register_revert_table_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="revert_table",
        annotations=mutating_tool_annotations("Revert table", destructive=True),
        tags=remote_write_tags(destructive=True),
    )
    async def revert_table(
        table: Annotated[
            str,
            Field(
                description="Table to restore. Use namespace.table for non-default namespaces.",
            ),
        ],
        source_ref: Annotated[
            str,
            Field(
                description="Source branch, tag, or commit ref to restore the table from.",
            ),
        ],
        into_branch: Annotated[
            str,
            Field(
                description="Target branch where the table will be reverted.",
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
        replace: Annotated[
            bool | None,
            Field(
                description="Whether to replace the destination table if it already exists.",
            ),
        ] = None,
        commit_body: Annotated[
            str | None,
            Field(
                description="Optional commit body to attach to the revert operation.",
            ),
        ] = None,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> BranchOut:
        """
        Restore a table on a target branch from its state at another branch, tag, or commit ref.
        Use this to roll back a table or copy a known-good table state.
        """

        try:
            into_branch = require_writable_branch(into_branch, "revert_table")

            if ctx:
                await ctx.info(
                    f"Reverting table '{table}' from source ref '{source_ref}' into branch '{into_branch}'"
                )

            result = await asyncio.to_thread(
                lambda: bauplan_client.revert_table(
                    table=table,
                    namespace=namespace or None,
                    source_ref=source_ref,
                    into_branch=into_branch,
                    replace=replace,
                    commit_body=commit_body or None,
                )
            )

            logger.info(
                f"Successfully reverted table '{table}' from '{source_ref}' into branch '{into_branch}'"
            )

            return BranchOut(branch=BranchInfo(name=result.name, hash=result.hash))

        except Exception as e:
            raise ToolError(
                f"Error executing revert_table '{table}' into branch '{into_branch}': {e!s}"
            ) from e
