"""
Revert a table from a source reference to a target branch.
"""

import asyncio
import logging

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError

from ._guards import require_writable_branch
from .create_client import get_bauplan_client
from .get_branch import BranchInfo, BranchOut

logger = logging.getLogger(__name__)


def register_revert_table_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="revert_table")
    async def revert_table(
        table: str,
        source_ref: str,
        into_branch: str,
        namespace: str | None = None,
        replace: bool | None = None,
        commit_body: str | None = None,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> BranchOut:
        """
        Revert a specified table from a source reference to a target branch in the user's Bauplan data catalog using a table name, source reference, and target branch.
        Revert a table from a source reference to a target branch.

        Args:
            table: The table to revert.
            source_ref: The name of the source ref.
            into_branch: The name of the target branch where the table will be reverted.
            namespace: Optional namespace. If omitted, resolution uses the default namespace.
            replace: Optional, whether to replace the table if it already exists.
            commit_body: Optional commit body to attach to the revert operation.

        Returns:
            BranchOut: Object containing the updated target branch name and head commit hash.
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

            # Log successful revert
            logger.info(
                f"Successfully reverted table '{table}' from '{source_ref}' into branch '{into_branch}'"
            )

            return BranchOut(branch=BranchInfo(name=result.name, hash=result.hash))

        except Exception as e:
            logger.error(f"Error reverting table {table}: {e!s}")
            raise ToolError(f"Error executing revert_table '{table}' into branch '{into_branch}': {e!s}") from e
