"""
Create a table from an S3 location.
"""

import asyncio
import logging

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError

from ._guards import require_writable_branch
from .create_client import get_bauplan_client
from .get_table import TableOut, table_to_out

logger = logging.getLogger(__name__)


def register_create_table_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="create_table")
    async def create_table(
        table: str,
        search_uri: str,
        branch: str,
        namespace: str | None = None,
        partitioned_by: str | None = None,
        replace: bool | None = None,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> TableOut:
        """
        Create an empty table from an S3 URI identifying parquet, csv or JSONL files in S3.
        The table schema is automatically inferred from the files at the given search uri.

        Args:
            table: Name of the table to create.
            search_uri: S3 URI to search for parquet files.
            branch: branch name.
            namespace: Optional namespace. If omitted, resolution uses the default namespace.
            partitioned_by: Optional partitioning column.
            replace: Optional flag to replace existing table.

        Returns:
            TableOut: Created table metadata and schema fields.

        NOTE: This tool creates a ICEBERG table with the schema of the file(s) in the URI but it does not populate the table.
        """

        try:
            branch = require_writable_branch(branch, "create_table")

            if ctx:
                await ctx.info(f"Creating table '{table}' from search URI '{search_uri}'")

            result = await asyncio.to_thread(
                lambda: bauplan_client.create_table(
                    table=table,
                    search_uri=search_uri,
                    namespace=namespace or None,
                    branch=branch,
                    partitioned_by=partitioned_by or None,
                    replace=replace,
                )
            )

            # Log successful creation with Table object attributes
            logger.info(f"Successfully created table: {result.name} in namespace: {result.namespace}")

            return table_to_out(result)

        except Exception as e:
            logger.error(f"Error creating table {table}: {e!s}")
            raise ToolError(f"Error executing create_table '{table}' in branch '{branch}': {e!s}") from e
