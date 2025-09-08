"""
Create a table from an S3 location.
"""

from fastmcp import FastMCP
from pydantic import BaseModel
from typing import Optional
from fastmcp.exceptions import ToolError

from .create_client import with_bauplan_client
import bauplan
import logging
from fastmcp import Context

logger = logging.getLogger(__name__)


class TableCreated(BaseModel):
    table_name: str
    namespace: str
    success: bool
    message: str


def register_create_table_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="create_table", exclude_args=["bauplan_client"])
    @with_bauplan_client
    async def create_table(
        table: str,
        search_uri: str,
        branch: str,
        namespace: Optional[str] = None,
        partitioned_by: Optional[str] = None,
        replace: Optional[bool] = None,
        ctx: Context = None,
        bauplan_client: bauplan.Client = None,
    ) -> TableCreated:
        """
        Create an empty table from an S3 URI identifying parquet, csv or JSONL files in S3.
        The table schema is automatically inferred from the files at the given search uri.

        Args:
            table: Name of the table to create.
            search_uri: S3 URI to search for parquet files.
            branch: branch name.
            namespace: Optional namespace (defaults to "bauplan").
            partitioned_by: Optional partitioning column.
            replace: Optional flag to replace existing table.

        Returns:
            TableCreated: Object indicating success/failure with table details.

        NOTE: This tool creates a ICEBERG table with the schema of the file(s) in the URI but it does not populate the table.
        """
        try:
            if ctx:
                await ctx.info(
                    f"Creating table '{table}' from search URI '{search_uri}'"
                )

            assert branch and branch != "main", (
                "Branch name must be provided, and it cannot be 'main'"
            )

            # Call create_table function
            result = bauplan_client.create_table(
                table=table,
                search_uri=search_uri,
                namespace=namespace,
                branch=branch,
                partitioned_by=partitioned_by,
                replace=replace,
            )

            # Log successful creation with Table object attributes
            logger.info(
                f"Successfully created table: {result.name} in namespace: {result.namespace}"
            )

            return TableCreated(
                table_name=result.name,
                namespace=result.namespace,
                success=True,
                message=f"Table {result.name} created successfully in namespace {result.namespace}",
            )

        except Exception as e:
            logger.error(f"Error creating table {table}: {str(e)}")
            raise ToolError(f"Failed to create table {table}: {str(e)}")
