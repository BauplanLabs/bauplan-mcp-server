"""
Imports data into an existing table.
"""

import asyncio
import logging

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError
from pydantic import BaseModel

from .create_client import with_bauplan_client

logger = logging.getLogger(__name__)


class DataImported(BaseModel):
    table_name: str
    job_id: str
    success: bool
    message: str


def register_import_data_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="import_data", exclude_args=["bauplan_client"])
    @with_bauplan_client
    async def import_data(
        bauplan_client: bauplan.Client,
        table: str,
        search_uri: str,
        branch: str,
        namespace: str | None = None,
        ctx: Context | None = None,
    ) -> DataImported:
        """
        Import data into a specified existing table using a table name and data source.

        Args:
            table: Name of the table to import data into, it needs to exist beforehand.
            search_uri: URI to search for data files to import.
            branch:  branch name.
            namespace: Optional namespace (defaults to "bauplan").

        Returns:
            DataImported: Object indicating success/failure with job details
        """
        try:
            if ctx:
                await ctx.info(f"Importing data into table '{table}' from search URI '{search_uri}'")

            assert branch and branch != "main", "Branch name must be provided, and it cannot be 'main'"

            # Call import_data function
            result = await asyncio.to_thread(
                bauplan_client.import_data,
                table=table,
                search_uri=search_uri,
                namespace=namespace,
                branch=branch,
                continue_on_error=False,
                client_timeout=180,
            )

            # Log successful import with job_id from TableDataImportState object
            logger.info(f"Successfully started data import for table '{table}' with job_id: {result.job_id}")

            return DataImported(
                table_name=table,
                job_id=result.job_id,
                success=True,
                message=f"Data import started successfully for table '{table}' with job_id: {result.job_id}",
            )

        except Exception as e:
            logger.error(f"Error importing data into table {table}: {e!s}")
            raise ToolError(f"Failed to import data into table {table}: {e!s}") from e
