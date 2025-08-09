"""
Imports data into an existing table.
"""

from fastmcp import FastMCP
from pydantic import BaseModel
from typing import Optional
from fastmcp.exceptions import ToolError

from .create_client import create_bauplan_client
import logging
from fastmcp import Context

logger = logging.getLogger(__name__)


class DataImported(BaseModel):
    table_name: str
    job_id: str
    success: bool
    message: str


def register_import_data_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="import_data",
        description="Import data into a specified existing table in the user's Bauplan data catalog using a table name and data source.",
    )
    async def import_data(
        table: str,
        search_uri: str,
        client_timeout: int = 120,
        namespace: Optional[str] = None,
        branch: Optional[str] = None,
        continue_on_error: Optional[bool] = False,
        api_key: Optional[str] = None,
        ctx: Context = None,
    ) -> DataImported:
        """
        Import data into an existing table in the user's Bauplan data lake.

        Args:
            table: Name of the table to import data into.
            search_uri: URI to search for data files to import.
            client_timeout: Timeout in seconds for the import operation (defaults to 120).
            namespace: Optional namespace (defaults to "bauplan").
            branch: Optional branch name.
            continue_on_error: Optional flag to continue on errors during import (defaults to False).
            api_key: The Bauplan API key for authentication.

        Returns:
            DataImported: Object indicating success/failure with job details
        """
        try:
            # Create a fresh Bauplan client
            bauplan_client = create_bauplan_client(api_key)
            if ctx:
                await ctx.info(
                    f"Importing data into table '{table}' from search URI '{search_uri}'"
                )

            # Call import_data function
            result = bauplan_client.import_data(
                table=table,
                search_uri=search_uri,
                namespace=namespace,
                branch=branch,
                continue_on_error=continue_on_error,
                client_timeout=client_timeout,
            )

            # Log successful import with job_id from TableDataImportState object
            logger.info(
                f"Successfully started data import for table '{table}' with job_id: {result.job_id}"
            )

            return DataImported(
                table_name=table,
                job_id=result.job_id,
                success=True,
                message=f"Data import started successfully for table '{table}' with job_id: {result.job_id}",
            )

        except Exception as e:
            logger.error(f"Error importing data into table {table}: {str(e)}")
            raise ToolError(f"Failed to import data into table {table}: {str(e)}")
