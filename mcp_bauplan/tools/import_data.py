"""
Imports data into an existing table.
"""

import asyncio
import logging

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel

from ._guards import require_writable_branch
from .create_client import get_bauplan_client

logger = logging.getLogger(__name__)


class DataImported(BaseModel):
    table_name: str
    job_id: str | None = None
    job_status: str | None = None
    error: str | None = None
    success: bool
    message: str


def register_import_data_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="import_data")
    async def import_data(
        table: str,
        search_uri: str,
        branch: str,
        namespace: str | None = None,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> DataImported:
        """
        Import data into a specified existing table using a table name and data source.

        Args:
            table: Name of the table to import data into, it needs to exist beforehand.
            search_uri: URI to search for data files to import.
            branch:  branch name.
            namespace: Optional namespace. If omitted, resolution uses the default namespace.

        Returns:
            DataImported: Object indicating success/failure with job details
        """

        try:
            branch = require_writable_branch(branch, "import_data")

            if ctx:
                await ctx.info(f"Importing data into table '{table}' from search URI '{search_uri}'")

            result = await asyncio.to_thread(
                lambda: bauplan_client.import_data(
                    table=table,
                    search_uri=search_uri,
                    namespace=namespace,
                    branch=branch,
                    continue_on_error=False,
                    client_timeout=180,
                )
            )

            # Log successful import with job_id from TableDataImportState object
            job_id = result.job_id
            logger.info(f"Successfully started data import for table '{table}' with job_id: {job_id}")
            success = result.error is None

            return DataImported(
                table_name=table,
                job_id=job_id,
                job_status=result.job_status,
                error=result.error,
                success=success,
                message=(
                    f"Data import completed for table '{table}' with job_id: {job_id}"
                    if success
                    else f"Data import failed for table '{table}' with job_id: {job_id}: {result.error}"
                ),
            )

        except Exception as e:
            logger.error(f"Error importing data into table {table}: {e!s}")
            raise ToolError(f"Error executing import_data '{table}' in branch '{branch}': {e!s}") from e
