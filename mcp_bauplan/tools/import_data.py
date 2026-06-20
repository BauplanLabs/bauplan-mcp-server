"""
Imports data into an existing table.
"""

import asyncio
import logging
from typing import Annotated

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel, Field

from ._guards import require_writable_branch
from ._schema import mutating_tool_annotations
from .create_client import get_bauplan_client

logger = logging.getLogger(__name__)


class DataImported(BaseModel):
    table_name: Annotated[
        str,
        Field(
            description="Destination table for the import.",
        ),
    ]
    job_id: Annotated[
        str | None,
        Field(
            description="Bauplan job ID assigned to the import job.",
        ),
    ] = None
    job_status: Annotated[
        str | None,
        Field(
            description="Final status string for the import job.",
        ),
    ] = None
    error: Annotated[
        str | None,
        Field(
            description="Import error message when the job failed.",
        ),
    ] = None
    success: Annotated[
        bool,
        Field(
            description="Whether the import job completed without an error.",
        ),
    ]
    message: Annotated[
        str,
        Field(
            description="Human-readable summary of the import result.",
        ),
    ]


def register_import_data_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="import_data", annotations=mutating_tool_annotations("Import data"))
    async def import_data(
        table: Annotated[
            str,
            Field(
                description="Destination table to import data into.",
            ),
        ],
        search_uri: Annotated[
            str,
            Field(
                description="S3 URI pattern used to locate source files.",
            ),
        ],
        branch: Annotated[
            str,
            Field(
                description="Branch where data will be imported.",
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
    ) -> DataImported:
        """
        Import files matched by an S3 URI into an existing table on a writable branch.
        Use this when the table already exists and the user wants to load additional source data.
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
            raise ToolError(f"Error executing import_data '{table}' in branch '{branch}': {e!s}") from e
