"""
Imports data into an existing table.
"""

import asyncio
from typing import Annotated

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import Field

from ._guards import require_writable_branch
from ._schema import mutating_tool_annotations, remote_write_tags
from .create_client import get_bauplan_client
from .get_job import JobOut, submitted_job_out


def register_import_data_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="import_data", annotations=mutating_tool_annotations("Import data"), tags=remote_write_tags()
    )
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
        import_duplicate_files: Annotated[
            bool,
            Field(
                description=(
                    "Whether to also import files that were already imported into this table. "
                    "Their rows will be duplicated. Defaults to false."
                ),
            ),
        ] = False,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> JobOut:
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
                    import_duplicate_files=import_duplicate_files,
                    client_timeout=180,
                    detach=True,
                )
            )

            return await submitted_job_out(result.job_id, bauplan_client)

        except Exception as e:
            raise ToolError(f"Error executing import_data '{table}' in branch '{branch}': {e!s}") from e
