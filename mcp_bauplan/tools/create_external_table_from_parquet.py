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


def register_create_external_table_from_parquet_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="create_external_table_from_parquet",
        annotations=mutating_tool_annotations("Create external table from Parquet", destructive=True),
        tags=remote_write_tags(destructive=True),
    )
    async def create_external_table_from_parquet(
        table: Annotated[
            str,
            Field(
                description="Name of the external table to create.",
            ),
        ],
        search_patterns: Annotated[
            list[str],
            Field(
                description="S3 or GCS URI patterns of the Parquet files, e.g. s3://bucket/path/*.parquet.",
                min_length=1,
            ),
        ],
        branch: Annotated[
            str,
            Field(
                description="Writable branch where the table will be created.",
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
        overwrite: Annotated[
            bool,
            Field(
                description=(
                    "Whether to drop and recreate the table if it exists. If false and the table "
                    "exists, the files are added to it. Defaults to false."
                ),
            ),
        ] = False,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> JobOut:
        """
        Create an external Iceberg table that reads existing Parquet files where they are, without copying them.
        To create a table with its own copy of the data, use create_table and import_data.
        """

        try:
            branch = require_writable_branch(branch, "create_external_table_from_parquet")

            if ctx:
                await ctx.info(
                    f"Creating external table '{table}' from {len(search_patterns)} search pattern(s)"
                )

            result = await asyncio.to_thread(
                lambda: bauplan_client.create_external_table_from_parquet(
                    table=table,
                    search_patterns=search_patterns,
                    branch=branch,
                    namespace=namespace or None,
                    overwrite=overwrite,
                    client_timeout=180,
                    detach=True,
                )
            )

            return await submitted_job_out(result.job_id, bauplan_client)

        except Exception as e:
            raise ToolError(
                f"Error executing create_external_table_from_parquet '{table}' in branch '{branch}': {e!s}"
            ) from e
