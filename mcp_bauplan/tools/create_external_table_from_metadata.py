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
from .get_table import TableOut, table_to_out


def register_create_external_table_from_metadata_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="create_external_table_from_metadata",
        annotations=mutating_tool_annotations("Create external table from metadata", destructive=True),
        tags=remote_write_tags(destructive=True),
    )
    async def create_external_table_from_metadata(
        table: Annotated[
            str,
            Field(
                description="Name of the external table to create.",
            ),
        ],
        metadata_json_uri: Annotated[
            str,
            Field(
                description="S3 or GCS URI of the Iceberg metadata.json file. It must be outside the Bauplan warehouse.",
            ),
        ],
        namespace: Annotated[
            str,
            Field(
                description="Existing namespace where the table will be registered.",
            ),
        ],
        branch: Annotated[
            str,
            Field(
                description="Writable branch where the table will be registered.",
            ),
        ],
        overwrite: Annotated[
            bool,
            Field(
                description="Whether to replace the table if it already exists. Defaults to false.",
            ),
        ] = False,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> TableOut:
        """
        Register an Iceberg table that lives outside Bauplan, using its metadata.json.
        Use this to query an existing Iceberg table without copying it.
        """

        try:
            branch = require_writable_branch(branch, "create_external_table_from_metadata")

            if ctx:
                await ctx.info(f"Registering external table '{table}' from '{metadata_json_uri}'")

            result = await asyncio.to_thread(
                lambda: bauplan_client.create_external_table_from_metadata(
                    table=table,
                    metadata_json_uri=metadata_json_uri,
                    namespace=namespace,
                    branch=branch,
                    overwrite=overwrite,
                )
            )
            return table_to_out(result)

        except Exception as e:
            raise ToolError(
                f"Error executing create_external_table_from_metadata '{table}' in branch '{branch}': {e!s}"
            ) from e
