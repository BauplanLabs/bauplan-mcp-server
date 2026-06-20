"""
Create a table from an S3 location.
"""

import asyncio
import logging
from typing import Annotated

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import Field

from ._guards import require_writable_branch
from ._schema import mutating_tool_annotations
from .create_client import get_bauplan_client
from .get_table import TableOut, table_to_out

logger = logging.getLogger(__name__)


def register_create_table_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="create_table", annotations=mutating_tool_annotations("Create table", destructive=True))
    async def create_table(
        table: Annotated[
            str,
            Field(
                description="Name of the table to create.",
            ),
        ],
        search_uri: Annotated[
            str,
            Field(
                description="S3 URI pattern used to discover source files and infer the table schema.",
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
        partitioned_by: Annotated[
            str | None,
            Field(
                description="Optional table partitioning expression.",
            ),
        ] = None,
        replace: Annotated[
            bool | None,
            Field(
                description="Whether to replace an existing table with the same name.",
            ),
        ] = None,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> TableOut:
        """
        Create an Iceberg table from files matched by an S3 URI.
        Use this when the user wants Bauplan to infer a schema and create the table before importing data.
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
