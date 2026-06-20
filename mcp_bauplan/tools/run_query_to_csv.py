"""
Execute queries and save results to CSV files.
"""

import asyncio
import logging
import shutil
import tempfile
from contextlib import suppress
from pathlib import Path
from typing import Annotated

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel, Field

from ._schema import local_write_tags, mutating_tool_annotations
from .create_client import get_bauplan_client

logger = logging.getLogger(__name__)


def _publish_without_overwrite(source: Path, destination: Path) -> None:
    created_destination = False
    try:
        with source.open("rb") as source_file, destination.open("xb") as destination_file:
            created_destination = True
            shutil.copyfileobj(source_file, destination_file)
    except Exception:
        if created_destination:
            with suppress(FileNotFoundError):
                destination.unlink()
        raise


class QueryToCSVResult(BaseModel):
    path: Annotated[
        str,
        Field(
            description="CSV file path written by the Bauplan SDK.",
        ),
    ]
    query: Annotated[
        str,
        Field(
            description="SQL query exported to CSV.",
        ),
    ]
    ref: Annotated[
        str | None,
        Field(
            description="Branch, tag, or commit ref used for the query, or null for the default ref.",
        ),
    ]
    namespace: Annotated[
        str | None,
        Field(
            description="Namespace used for the query, or null for the default namespace.",
        ),
    ]
    success: Annotated[
        bool,
        Field(
            description="Whether the CSV export completed successfully.",
        ),
    ]
    message: Annotated[
        str,
        Field(
            description="Human-readable summary of the export result.",
        ),
    ]


def register_run_query_to_csv_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="run_query_to_csv",
        annotations=mutating_tool_annotations("Run query to CSV"),
        tags=local_write_tags(),
    )
    async def run_query_to_csv(
        path: Annotated[
            str,
            Field(
                description="Server-side CSV output path. The file must not already exist.",
            ),
        ],
        query: Annotated[
            str,
            Field(
                description="SQL query to export.",
            ),
        ],
        ref: Annotated[
            str | None,
            Field(
                description="Branch, tag, or commit ref to query, or null for the default ref.",
            ),
        ] = None,
        namespace: Annotated[
            str | None,
            Field(
                description="Namespace to query in, or null for the default namespace.",
            ),
        ] = None,
        client_timeout: Annotated[
            int,
            Field(
                description="Client timeout in seconds.",
            ),
        ] = 120,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> QueryToCSVResult:
        """
        Execute a SQL query and write scalar results to a server-local CSV file.
        The output path must not exist because this tool never overwrites files.
        Use this only when the caller can access the output path; prefer run_query for remote MCP clients.
        """

        try:
            destination = Path(path)
            if destination.exists():
                raise ToolError(f"Output path already exists and will not be overwritten: {path}")
            if not destination.parent.is_dir():
                raise ToolError(f"Output directory does not exist: {destination.parent}")

            if ctx:
                await ctx.info(f"Executing query to CSV file: {path}")

            with tempfile.NamedTemporaryFile(
                prefix="bauplan_query_",
                suffix=".csv",
                delete=True,
                delete_on_close=False,
            ) as temporary_file:
                temporary_path = Path(temporary_file.name)
                temporary_file.close()
                await asyncio.to_thread(
                    lambda: bauplan_client.query_to_csv_file(
                        path=temporary_path,
                        query=query,
                        ref=ref or None,
                        namespace=namespace or None,
                        client_timeout=client_timeout,
                    )
                )
                _publish_without_overwrite(temporary_path, destination)

            # Log successful execution
            logger.info(f"Successfully executed query and saved results to CSV: {path}")

            return QueryToCSVResult(
                path=path,
                query=query,
                ref=ref,
                namespace=namespace,
                success=True,
                message=f"Query executed successfully and results saved to: {path}",
            )

        except Exception as e:
            error_msg = str(e)
            # Handle complex data type errors specifically
            if "Unsupported Type" in error_msg and ("list" in error_msg or "array" in error_msg):
                raise ToolError(
                    f"Cannot export to CSV: Query contains complex data types (arrays/lists) that are not supported in CSV format. "
                    f"Consider: 1) Using run_query tool instead for complex data, 2) Flattening arrays in SQL with functions like unnest(), "
                    f"3) Converting arrays to strings with array_to_string() or similar functions. Original error: {error_msg}"
                ) from e
            else:
                raise ToolError(f"Error executing run_query_to_csv '{path}': {error_msg}") from e
