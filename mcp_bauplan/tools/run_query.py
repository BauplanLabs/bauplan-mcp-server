import asyncio
import datetime
from collections import Counter
from typing import Annotated, Any

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel, Field

from ._schema import read_only_tool_annotations
from .create_client import get_bauplan_client


class QueryMetadata(BaseModel):
    row_count: Annotated[
        int,
        Field(
            description="Number of rows returned by the query.",
        ),
    ]
    column_names: Annotated[
        list[str],
        Field(
            description="Column names returned by the query result.",
        ),
    ]
    column_types: Annotated[
        list[str],
        Field(
            description="Arrow data types for the returned columns.",
        ),
    ]
    query_time: Annotated[
        str,
        Field(
            description="ISO timestamp recorded by the MCP server after the query completed.",
        ),
    ]
    query: Annotated[
        str,
        Field(
            description="SQL query that was executed.",
        ),
    ]


class QueryOut(BaseModel):
    status: Annotated[
        str,
        Field(
            description="Query result status. The tool returns success when the query completes.",
        ),
    ]
    data: Annotated[
        list[dict[str, Any]],
        Field(
            description="Query rows converted from the Arrow result to JSON objects.",
        ),
    ]
    metadata: Annotated[
        QueryMetadata | None,
        Field(
            description="Column and row metadata for successful query results.",
        ),
    ] = None
    warnings: Annotated[
        list[str],
        Field(
            description="Warnings about lossy JSON conversion or other non-fatal result issues.",
        ),
    ] = Field(default_factory=list)
    error: Annotated[
        str | None,
        Field(
            description="Query error message when available, otherwise null.",
        ),
    ] = None


def _duplicate_column_names(column_names: list[str]) -> list[str]:
    counts = Counter(column_names)
    return list(dict.fromkeys(name for name in column_names if counts[name] > 1))


def register_run_query_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="run_query", annotations=read_only_tool_annotations("Run query"))
    async def run_query(
        query: Annotated[
            str,
            Field(
                description="SQL SELECT query to execute.",
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
        max_rows: Annotated[
            int,
            Field(
                description="Maximum number of rows to return. Defaults to 25.",
                ge=1,
                le=250,
            ),
        ] = 25,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> QueryOut:
        """
        Execute a read-only SQL query and return a bounded JSON result set.
        Use this for inspection, validation, and small previews rather than exporting or materializing data.
        """

        try:
            query_time = datetime.datetime.now().isoformat()
            result = await asyncio.to_thread(
                lambda: bauplan_client.query(
                    query=query,
                    ref=ref or None,
                    namespace=namespace or None,
                    max_rows=max_rows,
                )
            )
            column_names = list(result.column_names)
            duplicate_columns = _duplicate_column_names(column_names)
            warnings = []
            if duplicate_columns:
                duplicate_label = ", ".join(duplicate_columns[:5])
                if len(duplicate_columns) > 5:
                    duplicate_label = f"{duplicate_label}, and {len(duplicate_columns) - 5} more"
                warnings.append(
                    f"Duplicate result columns: {duplicate_label}. "
                    "JSON keeps one value per name; use SQL aliases."
                )

            return QueryOut(
                status="success",
                data=result.to_pylist(),
                metadata=QueryMetadata(
                    row_count=result.num_rows,
                    column_names=column_names,
                    column_types=[str(field.type) for field in result.schema],
                    query_time=query_time,
                    query=query,
                ),
                warnings=warnings,
                error=None,
            )

        except Exception as e:
            raise ToolError(f"Error executing run_query: {e!s}") from e
