import asyncio
import datetime
from collections import Counter
from typing import Any

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel, Field

from .create_client import get_bauplan_client


class QueryMetadata(BaseModel):
    row_count: int
    column_names: list[str]
    column_types: list[str]
    query_time: str
    query: str


class QueryOut(BaseModel):
    status: str
    data: list[dict[str, Any]]
    metadata: QueryMetadata | None = None
    warnings: list[str] = Field(default_factory=list)
    error: str | None = None


def _duplicate_column_names(column_names: list[str]) -> list[str]:
    counts = Counter(column_names)
    return list(dict.fromkeys(name for name in column_names if counts[name] > 1))


def register_run_query_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="run_query")
    async def run_query(
        query: str,
        ref: str | None = None,
        namespace: str | None = None,
        max_rows: int = 10,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> QueryOut:
        """
        Execute a SQL SELECT query on the user's Bauplan data catalog, returning results as a QueryOut object using a query, optional ref, and optional namespace.
        Executes a SQL query against the user's Bauplan data lake.

        Args:
            query: SQL query to execute
            ref: a reference to a commit that is a state of the user data lake: can be either a hash that starts with "@" and
            has 64 additional characters or a branch name, that is a mnemonic reference to the last commit that follows the "username.name" format.
            namespace: Optional namespace to use.
            max_rows: Maximum number of rows to return.

        Returns:
            QueryOut: Response object with query results or error
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
