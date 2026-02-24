from typing import Any

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError
from pydantic import BaseModel

from .create_client import with_bauplan_client


class TableSchema(BaseModel):
    name: str
    fields: list[dict[str, Any]]


class TableOut(BaseModel):
    table: TableSchema


def register_get_table_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="get_table", exclude_args=["bauplan_client"])
    @with_bauplan_client
    async def get_table(
        ref: str,
        table_name: str,
        namespace: str | None = None,
        ctx: Context = None,
        bauplan_client: bauplan.Client = None,
    ) -> TableOut:
        """
        Retrieve the schema of a specified data table in the user's Bauplan data catalog using a table name, returning a schema object.

        Args:
            ref: a reference to a commit that is a state of the user data lake: can be either a hash that starts with "@" and
            has 64 additional characters or a branch name, that is a mnemonic reference to the last commit that follows the "username.name" format.
            table_name: Name of the specific table to get schema for.
            namespace: Optional namespace to use (defaults to "bauplan").

        Returns:
            TableOut: Schema object with table fields for the specified table
        """

        try:
            if namespace is None:
                namespace = "bauplan"  # get_table() needs a not null namespace in the table name

            # Get the specific table schema
            # If table_name already contains namespace (has a dot), use it as-is
            if "." in table_name:
                full_table_name = table_name
            else:
                full_table_name = f"{namespace}.{table_name}"
            table_info = bauplan_client.get_table(table=full_table_name, ref=ref, include_raw=True)

            table_schema = TableSchema(name=table_name, fields=table_info.raw["schemas"][0]["fields"])

            return TableOut(table=table_schema)

        except Exception as e:
            raise ToolError(f"Error executing get_table: {e}") from e
