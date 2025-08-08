from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError

from pydantic import BaseModel
from typing import List, Dict, Any, Optional

from .create_client import create_bauplan_client


class TableSchema(BaseModel):
    name: str
    fields: List[Dict[str, Any]]


class TableOut(BaseModel):
    table: TableSchema


def register_get_table_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="get_table",
        description="Retrieve the schema of a specified data table in the user's Bauplan data catalog using a table name, returning a schema object.",
    )
    async def get_table(
        api_key: str,
        ref: str,
        table_name: str,
        namespace: Optional[str] = None,
        ctx: Context = None,
    ) -> TableOut:
        """
        Return the schema of a specific data table in the user's Bauplan data lake.

        Args:
            api_key: The Bauplan API key for authentication.
            ref: a reference to a commit that is a state of the user data lake: can be either a hash that starts with "@" and
            has 64 additional characters or a branch name, that is a mnemonic reference to the last commit that follows the "username.name" format.

            table_name: Name of the specific table to get schema for.

            namespace: Optional namespace to use (defaults to "bauplan").

        Returns:
            TableOut: Schema object with table fields for the specified table
        """

        try:
            # Create a fresh Bauplan client
            bauplan_client = create_bauplan_client(api_key)
            if namespace is None:
                namespace = "bauplan"  # get_table() needs a not null namespace in the table name

            # Get the specific table schema
            # If table_name already contains namespace (has a dot), use it as-is
            if "." in table_name:
                full_table_name = table_name
            else:
                full_table_name = f"{namespace}.{table_name}"
            table_info = bauplan_client.get_table(
                table=full_table_name, ref=ref, include_raw=True
            )

            table_schema = TableSchema(
                name=table_name, fields=table_info.raw["schemas"][0]["fields"]
            )

            return TableOut(table=table_schema)

        except Exception as err:
            raise ToolError(f"Error executing get_table: {err}")
