from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError

from pydantic import BaseModel
from typing import List, Dict, Any, Optional

from .create_client import create_bauplan_client


class TableSchema(BaseModel):
    name: str
    fields: List[Dict[str, Any]]


class TableWrapper(BaseModel):
    table: TableSchema


class SchemasOut(BaseModel):
    tables: List[TableWrapper]


def register_get_schema_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="get_schema",
        description="Retrieve schemas of all data tables in a specified branch or reference of the user's Bauplan data catalog as a list, using a branch name.",
    )
    async def get_schema(
        ref: str,
        namespace: Optional[str] = None,
        api_key: Optional[str] = None,
        ctx: Context = None,
    ) -> SchemasOut:
        """
        Return the schema of all data tables in the user's Bauplan data lake.

        Args:
            ref: a reference to a commit that is a state of the user data lake: can be either a hash that starts with "@" and
            has 64 additional characters or a branch name, that is a mnemonic reference to the last commit that follows the "username.name" format.
            namespace: Optional namespace table filter to use.
            api_key: The Bauplan API key for authentication.

        Returns:
            SchemasOut: Schema object with table fields
        """

        try:
            # Create a fresh Bauplan client
            bauplan_client = create_bauplan_client(api_key)
            if namespace is None:
                namespace = "bauplan"  # get_table() needs a not null namespace in the table name
            # Get the tables list
            ret = bauplan_client.get_tables(ref=ref, filter_by_namespace=namespace)
            tables = {"tables": [{"name": table.name} for table in ret]}
            # Iterate to get schemas and build final structure
            schema_list = []
            for t in tables["tables"]:
                table_info = bauplan_client.get_table(
                    table=f"{namespace}.{t['name']}", ref=ref, include_raw=True
                )
                table_schema = TableSchema(
                    name=t["name"], fields=table_info.raw["schemas"][0]["fields"]
                )
                schema_list.append(TableWrapper(table=table_schema))

            return SchemasOut(tables=schema_list)

        except Exception as err:
            raise ToolError(f"Error executing get_table: {err}")
