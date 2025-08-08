from pydantic import BaseModel
from typing import List
from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError

from typing import Optional

from .create_client import create_bauplan_client


class TablesOut(BaseModel):
    tables: List[str]
    ref: str
    namespace: Optional[str] = None


def register_list_tables_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="list_tables",
        description="Retrieve a list of all data tables in a specified branch or reference of the user's Bauplan data catalog using a ref name.",
    )
    async def list_tables(
        api_key: str, ref: str, namespace: Optional[str] = None, ctx: Context = None
    ) -> TablesOut:
        """
        List all data tables in the user's Bauplan data lake.

        Args:
            api_key: The Bauplan API key for authentication.
            ref: a reference to a commit that is a state of the user data lake: can be either a hash that starts with "@" and
            has 64 additional characters or a branch name, that is a mnemonic reference to the last commit that follows the "username.name" format.

            namespace: Optional namespace to use.

        IMPORTANT: Always use this tool BEFORE generating code with generate_code to ensure you're referencing tables that actually exist.

        Returns:
            dict: TablesOut object with list of table names
        """

        try:
            # Create a fresh Bauplan client
            bauplan_client = create_bauplan_client(api_key)
            ret = bauplan_client.get_tables(ref=ref, filter_by_namespace=namespace)
            # Extract table names from TableWithMetadata objects
            table_names = [table.name for table in ret]
            return TablesOut(tables=table_names, ref=ref, namespace=namespace)
        except Exception as err:
            raise ToolError(f"Error executing get_table: {err}")
