import asyncio

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel

from .create_client import get_bauplan_client


class TablesOut(BaseModel):
    tables: list[str]
    ref: str
    namespace: str | None = None


def register_list_tables_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="list_tables")
    async def list_tables(
        ref: str,
        namespace: str | None = None,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> TablesOut:
        """
        Retrieve a list of all data tables in a specified branch or reference of the user's Bauplan data catalog using a ref name.

        Args:
            ref: a reference to a commit that is a state of the user data lake: can be either a hash that starts with "@" and
            has 64 additional characters or a branch name, that is a mnemonic reference to the last commit that follows the "username.name" format.
            namespace: Optional namespace to use.

        IMPORTANT: Always use this tool BEFORE generating code with generate_code to ensure you're referencing tables that actually exist.

        Returns:
            TablesOut: object with list of table names
        """

        try:
            ret = await asyncio.to_thread(
                bauplan_client.get_tables,
                ref=ref,
                filter_by_namespace=namespace,
            )
            # Extract table names from TableWithMetadata objects
            table_names = [table.name for table in ret]
            return TablesOut(tables=table_names, ref=ref, namespace=namespace)
        except Exception as e:
            raise ToolError(f"Error executing list_tables: {e!s}") from e
