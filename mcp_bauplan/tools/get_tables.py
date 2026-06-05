import asyncio
from typing import Any

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel

from ._schema import field_to_dict
from .create_client import get_bauplan_client


class PartitionInfo(BaseModel):
    name: str
    transform: str


class TableInfo(BaseModel):
    id: str
    name: str
    namespace: str
    kind: str
    is_external: bool
    records: int | None = None
    size: int | None = None
    partitions: list[PartitionInfo] | None = None
    fields: list[dict[str, Any]] | None = None


class TablesOut(BaseModel):
    tables: list[TableInfo]


def register_get_tables_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="get_tables")
    async def get_tables(
        ref: str,
        namespace: str | None = None,
        include_schema: bool = False,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> TablesOut:
        """
        Retrieve a list of all data tables in a specified branch or reference of the user's Bauplan data catalog using a ref name.

        Args:
            ref: a reference to a commit that is a state of the user data lake: can be either a hash that starts with "@" and
            has 64 additional characters or a branch name, that is a mnemonic reference to the last commit that follows the "username.name" format.
            namespace: Optional namespace to use.
            include_schema: Include partitions and fields for each returned table.

        IMPORTANT: Always use this tool BEFORE generating code with generate_code to ensure you're referencing tables that actually exist.
        Use include_schema=True when you need fields and partitions for all returned tables.

        Returns:
            TablesOut: object with table names and namespaces
        """

        try:
            ret = await asyncio.to_thread(
                lambda: bauplan_client.get_tables(
                    ref=ref,
                    filter_by_namespace=namespace or None,
                )
            )
            tables = [
                TableInfo(
                    id=str(table.id),
                    name=table.name,
                    namespace=table.namespace,
                    kind=str(table.kind),
                    is_external=table.is_external(),
                    records=table.records,
                    size=table.size,
                    partitions=[
                        PartitionInfo(
                            name=partition.name,
                            transform=partition.transform,
                        )
                        for partition in table.partitions
                    ]
                    if include_schema
                    else None,
                    fields=[field_to_dict(field) for field in table.fields] if include_schema else None,
                )
                for table in ret
            ]
            return TablesOut(tables=tables)
        except Exception as e:
            raise ToolError(f"Error executing get_tables: {e!s}") from e
