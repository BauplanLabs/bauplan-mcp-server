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
    current_schema_id: int | None = None
    current_snapshot_id: int | None = None
    last_updated_at: str
    metadata_location: str
    partitions: list[PartitionInfo]
    properties: dict[str, str]
    records: int | None = None
    size: int | None = None
    snapshots: int | None = None
    fields: list[dict[str, Any]]


class TableOut(BaseModel):
    table: TableInfo


def table_to_out(table_info: Any) -> TableOut:
    return TableOut(
        table=TableInfo(
            id=str(table_info.id),
            name=table_info.name,
            namespace=table_info.namespace,
            kind=str(table_info.kind),
            is_external=table_info.is_external(),
            current_schema_id=table_info.current_schema_id,
            current_snapshot_id=table_info.current_snapshot_id,
            last_updated_at=table_info.last_updated_at.isoformat(),
            metadata_location=table_info.metadata_location,
            partitions=[
                PartitionInfo(
                    name=partition.name,
                    transform=partition.transform,
                )
                for partition in table_info.partitions
            ],
            properties=dict(table_info.properties),
            records=table_info.records,
            size=table_info.size,
            snapshots=table_info.snapshots,
            fields=[field_to_dict(field) for field in table_info.fields],
        )
    )


def register_get_table_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="get_table")
    async def get_table(
        ref: str,
        table_name: str,
        namespace: str | None = None,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> TableOut:
        """
        Retrieve the schema of a specified data table in the user's Bauplan data catalog using a table name, returning a schema object.

        Args:
            ref: a reference to a commit that is a state of the user data lake: can be either a hash that starts with "@" and
            has 64 additional characters or a branch name, that is a mnemonic reference to the last commit that follows the "username.name" format.
            table_name: Name of the specific table to get schema for.
            namespace: Optional namespace to use.

        Returns:
            TableOut: Table metadata and schema fields for the specified table
        """

        try:
            table_info = await asyncio.to_thread(
                lambda: bauplan_client.get_table(
                    table=table_name,
                    ref=ref,
                    namespace=namespace or None,
                )
            )
            return table_to_out(table_info)

        except Exception as e:
            table_ref = f"{namespace}.{table_name}" if namespace else table_name
            raise ToolError(f"Error executing get_table '{table_ref}' in ref '{ref}': {e}") from e
