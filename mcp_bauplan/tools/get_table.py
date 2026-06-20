import asyncio
from typing import Annotated, Any

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel, Field

from ._schema import field_to_dict, read_only_tool_annotations, remote_read_tags
from .create_client import get_bauplan_client


class PartitionInfo(BaseModel):
    name: Annotated[
        str,
        Field(
            description="Partition field name.",
        ),
    ]
    transform: Annotated[
        str,
        Field(
            description="Partition transform applied to the field.",
        ),
    ]


class TableInfo(BaseModel):
    id: Annotated[
        str,
        Field(
            description="Table ID.",
        ),
    ]
    name: Annotated[
        str,
        Field(
            description="Table name.",
        ),
    ]
    namespace: Annotated[
        str,
        Field(
            description="Namespace containing the table.",
        ),
    ]
    kind: Annotated[
        str,
        Field(
            description="Table kind.",
        ),
    ]
    is_external: Annotated[
        bool,
        Field(
            description="Whether the table is external.",
        ),
    ]
    current_schema_id: Annotated[
        int | None,
        Field(
            description="Current Iceberg schema ID, or null when unavailable.",
        ),
    ] = None
    current_snapshot_id: Annotated[
        int | None,
        Field(
            description="Current Iceberg snapshot ID, or null when unavailable.",
        ),
    ] = None
    last_updated_at: Annotated[
        str,
        Field(
            description="ISO timestamp for the last table update.",
        ),
    ]
    metadata_location: Annotated[
        str,
        Field(
            description="Iceberg metadata file URI.",
        ),
    ]
    partitions: Annotated[
        list[PartitionInfo],
        Field(
            description="Partition fields for the table.",
        ),
    ]
    properties: Annotated[
        dict[str, str],
        Field(
            description="Table properties.",
        ),
    ]
    records: Annotated[
        int | None,
        Field(
            description="Number of records in the table, or null when unavailable.",
        ),
    ] = None
    size: Annotated[
        int | None,
        Field(
            description="Table size, or null when unavailable.",
        ),
    ] = None
    snapshots: Annotated[
        int | None,
        Field(
            description="Number of snapshots, or null when unavailable.",
        ),
    ] = None
    fields: Annotated[
        list[dict[str, Any]],
        Field(
            description="Schema fields for the table, each with id, name, required flag, and type.",
        ),
    ]


class TableOut(BaseModel):
    table: Annotated[
        TableInfo,
        Field(
            description="Table metadata and schema fields.",
        ),
    ]


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
    @mcp.tool(name="get_table", annotations=read_only_tool_annotations("Get table"), tags=remote_read_tags())
    async def get_table(
        ref: Annotated[
            str,
            Field(
                description="Branch, tag, or commit ref to inspect.",
            ),
        ],
        table: Annotated[
            str,
            Field(
                description="Name of the table to retrieve.",
            ),
        ],
        namespace: Annotated[
            str | None,
            Field(
                description=(
                    "Namespace for a bare table name. Leave null when the table name is fully "
                    "qualified or should resolve through the default namespace."
                ),
            ),
        ] = None,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> TableOut:
        """
        Get metadata and schema fields for one table on a branch, tag, or commit ref.
        Use this when the table name is known and the model needs table details, partitions, column names, types, required flags, and field IDs.
        """

        try:
            table_info = await asyncio.to_thread(
                lambda: bauplan_client.get_table(
                    table=table,
                    ref=ref,
                    namespace=namespace or None,
                )
            )
            return table_to_out(table_info)

        except Exception as e:
            table_ref = f"{namespace}.{table}" if namespace else table
            raise ToolError(f"Error executing get_table '{table_ref}' in ref '{ref}': {e}") from e
