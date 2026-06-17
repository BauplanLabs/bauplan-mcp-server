import asyncio
from typing import Annotated, Any

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel, Field

from ._schema import EXACT_OR_REGEX_FILTER_DESCRIPTION, field_to_dict
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
    partitions: Annotated[
        list[PartitionInfo] | None,
        Field(
            description="Partition fields when include_schema is true, otherwise null.",
        ),
    ] = None
    fields: Annotated[
        list[dict[str, Any]] | None,
        Field(
            description="Schema fields when include_schema is true, otherwise null.",
        ),
    ] = None


class TablesOut(BaseModel):
    tables: Annotated[
        list[TableInfo],
        Field(
            description=(
                "Tables available on the requested ref. Use include_schema to include "
                "fields and partitions for each table."
            ),
        ),
    ]


def register_get_tables_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="get_tables")
    async def get_tables(
        ref: Annotated[
            str,
            Field(
                description="Branch, tag, or commit ref to inspect.",
            ),
        ],
        namespace: Annotated[
            str | None,
            Field(
                description=f"Optional namespace filter for the returned tables. {EXACT_OR_REGEX_FILTER_DESCRIPTION}",
            ),
        ] = None,
        table: Annotated[
            str | None,
            Field(
                description=f"Optional table name filter. {EXACT_OR_REGEX_FILTER_DESCRIPTION}",
            ),
        ] = None,
        include_schema: Annotated[
            bool,
            Field(
                description="Include partitions and fields for each returned table.",
            ),
        ] = False,
        limit: Annotated[
            int,
            Field(
                description="Maximum number of tables to return. Defaults to 25.",
                ge=1,
                le=250,
            ),
        ] = 25,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> TablesOut:
        """
        List Iceberg tables visible on a branch, tag, or commit ref.
        Use table and namespace filters to narrow discovery when many tables exist.
        Use include_schema=True when you need fields and partitions for all returned tables.
        """

        try:
            ret = await asyncio.to_thread(
                lambda: bauplan_client.get_tables(
                    ref=ref,
                    filter_by_name=table or None,
                    filter_by_namespace=namespace or None,
                    limit=limit,
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
