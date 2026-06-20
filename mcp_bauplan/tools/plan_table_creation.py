"""
Create a table import plan from an S3 location.
"""

import asyncio
import logging
from typing import Annotated

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel, Field

from ._guards import require_writable_branch
from ._schema import mutating_tool_annotations
from .create_client import get_bauplan_client

logger = logging.getLogger(__name__)


class TablePlanCreated(BaseModel):
    job_id: Annotated[
        str | None,
        Field(
            description="Bauplan job ID assigned to the planning job.",
        ),
    ] = None
    job_status: Annotated[
        str | None,
        Field(
            description="Final status string for the planning job.",
        ),
    ] = None
    table_name: Annotated[
        str,
        Field(
            description="Table name the plan was created for.",
        ),
    ]
    search_uri: Annotated[
        str,
        Field(
            description="S3 URI pattern used to discover source files.",
        ),
    ]
    success: Annotated[
        bool,
        Field(
            description="Whether the table plan was created without requiring manual attention. "
            "False can also mean the plan was created with schema conflicts to resolve.",
        ),
    ]
    message: Annotated[
        str,
        Field(
            description="Human-readable summary of the planning result.",
        ),
    ]
    namespace: Annotated[
        str | None,
        Field(
            description="Namespace argument passed to the tool, or null if omitted.",
        ),
    ] = None
    branch: Annotated[
        str,
        Field(
            description="Writable non-main branch argument passed to the tool.",
        ),
    ]
    error: Annotated[
        str | None,
        Field(
            description="Planning error message when the job failed or needs attention.",
        ),
    ] = None
    plan: Annotated[
        str | None,
        Field(
            description="Generated YAML table creation plan. Edit this before applying when conflicts require manual resolution.",
        ),
    ] = None
    can_auto_apply: Annotated[
        bool,
        Field(
            description="Whether the plan has no schema conflicts. If false, resolve conflicts before applying it.",
        ),
    ]
    files_to_be_imported: Annotated[
        list[str],
        Field(
            description="Source files matched by the plan.",
        ),
    ]


def register_plan_table_creation_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="plan_table_creation", annotations=mutating_tool_annotations("Plan table creation"))
    async def plan_table_creation(
        table: Annotated[
            str,
            Field(
                description="Name of the table to plan creation for.",
            ),
        ],
        search_uri: Annotated[
            str,
            Field(
                description="S3 URI pattern used to discover source files.",
            ),
        ],
        branch: Annotated[
            str,
            Field(
                description="Writable non-main branch where the table will be created.",
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
        partitioned_by: Annotated[
            str | None,
            Field(
                description="Optional table partitioning expression.",
            ),
        ] = None,
        replace: Annotated[
            bool | None,
            Field(
                description="Whether to replace an existing table with the same name.",
            ),
        ] = None,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> TablePlanCreated:
        """
        Generate a YAML table creation plan from files matched by an S3 URI.
        Use this when the user needs to inspect or edit inferred schema, partitioning, or conflicts before apply_table_creation_plan.
        """

        try:
            branch = require_writable_branch(branch, "plan_table_creation")

            if ctx:
                await ctx.info(f"Creating table plan for '{table}' from search URI '{search_uri}'")

            result = await asyncio.to_thread(
                lambda: bauplan_client.plan_table_creation(
                    table=table,
                    search_uri=search_uri,
                    namespace=namespace,
                    branch=branch,
                    partitioned_by=partitioned_by,
                    replace=replace,
                )
            )

            # Extract job_id from TableCreatePlanState object
            job_id = result.job_id

            # Log successful plan creation with job_id
            logger.info(f"Successfully created table plan for '{table}' with job_id: {job_id}")
            success = result.error is None

            return TablePlanCreated(
                job_id=job_id,
                job_status=result.job_status,
                table_name=table,
                search_uri=search_uri,
                success=success,
                message=(
                    f"Table plan created successfully for '{table}' with job_id: {job_id}"
                    if success
                    else f"Table plan for '{table}' needs attention with job_id: {job_id}: {result.error}"
                ),
                namespace=namespace,
                branch=branch,
                error=result.error,
                plan=result.plan,
                can_auto_apply=result.can_auto_apply,
                files_to_be_imported=result.files_to_be_imported,
            )

        except Exception as e:
            raise ToolError(f"Error executing plan_table_creation '{table}': {e!s}") from e
