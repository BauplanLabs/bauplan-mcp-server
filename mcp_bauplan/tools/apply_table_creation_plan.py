"""
Apply a table creation plan to resolve schema conflicts.
"""

import asyncio
import json
import logging
from typing import Annotated, Any

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel, Field

from ._schema import mutating_tool_annotations, remote_write_tags
from .create_client import get_bauplan_client

logger = logging.getLogger(__name__)


class TablePlanApplied(BaseModel):
    job_id: Annotated[
        str | None,
        Field(
            description="Bauplan job ID assigned to the plan apply job.",
        ),
    ] = None
    job_status: Annotated[
        str | None,
        Field(
            description="Final status string for the plan apply job.",
        ),
    ] = None
    error: Annotated[
        str | None,
        Field(
            description="Apply error message when the job failed.",
        ),
    ] = None
    success: Annotated[
        bool,
        Field(
            description="Whether the plan apply job completed without an error.",
        ),
    ]
    message: Annotated[
        str,
        Field(
            description="Human-readable summary of the apply result.",
        ),
    ]


def register_apply_table_creation_plan_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="apply_table_creation_plan",
        annotations=mutating_tool_annotations("Apply table creation plan", destructive=True),
        tags=remote_write_tags(destructive=True),
    )
    async def apply_table_creation_plan(
        plan: Annotated[
            str | dict[str, Any],
            Field(
                description="YAML table creation plan, either as a string or JSON object.",
            ),
        ],
        args: Annotated[
            dict[str, str] | None,
            Field(
                description="Optional backend arguments for the apply job.",
            ),
        ] = None,
        priority: Annotated[
            int | None,
            Field(
                description="Optional job priority.",
            ),
        ] = None,
        client_timeout: Annotated[
            int,
            Field(
                description="Client timeout in seconds.",
            ),
        ] = 120,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> TablePlanApplied:
        """
        Apply a table creation plan produced by plan_table_creation.
        Use this after reviewing or editing a plan, especially when schema conflicts require manual resolution.
        """

        try:
            if ctx:
                await ctx.info("Applying table creation plan")

            plan_payload = json.dumps(plan) if isinstance(plan, dict) else plan
            try:
                result = await asyncio.to_thread(
                    lambda: bauplan_client.apply_table_creation_plan(
                        plan=plan_payload,
                        args=args,
                        priority=priority,
                        client_timeout=client_timeout,
                    )
                )
            except bauplan.exceptions.TableCreatePlanApplyStatusError as e:
                result = e.state

            # Extract job_id from TableCreatePlanApplyState object
            job_id = result.job_id
            success = result.error is None

            # Log successful plan application with job_id
            logger.info(f"Successfully applied table creation plan with job_id: {job_id}")

            return TablePlanApplied(
                job_id=job_id,
                job_status=result.job_status,
                error=result.error,
                success=success,
                message=(
                    f"Table creation plan applied successfully with job_id: {job_id}"
                    if success
                    else f"Table creation plan apply failed with job_id: {job_id}: {result.error}"
                ),
            )

        except Exception as e:
            raise ToolError(f"Error executing apply_table_creation_plan: {e!s}") from e
