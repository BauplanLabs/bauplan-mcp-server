"""
Cancel a job by ID.
"""

import asyncio
import logging
from typing import Annotated

import bauplan
from bauplan import exceptions as bauplan_exceptions
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel, Field

from ._schema import (
    JobKindOut,
    JobStatusOut,
    job_kind_out,
    job_status_out,
    mutating_tool_annotations,
    remote_write_tags,
)
from .create_client import get_bauplan_client

logger = logging.getLogger(__name__)


class JobInfo(BaseModel):
    id: Annotated[
        str,
        Field(
            description="Unique Bauplan job ID.",
        ),
    ]
    kind: Annotated[
        JobKindOut,
        Field(
            description="Bauplan job kind.",
        ),
    ]
    user: Annotated[
        str,
        Field(
            description="User who submitted the job.",
        ),
    ]
    human_readable_status: Annotated[
        str,
        Field(
            description="User-facing job status string after cancellation.",
        ),
    ]
    created_at: Annotated[
        str | None,
        Field(
            description="ISO creation timestamp, or null when unavailable.",
        ),
    ]
    finished_at: Annotated[
        str | None,
        Field(
            description="ISO finish timestamp, or null when the job has not finished.",
        ),
    ]
    status: Annotated[
        JobStatusOut,
        Field(
            description="Bauplan job state after cancellation.",
        ),
    ]
    error_message: Annotated[
        str | None,
        Field(
            description="Error message for failed jobs, when available.",
        ),
    ] = None


def register_cancel_job_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="cancel_job",
        annotations=mutating_tool_annotations("Cancel job", destructive=True),
        tags=remote_write_tags(destructive=True),
    )
    async def cancel_job(
        job_id: Annotated[
            str,
            Field(
                description="Bauplan job ID to cancel.",
            ),
        ],
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> JobInfo:
        """
        Cancel a Bauplan job by ID.
        Use this only when the user explicitly wants to stop a running execution or long operation.
        """

        try:
            if ctx:
                await ctx.info(f"Cancelling job with ID: {job_id}")

            try:
                await asyncio.to_thread(
                    lambda: bauplan_client.cancel_job(job_id),
                )
            except bauplan_exceptions.BauplanError as e:
                raise ToolError(f"Failed to cancel job {job_id}: {e}") from e
            except Exception as e:
                raise ToolError(f"Unexpected error cancelling job {job_id}: {e}") from e

            logger.info("Successfully cancelled job with ID: %s", job_id)

            try:
                job = await asyncio.to_thread(
                    lambda: bauplan_client.get_job(job_id),
                )
            except bauplan_exceptions.BauplanError as e:
                raise ToolError(f"Cancelled job {job_id}, but failed to retrieve updated status: {e}") from e
            except Exception as e:
                raise ToolError(f"Cancelled job {job_id}, but failed to retrieve updated status: {e}") from e

            return JobInfo(
                id=job.id,
                kind=job_kind_out(job.kind),
                user=job.user,
                human_readable_status=job.human_readable_status,
                created_at=job.created_at.isoformat() if job.created_at else None,
                finished_at=job.finished_at.isoformat() if job.finished_at else None,
                status=job_status_out(job.status),
                error_message=job.error_message or None,
            )
        except ToolError:
            raise
        except Exception as e:
            raise ToolError(f"Error executing cancel_job '{job_id}': {e}") from e
