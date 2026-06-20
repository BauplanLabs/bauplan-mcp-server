"""
List jobs in the Bauplan system.
"""

import asyncio
import logging
from datetime import datetime
from typing import Annotated

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel, Field

from ._schema import (
    JobKindFilter,
    JobKindOut,
    JobStatusFilter,
    JobStatusOut,
    job_kind_out,
    job_status_out,
    read_only_tool_annotations,
    remote_read_tags,
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
            description="User-facing job status string.",
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
            description="Bauplan job state.",
        ),
    ]
    error_message: Annotated[
        str | None,
        Field(
            description="Error message for failed jobs, when available.",
        ),
    ] = None


class JobsList(BaseModel):
    jobs: Annotated[
        list[JobInfo],
        Field(
            description="Jobs matching the requested filters.",
        ),
    ]


def register_get_jobs_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="get_jobs", annotations=read_only_tool_annotations("Get jobs"), tags=remote_read_tags())
    async def get_jobs(
        job_ids: Annotated[
            list[str] | None,
            Field(
                description="Optional list of job IDs.",
            ),
        ] = None,
        job_kinds: Annotated[
            list[JobKindFilter] | None,
            Field(
                description="Optional list of job kinds.",
            ),
        ] = None,
        statuses: Annotated[
            list[JobStatusFilter] | None,
            Field(
                description="Optional list of Bauplan job states.",
            ),
        ] = None,
        user_names: Annotated[
            list[str] | None,
            Field(
                description="Optional list of job users.",
            ),
        ] = None,
        start_time: Annotated[
            str | None,
            Field(
                description="Optional UTC creation lower bound, formatted as MM/DD/YY HH:MM:SS.",
            ),
        ] = None,
        end_time: Annotated[
            str | None,
            Field(
                description="Optional UTC creation upper bound, formatted as MM/DD/YY HH:MM:SS.",
            ),
        ] = None,
        limit: Annotated[
            int,
            Field(
                description="Maximum number of jobs to return.",
                ge=1,
                le=250,
            ),
        ] = 25,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> JobsList:
        """
        List Bauplan jobs with optional filters for IDs, users, kinds, statuses, and creation time.
        Use this to find recent executions or identify the job ID to inspect with get_job.
        """

        try:
            if ctx:
                await ctx.info(
                    f"Listing jobs (kinds: {job_kinds}, statuses: {statuses}, start_time: {start_time}, finish_time: {end_time})"
                )

            jobs_result = await asyncio.to_thread(
                lambda: list(
                    bauplan_client.get_jobs(
                        filter_by_ids=job_ids or None,
                        filter_by_users=user_names or None,
                        filter_by_kinds=[str(kind) for kind in job_kinds] if job_kinds else None,
                        filter_by_statuses=[str(status) for status in statuses] if statuses else None,
                        limit=limit,
                        filter_by_created_after=datetime.strptime(start_time, "%m/%d/%y %H:%M:%S")
                        if start_time
                        else None,
                        filter_by_created_before=datetime.strptime(end_time, "%m/%d/%y %H:%M:%S")
                        if end_time
                        else None,
                    )
                )
            )

            # Convert Job objects to JobInfo BaseModel instances
            job_info_list = []
            for job in jobs_result:
                job_info = JobInfo(
                    id=job.id,
                    kind=job_kind_out(job.kind),
                    user=job.user,
                    human_readable_status=job.human_readable_status,
                    created_at=job.created_at.isoformat() if job.created_at else None,
                    finished_at=job.finished_at.isoformat() if job.finished_at else None,
                    status=job_status_out(job.status),
                    error_message=job.error_message or None,
                )
                job_info_list.append(job_info)

            # Log successful retrieval
            logger.info(f"Successfully retrieved {len(job_info_list)} jobs")

            return JobsList(jobs=job_info_list)

        except Exception as e:
            # Handle job-related errors more gracefully
            error_msg = str(e)
            logger.error(f"Error listing jobs: {error_msg}")
            raise ToolError(f"Error executing get_jobs: {error_msg}") from e
