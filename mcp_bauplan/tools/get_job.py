"""
Get a specific job by ID.
"""

import asyncio
import logging
from typing import Annotated

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel, Field

from ._schema import (
    JobKindOut,
    JobStatusOut,
    job_kind_out,
    job_status_out,
    read_only_tool_annotations,
    remote_read_tags,
)
from .create_client import get_bauplan_client

logger = logging.getLogger(__name__)

PROJECT_CONFIG_FILES = ("bauplan_project.yml", "bauplan_project.yaml")


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
    logs: Annotated[
        str | None,
        Field(
            description="User log messages emitted by the job, joined as text.",
        ),
    ] = None
    ref: Annotated[
        str | None,
        Field(
            description="Data ref used by the job, when available from job context.",
        ),
    ] = None
    transactional_branch: Annotated[
        str | None,
        Field(
            description="Temporary transaction branch used by the job, when available.",
        ),
    ] = None
    project_yml: Annotated[
        str | None,
        Field(
            description="bauplan_project.yml or bauplan_project.yaml content from the job snapshot.",
        ),
    ] = None
    project_files: Annotated[
        dict[str, str] | None,
        Field(
            description="Python and SQL files from the job snapshot, keyed by project-relative path.",
        ),
    ] = None
    sql_query: Annotated[
        str | None,
        Field(
            description="SQL query associated with the job, when available from job context.",
        ),
    ] = None


class JobOut(BaseModel):
    job: Annotated[
        JobInfo,
        Field(
            description="Requested job with metadata, logs, context, and source snapshot when available.",
        ),
    ]


async def get_job_out(job_id: str, bauplan_client: bauplan.Client) -> JobOut:
    job = await asyncio.to_thread(
        lambda: bauplan_client.get_job(job_id),
    )
    logs_as_string = None
    ref = None
    transactional_branch = None
    project_yml = None
    project_files = None
    sql_query = None
    try:
        job_context = await asyncio.to_thread(
            lambda: bauplan_client.get_job_context(
                job.id,
                include_snapshot=True,
                include_logs=True,
            )
        )
    except Exception as e:
        logger.info("Job context is not available for job %s: %s", job_id, e)
    else:
        logs_as_string = "\n".join(log.message for log in job_context.logs) if job_context.logs else None
        snapshot = job_context.snapshot_dict or {}
        project_yml = next(
            (snapshot[path] for path in PROJECT_CONFIG_FILES if path in snapshot),
            None,
        )
        project_files = {
            path: content
            for path, content in snapshot.items()
            if path not in PROJECT_CONFIG_FILES and (path.endswith(".py") or path.endswith(".sql"))
        }
        ref = str(job_context.ref) if job_context.ref else None
        transactional_branch = str(job_context.tx_ref) if job_context.tx_ref else None
        sql_query = getattr(job_context, "sql_query", None) or None

    job_info = JobInfo(
        id=job.id,
        kind=job_kind_out(job.kind),
        user=job.user,
        human_readable_status=job.human_readable_status,
        created_at=job.created_at.isoformat() if job.created_at else None,
        finished_at=job.finished_at.isoformat() if job.finished_at else None,
        status=job_status_out(job.status),
        error_message=job.error_message or None,
        logs=logs_as_string,
        ref=ref,
        transactional_branch=transactional_branch,
        project_yml=project_yml,
        project_files=project_files,
        sql_query=sql_query,
    )
    return JobOut(job=job_info)


def register_get_job_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="get_job", annotations=read_only_tool_annotations("Get job"), tags=remote_read_tags())
    async def get_job(
        job_id: Annotated[
            str,
            Field(
                description="Bauplan job ID to retrieve.",
            ),
        ],
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> JobOut:
        """
        Get detailed context for one Bauplan job, including status, logs, and code snapshot when available.
        Use this after a tool returns a job ID or after get_jobs identifies an execution that needs inspection.
        """

        try:
            if ctx:
                await ctx.info(f"Getting job details for job ID: {job_id}")

            result = await get_job_out(job_id, bauplan_client)

            # Log successful retrieval
            logger.info(f"Successfully retrieved job details for job ID: {job_id}")

            return result

        except Exception as e:
            # Handle job-related errors more gracefully
            error_msg = str(e)
            if "JobGetError" in error_msg or "Failed to get job" in error_msg:
                raise ToolError(
                    f"Error executing get_job '{job_id}': job not found or could not be retrieved"
                ) from e
            else:
                raise ToolError(f"Error executing get_job '{job_id}': {error_msg}") from e
