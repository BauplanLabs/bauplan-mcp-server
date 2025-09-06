"""
Get a specific job by ID.
"""

from fastmcp import FastMCP
from pydantic import BaseModel
from typing import Optional
from fastmcp.exceptions import ToolError
from .create_client import with_bauplan_client
import bauplan
import logging
from fastmcp import Context
from pathlib import Path

logger = logging.getLogger(__name__)


class JobInfo(BaseModel):
    id: str
    kind: str
    user: str
    human_readable_status: str
    created_at: Optional[str]
    finished_at: Optional[str]
    status: str
    logs: Optional[str] = None
    code_snapshot_path: Optional[Path] = None


def register_get_job_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="get_job", exclude_args=["bauplan_client"])
    @with_bauplan_client
    async def get_job(
        job_id: str, ctx: Context = None, bauplan_client: bauplan.Client = None
    ) -> JobInfo:
        """
        Retrieve details of a job by job ID, such as user logs, code snapshot, project id.
        Get details of a specific job by its ID.

        Human_readable_status in the response will be "Failed" for failed jobs, "Completed" for completed jobs.

        Args:
            job_id: The ID of the job to retrieve.

        Returns:
            JobInfo: Object containing job details
        """
        try:
            if ctx:
                await ctx.info(f"Getting job details for job ID: {job_id}")

            # First get the job by id, if there, then add the the context
            jobs = bauplan_client.list_jobs(filter_by_id=job_id)
            if not jobs:
                raise ToolError(f"Job {job_id} not found")
            job = jobs[0]
            job_context = bauplan_client.get_job_context(jobs=[job_id])[0]
            logs_as_string = (
                "\n".join(log.message for log in job_context.logs)
                if job_context.logs
                else None
            )

            # Convert Job object to JobInfo BaseModel instance
            job_info = JobInfo(
                id=job.id,
                kind=job.kind,
                user=job.user,
                human_readable_status=job.human_readable_status,
                created_at=job.created_at.isoformat() if job.created_at else None,
                finished_at=job.finished_at.isoformat() if job.finished_at else None,
                status=str(job.status),
                logs=logs_as_string,
                code_snapshot_path=job_context.snapshot_dirpath,
            )

            # Log successful retrieval
            logger.info(f"Successfully retrieved job details for job ID: {job_id}")

            return job_info

        except Exception as e:
            # Handle job-related errors more gracefully
            error_msg = str(e)
            if "JobGetError" in error_msg or "Failed to get job" in error_msg:
                logger.error(
                    f"Job not found or error getting job {job_id}: {error_msg}"
                )
                raise ToolError(f"Job {job_id} not found or could not be retrieved")
            else:
                logger.error(f"Unexpected error getting job {job_id}: {error_msg}")
                raise ToolError(f"Failed to get job {job_id}: {error_msg}")
