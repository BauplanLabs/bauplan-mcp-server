"""
Get a specific job by ID.
"""

from fastmcp import FastMCP
from pydantic import BaseModel
from typing import Optional
from fastmcp.exceptions import ToolError

from .create_client import with_fresh_client
import logging
from fastmcp import Context

logger = logging.getLogger(__name__)


class JobInfo(BaseModel):
    id: str
    kind: str
    user: str
    human_readable_status: str
    created_at: Optional[str]
    finished_at: Optional[str]
    status: str


def register_get_job_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="get_job",
        description="Retrieve details of a specified job using a job ID, returning a job detail object.",
    )
    @with_fresh_client
    async def get_job(job_id: str, bauplan_client, ctx: Context = None) -> JobInfo:
        """
        Get details of a specific job by its ID.

        Args:
            job_id: The ID of the job to retrieve.

        Returns:
            JobInfo: Object containing job details
        """
        try:
            if ctx:
                await ctx.info(f"Getting job details for job ID: {job_id}")

            # Call get_job function
            job = bauplan_client.get_job(job_id=job_id)

            # Convert Job object to JobInfo BaseModel instance
            job_info = JobInfo(
                id=job.id,
                kind=job.kind,
                user=job.user,
                human_readable_status=job.human_readable_status,
                created_at=job.created_at.isoformat() if job.created_at else None,
                finished_at=job.finished_at.isoformat() if job.finished_at else None,
                status=str(job.status),
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
