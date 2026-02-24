"""
Cancel a job by ID.
"""

import logging

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError
from pydantic import BaseModel

from .create_client import with_bauplan_client

logger = logging.getLogger(__name__)


class JobInfo(BaseModel):
    id: str
    kind: str
    user: str
    human_readable_status: str
    created_at: str | None
    finished_at: str | None
    status: str


def register_cancel_job_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="cancel_job", exclude_args=["bauplan_client"])
    @with_bauplan_client
    async def cancel_job(
        bauplan_client: bauplan.Client,
        job_id: str,
        ctx: Context | None = None,
    ) -> JobInfo:
        """
        Cancel a running job in the Bauplan system by its job_id and return the updated job status.

        Args:
            job_id: The ID of the job to cancel.

        Returns:
            JobInfo: Object containing updated job details after cancellation
        """
        try:
            if ctx:
                await ctx.info(f"Cancelling job with ID: {job_id}")

            # Call cancel_job function
            bauplan_client.cancel_job(job_id=job_id)

            # Get the updated job details after cancellation
            job = bauplan_client.get_job(job_id=job_id)

            # Convert Job object to JobInfo BaseModel instance
            job_info = JobInfo(
                id=job.id,
                kind=str(job.kind),
                user=job.user,
                human_readable_status=job.human_readable_status,
                created_at=job.created_at.isoformat() if job.created_at else None,
                finished_at=job.finished_at.isoformat() if job.finished_at else None,
                status=str(job.status),
            )

            # Log successful cancellation
            logger.info(f"Successfully cancelled job with ID: {job_id}")

            return job_info

        except Exception as e:
            # Handle job-related errors more gracefully
            error_msg = str(e)
            if (
                "JobGetError" in error_msg
                or "Failed to cancel job" in error_msg
                or "job" in error_msg.lower()
            ):
                logger.error(f"Job not found or error cancelling job {job_id}: {error_msg}")
                raise ToolError(f"Job {job_id} not found or could not be cancelled") from e
            else:
                logger.error(f"Unexpected error cancelling job {job_id}: {error_msg}")
                raise ToolError(f"Failed to cancel job {job_id}: {error_msg}") from e
