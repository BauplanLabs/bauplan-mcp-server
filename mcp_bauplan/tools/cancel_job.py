"""
Cancel a job by ID.
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

def register_cancel_job_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="cancel_job", 
        description="Cancel a running job in the Bauplan system by its job_id and return the updated job status."    )
    @with_fresh_client
    async def cancel_job(
        job_id: str,
        bauplan_client,
        ctx: Context = None
    ) -> JobInfo:
        """
        Cancel a running job by its ID and get updated job status.
        
        Args:
            job_id: The ID of the job to cancel.
            
        Returns:
            JobInfo: Object containing updated job details after cancellation
        """
        try:
            
            if ctx:
                await ctx.info(f"Cancelling job with ID: {job_id}")
            
            # Call cancel_job function
            job = bauplan_client.cancel_job(job_id=job_id)
            
            # Convert Job object to JobInfo BaseModel instance
            job_info = JobInfo(
                id=job.id,
                kind=job.kind,
                user=job.user,
                human_readable_status=job.human_readable_status,
                created_at=job.created_at.isoformat() if job.created_at else None,
                finished_at=job.finished_at.isoformat() if job.finished_at else None,
                status=str(job.status)
            )
            
            # Log successful cancellation
            logger.info(f"Successfully cancelled job with ID: {job_id}")
            
            return job_info
            
        except Exception as e:
            # Handle job-related errors more gracefully
            error_msg = str(e)
            if "JobGetError" in error_msg or "Failed to cancel job" in error_msg or "job" in error_msg.lower():
                logger.error(f"Job not found or error cancelling job {job_id}: {error_msg}")
                raise ToolError(f"Job {job_id} not found or could not be cancelled")
            else:
                logger.error(f"Unexpected error cancelling job {job_id}: {error_msg}")
                raise ToolError(f"Failed to cancel job {job_id}: {error_msg}")