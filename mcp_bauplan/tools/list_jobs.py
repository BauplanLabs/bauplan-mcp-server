"""
List jobs in the Bauplan system.
"""

from fastmcp import FastMCP
from pydantic import BaseModel
from typing import Optional, List
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

class JobsList(BaseModel):
    jobs: List[JobInfo]
    total_count: int

def register_list_jobs_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="list_jobs", 
        description="Retrieve a list of jobs in Bauplan, with optional user filter."
    )
    @with_fresh_client
    async def list_jobs(
        bauplan_client,
        all_users: Optional[bool] = None,
        ctx: Context = None
    ) -> JobsList:
        """
        List jobs in the Bauplan system.
        
        Args:
            all_users: Whether to list jobs for all users (optional, defaults to None for current user only).
            
        Returns:
            JobsList: Object containing list of jobs with their details
        """
        try:
            
            if ctx:
                await ctx.info(f"Listing jobs (all_users: {all_users})")
            
            # Call list_jobs function
            jobs_result = bauplan_client.list_jobs(all_users=all_users)
            
            # Convert Job objects to JobInfo BaseModel instances
            job_info_list = []
            for job in jobs_result:
                job_info = JobInfo(
                    id=job.id,
                    kind=job.kind,
                    user=job.user,
                    human_readable_status=job.human_readable_status,
                    created_at=job.created_at.isoformat() if job.created_at else None,
                    finished_at=job.finished_at.isoformat() if job.finished_at else None,
                    status=str(job.status)
                )
                job_info_list.append(job_info)
            
            # Log successful retrieval
            logger.info(f"Successfully retrieved {len(job_info_list)} jobs")
            
            return JobsList(
                jobs=job_info_list,
                total_count=len(job_info_list)
            )
            
        except Exception as e:
            # Handle job-related errors more gracefully
            error_msg = str(e)
            logger.error(f"Error listing jobs: {error_msg}")
            raise ToolError(f"Failed to list jobs: {error_msg}")