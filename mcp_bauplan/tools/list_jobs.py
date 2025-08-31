"""
List jobs in the Bauplan system.
"""

from fastmcp import FastMCP
from pydantic import BaseModel
from bauplan import JobState
from typing import Optional, List
from fastmcp.exceptions import ToolError
from datetime import datetime
from .create_client import create_bauplan_client
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
        description="Retrieve a list of jobs in Bauplan, optionally filter by job id, status (COMPLETE, FAIL, ABORT, RUNNING), user name, start and end time (UTC, format '%m/%d/%y %H:%M:%S').",
    )
    async def list_jobs(
        job_id: Optional[str] = None,
        status: Optional[str] = None,
        user_name: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        api_key: Optional[str] = None,
        ctx: Context = None,
    ) -> JobsList:
        """
        List jobs in the Bauplan system.

        Args:
            job_id: Optional filter by job ID
            status: Optional filter by job status, either COMPLETE, FAIL, ABORT or RUNNING
            user_name: Optional filter by user name
            start_time: Optional filter by job start time, UTC time, '%m/%d/%y %H:%M:%S', e.g. '09/19/22 13:55:26'
            end_time: Optional filter by job finish time, UTC time, '%m/%d/%y %H:%M:%S', e.g. '09/19/22 13:55:26'
            api_key: The Bauplan API key for authentication.

        Returns:
            JobsList: Object containing list of jobs with their details
        """
        try:
            # Create a fresh Bauplan client
            bauplan_client = create_bauplan_client(api_key)
            if ctx:
                await ctx.info(
                    f"Listing jobs (status: {status}, start_time: {start_time}, finish_time: {end_time})"
                )
            # Make sure the status is acceptable
            if status:
                job_statuses = ["COMPLETE", "FAIL", "ABORT", "RUNNING"]
                assert status.upper() in job_statuses, (
                    f"Invalid job status: {status}: should be one of {job_statuses}"
                )

            start_date_time = (
                datetime.strptime(start_time, "%m/%d/%y %H:%M:%S")
                if start_time
                else None
            )
            end_date_time = (
                datetime.strptime(end_time, "%m/%d/%y %H:%M:%S") if end_time else None
            )

            # Call list_jobs function
            jobs_result = bauplan_client.list_jobs(
                filter_by_id=job_id if job_id else None,
                filter_by_status=JobState[status.upper()] if status else None,
                filter_by_finish_time=(start_date_time, end_date_time),
            )
            # filter for jobs which have a code snapshot associated to them
            jobs_result = list(
                filter(
                    lambda j: j.kind == "CodeSnapshotRun",
                    jobs_result,
                )
            )
            # if user_name, filter for jobs by user_name
            if user_name:
                jobs_result = list(
                    filter(lambda j: j.user == user_name, jobs_result)
                )

            # Convert Job objects to JobInfo BaseModel instances
            job_info_list = []
            for job in jobs_result:
                job_info = JobInfo(
                    id=job.id,
                    kind=job.kind,
                    user=job.user,
                    human_readable_status=job.human_readable_status,
                    created_at=job.created_at.isoformat() if job.created_at else None,
                    finished_at=job.finished_at.isoformat()
                    if job.finished_at
                    else None,
                    status=str(job.status),
                )
                job_info_list.append(job_info)

            # Log successful retrieval
            logger.info(f"Successfully retrieved {len(job_info_list)} jobs")

            return JobsList(jobs=job_info_list, total_count=len(job_info_list))

        except Exception as e:
            # Handle job-related errors more gracefully
            error_msg = str(e)
            logger.error(f"Error listing jobs: {error_msg}")
            raise ToolError(f"Failed to list jobs: {error_msg}")
