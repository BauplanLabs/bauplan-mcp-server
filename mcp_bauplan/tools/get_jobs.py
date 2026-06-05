"""
List jobs in the Bauplan system.
"""

import asyncio
import logging
from datetime import datetime

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel

from .create_client import get_bauplan_client

logger = logging.getLogger(__name__)


class JobInfo(BaseModel):
    id: str
    kind: str
    user: str
    human_readable_status: str
    created_at: str | None
    finished_at: str | None
    status: str
    error_message: str | None = None


class JobsList(BaseModel):
    jobs: list[JobInfo]


def register_get_jobs_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="get_jobs")
    async def get_jobs(
        job_ids: list[str] | None = None,
        job_kinds: list[str] | None = None,
        statuses: list[str] | None = None,
        user_names: list[str] | None = None,
        start_time: str | None = None,
        end_time: str | None = None,
        limit: int = 25,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> JobsList:
        """
        Retrieve a list of Bauplan jobs, optionally filtered by one or more job IDs, kinds, statuses, users, and created time window (UTC, format '%m/%d/%y %H:%M:%S').

        Args:
            job_ids: Optional list of job IDs
            job_kinds: Optional list of job kinds
            statuses: Optional list of Bauplan job states
            user_names: Optional list of user names
            start_time: Optional filter for jobs created after this UTC time, '%m/%d/%y %H:%M:%S', e.g. '09/19/22 13:55:26'
            end_time: Optional filter for jobs created before this UTC time, '%m/%d/%y %H:%M:%S', e.g. '09/19/22 13:55:26'
            limit: Maximum number of jobs to return

        Returns:
            JobsList: Object containing list of jobs with their details
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
                        filter_by_kinds=job_kinds or None,
                        filter_by_statuses=statuses or None,
                        limit=limit or 25,
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
                    kind=str(job.kind),
                    user=job.user,
                    human_readable_status=job.human_readable_status,
                    created_at=job.created_at.isoformat() if job.created_at else None,
                    finished_at=job.finished_at.isoformat() if job.finished_at else None,
                    status=str(job.status),
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
