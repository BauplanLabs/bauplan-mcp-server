"""
Get a specific job by ID.
"""

import asyncio
import logging

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel

from .create_client import get_bauplan_client

logger = logging.getLogger(__name__)

PROJECT_CONFIG_FILES = ("bauplan_project.yml", "bauplan_project.yaml")


class JobInfo(BaseModel):
    id: str
    kind: str
    user: str
    human_readable_status: str
    created_at: str | None
    finished_at: str | None
    status: str
    error_message: str | None = None
    logs: str | None = None
    ref: str | None = None
    transactional_branch: str | None = None
    project_yml: str | None = None
    project_files: dict[str, str] | None = None
    sql_query: str | None = None


class JobOut(BaseModel):
    job: JobInfo


def register_get_job_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="get_job")
    async def get_job(
        job_id: str,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> JobOut:
        """
        Retrieve details of a job by job ID, such as user logs, code snapshot, project id.
        Get details of a specific job by its ID.

        Human_readable_status in the response will be "Failed" for failed jobs, "Completed" for completed jobs.

        Args:
            job_id: The ID of the job to retrieve.

        Returns:
            JobOut: Object containing job details
            id (str): The ID of the job.
            kind (str): The kind of job.
            user (str): The user who created the job.
            human_readable_status (str): Human-readable status of the job.
            created_at (Optional[str]): ISO formatted creation timestamp of the job.
            finished_at (Optional[str]): ISO formatted finish timestamp of the job.
            status (str): The status of the job.
            error_message (Optional[str]): Error message for failed jobs, when available.
            logs (Optional[str]): Concatenated user logs from the job.
            ref (Optional[str]): The data commit reference when the job was run, i.e. the state of source tables for the job at that time.
            transactional_branch (Optional[str]): The transactional branch that was open when the job was run.
            project_yml (Optional[str]): The contents of the bauplan_project.yml or bauplan_project.yaml file from the snapshot.
            project_files (Optional[dict[str, str]]): A dictionary of other project files from the snapshot, with project-relative paths as keys and file contents as values.
            sql_query (Optional[str]): The SQL query associated with the job, when available.
        """

        try:
            if ctx:
                await ctx.info(f"Getting job details for job ID: {job_id}")

            # First get the job by id, if there, then add the context
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
                logs_as_string = (
                    "\n".join(log.message for log in job_context.logs) if job_context.logs else None
                )
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

            # Convert Job object to JobInfo BaseModel instance
            job_info = JobInfo(
                id=job.id,
                kind=str(job.kind),
                user=job.user,
                human_readable_status=job.human_readable_status,
                created_at=job.created_at.isoformat() if job.created_at else None,
                finished_at=job.finished_at.isoformat() if job.finished_at else None,
                status=str(job.status),
                error_message=job.error_message or None,
                logs=logs_as_string,
                ref=ref,
                transactional_branch=transactional_branch,
                project_yml=project_yml,
                project_files=project_files,
                sql_query=sql_query,
            )

            # Log successful retrieval
            logger.info(f"Successfully retrieved job details for job ID: {job_id}")

            return JobOut(job=job_info)

        except Exception as e:
            # Handle job-related errors more gracefully
            error_msg = str(e)
            if "JobGetError" in error_msg or "Failed to get job" in error_msg:
                logger.error(f"Job not found or error getting job {job_id}: {error_msg}")
                raise ToolError(f"Error executing get_job '{job_id}': job not found or could not be retrieved") from e
            else:
                logger.error(f"Unexpected error getting job {job_id}: {error_msg}")
                raise ToolError(f"Error executing get_job '{job_id}': {error_msg}") from e
