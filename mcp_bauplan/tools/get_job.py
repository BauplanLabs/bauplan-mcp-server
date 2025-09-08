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
import os

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
    ref: Optional[str] = None
    transactional_branch: Optional[str] = None
    project_yml: Optional[str] = None
    project_files: Optional[dict[str, str]] = None


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
            id (str): The ID of the job.
            kind (str): The kind of job.
            user (str): The user who created the job.
            human_readable_status (str): Human-readable status of the job.
            created_at (Optional[str]): ISO formatted creation timestamp of the job.
            finished_at (Optional[str]): ISO formatted finish timestamp of the job.
            status (str): The status of the job.
            logs (Optional[str]): Concatenated user logs from the job.
            code_snapshot_path (Optional[Path]): Path to the code snapshot directory.
            ref (Optional[str]): The data commit reference when the job was run.
            transactional_branch (Optional[str]): The transactional branch that was open when the job was run.
            project_yml (Optional[str]): The contents of the bauplan_project.yml file from the snapshot.
            project_files (Optional[dict[str, str]]): A dictionary of other project files from the snapshot, with filenames as keys and file contents as values.

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
            project_yml = None
            project_files = {}
            # list all the files in the snapshot directory
            if (
                job_context.snapshot_dirpath
                and not Path(job_context.snapshot_dirpath).exists()
            ):
                logger.warning(
                    f"Snapshot directory {job_context.snapshot_dirpath} does not exist."
                )
            elif job_context.snapshot_dirpath:
                snapshot_files = list(Path(job_context.snapshot_dirpath).rglob("*"))
                logger.info(
                    f"Snapshot directory {job_context.snapshot_dirpath} contains {len(snapshot_files)} files."
                )
                # check one of the file is bauplan_project.yml
                assert any(f.name == "bauplan_project.yml" for f in snapshot_files), (
                    "bauplan_project.yml not found in snapshot"
                )
                project_yml = next(
                    f for f in snapshot_files if f.name == "bauplan_project.yml"
                ).read_text()
                logger.info("Retrieved bauplan_project.yml from snapshot.")
                # check all other files ends with .py or .sql
                for f in snapshot_files:
                    assert (
                        f.name.endswith(".py")
                        or f.name.endswith(".sql")
                        or f.name.endswith("bauplan_project.yml")
                    ), f"Unexpected file {f} in snapshot"
                    # skip bauplan_project.yml
                    if f.name.endswith("bauplan_project.yml"):
                        continue
                    # extract file name only
                    path, file_name_only = os.path.split(f)
                    project_files[file_name_only] = f.read_text()

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
                ref=str(job_context.ref) if job_context.ref else None,
                transactional_branch=str(job_context.tx_ref)
                if job_context.tx_ref
                else None,
                project_yml=project_yml,
                project_files=project_files,
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
