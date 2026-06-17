import asyncio
import logging

import bauplan

from ._guards import require_writable_branch
from .get_job import JobOut, get_job_out


async def run_project(
    project_dir: str,
    ref: str,
    namespace: str | None,
    parameters: dict[str, str | int | float | bool | None] | None,
    dry_run: bool,
    client_timeout: int,
    detach: bool,
    strict: bool,
    logger: logging.Logger,
    bauplan_client: bauplan.Client,
) -> JobOut:
    # Ensure parameters are of correct type
    if parameters:
        for key, value in parameters.items():
            if value is not None and not isinstance(value, (str, int, float, bool)):
                raise ValueError(f"Parameter {key} has unsupported type {type(value)}")

    # Non-dry-run execution must target an explicit non-main ref.
    if not dry_run:
        ref = require_writable_branch(ref, "run_project")

    # Call run function
    run_state = await asyncio.to_thread(
        lambda: bauplan_client.run(
            project_dir=project_dir,
            ref=ref,
            namespace=namespace,
            parameters=parameters,
            dry_run=dry_run,
            client_timeout=client_timeout,
            detach=detach,
            strict="on" if strict else "off",
        )
    )

    # Log run
    job_id = run_state.job_id
    job_status = getattr(run_state, "job_status", None)
    logger.info(f"Run job, with ID: {job_id}, status {job_status}")

    if job_id is None:
        run_error = getattr(run_state, "error", None)
        raise ValueError(f"Run did not return a job ID (status: {job_status}, error: {run_error}).")

    try:
        return await get_job_out(job_id, bauplan_client)
    except Exception as e:
        raise RuntimeError(
            f"Run submitted with job_id '{job_id}', but job details could not be retrieved: {e}"
        ) from e
