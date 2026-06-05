import asyncio
import logging

import bauplan
from pydantic import BaseModel

from ._guards import require_writable_branch


class RunState(BaseModel):
    success: bool
    job_id: str | None = None


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
) -> RunState:
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

    is_success = job_status.lower() == "success" if job_status else job_id is not None
    if is_success and job_id is None:
        raise ValueError("Run succeeded without a job ID.")

    return RunState(success=is_success, job_id=job_id)
