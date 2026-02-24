import asyncio
import logging

import bauplan
from pydantic import BaseModel


class RunState(BaseModel):
    success: bool
    job_id: str | None = None


async def run_project(
    project_dir: str,
    ref: str,
    logger: logging.Logger,
    bauplan_client: bauplan.Client,
    namespace: str | None = None,
    parameters: dict[str, str | int | float | bool | None] | None = None,
    dry_run: bool | None = False,
    client_timeout: int | None = 120,
) -> RunState:
    # Ensure parameters are of correct type
    if parameters:
        for key, value in parameters.items():
            assert isinstance(value, (str, int, float, bool)), (
                f"Parameter {key} has unsupported type {type(value)}"
            )

    # We dry-run everywhere, but no non-dry-run can be done with ref = 'main'
    assert dry_run or ref != "main", "Runs not allowed with ref='main', unless dry_run=True"

    # Call run function
    run_state = await asyncio.to_thread(
        bauplan_client.run,
        project_dir=project_dir,
        ref=ref,
        namespace=namespace,
        parameters=parameters,
        dry_run=dry_run,
        client_timeout=client_timeout,
    )

    # Log run
    logger.info(f"Run job, with ID: {run_state.job_id}, status {run_state.job_status}")

    is_success = run_state.job_status.lower() == "success" if run_state.job_status else False
    if is_success:
        assert run_state.job_id is not None

    return RunState(success=is_success, job_id=run_state.job_id)
