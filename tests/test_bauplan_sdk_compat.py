import asyncio
from datetime import datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest
from bauplan import JobKind
from fastmcp import FastMCP

from mcp_bauplan.tools.apply_table_creation_plan import register_apply_table_creation_plan_tool
from mcp_bauplan.tools.get_job import register_get_job_tool
from mcp_bauplan.tools.import_data import register_import_data_tool
from mcp_bauplan.tools.list_jobs import register_list_jobs_tool
from mcp_bauplan.tools.plan_table_creation import register_plan_table_creation_tool


def test_list_jobs_uses_bauplan_0_1_get_jobs_filters():
    async def run():
        mcp = FastMCP("test")
        register_list_jobs_tool(mcp)
        tool = cast(Any, await mcp.get_tool("list_jobs"))

        captured = {}

        class Client:
            def get_jobs(self, **kwargs):
                captured.update(kwargs)
                return [
                    SimpleNamespace(
                        id="job-1",
                        kind=JobKind.RUN,
                        user="alice",
                        human_readable_status="Running",
                        created_at=datetime(2026, 6, 4, 12, 0, 0),
                        finished_at=None,
                        status="RUNNING",
                    )
                ]

        result = await tool.fn(
            status="running",
            user_name="alice",
            start_time="06/04/26 11:00:00",
            end_time="06/04/26 13:00:00",
            bauplan_client=Client(),
        )

        assert captured == {
            "all_users": True,
            "filter_by_ids": None,
            "filter_by_users": "alice",
            "filter_by_kinds": JobKind.RUN,
            "filter_by_statuses": "RUNNING",
            "filter_by_created_after": datetime(2026, 6, 4, 11, 0, 0),
            "filter_by_created_before": datetime(2026, 6, 4, 13, 0, 0),
        }
        assert result.jobs[0].kind == "Run"

    asyncio.run(run())


def test_list_jobs_does_not_force_run_kind_when_filtering_by_job_id():
    async def run():
        mcp = FastMCP("test")
        register_list_jobs_tool(mcp)
        tool = cast(Any, await mcp.get_tool("list_jobs"))

        captured = {}

        class Client:
            def get_jobs(self, **kwargs):
                captured.update(kwargs)
                return [
                    SimpleNamespace(
                        id="job-1",
                        kind="TableImport",
                        user="alice",
                        human_readable_status="Complete",
                        created_at=None,
                        finished_at=None,
                        status="COMPLETE",
                    )
                ]

        result = await tool.fn(job_id="job-1", bauplan_client=Client())

        assert captured["filter_by_ids"] == "job-1"
        assert captured["filter_by_kinds"] is None
        assert result.jobs[0].kind == "TableImport"

    asyncio.run(run())


def test_apply_table_creation_plan_accepts_sdk_plan_string():
    async def run():
        mcp = FastMCP("test")
        register_apply_table_creation_plan_tool(mcp)
        tool = cast(Any, await mcp.get_tool("apply_table_creation_plan"))

        captured = {}

        class Client:
            def apply_table_creation_plan(self, **kwargs):
                captured.update(kwargs)
                return SimpleNamespace(job_id="job-1", job_status="SUCCESS", error=None)

        result = await tool.fn(
            plan="schema_info:\n  fields: []\n",
            bauplan_client=Client(),
        )

        assert captured["plan"] == "schema_info:\n  fields: []\n"
        assert result.job_id == "job-1"

    asyncio.run(run())


def test_apply_table_creation_plan_preserves_sdk_status_error(monkeypatch: pytest.MonkeyPatch):
    async def run():
        mcp = FastMCP("test")
        register_apply_table_creation_plan_tool(mcp)
        tool = cast(Any, await mcp.get_tool("apply_table_creation_plan"))

        class ApplyError(Exception):
            def __init__(self):
                self.state = SimpleNamespace(job_id="job-1", job_status="FAILED", error="bad plan")

        monkeypatch.setattr(
            "bauplan.exceptions.TableCreatePlanApplyStatusError",
            ApplyError,
        )

        class Client:
            def apply_table_creation_plan(self, **kwargs):
                raise ApplyError()

        result = await tool.fn(
            plan="schema_info:\n  fields: []\n",
            bauplan_client=Client(),
        )

        assert result.job_id == "job-1"
        assert result.success is False
        assert result.job_status == "FAILED"
        assert result.error == "bad plan"

    asyncio.run(run())


def test_apply_table_creation_plan_accepts_dict_plan_for_mcp_clients():
    async def run():
        mcp = FastMCP("test")
        register_apply_table_creation_plan_tool(mcp)
        tool = cast(Any, await mcp.get_tool("apply_table_creation_plan"))

        captured = {}

        class Client:
            def apply_table_creation_plan(self, **kwargs):
                captured.update(kwargs)
                return SimpleNamespace(job_id="job-1", job_status="SUCCESS", error=None)

        result = await tool.fn(
            plan={"schema_info": {"fields": []}},
            bauplan_client=Client(),
        )

        assert captured["plan"] == '{"schema_info": {"fields": []}}'
        assert result.job_id == "job-1"

    asyncio.run(run())


def test_apply_table_creation_plan_reports_sdk_error_without_job_id():
    async def run():
        mcp = FastMCP("test")
        register_apply_table_creation_plan_tool(mcp)
        tool = cast(Any, await mcp.get_tool("apply_table_creation_plan"))

        class Client:
            def apply_table_creation_plan(self, **kwargs):
                return SimpleNamespace(job_id=None, job_status="FAILED", error="bad plan")

        result = await tool.fn(
            plan="schema_info:\n  fields: []\n",
            bauplan_client=Client(),
        )

        assert result.job_id is None
        assert result.success is False
        assert result.job_status == "FAILED"
        assert result.error == "bad plan"

    asyncio.run(run())


def test_get_job_uses_snapshot_dict_and_preserves_file_basename_keys():
    async def run():
        mcp = FastMCP("test")
        register_get_job_tool(mcp)
        tool = cast(Any, await mcp.get_tool("get_job"))

        class Client:
            def get_job(self, job_id):
                return SimpleNamespace(
                    id=job_id,
                    kind=JobKind.RUN,
                    user="alice",
                    human_readable_status="Complete",
                    created_at=None,
                    finished_at=None,
                    status="COMPLETE",
                )

            def get_job_context(self, job_id, *, include_snapshot, include_logs):
                return SimpleNamespace(
                    logs=[SimpleNamespace(message="hello")],
                    snapshot_dict={
                        "bauplan_project.yml": "project",
                        "models/model.py": "model code",
                        "queries/query.sql": "select 1",
                    },
                    ref="main",
                    tx_ref="alice.tx",
                )

        result = await tool.fn(job_id="job-1", bauplan_client=Client())

        assert result.project_yml == "project"
        assert result.project_files == {
            "model.py": "model code",
            "query.sql": "select 1",
        }
        assert result.logs == "hello"

    asyncio.run(run())


def test_get_job_returns_metadata_when_context_is_unavailable():
    async def run():
        mcp = FastMCP("test")
        register_get_job_tool(mcp)
        tool = cast(Any, await mcp.get_tool("get_job"))

        class Client:
            def get_job(self, job_id):
                return SimpleNamespace(
                    id=job_id,
                    kind=JobKind.RUN,
                    user="alice",
                    human_readable_status="Complete",
                    created_at=None,
                    finished_at=None,
                    status="COMPLETE",
                )

            def get_job_context(self, job_id, *, include_snapshot, include_logs):
                raise RuntimeError("context unavailable")

        result = await tool.fn(job_id="job-1", bauplan_client=Client())

        assert result.id == "job-1"
        assert result.logs is None
        assert result.project_yml is None
        assert result.project_files is None

    asyncio.run(run())


def test_import_data_reports_sdk_error_state():
    async def run():
        mcp = FastMCP("test")
        register_import_data_tool(mcp)
        tool = cast(Any, await mcp.get_tool("import_data"))

        class Client:
            def import_data(self, **kwargs):
                return SimpleNamespace(job_id="job-1", job_status="FAILED", error="bad import")

        result = await tool.fn(
            table="table",
            search_uri="s3://bucket/*.parquet",
            branch="alice.dev",
            bauplan_client=Client(),
        )

        assert result.success is False
        assert result.job_status == "FAILED"
        assert result.error == "bad import"

    asyncio.run(run())


def test_plan_table_creation_exposes_conflict_plan():
    async def run():
        mcp = FastMCP("test")
        register_plan_table_creation_tool(mcp)
        tool = cast(Any, await mcp.get_tool("plan_table_creation"))

        class Client:
            def plan_table_creation(self, **kwargs):
                return SimpleNamespace(
                    job_id=None,
                    job_status="SUCCESS",
                    error="table plan created but has conflicts",
                    plan="schema_info:\n  fields: []\n",
                    can_auto_apply=False,
                    files_to_be_imported=["s3://bucket/file.parquet"],
                )

        result = await tool.fn(
            table="table",
            search_uri="s3://bucket/*.parquet",
            branch="alice.dev",
            bauplan_client=Client(),
        )

        assert result.success is False
        assert result.job_id is None
        assert result.error == "table plan created but has conflicts"
        assert result.plan == "schema_info:\n  fields: []\n"
        assert result.can_auto_apply is False
        assert result.files_to_be_imported == ["s3://bucket/file.parquet"]

    asyncio.run(run())
