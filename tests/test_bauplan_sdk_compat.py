import asyncio
from datetime import datetime
from types import SimpleNamespace
from typing import Any, cast

import pyarrow as pa
import pytest
from bauplan import JobKind
from fastmcp import FastMCP
from fastmcp.exceptions import ToolError

from mcp_bauplan.tools._guards import require_writable_branch
from mcp_bauplan.tools.apply_table_creation_plan import register_apply_table_creation_plan_tool
from mcp_bauplan.tools.code_run import register_code_run_tool
from mcp_bauplan.tools.create_branch import register_create_branch_tool
from mcp_bauplan.tools.create_namespace import register_create_namespace_tool
from mcp_bauplan.tools.create_table import register_create_table_tool
from mcp_bauplan.tools.create_tag import register_create_tag_tool
from mcp_bauplan.tools.delete_branch import register_delete_branch_tool
from mcp_bauplan.tools.delete_namespace import register_delete_namespace_tool
from mcp_bauplan.tools.delete_table import register_delete_table_tool
from mcp_bauplan.tools.delete_tag import register_delete_tag_tool
from mcp_bauplan.tools.get_branch import register_get_branch_tool
from mcp_bauplan.tools.get_commits import register_get_commits_tool
from mcp_bauplan.tools.get_job import register_get_job_tool
from mcp_bauplan.tools.get_jobs import register_get_jobs_tool
from mcp_bauplan.tools.get_namespace import register_get_namespace_tool
from mcp_bauplan.tools.get_namespaces import register_get_namespaces_tool
from mcp_bauplan.tools.get_table import register_get_table_tool
from mcp_bauplan.tools.get_tables import register_get_tables_tool
from mcp_bauplan.tools.get_tag import register_get_tag_tool
from mcp_bauplan.tools.get_tags import register_get_tags_tool
from mcp_bauplan.tools.import_data import register_import_data_tool
from mcp_bauplan.tools.merge_branch import register_merge_branch_tool
from mcp_bauplan.tools.plan_table_creation import register_plan_table_creation_tool
from mcp_bauplan.tools.project_run import register_project_run_tool
from mcp_bauplan.tools.revert_table import register_revert_table_tool
from mcp_bauplan.tools.run_query import register_run_query_tool


def _sdk_table(**overrides):
    values = {
        "id": "table-id",
        "name": "titanic",
        "namespace": "bauplan",
        "kind": "EXTERNAL_TABLE",
        "is_external": lambda: True,
        "current_schema_id": 3,
        "current_snapshot_id": 42,
        "last_updated_at": datetime(2026, 6, 20, 12, 30, 0),
        "metadata_location": "s3://bucket/table/metadata.json",
        "partitions": [SimpleNamespace(name="ds", transform="day")],
        "properties": {"owner": "analytics"},
        "records": 123,
        "size": 456,
        "snapshots": 7,
        "fields": [
            SimpleNamespace(
                id=1,
                name="passenger_id",
                required=True,
                type="int64",
            )
        ],
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _sdk_job(**overrides):
    values = {
        "id": "job-1",
        "kind": JobKind.RUN,
        "user": "alice",
        "human_readable_status": "Complete",
        "created_at": None,
        "finished_at": None,
        "status": "COMPLETE",
        "error_message": None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class _JobLookupClient:
    def get_job(self, job_id):
        return _sdk_job(id=job_id)

    def get_job_context(self, job_id, *, include_snapshot, include_logs):
        return SimpleNamespace(
            logs=[],
            snapshot_dict={},
            ref=None,
            tx_ref=None,
        )


def test_writable_branch_guard_rejects_default_main_targets():
    assert require_writable_branch("alice.dev", "test") == "alice.dev"
    assert require_writable_branch(" alice.dev ", "test") == "alice.dev"

    for branch in (None, "", " ", "main", "MAIN"):
        with pytest.raises(ToolError):
            require_writable_branch(branch, "test")


def test_create_table_rejects_main_branch_without_calling_sdk():
    async def run():
        mcp = FastMCP("test")
        register_create_table_tool(mcp)
        tool = cast(Any, await mcp.get_tool("create_table"))

        class Client:
            def create_table(self, **kwargs):
                raise AssertionError("create_table should not be called")

        with pytest.raises(ToolError, match="cannot target the main branch"):
            await tool.fn(
                table="titanic",
                search_uri="s3://bucket/*.parquet",
                branch="main",
                bauplan_client=Client(),
            )

    asyncio.run(run())


def test_create_branch_returns_created_branch():
    async def run():
        mcp = FastMCP("test")
        register_create_branch_tool(mcp)
        tool = cast(Any, await mcp.get_tool("create_branch"))

        captured = {}

        class Client:
            def create_branch(self, **kwargs):
                captured.update(kwargs)
                return SimpleNamespace(name="alice.dev", hash="abc123")

        result = await tool.fn(branch="alice.dev", from_ref="main", bauplan_client=Client())

        assert captured == {"branch": "alice.dev", "from_ref": "main"}
        assert result.branch.name == "alice.dev"
        assert result.branch.hash == "abc123"

    asyncio.run(run())


def test_create_namespace_returns_created_namespace():
    async def run():
        mcp = FastMCP("test")
        register_create_namespace_tool(mcp)
        tool = cast(Any, await mcp.get_tool("create_namespace"))

        captured = {}

        class Client:
            def create_namespace(self, **kwargs):
                captured.update(kwargs)
                return SimpleNamespace(name="analytics")

        result = await tool.fn(namespace="analytics", branch="alice.dev", bauplan_client=Client())

        assert captured == {"namespace": "analytics", "branch": "alice.dev"}
        assert result.namespace.name == "analytics"

    asyncio.run(run())


def test_delete_namespace_returns_updated_branch():
    async def run():
        mcp = FastMCP("test")
        register_delete_namespace_tool(mcp)
        tool = cast(Any, await mcp.get_tool("delete_namespace"))

        captured = {}

        class Client:
            def delete_namespace(self, **kwargs):
                captured.update(kwargs)
                return SimpleNamespace(name="alice.dev", hash="abc123")

        result = await tool.fn(namespace="analytics", branch="alice.dev", bauplan_client=Client())

        assert captured == {"namespace": "analytics", "branch": "alice.dev"}
        assert result.branch.name == "alice.dev"
        assert result.branch.hash == "abc123"

    asyncio.run(run())


def test_delete_branch_returns_deleted_bool():
    async def run():
        mcp = FastMCP("test")
        register_delete_branch_tool(mcp)
        tool = cast(Any, await mcp.get_tool("delete_branch"))

        captured = {}

        class Client:
            def delete_branch(self, **kwargs):
                captured.update(kwargs)
                return True

        result = await tool.fn(branch="alice.dev", bauplan_client=Client())

        assert captured == {"branch": "alice.dev"}
        assert result.deleted is True

    asyncio.run(run())


def test_delete_table_returns_updated_branch():
    async def run():
        mcp = FastMCP("test")
        register_delete_table_tool(mcp)
        tool = cast(Any, await mcp.get_tool("delete_table"))

        captured = {}

        class Client:
            def delete_table(self, **kwargs):
                captured.update(kwargs)
                return SimpleNamespace(name="alice.dev", hash="abc123")

        result = await tool.fn(table="titanic", branch="alice.dev", namespace="", bauplan_client=Client())

        assert captured == {"table": "titanic", "branch": "alice.dev", "namespace": None}
        assert result.branch.name == "alice.dev"
        assert result.branch.hash == "abc123"

    asyncio.run(run())


def test_merge_branch_returns_updated_target_branch_and_omits_blank_commit_text():
    async def run():
        mcp = FastMCP("test")
        register_merge_branch_tool(mcp)
        tool = cast(Any, await mcp.get_tool("merge_branch"))

        captured = {}

        class Client:
            def merge_branch(self, **kwargs):
                captured.update(kwargs)
                return SimpleNamespace(name="alice.dev", hash="abc123")

        result = await tool.fn(
            source_ref="alice.feature",
            into_branch="alice.dev",
            commit_message="",
            commit_body="",
            bauplan_client=Client(),
        )

        assert captured == {
            "source_ref": "alice.feature",
            "into_branch": "alice.dev",
            "commit_message": None,
            "commit_body": None,
        }
        assert result.branch.name == "alice.dev"
        assert result.branch.hash == "abc123"

    asyncio.run(run())


def test_merge_branch_rejects_main_target_without_calling_sdk():
    async def run():
        mcp = FastMCP("test")
        register_merge_branch_tool(mcp)
        tool = cast(Any, await mcp.get_tool("merge_branch"))

        class Client:
            def merge_branch(self, **kwargs):
                raise AssertionError("merge_branch should not be called")

        with pytest.raises(ToolError, match="cannot target the main branch"):
            await tool.fn(source_ref="alice.feature", into_branch="main", bauplan_client=Client())

    asyncio.run(run())


def test_revert_table_returns_updated_target_branch_and_omits_blank_optional_values():
    async def run():
        mcp = FastMCP("test")
        register_revert_table_tool(mcp)
        tool = cast(Any, await mcp.get_tool("revert_table"))

        captured = {}

        class Client:
            def revert_table(self, **kwargs):
                captured.update(kwargs)
                return SimpleNamespace(name="alice.dev", hash="abc123")

        result = await tool.fn(
            table="titanic",
            source_ref="main",
            into_branch="alice.dev",
            namespace="",
            commit_body="",
            bauplan_client=Client(),
        )

        assert captured == {
            "table": "titanic",
            "namespace": None,
            "source_ref": "main",
            "into_branch": "alice.dev",
            "replace": None,
            "commit_body": None,
        }
        assert result.branch.name == "alice.dev"
        assert result.branch.hash == "abc123"

    asyncio.run(run())


def test_revert_table_rejects_main_target_without_calling_sdk():
    async def run():
        mcp = FastMCP("test")
        register_revert_table_tool(mcp)
        tool = cast(Any, await mcp.get_tool("revert_table"))

        class Client:
            def revert_table(self, **kwargs):
                raise AssertionError("revert_table should not be called")

        with pytest.raises(ToolError, match="cannot target the main branch"):
            await tool.fn(
                table="titanic", source_ref="alice.feature", into_branch="main", bauplan_client=Client()
            )

    asyncio.run(run())


def test_delete_tag_returns_deleted_bool():
    async def run():
        mcp = FastMCP("test")
        register_delete_tag_tool(mcp)
        tool = cast(Any, await mcp.get_tool("delete_tag"))

        captured = {}

        class Client:
            def delete_tag(self, **kwargs):
                captured.update(kwargs)
                return True

        result = await tool.fn(tag="v1", bauplan_client=Client())

        assert captured == {"tag": "v1"}
        assert result.deleted is True

    asyncio.run(run())


def test_create_tag_returns_created_tag():
    async def run():
        mcp = FastMCP("test")
        register_create_tag_tool(mcp)
        tool = cast(Any, await mcp.get_tool("create_tag"))

        captured = {}

        class Client:
            def create_tag(self, **kwargs):
                captured.update(kwargs)
                return SimpleNamespace(name="v1", hash="abc123")

        result = await tool.fn(tag="v1", from_ref="main", bauplan_client=Client())

        assert captured == {"tag": "v1", "from_ref": "main"}
        assert result.tag.name == "v1"
        assert result.tag.hash == "abc123"

    asyncio.run(run())


def test_create_table_returns_created_table_and_omits_blank_optional_values():
    async def run():
        mcp = FastMCP("test")
        register_create_table_tool(mcp)
        tool = cast(Any, await mcp.get_tool("create_table"))

        captured = {}

        class Client:
            def create_table(self, **kwargs):
                captured.update(kwargs)
                return _sdk_table()

        result = await tool.fn(
            table="titanic",
            search_uri="s3://bucket/*.parquet",
            branch="alice.dev",
            namespace="",
            partitioned_by="",
            bauplan_client=Client(),
        )

        assert captured == {
            "table": "titanic",
            "search_uri": "s3://bucket/*.parquet",
            "namespace": None,
            "branch": "alice.dev",
            "partitioned_by": None,
            "replace": None,
        }
        assert result.table.name == "titanic"
        assert result.table.namespace == "bauplan"
        assert result.table.fields[0]["name"] == "passenger_id"

    asyncio.run(run())


def test_project_run_rejects_default_main_ref_without_calling_sdk():
    async def run():
        mcp = FastMCP("test")
        register_project_run_tool(mcp)
        tool = cast(Any, await mcp.get_tool("project_run"))

        class Client:
            def run(self, **kwargs):
                raise AssertionError("run should not be called")

        for ref in (None, "", "main"):
            with pytest.raises(ToolError, match=r"requires an explicit non-main branch|cannot target"):
                await tool.fn(project_dir="/tmp/project", ref=ref, bauplan_client=Client())

    asyncio.run(run())


def test_plan_table_creation_rejects_default_main_branch_without_calling_sdk():
    async def run():
        mcp = FastMCP("test")
        register_plan_table_creation_tool(mcp)
        tool = cast(Any, await mcp.get_tool("plan_table_creation"))

        class Client:
            def plan_table_creation(self, **kwargs):
                raise AssertionError("plan_table_creation should not be called")

        for branch in (None, "", "main"):
            with pytest.raises(ToolError, match=r"requires an explicit non-main branch|cannot target"):
                await tool.fn(
                    table="titanic",
                    search_uri="s3://bucket/*.parquet",
                    branch=branch,
                    bauplan_client=Client(),
                )

    asyncio.run(run())


def test_plan_table_creation_requires_branch_in_schema():
    async def run():
        mcp = FastMCP("test")
        register_plan_table_creation_tool(mcp)
        tool = cast(Any, await mcp.get_tool("plan_table_creation"))

        assert "branch" in tool.parameters["required"]

    asyncio.run(run())


def test_get_jobs_uses_requested_bauplan_0_1_get_jobs_filters():
    async def run():
        mcp = FastMCP("test")
        register_get_jobs_tool(mcp)
        tool = cast(Any, await mcp.get_tool("get_jobs"))

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
                        error_message=None,
                    )
                ]

        result = await tool.fn(
            job_kinds=["run"],
            statuses=["running"],
            user_names=["alice"],
            start_time="06/04/26 11:00:00",
            end_time="06/04/26 13:00:00",
            bauplan_client=Client(),
        )

        assert captured == {
            "filter_by_ids": None,
            "filter_by_users": ["alice"],
            "filter_by_kinds": ["run"],
            "filter_by_statuses": ["running"],
            "limit": 25,
            "filter_by_created_after": datetime(2026, 6, 4, 11, 0, 0),
            "filter_by_created_before": datetime(2026, 6, 4, 13, 0, 0),
        }
        assert result.jobs[0].kind == "Run"
        assert result.jobs[0].error_message is None

    asyncio.run(run())


def test_get_table_delegates_table_and_namespace_to_sdk():
    async def run():
        mcp = FastMCP("test")
        register_get_table_tool(mcp)
        tool = cast(Any, await mcp.get_tool("get_table"))

        captured = {}

        class Client:
            def get_table(self, **kwargs):
                captured.update(kwargs)
                return _sdk_table()

        result = await tool.fn(
            ref="main",
            table="bauplan.titanic",
            namespace="",
            bauplan_client=Client(),
        )

        assert captured == {
            "table": "bauplan.titanic",
            "ref": "main",
            "namespace": None,
        }
        assert result.table.name == "titanic"
        assert result.table.namespace == "bauplan"
        assert result.table.id == "table-id"
        assert result.table.kind == "EXTERNAL_TABLE"
        assert result.table.is_external is True
        assert result.table.current_schema_id == 3
        assert result.table.current_snapshot_id == 42
        assert result.table.last_updated_at == "2026-06-20T12:30:00"
        assert result.table.metadata_location == "s3://bucket/table/metadata.json"
        assert result.table.partitions[0].name == "ds"
        assert result.table.partitions[0].transform == "day"
        assert result.table.properties == {"owner": "analytics"}
        assert result.table.records == 123
        assert result.table.size == 456
        assert result.table.snapshots == 7
        assert result.table.fields[0]["name"] == "passenger_id"

    asyncio.run(run())


def test_get_table_returns_clear_error_when_missing():
    async def run():
        mcp = FastMCP("test")
        register_get_table_tool(mcp)
        tool = cast(Any, await mcp.get_tool("get_table"))

        class Client:
            def get_table(self, **kwargs):
                raise RuntimeError(f"table {kwargs['table']} not found")

        with pytest.raises(ToolError, match=r"Error executing get_table 'bauplan\.missing' in ref 'main'"):
            await tool.fn(ref="main", table="missing", namespace="bauplan", bauplan_client=Client())

    asyncio.run(run())


def test_get_branch_returns_name_with_hash():
    async def run():
        mcp = FastMCP("test")
        register_get_branch_tool(mcp)
        tool = cast(Any, await mcp.get_tool("get_branch"))

        captured = {}

        class Client:
            def get_branch(self, **kwargs):
                captured.update(kwargs)
                return SimpleNamespace(name="main", hash="abc123")

        result = await tool.fn(branch="main", bauplan_client=Client())

        assert captured == {"branch": "main"}
        assert result.branch.name == "main"
        assert result.branch.hash == "abc123"

    asyncio.run(run())


def test_get_commits_returns_full_message_and_properties():
    async def run():
        mcp = FastMCP("test")
        register_get_commits_tool(mcp)
        tool = cast(Any, await mcp.get_tool("get_commits"))

        class Client:
            def get_commits(self, **kwargs):
                assert kwargs["ref"] == "main"
                return [
                    SimpleNamespace(
                        hash="commit-hash",
                        message="Commit subject\nCommit body",
                        author=SimpleNamespace(name="Alice", email="alice@example.com"),
                        authored_date=datetime(2026, 6, 20, 12, 30, 0),
                        parent_hashes=["parent-hash"],
                        properties={"owner": "analytics"},
                    )
                ]

        result = await tool.fn(ref="main", bauplan_client=Client())

        assert result.commits[0].message == "Commit subject\nCommit body"
        assert result.commits[0].properties == {"owner": "analytics"}

    asyncio.run(run())


def test_get_branch_returns_clear_error_when_missing():
    async def run():
        mcp = FastMCP("test")
        register_get_branch_tool(mcp)
        tool = cast(Any, await mcp.get_tool("get_branch"))

        class Client:
            def get_branch(self, **kwargs):
                raise RuntimeError(f"branch {kwargs['branch']} not found")

        with pytest.raises(ToolError, match="Error executing get_branch 'missing'"):
            await tool.fn(branch="missing", bauplan_client=Client())

    asyncio.run(run())


def test_get_namespaces_returns_names():
    async def run():
        mcp = FastMCP("test")
        register_get_namespaces_tool(mcp)
        tool = cast(Any, await mcp.get_tool("get_namespaces"))

        captured = {}

        class Client:
            def get_namespaces(self, **kwargs):
                captured.update(kwargs)
                return [
                    SimpleNamespace(name="bauplan"),
                    SimpleNamespace(name="analytics"),
                ]

        result = await tool.fn(ref="main", namespace="", bauplan_client=Client())

        assert captured == {
            "ref": "main",
            "filter_by_name": None,
            "limit": 25,
        }
        assert [namespace.name for namespace in result.namespaces] == ["bauplan", "analytics"]

    asyncio.run(run())


def test_get_namespace_returns_name():
    async def run():
        mcp = FastMCP("test")
        register_get_namespace_tool(mcp)
        tool = cast(Any, await mcp.get_tool("get_namespace"))

        captured = {}

        class Client:
            def get_namespace(self, **kwargs):
                captured.update(kwargs)
                return SimpleNamespace(name="analytics")

        result = await tool.fn(ref="main", namespace="analytics", bauplan_client=Client())

        assert captured == {"namespace": "analytics", "ref": "main"}
        assert result.namespace.name == "analytics"

    asyncio.run(run())


def test_get_namespace_returns_clear_error_when_missing():
    async def run():
        mcp = FastMCP("test")
        register_get_namespace_tool(mcp)
        tool = cast(Any, await mcp.get_tool("get_namespace"))

        class Client:
            def get_namespace(self, **kwargs):
                raise RuntimeError(f"namespace {kwargs['namespace']} not found")

        with pytest.raises(ToolError, match="Error executing get_namespace 'missing' in ref 'main'"):
            await tool.fn(ref="main", namespace="missing", bauplan_client=Client())

    asyncio.run(run())


def test_get_tables_returns_names_with_namespaces():
    async def run():
        mcp = FastMCP("test")
        register_get_tables_tool(mcp)
        tool = cast(Any, await mcp.get_tool("get_tables"))

        captured = {}

        class Client:
            def get_tables(self, **kwargs):
                captured.update(kwargs)
                return [
                    SimpleNamespace(
                        id="staging-table-id",
                        name="orders",
                        namespace="staging",
                        kind="TABLE",
                        is_external=lambda: False,
                        records=10,
                        size=100,
                    ),
                    SimpleNamespace(
                        id="prod-table-id",
                        name="orders",
                        namespace="prod",
                        kind="EXTERNAL_TABLE",
                        is_external=lambda: True,
                        records=None,
                        size=None,
                    ),
                ]

        result = await tool.fn(ref="main", namespace="", bauplan_client=Client())

        assert captured == {
            "ref": "main",
            "filter_by_name": None,
            "filter_by_namespace": None,
            "limit": 25,
        }
        assert [(table.name, table.namespace) for table in result.tables] == [
            ("orders", "staging"),
            ("orders", "prod"),
        ]
        assert result.tables[0].id == "staging-table-id"
        assert result.tables[0].kind == "TABLE"
        assert result.tables[0].is_external is False
        assert result.tables[0].records == 10
        assert result.tables[0].size == 100
        assert result.tables[0].partitions is None
        assert result.tables[0].fields is None
        assert result.tables[1].id == "prod-table-id"
        assert result.tables[1].kind == "EXTERNAL_TABLE"
        assert result.tables[1].is_external is True
        assert result.tables[1].records is None
        assert result.tables[1].size is None

    asyncio.run(run())


def test_get_tables_can_include_schema():
    async def run():
        mcp = FastMCP("test")
        register_get_tables_tool(mcp)
        tool = cast(Any, await mcp.get_tool("get_tables"))

        captured = {}

        class Client:
            def get_tables(self, **kwargs):
                captured.update(kwargs)
                return [
                    SimpleNamespace(
                        id="table-id",
                        name="orders",
                        namespace="staging",
                        kind="TABLE",
                        is_external=lambda: False,
                        records=10,
                        size=100,
                        partitions=[SimpleNamespace(name="ds", transform="day")],
                        fields=[
                            SimpleNamespace(
                                id=1,
                                name="order_id",
                                required=True,
                                type="int64",
                            )
                        ],
                    )
                ]

        result = await tool.fn(
            ref="main",
            table="orders",
            include_schema=True,
            limit=7,
            bauplan_client=Client(),
        )

        assert captured == {
            "ref": "main",
            "filter_by_name": "orders",
            "filter_by_namespace": None,
            "limit": 7,
        }
        assert result.tables[0].partitions is not None
        assert result.tables[0].partitions[0].name == "ds"
        assert result.tables[0].partitions[0].transform == "day"
        assert result.tables[0].fields is not None
        assert result.tables[0].fields[0]["name"] == "order_id"

    asyncio.run(run())


def test_get_tags_returns_names_with_hashes():
    async def run():
        mcp = FastMCP("test")
        register_get_tags_tool(mcp)
        tool = cast(Any, await mcp.get_tool("get_tags"))

        captured = {}

        class Client:
            def get_tags(self, **kwargs):
                captured.update(kwargs)
                return [
                    SimpleNamespace(name="v1", hash="abc123"),
                    SimpleNamespace(name="v2", hash="def456"),
                ]

        result = await tool.fn(filter_by_name="", bauplan_client=Client())

        assert captured == {
            "filter_by_name": None,
            "limit": 25,
        }
        assert [(tag.name, tag.hash) for tag in result.tags] == [
            ("v1", "abc123"),
            ("v2", "def456"),
        ]

    asyncio.run(run())


def test_get_tag_returns_name_with_hash():
    async def run():
        mcp = FastMCP("test")
        register_get_tag_tool(mcp)
        tool = cast(Any, await mcp.get_tool("get_tag"))

        captured = {}

        class Client:
            def get_tag(self, **kwargs):
                captured.update(kwargs)
                return SimpleNamespace(name="v1", hash="abc123")

        result = await tool.fn(tag="v1", bauplan_client=Client())

        assert captured == {"tag": "v1"}
        assert result.tag.name == "v1"
        assert result.tag.hash == "abc123"

    asyncio.run(run())


def test_get_tag_returns_clear_error_when_missing():
    async def run():
        mcp = FastMCP("test")
        register_get_tag_tool(mcp)
        tool = cast(Any, await mcp.get_tool("get_tag"))

        class Client:
            def get_tag(self, **kwargs):
                raise RuntimeError(f"tag {kwargs['tag']} not found")

        with pytest.raises(ToolError, match="Error executing get_tag 'missing'"):
            await tool.fn(tag="missing", bauplan_client=Client())

    asyncio.run(run())


def test_get_jobs_does_not_filter_by_kind_unless_requested():
    async def run():
        mcp = FastMCP("test")
        register_get_jobs_tool(mcp)
        tool = cast(Any, await mcp.get_tool("get_jobs"))

        captured = {}

        class Client:
            def get_jobs(self, **kwargs):
                captured.update(kwargs)
                return []

        await tool.fn(bauplan_client=Client())

        assert captured["filter_by_kinds"] is None
        assert captured["limit"] == 25

    asyncio.run(run())


def test_get_jobs_accepts_custom_limit():
    async def run():
        mcp = FastMCP("test")
        register_get_jobs_tool(mcp)
        tool = cast(Any, await mcp.get_tool("get_jobs"))

        captured = {}

        class Client:
            def get_jobs(self, **kwargs):
                captured.update(kwargs)
                return []

        await tool.fn(limit=10, bauplan_client=Client())

        assert captured["limit"] == 10

    asyncio.run(run())


def test_get_jobs_does_not_force_run_kind_when_filtering_by_job_id():
    async def run():
        mcp = FastMCP("test")
        register_get_jobs_tool(mcp)
        tool = cast(Any, await mcp.get_tool("get_jobs"))

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
                        error_message="table import failed",
                    )
                ]

        result = await tool.fn(job_ids=["job-1"], bauplan_client=Client())

        assert captured["filter_by_ids"] == ["job-1"]
        assert captured["filter_by_kinds"] is None
        assert result.jobs[0].error_message == "table import failed"
        assert result.jobs[0].kind == "TableImport"

    asyncio.run(run())


def test_get_jobs_accepts_multiple_job_ids():
    async def run():
        mcp = FastMCP("test")
        register_get_jobs_tool(mcp)
        tool = cast(Any, await mcp.get_tool("get_jobs"))

        captured = {}

        class Client:
            def get_jobs(self, **kwargs):
                captured.update(kwargs)
                return []

        await tool.fn(job_ids=["job-1", "job-2"], bauplan_client=Client())

        assert captured["filter_by_ids"] == ["job-1", "job-2"]

    asyncio.run(run())


def test_get_jobs_accepts_multiple_filter_values():
    async def run():
        mcp = FastMCP("test")
        register_get_jobs_tool(mcp)
        tool = cast(Any, await mcp.get_tool("get_jobs"))

        captured = {}

        class Client:
            def get_jobs(self, **kwargs):
                captured.update(kwargs)
                return []

        await tool.fn(
            job_kinds=["run", "query"],
            statuses=["not-started", "running"],
            user_names=["alice", "bob"],
            bauplan_client=Client(),
        )

        assert captured["filter_by_users"] == ["alice", "bob"]
        assert captured["filter_by_kinds"] == ["run", "query"]
        assert captured["filter_by_statuses"] == ["not-started", "running"]

    asyncio.run(run())


def test_run_query_passes_default_and_custom_max_rows_to_sdk():
    async def run():
        mcp = FastMCP("test")
        register_run_query_tool(mcp)
        tool = cast(Any, await mcp.get_tool("run_query"))

        captured = []

        class Client:
            def query(self, **kwargs):
                captured.append(kwargs)
                return pa.table({"answer": [1]})

        await tool.fn(query="select 1", bauplan_client=Client())
        await tool.fn(query="select 1", max_rows=25, bauplan_client=Client())

        assert captured[0]["max_rows"] == 25
        assert captured[1]["max_rows"] == 25

    asyncio.run(run())


def test_run_query_passes_sql_to_sdk_without_local_keyword_filtering():
    async def run():
        mcp = FastMCP("test")
        register_run_query_tool(mcp)
        tool = cast(Any, await mcp.get_tool("run_query"))
        query = "select created_at, replace(name, 'a', 'b') as updated_name from analytics.table"
        captured = {}

        class Client:
            def query(self, **kwargs):
                captured.update(kwargs)
                return pa.table({"created_at": [], "updated_name": []})

        result = await tool.fn(query=query, bauplan_client=Client())

        assert captured["query"] == query
        assert result.metadata is not None
        assert result.metadata.query == query

    asyncio.run(run())


def test_run_query_warns_when_json_output_has_duplicate_column_names():
    async def run():
        mcp = FastMCP("test")
        register_run_query_tool(mcp)
        tool = cast(Any, await mcp.get_tool("run_query"))
        table = pa.Table.from_arrays([pa.array([1]), pa.array([2])], names=["value", "value"])

        class Client:
            def query(self, **kwargs):
                return table

        result = await tool.fn(query="select 1 as value, 2 as value", bauplan_client=Client())

        assert result.data == [{"value": 2}]
        assert result.metadata is not None
        assert result.metadata.column_names == ["value", "value"]
        assert len(result.warnings) == 1
        assert result.warnings[0] == (
            "Duplicate result columns: value. JSON keeps one value per name; use SQL aliases."
        )

    asyncio.run(run())


def test_project_run_delegates_omitted_run_defaults_to_sdk():
    async def run():
        mcp = FastMCP("test")
        register_project_run_tool(mcp)
        tool = cast(Any, await mcp.get_tool("project_run"))

        captured = {}

        class Client(_JobLookupClient):
            def run(self, **kwargs):
                captured.update(kwargs)
                return SimpleNamespace(job_id="job-1", job_status="SUCCESS")

        result = await tool.fn(project_dir="/tmp/project", ref="alice.dev", bauplan_client=Client())

        assert result.job.id == "job-1"
        assert captured["dry_run"] is False
        assert captured["client_timeout"] == 30
        assert captured["detach"] is True
        assert captured["strict"] == "on"

    asyncio.run(run())


def test_project_run_accepts_none_parameter_values():
    async def run():
        mcp = FastMCP("test")
        register_project_run_tool(mcp)
        tool = cast(Any, await mcp.get_tool("project_run"))

        captured = {}

        class Client(_JobLookupClient):
            def run(self, **kwargs):
                captured.update(kwargs)
                return SimpleNamespace(job_id="job-1", job_status="SUCCESS")

        result = await tool.fn(
            project_dir="/tmp/project",
            ref="alice.dev",
            parameters={"optional_value": None},
            bauplan_client=Client(),
        )

        assert result.job.id == "job-1"
        assert captured["parameters"] == {"optional_value": None}

    asyncio.run(run())


def test_code_run_delegates_omitted_run_defaults_to_sdk():
    async def run():
        mcp = FastMCP("test")
        register_code_run_tool(mcp)
        tool = cast(Any, await mcp.get_tool("code_run"))

        captured = {}

        class Client(_JobLookupClient):
            def run(self, **kwargs):
                captured.update(kwargs)
                return SimpleNamespace(job_id="job-1", job_status=None)

        result = await tool.fn(
            project_files={
                "bauplan_project.yml": "project:\n  id: test\n",
                "models/model.py": "def model(): pass\n",
            },
            ref="alice.dev",
            bauplan_client=Client(),
        )

        assert result.job.id == "job-1"
        assert captured["namespace"] is None
        assert captured["dry_run"] is False
        assert captured["client_timeout"] == 30
        assert captured["detach"] is True
        assert captured["strict"] == "on"

    asyncio.run(run())


def test_code_run_passes_strict_argument_to_sdk():
    async def run():
        mcp = FastMCP("test")
        register_code_run_tool(mcp)
        tool = cast(Any, await mcp.get_tool("code_run"))

        captured = {}

        class Client(_JobLookupClient):
            def run(self, **kwargs):
                captured.update(kwargs)
                return SimpleNamespace(job_id="job-1", job_status="SUCCESS")

        result = await tool.fn(
            project_files={
                "bauplan_project.yml": "project:\n  id: test\n",
                "models/model.py": "def model(): pass\n",
            },
            ref="alice.dev",
            namespace="alice",
            dry_run=True,
            client_timeout=10,
            detach=False,
            strict=False,
            bauplan_client=Client(),
        )

        assert result.job.id == "job-1"
        assert captured["namespace"] == "alice"
        assert captured["dry_run"] is True
        assert captured["client_timeout"] == 10
        assert captured["detach"] is False
        assert captured["strict"] == "off"

    asyncio.run(run())


def test_code_run_rejects_paths_outside_temp_project():
    async def run():
        mcp = FastMCP("test")
        register_code_run_tool(mcp)
        tool = cast(Any, await mcp.get_tool("code_run"))

        class Client:
            def run(self, **kwargs):
                raise AssertionError("run should not be called")

        with pytest.raises(ToolError, match="Invalid project file path"):
            await tool.fn(
                project_files={
                    "bauplan_project.yml": "project:\n  id: test\n",
                    "../model.py": "def model(): pass\n",
                },
                ref="alice.dev",
                bauplan_client=Client(),
            )

    asyncio.run(run())


def test_code_run_requires_non_main_ref_for_non_dry_run():
    async def run():
        mcp = FastMCP("test")
        register_code_run_tool(mcp)
        tool = cast(Any, await mcp.get_tool("code_run"))

        class Client:
            def run(self, **kwargs):
                raise AssertionError("run should not be called")

        with pytest.raises(ToolError, match=r"requires an explicit non-main branch|cannot target"):
            await tool.fn(
                project_files={
                    "bauplan_project.yml": "project:\n  id: test\n",
                    "models/model.py": "def model(): pass\n",
                },
                ref="main",
                bauplan_client=Client(),
            )

    asyncio.run(run())


def test_code_run_requires_ref_even_for_dry_run():
    async def run():
        mcp = FastMCP("test")
        register_code_run_tool(mcp)
        tool = cast(Any, await mcp.get_tool("code_run"))

        class Client:
            def run(self, **kwargs):
                raise AssertionError("run should not be called")

        with pytest.raises(TypeError, match=r"missing .*ref"):
            await tool.fn(
                project_files={
                    "bauplan_project.yml": "project:\n  id: test\n",
                    "models/model.py": "def model(): pass\n",
                },
                dry_run=True,
                bauplan_client=Client(),
            )

    asyncio.run(run())


def test_get_jobs_uses_requested_sdk_job_status():
    async def run():
        mcp = FastMCP("test")
        register_get_jobs_tool(mcp)
        tool = cast(Any, await mcp.get_tool("get_jobs"))

        captured = {}

        class Client:
            def get_jobs(self, **kwargs):
                captured.update(kwargs)
                return []

        await tool.fn(statuses=["not-started"], bauplan_client=Client())

        assert captured["filter_by_statuses"] == ["not-started"]

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


def test_code_run_passes_optional_run_arguments_to_sdk():
    async def run():
        mcp = FastMCP("test")
        register_code_run_tool(mcp)
        tool = cast(Any, await mcp.get_tool("code_run"))
        captured = {}

        class Client(_JobLookupClient):
            def run(self, **kwargs):
                captured.update(kwargs)
                return SimpleNamespace(job_id="job-1", job_status="SUCCESS")

        result = await tool.fn(
            project_files={
                "bauplan_project.yaml": "project:\n  id: test\n",
                "models/model.py": "def model(): pass\n",
            },
            ref="alice.dev",
            namespace="alice",
            dry_run=True,
            strict=False,
            bauplan_client=Client(),
        )

        assert captured["ref"] == "alice.dev"
        assert captured["namespace"] == "alice"
        assert captured["dry_run"] is True
        assert captured["strict"] == "off"
        assert captured["client_timeout"] == 30
        assert captured["detach"] is True
        assert result.job.id == "job-1"

    asyncio.run(run())


def test_get_job_uses_snapshot_dict_and_preserves_project_file_paths():
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
                    error_message=None,
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
                    sql_query="select 1",
                )

        result = await tool.fn(job_id="job-1", bauplan_client=Client())

        assert result.job.project_yml == "project"
        assert result.job.project_files == {
            "models/model.py": "model code",
            "queries/query.sql": "select 1",
        }
        assert result.job.logs == "hello"
        assert result.job.error_message is None
        assert result.job.sql_query == "select 1"

    asyncio.run(run())


def test_get_job_supports_yaml_project_config():
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
                    error_message=None,
                )

            def get_job_context(self, job_id, *, include_snapshot, include_logs):
                return SimpleNamespace(
                    logs=[],
                    snapshot_dict={
                        "bauplan_project.yaml": "project",
                        "models/model.py": "model code",
                    },
                    ref="main",
                    tx_ref="alice.tx",
                )

        result = await tool.fn(job_id="job-1", bauplan_client=Client())

        assert result.job.project_yml == "project"
        assert result.job.project_files == {"models/model.py": "model code"}

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
                    error_message="failed before context",
                )

            def get_job_context(self, job_id, *, include_snapshot, include_logs):
                raise RuntimeError("context unavailable")

        result = await tool.fn(job_id="job-1", bauplan_client=Client())

        assert result.job.id == "job-1"
        assert result.job.error_message == "failed before context"
        assert result.job.logs is None
        assert result.job.project_yml is None
        assert result.job.project_files is None
        assert result.job.sql_query is None

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
