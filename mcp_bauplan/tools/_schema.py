from typing import Annotated, Literal, cast

from mcp.types import ToolAnnotations
from pydantic import BaseModel, Field

EXACT_OR_REGEX_FILTER_DESCRIPTION = (
    "Use plain text for a normal name search, or a regex that starts with ^ and ends with $."
)

TOOL_TAG_LOCAL = "local"
TOOL_TAG_REMOTE = "remote"
TOOL_TAG_READ = "read"
TOOL_TAG_WRITE = "write"
TOOL_TAG_DESTRUCTIVE = "destructive"
TOOL_TAGS = {
    TOOL_TAG_LOCAL,
    TOOL_TAG_REMOTE,
    TOOL_TAG_READ,
    TOOL_TAG_WRITE,
    TOOL_TAG_DESTRUCTIVE,
}


def remote_read_tags() -> set[str]:
    return {TOOL_TAG_REMOTE, TOOL_TAG_READ}


def remote_write_tags(*, destructive: bool = False) -> set[str]:
    tags = {TOOL_TAG_REMOTE, TOOL_TAG_WRITE}
    if destructive:
        tags.add(TOOL_TAG_DESTRUCTIVE)
    return tags


def local_write_tags(*, destructive: bool = False) -> set[str]:
    tags = {TOOL_TAG_LOCAL, TOOL_TAG_WRITE}
    if destructive:
        tags.add(TOOL_TAG_DESTRUCTIVE)
    return tags


def read_only_tool_annotations(title: str, *, open_world: bool = True) -> ToolAnnotations:
    return ToolAnnotations(
        title=title,
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=open_world,
    )


def mutating_tool_annotations(
    title: str,
    *,
    destructive: bool = False,
    open_world: bool = True,
) -> ToolAnnotations:
    return ToolAnnotations(
        title=title,
        readOnlyHint=False,
        destructiveHint=destructive,
        idempotentHint=False,
        openWorldHint=open_world,
    )


# TODO: Replace these Literals with SDK enum-like types when they support Pydantic schemas:
# https://github.com/BauplanLabs/bauplan/issues/333
JobKindFilter = Literal[
    "run",
    "query",
    "external-table-create",
    "import-plan-create",
    "import-plan-apply",
    "table-plan-create",
    "table-plan-create-apply",
    "table-import",
]

JobStatusFilter = Literal[
    "not-started",
    "running",
    "complete",
    "abort",
    "fail",
    "other",
    "unspecified",
]

JobKindOut = Literal[
    "Unknown",
    "Run",
    "Query",
    "ExternalTableCreate",
    "ImportPlanCreate",
    "ImportPlanApply",
    "TablePlanCreate",
    "TablePlanCreateApply",
    "TableImport",
]

JobStatusOut = Literal[
    "Unspecified",
    "Not Started",
    "Running",
    "Complete",
    "Abort",
    "Fail",
    "Other",
]

_JOB_STATUS_OUT_BY_NORMALIZED_VALUE: dict[str, JobStatusOut] = {
    "unspecified": "Unspecified",
    "not started": "Not Started",
    "running": "Running",
    "complete": "Complete",
    "abort": "Abort",
    "fail": "Fail",
    "other": "Other",
}


class TableFieldInfo(BaseModel):
    id: Annotated[int, Field(description="Field ID.")]
    name: Annotated[str, Field(description="Field name.")]
    required: Annotated[bool, Field(description="Whether the field is required.")]
    type: Annotated[str, Field(description="Field type.")]
    doc: Annotated[
        str | None,
        Field(description="Field documentation."),
    ] = None


def job_kind_out(kind: object) -> JobKindOut:
    return cast(JobKindOut, str(kind))


def job_status_out(status: object) -> JobStatusOut:
    candidates = (
        getattr(status, "value", None),
        getattr(status, "name", None),
        str(status),
    )
    for candidate in candidates:
        if candidate is None:
            continue
        normalized = str(candidate).replace("_", " ").replace("-", " ").strip().lower()
        if normalized in _JOB_STATUS_OUT_BY_NORMALIZED_VALUE:
            return _JOB_STATUS_OUT_BY_NORMALIZED_VALUE[normalized]
    return cast(JobStatusOut, str(status))
