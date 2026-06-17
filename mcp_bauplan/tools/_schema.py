from typing import Any, Literal, cast

EXACT_OR_REGEX_FILTER_DESCRIPTION = (
    "Use plain text for a normal name search, or a regex that starts with ^ and ends with $."
)

# TODO: Replace these Literals with SDK enum-like types when they support Pydantic schemas:
# https://github.com/BauplanLabs/bauplan/issues/333
JobKindFilter = Literal[
    "run",
    "query",
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


def field_to_dict(field: Any) -> dict[str, Any]:
    return {
        "id": field.id,
        "name": field.name,
        "required": field.required,
        "type": field.type,
    }


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
