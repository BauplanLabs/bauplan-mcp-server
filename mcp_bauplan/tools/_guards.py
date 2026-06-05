from fastmcp.exceptions import ToolError


def require_writable_branch(branch: str | None, operation: str) -> str:
    branch_name = branch.strip() if branch else ""
    if not branch_name:
        raise ToolError(f"{operation} requires an explicit non-main branch.")
    if branch_name.lower() == "main":
        raise ToolError(f"{operation} cannot target the main branch.")
    return branch_name


def require_truthy_result(result: object, operation: str) -> None:
    if not result:
        raise ToolError(f"{operation} did not complete successfully.")
