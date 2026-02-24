import asyncio

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel

from .create_client import get_bauplan_client


class BranchInfo(BaseModel):
    name: str
    hash: str


class BranchesOut(BaseModel):
    branches: list[BranchInfo]
    total_count: int


def register_get_branches_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="get_branches")
    async def get_branches(
        name: str | None = None,
        user: str | None = None,
        limit: int | None = 10,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> BranchesOut:
        """
        Retrieve branches from the user's Bauplan data catalog as a list, with optional user and limit (integer) filters to reduce response size.
        Get branches from the Bauplan catalog with optional filtering.
        NOTE: This can return a large response. Always use limit parameter.

        Args:
            name: Optional filter to get branches by name (substring match)
            user: Optional filter to get branches by user
            limit: Maximum number of branches to return (needs to be an integer, default 10)

        Returns:
            BranchesOut: Object containing list of branches with their names and hashes
        """
        try:
            limit = limit or 10

            # Get branches with filters
            branches_list = []

            all_branches = await asyncio.to_thread(
                lambda: list(
                    bauplan_client.get_branches(
                        name=name or None,
                        user=user or None,
                        limit=limit,
                    )
                )
            )
            for branch in all_branches:
                if branch.hash is None:
                    raise ToolError(f"Branch '{branch.name}' does not have a valid hash.")

                branch_info = BranchInfo(name=branch.name, hash=branch.hash)
                branches_list.append(branch_info)

                # If we have a limit and reached it, break
                if limit and len(branches_list) >= limit:
                    break

            return BranchesOut(branches=branches_list, total_count=len(branches_list))

        except Exception as e:
            raise ToolError(f"Error executing get_branches: {e}") from e
