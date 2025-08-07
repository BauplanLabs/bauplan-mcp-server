from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError

from pydantic import BaseModel
from typing import List, Optional

from .create_client import with_fresh_client

class BranchInfo(BaseModel):
    name: str
    hash: str

class BranchesOut(BaseModel):
    branches: List[BranchInfo]
    total_count: int

def register_get_branches_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="get_branches", 
        description="Retrieve branches from the user's Bauplan data catalog as a list, with optional user and limit (integer) filters to reduce response size."    
)
    @with_fresh_client
    async def get_branches(
        bauplan_client,
        name: Optional[str] = None,
        user: Optional[str] = None,
        limit: Optional[int] = 10,
        ctx: Context = None
    ) -> BranchesOut:
        """
        Get branches from the Bauplan catalog with optional filtering.
        NOTE: This can return a large response. Always use limit parameter.
        
        Args:
            name: Optional filter to get branches by name (substring match)
            user: Optional filter to get branches by user
            limit: Maximum number of branches to return
            
        Returns:
            BranchesOut: Object containing list of branches with their names and hashes
        """
        try:
            
            # Build kwargs for the API call
            kwargs = {}
            if name:
                kwargs['name'] = name
            if user:
                kwargs['user'] = user
            if limit:
                kwargs['limit'] = limit
            
            # Get branches with filters
            branches_list = []
            branch_count = 0
            
            for branch in bauplan_client.get_branches(**kwargs):
                branch_info = BranchInfo(
                    name=branch.name,
                    hash=branch.hash
                )
                branches_list.append(branch_info)
                branch_count += 1
                
                # If we have a limit and reached it, break
                if limit and branch_count >= limit:
                    break
            
            return BranchesOut(
                branches=branches_list,
                total_count=len(branches_list)
            )
            
        except Exception as err:
            raise ToolError(f"Error executing get_table: {err}")
