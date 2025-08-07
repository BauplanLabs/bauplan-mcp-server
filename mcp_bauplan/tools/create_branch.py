from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError

from pydantic import BaseModel
from typing import Optional

from .create_client import get_bauplan_client

class BranchCreated(BaseModel):
    created: bool
    name: Optional[str] = None
    hash: Optional[str] = None
    message: Optional[str] = None

def register_create_branch_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="create_branch", 
        description="Create a new branch in the user's Bauplan data catalog using a branch name, returning a confirmation."    )
    async def create_branch(
        branch: str,
        from_ref: str,
        api_key: Optional[str] = None,
        ctx: Context = None
    ) -> BranchCreated:
        """
        Create a new branch in the user's Bauplan catalog.
        
        Args:
            branch: Name of the new branch to create. Mustllow the format <username.branch_name>. 
            from_ref: Reference (branch/commit) to create the branch from. Can be either a branch name or a hash that starts with "@" and
            has 64 additional characters.
            
        Returns:
            BranchCreated: Object indicating success/failure with branch details
        """
        try:
            bauplan_client = get_bauplan_client(api_key)
                
            if ctx:
                await ctx.info(f"Creating branch '{branch}' from ref '{from_ref}'")
            
            # Create the branch
            result = bauplan_client.create_branch(branch=branch, from_ref=from_ref)
            return BranchCreated(
                    created=True,
                    name=result.name,
                    hash=result.hash,
                    message=f"Successfully created branch: {result.name}. Commit: {result.hash}'"
                )
                 
        except Exception as err:
            raise ToolError(f"Error creating branch: {err}")
