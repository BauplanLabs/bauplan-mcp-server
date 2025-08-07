"""
Check if a namespace exists.
"""

from fastmcp import FastMCP
from pydantic import BaseModel
from typing import Optional
from fastmcp.exceptions import ToolError

from .create_client import get_bauplan_client
import logging
from fastmcp import Context

logger = logging.getLogger(__name__)

class NamespaceExists(BaseModel):
    namespace_name: str
    branch_name: str
    exists: bool
    message: str

def register_has_namespace_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="has_namespace", 
        description="Check if a specified namespace exists in a given branch of the user's Bauplan data catalog using a namespace name and branch name."
    )
    async def has_namespace(
        namespace: str,
        branch: str,
        api_key: Optional[str] = None,
        ctx: Context = None
    ) -> NamespaceExists:
        """
        Check if a specific namespace exists in a branch.
        
        Args:
            namespace: Name of the namespace to check for existence.
            branch: Branch name where to check for the namespace.
            
        Returns:
            NamespaceExists: Object indicating whether the namespace exists with details
        """
        try:
            bauplan_client = get_bauplan_client(api_key)
            
            if ctx:
                await ctx.info(f"Checking if namespace '{namespace}' exists in branch '{branch}'")
            
            # Call has_namespace function
            exists = bauplan_client.has_namespace(namespace=namespace, branch=branch)
            
            # Log the result
            logger.info(f"Namespace '{namespace}' exists in branch '{branch}': {exists}")
            
            return NamespaceExists(
                namespace_name=namespace,
                branch_name=branch,
                exists=exists,
                message=f"Namespace '{namespace}' {'exists' if exists else 'does not exist'} in branch '{branch}'"
            )
            
        except Exception as e:
            logger.error(f"Error checking namespace {namespace} in branch {branch}: {str(e)}")
            raise ToolError(f"Failed to check namespace {namespace} in branch {branch}: {str(e)}")