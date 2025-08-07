from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError

from pydantic import BaseModel
from typing import List, Optional

from .create_client import with_fresh_client

class NamespaceInfo(BaseModel):
    name: str

class NamespacesOut(BaseModel):
    namespaces: List[NamespaceInfo]
    total_count: int

def register_get_namespaces_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="get_namespaces", 
        description="Retrieve namespaces for a branch from the user's Bauplan data catalog as a list. Use 'limit' (integer) to reduce response size."    )
    @with_fresh_client
    async def get_namespaces(
        ref: str,
        bauplan_client,
        namespace: Optional[str] = None,
        limit: Optional[int] = 10,
        ctx: Context = None
    ) -> NamespacesOut:
        """
        Get the namespaces of a branch using optional filters.
        
        Args:
            ref: branch or commit hash to get namespaces from. Can be either a hash that starts with "@" and
                has 64 additional characters or a branch name, that is a mnemonic reference to the last commit that follows the "username.name" format.
            namespace: Optional filter for namespace names (substring match)
            limit: Optional maximum number of namespaces to return (default: 50)
            
        Returns:
            NamespacesOut: Object containing list of namespaces and total count
        """
            
        try:
            
            # Debug logging
            if ctx:
                await ctx.debug(f"Calling get_namespaces with ref='{ref}', filter_by_name='{namespace}', limit={limit}")
            
            # Get namespaces with filters
            namespaces_list = []
            namespace_count = 0
            
            try:
                # Call with direct parameters instead of kwargs
                for ns in bauplan_client.get_namespaces(
                    ref=ref,
                    filter_by_name=namespace,
                    limit=limit
                ):
                    try:
                        # Extract namespace information
                        namespace_info = NamespaceInfo(
                            name=getattr(ns, 'name', str(ns))
                        )
                        namespaces_list.append(namespace_info)
                        namespace_count += 1
                        
                        # If we have a limit and reached it, break
                        if limit and namespace_count >= limit:
                            break
                            
                    except Exception as e:
                        if ctx:
                            await ctx.debug(f"Error processing namespace: {str(e)}")
                        continue
                        
            except Exception as e:
                if ctx:
                    await ctx.error(f"Error iterating namespaces: {str(e)}")
            
            return NamespacesOut(
                namespaces=namespaces_list,
                total_count=len(namespaces_list)
            )
            
        except Exception as err:
            raise ToolError(f"Error executing get_table: {err}")