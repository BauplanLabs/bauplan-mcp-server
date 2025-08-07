from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError

from pydantic import BaseModel
from typing import Optional

from .create_client import with_fresh_client

class TagDeleted(BaseModel):
    deleted: bool
    tag: str
    message: Optional[str] = None

def register_delete_tag_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="delete_tag", 
        description="Delete a specified tag from a given branch in the user's Bauplan data catalog using a tag name and branch name."    )
    @with_fresh_client
    async def delete_tag(
        tag: str,
        bauplan_client,
        ctx: Context = None
    ) -> TagDeleted:
        """
        Delete a tag from a specific branch of the user's Bauplan catalog.
        
        Args:
            tag: Name of the tag to delete.
            
        Returns:
            TagDeleted: Object indicating success/failure of the deletion
        """         
        try:
            
            if ctx:
                await ctx.info(f"Deleting tag '{tag}'")
            
            # Delete the tag
            result = bauplan_client.delete_tag(tag=tag)
            
            return TagDeleted(
                deleted=True,
                tag=tag,
                message=f"Successfully deleted tag '{tag}'"
            )              
         
        except Exception as err:
            raise ToolError(f"Error executing delete_tag: {err}")