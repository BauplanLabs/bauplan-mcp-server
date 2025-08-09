"""
Check if a tag exists.
"""

from fastmcp import FastMCP
from pydantic import BaseModel
from typing import Optional
from fastmcp.exceptions import ToolError

from .create_client import create_bauplan_client
import logging
from fastmcp import Context

logger = logging.getLogger(__name__)


class TagExists(BaseModel):
    tag_name: str
    exists: bool
    message: str


def register_has_tag_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="has_tag",
        description="Check if a specified tag exists in a given branch of the user's Bauplan data catalog using a tag name and branch name.",
    )
    async def has_tag(
        tag: str, api_key: Optional[str] = None, ctx: Context = None
    ) -> TagExists:
        """
        Check if a specific tag exists in a branch.

        Args:
            tag: Name of the tag to check for existence.
            api_key: The Bauplan API key for authentication.

        Returns:
            TagExists: Object indicating whether the tag exists with details
        """
        try:
            # Create a fresh Bauplan client
            bauplan_client = create_bauplan_client(api_key)
            if ctx:
                await ctx.info(f"Checking if tag '{tag}' exists")

            # Call has_tag function
            exists = bauplan_client.has_tag(tag=tag)

            # Log the result
            logger.info(f"Tag '{tag}' exists: {exists}")

            return TagExists(
                tag_name=tag,
                exists=exists,
                message=f"Tag '{tag}' {'exists' if exists else 'does not exist'}",
            )

        except Exception as e:
            logger.error(f"Error checking tag {tag}: {str(e)}")
            raise ToolError(f"Failed to check tag {tag}: {str(e)}")
