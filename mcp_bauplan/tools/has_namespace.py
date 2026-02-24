"""
Check if a namespace exists.
"""

import asyncio
import logging

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel

from .create_client import get_bauplan_client

logger = logging.getLogger(__name__)


class NamespaceExists(BaseModel):
    namespace_name: str
    ref_name: str
    exists: bool
    message: str


def register_has_namespace_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="has_namespace")
    async def has_namespace(
        namespace: str,
        ref: str,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> NamespaceExists:
        """
        Check if a specified namespace exists in a given branch of the user's Bauplan data catalog using a namespace name and branch name.

        Args:
            namespace: Name of the namespace to check for existence.
            ref: The ref, branch name or tag name to check the namespace on.

        Returns:
            NamespaceExists: Object indicating whether the namespace exists with details
        """
        try:
            if ctx:
                await ctx.info(f"Checking if namespace '{namespace}' exists in ref '{ref}'")

            # Call has_namespace function
            exists = await asyncio.to_thread(
                bauplan_client.has_namespace,
                namespace=namespace,
                ref=ref,
            )

            # Log the result
            logger.info(f"Namespace '{namespace}' exists in ref '{ref}': {exists}")

            return NamespaceExists(
                namespace_name=namespace,
                ref_name=ref,
                exists=exists,
                message=f"Namespace '{namespace}' {'exists' if exists else 'does not exist'} in ref '{ref}'",
            )

        except Exception as e:
            logger.error(f"Error checking namespace {namespace} in ref {ref}: {e!s}")
            raise ToolError(f"Failed to check namespace {namespace} in ref {ref}: {e!s}") from e
