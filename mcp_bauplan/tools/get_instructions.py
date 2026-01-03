from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError

from pydantic import BaseModel
from .use_case_to_prompts import USE_CASE_TO_PROMPT

from .create_client import with_bauplan_client
import bauplan


class Prompt(BaseModel):
    prompt: str


def register_get_instructions_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="get_instructions", exclude_args=["bauplan_client"])
    @with_bauplan_client
    async def get_instructions(
        # can only be one of the keys in USE_CASE_TO_PROMPT
        use_case: str,
        ctx: Context = None,
        bauplan_client: bauplan.Client = None,
    ) -> Prompt:
        """
        Get detailed instructions for specific Bauplan use cases to be used to solve the task,
        possibly suggesting further tool usage.

        Args:
            use_case: The use case to get instructions for. Must be one of:
                - 'pipeline': Instructions for creating and managing data pipelines
                - 'data': Instructions for reading data and metadata, including data lineage information
                - 'repair': Instructions for repairing failed pipelines
                - 'wap': Instructions for data ingestion using the Write-Audit-Publish (WAP) pattern
                - 'test': Instructions for creating and managing data expectations and quality tests
                - 'sdk': Instructions for explaining Bauplan SDK methods and verifying their syntax and usage

        Returns:
            Prompt: Object containing the detailed instructions for the specified use case, to be used by the
            caller to further plan which tools to use and how to use them.
        """

        try:
            if ctx:
                await ctx.info(f"Getting instructions for use case '{use_case}'")

            supported_use_cases = [k.lower() for k in USE_CASE_TO_PROMPT.keys()]
            assert use_case.lower() in supported_use_cases, (
                f"Invalid use_case '{use_case}', must be one of {supported_use_cases}"
            )
            instructions = USE_CASE_TO_PROMPT[use_case.lower()]

            return Prompt(prompt=instructions)

        except Exception as err:
            raise ToolError(f"Error executing get_instructions: {err}")
