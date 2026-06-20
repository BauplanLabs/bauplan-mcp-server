from typing import Annotated

from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError
from pydantic import BaseModel, Field

from ._schema import read_only_tool_annotations, remote_read_tags
from .use_case_to_prompts import USE_CASE_TO_PROMPT


class Prompt(BaseModel):
    prompt: Annotated[
        str,
        Field(
            description="Instruction text for the requested Bauplan use case.",
        ),
    ]


def register_get_instructions_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="get_instructions",
        annotations=read_only_tool_annotations("Get instructions", open_world=False),
        tags=remote_read_tags(),
    )
    async def get_instructions(
        use_case: Annotated[
            str,
            Field(
                description="Instruction set to retrieve, such as pipeline, data, repair, wap, test, or sdk.",
            ),
        ],
        ctx: Context | None = None,
    ) -> Prompt:
        """
        Get task-oriented Bauplan usage guidance for a specific workflow.
        Use this when the user asks how to approach a task and the model needs a short playbook before choosing tools.
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

        except Exception as e:
            raise ToolError(f"Error executing get_instructions: {e}") from e
