import asyncio
from typing import Annotated

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel, Field

from .create_client import get_bauplan_client


class AuthorInfo(BaseModel):
    username: Annotated[
        str | None,
        Field(
            description="Author username when available.",
        ),
    ] = None
    name: Annotated[
        str | None,
        Field(
            description="Author display name when available.",
        ),
    ] = None
    email: Annotated[
        str | None,
        Field(
            description="Author email address when available.",
        ),
    ] = None


class CommitInfo(BaseModel):
    hash: Annotated[
        str,
        Field(
            description="Commit hash.",
        ),
    ]
    message: Annotated[
        str,
        Field(
            description="Commit message.",
        ),
    ]
    author: Annotated[
        AuthorInfo,
        Field(
            description="First author of this commit, when available.",
        ),
    ]
    authored_date: Annotated[
        str,
        Field(
            description="Authored date as returned by the SDK.",
        ),
    ]
    parent_hashes: Annotated[
        list[str],
        Field(
            description="Parent commit hashes.",
        ),
    ]
    properties: Annotated[
        dict[str, str],
        Field(
            description="Custom properties attached to the commit.",
        ),
    ]


class CommitsOut(BaseModel):
    commits: Annotated[
        list[CommitInfo],
        Field(
            description="Commits returned for the requested ref and filters.",
        ),
    ]


def register_get_commits_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="get_commits")
    async def get_commits(
        ref: Annotated[
            str,
            Field(
                description="Branch, tag, or commit ref to read commits from.",
            ),
        ],
        message_filter: Annotated[
            str | None,
            Field(
                description="Optional commit message filter.",
            ),
        ] = None,
        author_username: Annotated[
            str | None,
            Field(
                description="Optional author username filter.",
            ),
        ] = None,
        author_email: Annotated[
            str | None,
            Field(
                description="Optional author email filter.",
            ),
        ] = None,
        date_start: Annotated[
            str | None,
            Field(
                description="Optional authored date lower bound in YYYY-MM-DD format.",
            ),
        ] = None,
        date_end: Annotated[
            str | None,
            Field(
                description="Optional authored date upper bound in YYYY-MM-DD format.",
            ),
        ] = None,
        limit: Annotated[
            int,
            Field(
                description="Maximum number of commits to return. Defaults to 25.",
                ge=1,
                le=250,
            ),
        ] = 25,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> CommitsOut:
        """
        List commits for a branch, tag, or commit ref.
        Use this to inspect catalog history, find a commit hash, or choose a stable ref for comparison or rollback.
        """

        try:
            response = await asyncio.to_thread(
                lambda: list(
                    bauplan_client.get_commits(
                        ref=ref,
                        filter_by_message=message_filter or None,
                        filter_by_author_username=author_username or None,
                        filter_by_author_email=author_email or None,
                        filter_by_authored_date_start_at=date_start or None,
                        filter_by_authored_date_end_at=date_end or None,
                        limit=limit,
                    )
                )
            )

            # Convert response to our model format
            commits_list = []

            for commit in response:
                author_obj = getattr(commit, "author", None)
                author = AuthorInfo(
                    username=getattr(author_obj, "username", None),
                    name=getattr(author_obj, "name", None),
                    email=getattr(author_obj, "email", None),
                )
                commit_hash = getattr(commit, "hash", getattr(commit, "ref", str(commit)))
                commit_info = CommitInfo(
                    hash=str(commit_hash),
                    message=getattr(commit, "message", ""),
                    author=author,
                    authored_date=str(getattr(commit, "authored_date", "")),
                    parent_hashes=getattr(commit, "parent_hashes", []),
                    properties=dict(getattr(commit, "properties", {}) or {}),
                )
                commits_list.append(commit_info)

            return CommitsOut(commits=commits_list)

        except Exception as e:
            raise ToolError(f"Error executing get_commits: {e}") from e
