import asyncio

import bauplan
from fastmcp import Context, FastMCP
from fastmcp.dependencies import Depends
from fastmcp.exceptions import ToolError
from pydantic import BaseModel

from .create_client import get_bauplan_client


class AuthorInfo(BaseModel):
    username: str | None = None
    name: str | None = None
    email: str | None = None


class CommitInfo(BaseModel):
    hash: str
    message: str
    author: AuthorInfo
    authored_date: str
    parent_hashes: list[str]


class CommitsOut(BaseModel):
    commits: list[CommitInfo]


def register_get_commits_tool(mcp: FastMCP) -> None:
    @mcp.tool(name="get_commits")
    async def get_commits(
        ref: str,
        message_filter: str | None = None,
        author_username: str | None = None,
        author_email: str | None = None,
        date_start: str | None = None,
        date_end: str | None = None,
        limit: int = 10,
        ctx: Context | None = None,
        bauplan_client: bauplan.Client = Depends(get_bauplan_client),
    ) -> CommitsOut:
        """
        Retrieve commit history for a specified branch in the user's Bauplan data catalog as a list, with optional filters including date range (ISO format: YYYY-MM-DD) and limit (integer).
        Retrieve commit history from a Bauplan branch.

        Args:
            ref: branch or commit hash to get commits from. Can be either a hash that starts with "@" and
                has 64 additional characters or a branch name, that is a mnemonic reference to the last commit that follows the "username.name" format.
            message_filter: Optional filter for commit messages (substring match)
            author_username: Optional filter by author's username
            author_email: Optional filter by author's email
            date_start: Optional start date for filtering (ISO format: YYYY-MM-DD)
            date_end: Optional end date for filtering (ISO format: YYYY-MM-DD)
            limit: Maximum number of commits to return (default: 10)

        Returns:
            CommitsOut: Object containing list of commits
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
                        limit=limit or 10,
                    )
                )
            )

            # Convert response to our model format
            commits_list = []

            try:
                for commit in response:
                    try:
                        # Extract author information safely
                        author = AuthorInfo(
                            username=getattr(commit.author, "username", None)
                            if hasattr(commit, "author")
                            else None,
                            name=getattr(commit.author, "name", None) if hasattr(commit, "author") else None,
                            email=getattr(commit.author, "email", None)
                            if hasattr(commit, "author")
                            else None,
                        )

                        # Create commit info
                        # Handle both ref and hash attributes for commit ID
                        commit_hash = getattr(commit, "hash", getattr(commit, "ref", str(commit)))

                        commit_info = CommitInfo(
                            hash=str(commit_hash),  # Ensure it's a string
                            message=getattr(commit, "message", ""),
                            author=author,
                            authored_date=str(getattr(commit, "authored_date", "")),
                            parent_hashes=getattr(commit, "parent_hashes", []),
                        )

                        commits_list.append(commit_info)
                    except Exception as e:
                        if ctx:
                            await ctx.debug(f"Error processing commit: {e!s}")
                        continue

            except Exception as e:
                if ctx:
                    await ctx.error(f"Error iterating commits: {e!s}")

            return CommitsOut(commits=commits_list)

        except Exception as e:
            raise ToolError(f"Error executing get_commits: {e}") from e
