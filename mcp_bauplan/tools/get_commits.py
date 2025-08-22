from fastmcp import FastMCP, Context
from fastmcp.exceptions import ToolError

from pydantic import BaseModel
from typing import List, Optional

from .create_client import create_bauplan_client


class AuthorInfo(BaseModel):
    username: Optional[str] = None
    name: Optional[str] = None
    email: Optional[str] = None


class CommitInfo(BaseModel):
    hash: str
    message: str
    author: AuthorInfo
    authored_date: str
    parent_hashes: List[str]


class CommitsOut(BaseModel):
    commits: List[CommitInfo]
    total_count: int


def register_get_commits_tool(mcp: FastMCP) -> None:
    @mcp.tool(
        name="get_commits",
        description="Retrieve commit history for a specified branch in the user's Bauplan data catalog as a list, with optional filters including date range (ISO format: YYYY-MM-DD) and limit (integer).",
    )
    async def get_commits(
        ref: str,
        message_filter: Optional[str] = None,
        author_username: Optional[str] = None,
        author_email: Optional[str] = None,
        date_start: Optional[str] = None,
        date_end: Optional[str] = None,
        limit: Optional[int] = 10,
        api_key: Optional[str] = None,
        ctx: Context = None,
    ) -> CommitsOut:
        """
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
            api_key: The Bauplan API key for authentication.

        Returns:
            CommitsOut: Object containing list of commits and total count
        """
        # Validate required parameters
        # if not ref:
        #    if ctx:
        #        await ctx.error("'ref' parameter is required for get_commits")
        #    return CommitsOut(
        #       commits=[],
        #       total_count=0
        #    )

        # Check if ref needs username prefix (only if it looks like a branch name)
        # Skip if it's a commit hash (40 hex chars) or a tag
        # is_commit_hash = re.match(r'^[a-fA-F0-9]{40}$', ref) is not None
        #
        # if not is_commit_hash:
        #    # Check if ref already contains a username pattern
        #    if '.' in ref and not ref.startswith('.') and not ref.endswith('.'):
        #        # ref appears to already have username format or is a tag
        #        if ctx:
        #            await ctx.info(f"Ref already includes username prefix or is a tag: '{ref}'")
        #    else:
        #        # Add the configured user's prefix (assuming it's a branch)
        #        ref = config.user + '.' + ref
        #        if ctx:
        #            await ctx.info(f"Added username prefix to ref: '{ref}'")
        # else:
        #    if ctx:
        #        await ctx.info(f"Ref is a commit hash: '{ref}'")

        try:
            # Create a fresh Bauplan client
            bauplan_client = create_bauplan_client(api_key)
            # Build filter parameters
            kwargs = {"ref": ref}

            if message_filter:
                kwargs["filter_by_message"] = message_filter

            if author_username:
                kwargs["filter_by_author_username"] = author_username

            if author_email:
                kwargs["filter_by_author_email"] = author_email

            if date_start:
                kwargs["filter_by_authored_date_start_at"] = date_start

            if date_end:
                kwargs["filter_by_authored_date_end_at"] = date_end

            # Always set a limit to avoid timeout
            kwargs["limit"] = limit if limit else 10

            # Get commits from Bauplan
            try:
                response = bauplan_client.get_commits(**kwargs)
            except Exception as e:
                if ctx:
                    await ctx.error(f"Error calling get_commits API: {str(e)}")
                # Try with just ref parameter if other filters caused issues
                response = bauplan_client.get_commits(ref=ref, limit=kwargs["limit"])

            # Convert response to our model format
            commits_list = []

            # Handle the response - it may be an iterator or a list
            commit_count = 0
            try:
                for commit in response:
                    try:
                        # Extract author information safely
                        author = AuthorInfo(
                            username=getattr(commit.author, "username", None)
                            if hasattr(commit, "author")
                            else None,
                            name=getattr(commit.author, "name", None)
                            if hasattr(commit, "author")
                            else None,
                            email=getattr(commit.author, "email", None)
                            if hasattr(commit, "author")
                            else None,
                        )

                        # Create commit info
                        # Handle both ref and hash attributes for commit ID
                        commit_hash = getattr(
                            commit, "hash", getattr(commit, "ref", str(commit))
                        )

                        commit_info = CommitInfo(
                            hash=str(commit_hash),  # Ensure it's a string
                            message=getattr(commit, "message", ""),
                            author=author,
                            authored_date=str(getattr(commit, "authored_date", "")),
                            parent_hashes=getattr(commit, "parent_hashes", []),
                        )

                        commits_list.append(commit_info)
                        commit_count += 1

                        # If we have a limit and reached it, break
                        if commit_count >= kwargs["limit"]:
                            break
                    except Exception as e:
                        if ctx:
                            await ctx.debug(f"Error processing commit: {str(e)}")
                        continue

            except Exception as e:
                if ctx:
                    await ctx.error(f"Error iterating commits: {str(e)}")

            return CommitsOut(commits=commits_list, total_count=len(commits_list))

        except Exception as err:
            raise ToolError(f"Error executing get_commits: {err}")
