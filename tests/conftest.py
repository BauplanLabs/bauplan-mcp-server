"""
Pytest configuration and fixtures for Claude Code integration tests.
"""

import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest


@dataclass
class ClaudeResult:
    """Result from a Claude Code CLI invocation."""

    returncode: int
    stdout: str
    stderr: str
    stream_messages: list[dict]
    result: dict[str, Any] | None

    @property
    def succeeded(self) -> bool:
        """Check if the run succeeded (or hit max turns without error)."""
        if self.result:
            subtype = self.result.get("subtype", "")
            # Both success and max_turns are acceptable outcomes
            return subtype in ("success", "error_max_turns")
        return self.returncode == 0

    @property
    def tool_calls(self) -> list[dict]:
        """Extract all tool calls from the conversation."""
        calls = []
        for event in self.stream_messages:
            if event.get("type") == "assistant":
                msg = event.get("message", {})
                for content in msg.get("content", []):
                    if content.get("type") == "tool_use":
                        calls.append(
                            {
                                "name": content.get("name"),
                                "input": content.get("input", {}),
                            }
                        )
        return calls

    def skill_was_invoked(self, skill_name: str) -> bool:
        """Check if a specific skill was invoked during the session."""
        for call in self.tool_calls:
            # Skills are invoked via the "Skill" tool
            if call.get("name") == "Skill":
                print(f"Checking skill call: {call}")
                invoked_skill = call.get("input", {}).get("skill", "")
                if skill_name == invoked_skill:
                    return True
        return False

    def tool_was_called(self, tool_name: str) -> bool:
        """Check if a specific tool was called."""
        return any(call.get("name") == tool_name for call in self.tool_calls)


@pytest.fixture
def project_root() -> Path:
    """Return the project root directory."""
    return Path(__file__).parent.parent


@pytest.fixture
def s3_test_path() -> str:
    """
    Return the S3 path for integration tests.

    Set the BAUPLAN_TEST_S3_PATH environment variable to a valid S3 path
    containing parquet files for testing.
    """
    path = os.environ.get("BAUPLAN_TEST_S3_PATH")
    if not path:
        pytest.skip("BAUPLAN_TEST_S3_PATH environment variable not set")
    return path


@pytest.fixture
def skills_dir(project_root: Path) -> Path:
    """Return the skills directory."""
    return project_root / "skills"


@pytest.fixture
def test_workspace(project_root: Path, tmp_path: Path) -> Path:
    """
    Create a temporary workspace with proper Claude Code configuration.

    This sets up .claude/skills/ symlinked to the project's skills directory
    so that Claude Code can discover the skills.
    """
    workspace = tmp_path / "workspace"
    workspace.mkdir()

    # Create .claude directory structure
    claude_dir = workspace / ".claude"
    claude_dir.mkdir()

    # Symlink the skills directory
    skills_link = claude_dir / "skills"
    skills_source = project_root / "skills"
    if skills_source.exists():
        skills_link.symlink_to(skills_source)

    # Copy settings if they exist
    settings_source = project_root / ".claude" / "settings.local.json"
    if settings_source.exists():
        shutil.copy(settings_source, claude_dir / "settings.local.json")

    return workspace


def run_claude(
    prompt: str,
    cwd: Path,
    max_turns: int = 6,
    timeout_seconds: int = 300,
    extra_args: list[str] | None = None,
) -> ClaudeResult:
    """
    Run Claude Code CLI with a prompt and return structured results.

    Args:
        prompt: The prompt to send to Claude
        cwd: Working directory for the Claude session
        max_turns: Maximum conversation turns (default: 6)
        timeout_seconds: Timeout in seconds (default: 5 minutes)
        extra_args: Additional CLI arguments

    Returns:
        ClaudeResult with parsed output
    """
    cmd = [
        "claude",
        "-p",
        prompt,
        "--output-format",
        "stream-json",
        "--verbose",
        "--max-turns",
        str(max_turns),
        "--dangerously-skip-permissions",
    ]

    if extra_args:
        cmd.extend(extra_args)

    try:
        cp = subprocess.run(
            cmd,
            cwd=cwd,
            text=True,
            capture_output=True,
            timeout=timeout_seconds,
            env={
                **os.environ,
                "CLAUDE_CODE_TELEMETRY_DISABLED": "1",
                "CLAUDECODE": "",  # Allow nested Claude Code invocations in tests
            },
        )
    except subprocess.TimeoutExpired as e:
        return ClaudeResult(
            returncode=-1,
            stdout=e.stdout or "",
            stderr=f"Timeout after {timeout_seconds}s",
            stream_messages=[],
            result=None,
        )

    # Parse newline-delimited JSON stream
    stream_messages = []
    result = None
    if cp.stdout:
        for line in cp.stdout.strip().split("\n"):
            if not line:
                continue
            try:
                event = json.loads(line)
                stream_messages.append(event)
                if event.get("type") == "result":
                    result = event
            except json.JSONDecodeError:
                pass

    return ClaudeResult(
        returncode=cp.returncode,
        stdout=cp.stdout,
        stderr=cp.stderr,
        stream_messages=stream_messages,
        result=result,
    )


@pytest.fixture
def claude_runner(test_workspace: Path):
    """
    Fixture that returns a function to run Claude with the test workspace.
    """

    def _run(prompt: str, **kwargs) -> ClaudeResult:
        return run_claude(prompt, cwd=test_workspace, **kwargs)

    return _run
