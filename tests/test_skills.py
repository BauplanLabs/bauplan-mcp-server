"""
Integration tests for Claude Code skills.

These tests verify that Claude Code correctly invokes the right skill
when given prompts related to each skill's domain.
"""

import pytest


class TestSafeIngestionSkillInvocation:
    """Test that ingestion-related prompts trigger the safe-ingestion skill."""

    @pytest.mark.integration
    def test_safe_ingestion_skill_invoked_for_s3_import(self, claude_runner, s3_test_path):
        """
        Given a prompt about importing S3 data into a bauplan table,
        Claude should invoke the safe-ingestion skill.
        """
        prompt = (
            f"Use WAP to ingest {s3_test_path} with these parameters: "
            "table_name='yellow_taxi', namespace='test_ns', "
            "on_success='inspect', on_failure='keep'."
        )

        result = claude_runner(prompt, max_turns=6)

        # Basic assertions
        assert result.succeeded, f"Claude failed: {result.stderr}"
        assert result.result is not None, "Expected result output"

        # Check that the safe-ingestion skill was invoked
        assert result.skill_was_invoked("safe-ingestion"), (
            f"Expected 'safe-ingestion' skill to be invoked. "
            f"Tool calls made: {[c['name'] for c in result.tool_calls]}"
        )


class TestDebugAndFixPipelineSkillInvocation:
    """Test that debug-related prompts trigger the debug-and-fix-pipeline skill."""

    @pytest.mark.integration
    def test_debug_skill_invoked_for_failed_run(self, claude_runner):
        """
        Given a prompt about diagnosing a failed bauplan run,
        Claude should invoke the debug-and-fix-pipeline skill.
        """
        prompt = (
            "Check the failed pipelines in the past 5 hours."
            "Take the most recent one, debug and fix it."
        )

        result = claude_runner(prompt, max_turns=6)

        assert result.succeeded, f"Claude failed: {result.stderr}"
        assert result.result is not None, "Expected result output"

        assert result.skill_was_invoked("debug-and-fix-pipeline"), (
            f"Expected 'debug-and-fix-pipeline' skill to be invoked. "
            f"Tool calls made: {[c['name'] for c in result.tool_calls]}"
        )


class TestNoSkillInvocation:
    """Test that unrelated prompts do not invoke skills."""

    @pytest.mark.integration
    def test_simple_prompt_invokes_no_skills(self, claude_runner):
        """
        A simple prompt should return valid JSON output and invoke no skills.
        """
        prompt = "What is 2 + 2? Just answer with the number."

        result = claude_runner(prompt, max_turns=2)

        assert result.succeeded
        assert result.result is not None
        # No skills should be invoked for simple math
        skill_calls = [c for c in result.tool_calls if c.get("name") == "Skill"]
        assert len(skill_calls) == 0, (
            f"Expected no skills to be invoked. Skills called: {skill_calls}"
        )


class TestClaudeResultParsing:
    """Test the ClaudeResult helper methods."""

    def test_tool_calls_extraction(self):
        """Test that tool calls are correctly extracted from stream output."""
        from .conftest import ClaudeResult

        # Mock stream format: assistant messages contain tool_use in content
        mock_stream = [
            {"type": "system", "subtype": "init"},
            {
                "type": "assistant",
                "message": {
                    "role": "assistant",
                    "content": [
                        {"type": "text", "text": "I'll help you"},
                        {
                            "type": "tool_use",
                            "name": "Skill",
                            "input": {"skill": "safe-ingestion"},
                        },
                    ],
                },
            },
            {"type": "result", "subtype": "success"},
        ]

        result = ClaudeResult(
            returncode=0,
            stdout="",
            stderr="",
            stream_messages=mock_stream,
            result={"type": "result", "subtype": "success"},
        )

        assert len(result.tool_calls) == 1
        assert result.tool_calls[0]["name"] == "Skill"
        assert result.skill_was_invoked("safe-ingestion")
        assert not result.skill_was_invoked("wap")

    def test_debug_skill_parsing(self):
        """Test that debug-and-fix-pipeline skill invocation is detected."""
        from .conftest import ClaudeResult

        mock_stream = [
            {"type": "system", "subtype": "init"},
            {
                "type": "assistant",
                "message": {
                    "role": "assistant",
                    "content": [
                        {"type": "text", "text": "Let me debug this"},
                        {
                            "type": "tool_use",
                            "name": "Skill",
                            "input": {"skill": "debug-and-fix-pipeline"},
                        },
                    ],
                },
            },
            {"type": "result", "subtype": "success"},
        ]

        result = ClaudeResult(
            returncode=0,
            stdout="",
            stderr="",
            stream_messages=mock_stream,
            result={"type": "result", "subtype": "success"},
        )

        assert len(result.tool_calls) == 1
        assert result.skill_was_invoked("debug-and-fix-pipeline")
        assert not result.skill_was_invoked("safe-ingestion")
