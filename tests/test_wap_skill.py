"""
Integration tests for the WAP (Write-Audit-Publish) skill.

These tests verify that Claude Code correctly invokes the wap-ingestion skill
when given prompts related to data ingestion from S3.
"""

import pytest


class TestWapSkillInvocation:
    """Test that WAP-related prompts trigger the wap-ingestion skill."""

    @pytest.mark.integration
    def test_wap_skill_invoked_for_s3_import(self, claude_runner, s3_test_path):
        """
        Given a prompt about importing S3 data into a bauplan table,
        Claude should invoke the wap-ingestion skill.
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

        # Check that the WAP skill was invoked
        # Note: Claude Code uses directory name ("wap") not SKILL.md name ("wap-ingestion")
        assert result.skill_was_invoked("quality-gated-updates"), (
            f"Expected 'quality-gated-updates' skill to be invoked. "
            f"Tool calls made: {[c['name'] for c in result.tool_calls]}"
        )

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
                            "input": {"skill": "wap"},
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
        assert result.skill_was_invoked("wap")
