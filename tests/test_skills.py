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
            "table_name='titanic', namespace='bauplan', "
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
        prompt = "Check the failed pipelines in the past 5 hours.Take the most recent one, debug and fix it."

        result = claude_runner(prompt, max_turns=6)

        assert result.succeeded, f"Claude failed: {result.stderr}"
        assert result.result is not None, "Expected result output"

        assert result.skill_was_invoked("debug-and-fix-pipeline"), (
            f"Expected 'debug-and-fix-pipeline' skill to be invoked. "
            f"Tool calls made: {[c['name'] for c in result.tool_calls]}"
        )


class TestDataPipelineSkillInvocation:
    """Test that pipeline-creation prompts trigger the data-pipeline skill."""

    @pytest.mark.integration
    def test_data_pipeline_skill_invoked_for_new_pipeline(self, claude_runner):
        """
        Given a prompt about creating a new bauplan pipeline,
        Claude should invoke the data-pipeline skill.
        """
        prompt = (
            "Create a new bauplan pipeline that reads from "
            "bauplan.titanic and produces bauplan.titanic_survival_stats. "
            "Use a Python model with DuckDB to compute survival rate by Pclass and Sex."
        )

        result = claude_runner(prompt, max_turns=6)

        assert result.succeeded, f"Claude failed: {result.stderr}"
        assert result.result is not None, "Expected result output"

        assert result.skill_was_invoked("data-pipeline"), (
            f"Expected 'data-pipeline' skill to be invoked. "
            f"Tool calls made: {[c['name'] for c in result.tool_calls]}"
        )


class TestDataQualityChecksSkillInvocation:
    """Test that data quality prompts trigger the data-quality-checks skill."""

    @pytest.mark.integration
    def test_data_quality_skill_invoked_for_expectations(self, claude_runner):
        """
        Given a prompt about adding data quality checks to a pipeline,
        Claude should invoke the data-quality-checks skill.
        """
        prompt = (
            "Add data quality checks for the table bauplan.titanic. "
            "Check that PassengerId is unique, Survived is 0 or 1, "
            "Pclass is between 1 and 3, and Fare is not negative."
        )

        result = claude_runner(prompt, max_turns=6)

        assert result.succeeded, f"Claude failed: {result.stderr}"
        assert result.result is not None, "Expected result output"

        assert result.skill_was_invoked("data-quality-checks"), (
            f"Expected 'data-quality-checks' skill to be invoked. "
            f"Tool calls made: {[c['name'] for c in result.tool_calls]}"
        )


class TestExploreDataSkillInvocation:
    """Test that data exploration prompts trigger the explore-data skill."""

    @pytest.mark.integration
    def test_explore_data_skill_invoked_for_table_inspection(self, claude_runner):
        """
        Given a prompt about exploring or inspecting data in the lakehouse,
        Claude should invoke the explore-data skill.
        """
        prompt = (
            "Explore the table bauplan.titanic on the main branch. "
            "Show me the schema, a sample of rows, and basic profiling "
            "like null rates for Age and Cabin, and the distribution of Pclass."
        )

        result = claude_runner(prompt, max_turns=6)

        assert result.succeeded, f"Claude failed: {result.stderr}"
        assert result.result is not None, "Expected result output"

        assert result.skill_was_invoked("explore-data"), (
            f"Expected 'explore-data' skill to be invoked. "
            f"Tool calls made: {[c['name'] for c in result.tool_calls]}"
        )


class TestDataAssessmentSkillInvocation:
    """Test that data assessment prompts trigger the data-assessment skill."""

    @pytest.mark.integration
    def test_data_assessment_skill_invoked_for_feasibility_check(self, claude_runner):
        """
        Given a prompt about assessing whether a business question can be
        answered with available data, Claude should invoke the data-assessment skill.
        """
        prompt = (
            "Can we figure out which passenger class on the Titanic had the "
            "highest survival rate, broken down by gender? Check if the data "
            "in bauplan.titanic supports this analysis."
        )

        result = claude_runner(prompt, max_turns=6)

        assert result.succeeded, f"Claude failed: {result.stderr}"
        assert result.result is not None, "Expected result output"

        assert result.skill_was_invoked("data-assessment"), (
            f"Expected 'data-assessment' skill to be invoked. "
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
        assert len(skill_calls) == 0, f"Expected no skills to be invoked. Skills called: {skill_calls}"


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

    def test_data_pipeline_skill_parsing(self):
        """Test that data-pipeline skill invocation is detected."""
        from .conftest import ClaudeResult

        mock_stream = [
            {"type": "system", "subtype": "init"},
            {
                "type": "assistant",
                "message": {
                    "role": "assistant",
                    "content": [
                        {"type": "text", "text": "I'll create a pipeline"},
                        {
                            "type": "tool_use",
                            "name": "Skill",
                            "input": {"skill": "data-pipeline"},
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
        assert result.skill_was_invoked("data-pipeline")
        assert not result.skill_was_invoked("safe-ingestion")

    def test_data_quality_checks_skill_parsing(self):
        """Test that data-quality-checks skill invocation is detected."""
        from .conftest import ClaudeResult

        mock_stream = [
            {"type": "system", "subtype": "init"},
            {
                "type": "assistant",
                "message": {
                    "role": "assistant",
                    "content": [
                        {"type": "text", "text": "I'll add quality checks"},
                        {
                            "type": "tool_use",
                            "name": "Skill",
                            "input": {"skill": "data-quality-checks"},
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
        assert result.skill_was_invoked("data-quality-checks")
        assert not result.skill_was_invoked("data-pipeline")

    def test_explore_data_skill_parsing(self):
        """Test that explore-data skill invocation is detected."""
        from .conftest import ClaudeResult

        mock_stream = [
            {"type": "system", "subtype": "init"},
            {
                "type": "assistant",
                "message": {
                    "role": "assistant",
                    "content": [
                        {"type": "text", "text": "I'll explore the data"},
                        {
                            "type": "tool_use",
                            "name": "Skill",
                            "input": {"skill": "explore-data"},
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
        assert result.skill_was_invoked("explore-data")
        assert not result.skill_was_invoked("safe-ingestion")

    def test_data_assessment_skill_parsing(self):
        """Test that data-assessment skill invocation is detected."""
        from .conftest import ClaudeResult

        mock_stream = [
            {"type": "system", "subtype": "init"},
            {
                "type": "assistant",
                "message": {
                    "role": "assistant",
                    "content": [
                        {"type": "text", "text": "I'll assess the data"},
                        {
                            "type": "tool_use",
                            "name": "Skill",
                            "input": {"skill": "data-assessment"},
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
        assert result.skill_was_invoked("data-assessment")
        assert not result.skill_was_invoked("explore-data")

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
