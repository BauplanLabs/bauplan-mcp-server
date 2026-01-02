from pathlib import Path

# Directory containing instruction markdown files
_INSTRUCTIONS_DIR = Path(__file__).parent / "instructions"

# Read instruction files at module load time
bauplan_pipeline_prompt = (_INSTRUCTIONS_DIR / "pipeline.md").read_text()
bauplan_data_prompt = (_INSTRUCTIONS_DIR / "data.md").read_text()
bauplan_repair_prompt = (_INSTRUCTIONS_DIR / "repair.md").read_text()
bauplan_ingest_prompt = (_INSTRUCTIONS_DIR / "ingest.md").read_text()
bauplan_test_prompt = (_INSTRUCTIONS_DIR / "test.md").read_text()
bauplan_sdk_prompt = (_INSTRUCTIONS_DIR / "sdk.md").read_text()

USE_CASE_TO_PROMPT = {
    "pipeline": bauplan_pipeline_prompt,
    "data": bauplan_data_prompt,
    "repair": bauplan_repair_prompt,
    "ingest": bauplan_ingest_prompt,
    "test": bauplan_test_prompt,
    "sdk": bauplan_sdk_prompt,
}
