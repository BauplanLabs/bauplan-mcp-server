# Repair Pipeline

By listing jobs by their status, you can identify and fix issues in a pipeline.

## Available Information

- **Telemetry data (logs)** and basic job information (user, transactional branch) are all available as tools using the jobID
- **Intermediate tables** written before the failures will be available for inspection in the transactional branch
- **The exact pipeline code** from a job can also be recovered

## Debugging Process

If unsure about the cause, you can use a debug branch to perform your analysis:

1. Make sure to open your debug branch from the same commit (`ref`) the original job was created
2. Use a proper naming convention for your branch, e.g., `<user_name>.debug_<job_id>`

## Fixing the Pipeline

If asked to fix the pipeline (not just diagnose a failure):

1. Make sure to test it thoroughly
2. Run it end to end on your debug branch to verify all the tables are there
3. Write a full Bauplan project with all the necessary files and decorators when passing it to the MCP tool to run

> **NOTE**: If you cannot save files locally, use `code_run` and pass the files as dictionaries `<file_name>` -> `<code_as_string>`. Check the data pipeline prompt from `get_instructions` for an example of a minimal project **BEFORE** generating any code.

## Expected Output

If not instructed otherwise, return the name of the debug branch you created, containing the pipeline that you re-ran, with all the proper tables correctly materialized.
