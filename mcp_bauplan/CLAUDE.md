# Bauplan MCP Server

The Bauplan MCP Server exposes operations for interacting with a Bauplan data lakehouse.

## Main Use Cases

The main use cases supported fall into six major types:

1. **Descriptive data tasks** - including data lineage information
2. **Data ingestion from S3** - using the Write-Audit-Publish (WAP) pattern
3. **Writing a data transformation pipeline** - as a Bauplan project, and run it
4. **Repairing broken pipelines**
5. **Creating and managing data expectations and quality tests**
6. **Explaining Bauplan SDK and methods** - verify syntax and usage of specific Bauplan methods

On top of these major scenarios, you can use the full set of tools to accomplish any task you need, in some cases by combining multiple tool calls.

## Important Notes

### API Token Configuration

If you (the model) have been configured to provide a custom header 'Bauplan', add the header with the content in every call to the tools. Otherwise, you can assume the Bauplan API token is already set, so no need to use it.

### Getting Detailed Instructions

Once the nature of the task is understood, specific instructions and guidelines for each of the six use cases can be obtained by calling the `get_instructions` tool with the appropriate `use_case` argument:

| Use Case | Argument | Description |
|----------|----------|-------------|
| Descriptive data tasks and lineage | `data` | Query and explore data, understand lineage (how changes in a table affect downstream tables) |
| Data ingestion from S3 | `ingest` | Ingest data using WAP pattern |
| Data transformation pipeline | `pipeline` | Write and run pipelines |
| Repairing broken pipelines | `repair` | Fix pipeline issues |
| Data expectations and quality tests | `test` | Create and manage tests |
| Bauplan SDK and methods | `sdk` | Explain and verify SDK method syntax and usage |

The `get_instructions` tool will return a detailed prompt that you **SHOULD** consider as you plan next steps. Note that you can call `get_instructions` multiple times if needed.

### User Information

Most operations require user's information, which can be retrieved at the beginning of reasoning by calling the `get_user_info` tool.
