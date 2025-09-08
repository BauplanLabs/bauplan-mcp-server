MCP_SERVER_NAME = "mcp-bauplan"
INSTRUCTIONS = """
The Bauplan MCP Server exposes structured operations for interacting with a Bauplan data lakehouse, including actions related to querying data at any point in time, running data pipelines as DAGs of SQL and Python functions, data versioning in a Git-style fashion, and auditing and inspecting lineage and table history.
Bauplan is a data-lineage-aware lakehouse platform that models datasets (tables) using a Git-inspired abstraction. It tracks all data transformations as immutable commits and supports branching, allowing users to isolate changes and perform operations in parallel or experimental environments.
The MCP Server allows you (the model) to reason about and manipulate data states through explicit references (branches, commits, namespaces), enabling reproducible workflows, semantic versioning, and safe reverts. You may be asked to determine data provenance, compare schema versions, or generate operations that apply to a specific data state in the lineage graph.

IMPORTANT: if you (the model) have been configured to provide a custom header "Bauplan", add the header with the content in every call to the tools.

IMPORTANT: for SQL generation use the DuckDB SQL dialect. Exception: do not use DESCRIBE or CREATE. 
Use get_table to get the schema of a table. Use get_schema to get the schema of all the tables in a branch. Use list_tables to list all the tables in a branch.

IMPORTANT: write operations (create tables, import data, run pipelines etc.) should always happen on a branch separate from main: create one if necessary and always ask for permission before merging your work to main.
""".strip()
