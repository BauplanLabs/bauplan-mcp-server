# Bauplan MCP Server

## Overview

The Bauplan Model Context Protocol (MCP) Server provides AI assistants with access to [Bauplan data lakehouse](https://www.bauplanlabs.com/) functionalities, including querying tables, schema inspection, data branch management, as well as [running ETL pipelines](https://docs.bauplanlabs.com/en/latest/).

*This server is now released in Alpha under a permissive license, but APIs and features may change without notice as we continue development.*

The intended usage for the current release is to help with _local development_ by providing AI assistants like Claude Code access to your Bauplan lakehouse, mediated by a local instance of this MCP server. We expect to generalize this to server-side deployments in the near future, possibly directly hosted by Bauplan: stay tuned! 

To know how to get the most out of the MCP server, check out our video walkthrough: [Link Coming Soon]. If you have preliminary questions or feedback, please reach out to us!

## Quick Start (1 minute)

You can get started in one minute with your existing AI assistant. You need:

* a Bauplan [API key](https://app.bauplanlabs.com/sign-up), with your config file properly configured - the server will pick it up automatically;
* [uv](https://docs.astral.sh/uv/guides/install-python/) (or a standard `pip` managed virtual environment).

Start the server with:

```bash
uv sync --extra dev --extra prompts
uv run python main.py --transport streamable-http
```

The MCP server is now available at `http://localhost:8000/mcp`. You can configure the server in Claude Code for example with:

```bash
claude mcp add -t http mcp-bauplan "http://localhost:8000/mcp" 
```

Et voilà! You can now start asking Claude Code questions about your data lakehouse: check out the video [Link Coming Soon]

For more advanced configurations, alternative transport options, and usage with the MCP Inspector, see the sections below.

## Advanced Configurations

### Python Setup
`
You can run the MCP server also using a standard Python virtual environment:

```bash
python -m venv venv && source venv/bin/activate
pip install -e .[dev,prompts]
```

### Bauplan Credentials

You could configure Claude (or your AI assistant of choice) to use a different Bauplan API key (i.e. not the one in your current config file) by passing it in every call as a header (key=`Bauplan`, value=`your_api_key`).

### CLI Options

The server supports the following CLI options, mostly useful for specifying alternative transport options:

| Option | Default | Description | Used With |
|--------|---------|-------------|-----------|
| `--transport` | `stdio` | Transport protocol: `stdio`, `sse`, or `streamable-http` | All commands |
| `--host` | `127.0.0.1` | Host to bind to (localhost by default) | `sse`, `streamable-http` only |
| `--port` | `8000` | Port to bind to | `sse`, `streamable-http` only |


**Note:** The `--host` and `--port` options are ignored when using `stdio` transport since it communicates through stdin/stdout.

### MCP Inspector

Start the [MCP Inspector](https://github.com/modelcontextprotocol/inspector) if you wish to manually test the server (Node is required):

```bash
npx @modelcontextprotocol/inspector
```

Now, configure the inspector with the proper variables, e.g. for Streamable HTTP:

- **Transport Type**: Streamable HTTP
- **URL**: http://localhost:8000/mcp
- **Session Token**: Use the token from inspector output

## Features

### Roadmap

The Alpha release exposes the core Bauplan functionalities for data lakehouse and pipeline management: thanks to the API-first nature of the platform, a competent AI assistant properly prompted should be already a very effective co-pilot for your lakehouse, including data exploration, schema inspection, iterating on pipelines etc.. 

We are actively working on improving the server and adding new features, including:

* first-class support for Bauplan documentation and related use cases;
* a server-side deployment option for existing Bauplan users;
* further iterations on MCP and best practices around it for improved code generation (both in co-pilot and in agentic use cases).

If you have specific features you would like to see, please get in touch with us!

### Tool List

#### Data Operations
- **`list_tables`**: List all tables in a branch/namespace
- **`get_schema`**: Get schema for all tables in a branch/namespace
- **`get_table`**: Get schema for a specific table (more efficient for single table)
- **`run_query`**: Execute SELECT queries on tables
- **`run_query_to_csv`**: Execute SELECT queries and save results directly to CSV file (scalar data types only)

#### Branch Management  
- **`get_branches`**: List branches with optional filters
- **`get_commits`**: Get commit history from branches
- **`create_branch`**: Create new branches from references
- **`has_branch`**: Check if a specific branch exists
- **`merge_branch`**: Merge branches with custom commit messages
- **`delete_branch`**: Delete branches (with safety checks)

#### Namespace Management
- **`get_namespaces`**: List available namespaces in a branch
- **`create_namespace`**: Create new namespaces in branches
- **`has_namespace`**: Check if a specific namespace exists in a branch
- **`delete_namespace`**: Delete namespaces from branches

#### Tag Management
- **`get_tags`**: Get tags with optional filters
- **`create_tag`**: Create a new tag from a reference
- **`has_tag`**: Check if a tag exists
- **`delete_tag`**: Delete a tag 

#### Table Management
- **`create_table`**: Create a table from S3 location using schema detection (creates ICEBERG table structure but doesn't populate data)
- **`plan_table_creation`**: Create a table import plan from S3 location (generates YAML schema plan with job tracking)
- **`apply_table_creation_plan`**: Apply a table creation plan to resolve schema conflicts (returns job_id for tracking)
- **`has_table`**: Check if a specific table exists in a branch/reference
- **`delete_table`**: Delete a table from a specific branch
- **`import_data`**: Import data into an existing table from S3 location (returns job_id for tracking)
- **`revert_table`**: Revert a table from a source reference to a target branch with optional replacement

#### Project Management
- **`project_run`**: Run a Bauplan project from a specified directory and reference with configurable parameters (dry-run, timeout, detach mode)

#### Job Management
- **`list_jobs`**: List jobs in the Bauplan system with optional filtering for all users
- **`get_job`**: Get detailed information about a specific job by its ID
- **`get_job_logs`**: Get job logs by job ID prefix for debugging and monitoring
- **`cancel_job`**: Cancel a running job by its ID and get updated job status

## Acknowledgements

We wish to thank [Marco](https://github.com/marcoeg) for his contributions to a previous version of this MCP server.

## License
This project is provided with no guarantees under the attached MIT License.
