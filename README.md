# Bauplan MCP Server

Manage your Bauplan Lakehouse using natural language commands with the Bauplan MCP Server.

> [!NOTE]
> This server is now released in Beta under MIT license, but APIs and features may change without notice as we continue development.


## Overview

The Bauplan Model Context Protocol (MCP) Server is an open source library that provides AI assistants with access to Bauplan data lakehouse functionalities, including querying tables, schema inspection, data branch management, as well as running pipelines.

The intended usage for the current release is to help with *local development* by providing AI assistants like Claude Code or Claude Desktop access to your Bauplan lakehouse. 

We expect to generalize this to server-side deployments in the near future, possibly directly hosted by Bauplan: stay tuned!

To know how to get the most out of the MCP server, check out our video walkthrough: [Link Coming Soon]. If you have preliminary questions or feedback, please reach out to us!

## Quick Start

You can get started in one minute with your existing AI assistant. You need:

- a Bauplan [API key](https://app.bauplanlabs.com/sign-up) properly configured in your local config file (default profile) - the server will pick it up automatically (see below for alternative authentication methods);
- [uv](https://docs.astral.sh/uv/guides/install-python/) (or a standard `pip` managed virtual environment, see below);
- an AI platform able to leverage the MCP, as for example Claude Code, Cursor, Claude Desktop.

<aside>


> [!WARNING]
> do not use an Admin Bauplan API key: while the server will refuse to write one `main`, it is good practice to use a non-admin key for AI-assisted development (see our roadmap below for more details on upcoming security features).

</aside>

Start the server with:

```bash
uv sync
uv run python main.py --transport streamable-http

```

The MCP server is now available at `http://localhost:8000/mcp`. You can configure the server in Claude Code for example with:

```bash
claude mcp add -t http mcp-bauplan "<http://localhost:8000/mcp>"

```

Similar commands can be run on [Claude Desktop](https://modelcontextprotocol.io/quickstart/user) or [Cursor](https://docs.cursor.com/en/context/mcp) to enable the AI to access the server.

Et voilà! You can now start asking your AI questions about your data lakehouse (and much more!).

## Advanced Configurations

### Python Setup

You can run the MCP server also using a standard Python virtual environment:

```bash
python -m venv venv && source venv/bin/activate
pip install -e .

```

### Bauplan Credentials

The Beta releases covers the local development use case. Authentication to your Bauplan lakehouse happens as follows:

- if you do not specify a Bauplan profile as a flag (see below CLI options), the default one on the machine running the server will be used at every interaction with the lakehouse.
- if you specify a profile as a flag, this profile will be used instead when instantiating a Bauplan client.
- if you specify a header in your assistant - key=`Bauplan`, value=`your_api_key` (e.g. in Claude code `claude mcp add -H "Bauplan: <your-bauplan-api-key>" ...`) -, `your_api_key` will be used instead when instantiating a Bauplan client. This is convenient for quick tests, and opens up the possibility of hosting the catalog on a shared infrastructure, delegating to clients the Bauplan API key management.

### CLI Options

The server supports the following CLI options, mostly useful for specifying alternative transport options:

| Option | Default | Description | Used With |
| --- | --- | --- | --- |
| `--transport` | `stdio` | Transport protocol: `stdio`, `sse`, or `streamable-http` | All commands |
| `--host` | `127.0.0.1` | Host to bind to (localhost by default) | `sse`, `streamable-http` only |
| `--port` | `8000` | Port to bind to | `sse`, `streamable-http` only |
| `--profile` | `None` | Bauplan profile to use | All commands |

**Note:** The `--host` and `--port` options are ignored when using `stdio` transport since it communicates through stdin/stdout.

### Claude Desktop

To add the Bauplan MCP server to Claude Desktop, follow the [guide](https://modelcontextprotocol.io/quickstart/user) to get to your `claude_desktop_config.json` file.

### Automatic Configuration Generation

Use the provided script to generate the configuration with the correct paths:

```bash
uv run scripts/generate-config.py
```

This will output a JSON configuration with all paths properly set to your installation directory. Copy the output and add it to your `claude_desktop_config.json` file.

### Manual Configuration

Alternatively, you can manually add this configuration:

```json
{
  "mcpServers": {
    "mcp-bauplan": {
      "command": "/path/to/bauplan-mcp-server/.venv/bin/python3",
      "args": [
        "/path/to/bauplan-mcp-server/main.py",
        "--transport",
        "stdio"
      ],
      "workingDirectory": "/path/to/bauplan-mcp-server/"
    }
  }
}
```

Quit and restart Claude Desktop. Now all Bauplan tools are available to your assistant, as this [video](https://www.loom.com/share/5eb09b3c30cc4cd984fb1ba21a70f349?sid=9e70c948-77e5-4cb0-b487-0db3adedf919) demonstrates.

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

The beta release exposes the core Bauplan functionalities for data lakehouse and pipeline management: thanks to the API-first nature of the platform, a competent AI assistant properly prompted should be already a very effective co-pilot for your lakehouse, including data exploration, schema inspection, iterating on pipelines, etc.

Bauplan platform is constantly evolving, with new agent-specific commands and fine-grained permissions coming soon. We are now actively improving the MCP server and adding new features, including:

- first-class support for Bauplan documentation and related use cases;
- a server-side deployment option for existing Bauplan users;
- further iterations on MCP and best practices around it for improved code generation (both in co-pilot and in agentic use cases).

If you have specific features you would like to see, please get in touch with us!

### Tool List

### Data Operations

- **`list_tables`**: List all tables in a branch/namespace
- **`get_schema`**: Get schema for all tables in a branch/namespace
- **`get_table`**: Get schema for a specific table (more efficient for single table)
- **`run_query`**: Execute SELECT queries on tables
- **`run_query_to_csv`**: Execute SELECT queries and save results directly to CSV file (scalar data types only)

### Branch Management

- **`get_branches`**: List branches with optional filters
- **`get_commits`**: Get commit history from branches
- **`create_branch`**: Create new branches from references
- **`has_branch`**: Check if a specific branch exists
- **`merge_branch`**: Merge branches with custom commit messages
- **`delete_branch`**: Delete branches (with safety checks)

### Namespace Management

- **`get_namespaces`**: List available namespaces in a branch
- **`create_namespace`**: Create new namespaces in branches
- **`has_namespace`**: Check if a specific namespace exists in a branch
- **`delete_namespace`**: Delete namespaces from branches

### Tag Management

- **`get_tags`**: Get tags with optional filters
- **`create_tag`**: Create a new tag from a reference
- **`has_tag`**: Check if a tag exists
- **`delete_tag`**: Delete a tag

### Table Management

- **`create_table`**: Create a table from S3 location using schema detection (creates ICEBERG table structure but doesn't populate data)
- **`plan_table_creation`**: Create a table import plan from S3 location (generates YAML schema plan with job tracking)
- **`apply_table_creation_plan`**: Apply a table creation plan to resolve schema conflicts (returns job_id for tracking)
- **`has_table`**: Check if a specific table exists in a branch/reference
- **`delete_table`**: Delete a table from a specific branch
- **`import_data`**: Import data into an existing table from S3 location (returns job_id for tracking)
- **`revert_table`**: Revert a table from a source reference to a target branch with optional replacement

### Project Management

- **`project_run`**: Run a Bauplan project from a specified directory and reference with configurable parameters (dry-run, timeout, detach mode)

### Job Management

- **`list_jobs`**: List jobs in the Bauplan system with optional filtering for all users
- **`get_job`**: Get detailed information about a specific job by its ID
- **`get_job_logs`**: Get job logs by job ID prefix for debugging and monitoring
- **`cancel_job`**: Cancel a running job by its ID and get updated job status

### User Management

- **`get_user_info`**: Get information about the current authenticated user (username and full name)

## License

This project is provided with no guarantees under the attached MIT License.
