# Bauplan MCP Server

A Model Context Protocol (MCP) server that gives AI assistants (Claude Code, Claude Desktop, Cursor) access to Bauplan lakehouse operations: querying tables, schema inspection, branch management, and running pipelines. A [video walkthrough](https://www.loom.com/share/651e2bd7ad4442928f539859a621c562) demonstrates setup and usage.

> [!NOTE]
> This project is released in Beta under MIT license. APIs and features may change without notice as we continue development.

> [!IMPORTANT]
> **Server-side deployment is now available** for existing Bauplan users. You no longer need to run the MCP server locally — contact your Bauplan account team for details.

## Overview

This repository contains the Bauplan MCP Server — a Model Context Protocol server that gives AI assistants access to Bauplan lakehouse operations. A blog post with context and background [is available here](https://www.bauplanlabs.com/post/bauplans-mcp-server).

> [!TIP]
> **Looking for the best local AI setup with Bauplan?** Check out **[BauplanLabs/bauplan-skills](https://github.com/BauplanLabs/bauplan-skills)** — it includes agent playbooks (`CLAUDE.md`), skills, and everything you need to get AI coding assistants working with Bauplan via CLI and SDK, without running an MCP server.

## MCP Quick Start

You can get started in one minute with your existing AI assistant: a video setup with Claude Desktop and Claude Code [is also available here](https://drive.google.com/file/d/1BIBBuxuCKrrHvxOfkut_8TuYy77HZ1rQ/view?usp=sharing) for reference.

You need:

* a Bauplan [API key](https://app.bauplanlabs.com/sign-up) properly configured in your local config file (default profile) - the server will pick it up automatically (see below for alternative authentication methods);
* [uv](https://docs.astral.sh/uv/guides/install-python/) (or a standard `pip` managed virtual environment, see below);
* an AI platform able to leverage the MCP, as for example Claude Code, Cursor, Claude Desktop.

> [!WARNING]
> do not use an Admin Bauplan API key: while the server will refuse to write on `main`, it is good practice to use a non-admin key for AI-assisted development (see our roadmap below for more details on upcoming security features).

Start the server with:

```bash
uv sync
uv run python main.py --transport streamable-http

```

The MCP server is now available at `http://localhost:8000/mcp`. You can configure the server in Claude Code for example with:

```bash
claude mcp add -t http mcp-bauplan http://localhost:8000/mcp

```

Similar commands can be run on [Claude Desktop](https://modelcontextprotocol.io/quickstart/user) or [Cursor](https://docs.cursor.com/en/context/mcp) to enable the AI to access the server.

Et voilà! You can now start asking your AI questions about your data lakehouse (and much more!).

## Advanced Configurations

### Bauplan Credentials

The Beta release covers the local development use case. Authentication to your Bauplan lakehouse happens as follows:

* if you do not specify a Bauplan profile as a flag, the default one on the machine running the server will be used at every interaction with the lakehouse.
* if you specify a profile as a flag, this profile will be used instead when instantiating a Bauplan client.
* if you specify a header in your assistant - either `Authorization: Bearer <your-bauplan-api-key>` or `Bauplan: <your-bauplan-api-key>` (e.g. in Claude Code `claude mcp add -H "Authorization: Bearer <your-bauplan-api-key>" ...`) -, that value will be used instead when instantiating a Bauplan client. This is convenient for quick tests, and opens up the possibility of hosting the catalog on a shared infrastructure, delegating to clients the Bauplan API key management.

For example, if you are connecting to a remotely hosted MCP server that delegates Bauplan authentication to the client, you can register it in Claude Code and pass your own bearer token with:

```bash
claude mcp add -t http -H "Authorization: Bearer <your-bauplan-api-key>" mcp-bauplan https://<your-mcp-host>/mcp

```

### Server CLI Options

The server supports the following CLI options, mostly useful for specifying alternative transport options:

| Option        | Default     | Description                                              | Used With                     |
|---------------|-------------|----------------------------------------------------------|-------------------------------|
| `--transport` | `stdio`     | Transport protocol: `stdio`, `sse`, or `streamable-http` | All commands                  |
| `--host`      | `127.0.0.1` | Host to bind to (localhost by default)                   | `sse`, `streamable-http` only |
| `--port`      | `8000`      | Port to bind to                                          | `sse`, `streamable-http` only |
| `--profile`   | `None`      | Bauplan profile to use                                   | All commands                  |

**Note:** The `--host` and `--port` options are ignored when using `stdio` transport since it communicates through stdin/stdout.

### Claude Desktop

To add the Bauplan MCP server to Claude Desktop, follow the [guide](https://modelcontextprotocol.io/quickstart/user) to get to your `claude_desktop_config.json` file.

You can then add this configuration (modify the paths as needed):

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

* **Transport Type**: Streamable HTTP
* **URL**: <http://localhost:8000/mcp>
* **Session Token**: Use the token from inspector output

## Features

### Tool List

#### Data Operations

* **`list_tables`**: List all tables in a branch/namespace
* **`get_schema`**: Get schema for all tables in a branch/namespace
* **`get_table`**: Get schema for a specific table (more efficient for single table)
* **`run_query`**: Execute SELECT queries on tables
* **`run_query_to_csv`**: Execute SELECT queries and save results directly to CSV file (scalar data types only)

#### Branch Management

* **`get_branches`**: List branches with optional filters
* **`get_commits`**: Get commit history from branches
* **`create_branch`**: Create new branches from references
* **`has_branch`**: Check if a specific branch exists
* **`merge_branch`**: Merge branches with custom commit messages
* **`delete_branch`**: Delete branches (with safety checks)

#### Namespace Management

* **`get_namespaces`**: List available namespaces in a branch
* **`create_namespace`**: Create new namespaces in branches
* **`has_namespace`**: Check if a specific namespace exists in a branch
* **`delete_namespace`**: Delete namespaces from branches

#### Tag Management

* **`get_tags`**: Get tags with optional filters
* **`create_tag`**: Create a new tag from a reference
* **`has_tag`**: Check if a tag exists
* **`delete_tag`**: Delete a tag

#### Table Management

* **`create_table`**: Create a table from S3 location using schema detection (creates ICEBERG table structure but doesn't populate data)
* **`plan_table_creation`**: Create a table import plan from S3 location (generates YAML schema plan with job tracking)
* **`apply_table_creation_plan`**: Apply a table creation plan to resolve schema conflicts (returns job_id for tracking)
* **`has_table`**: Check if a specific table exists in a branch/reference
* **`delete_table`**: Delete a table from a specific branch
* **`import_data`**: Import data into an existing table from S3 location (returns job_id for tracking)
* **`revert_table`**: Revert a table from a source reference to a target branch with optional replacement

#### Project Management

* **`project_run`**: Run a Bauplan project from a specified directory and reference with configurable parameters (dry-run, timeout, detach mode)
* **`code_run`**: Run a Bauplan project from code files provided as a dictionary (useful for clients that cannot submit paths), automatically creates temporary directory and validates project structure

#### Job Management

* **`list_jobs`**: List jobs in the Bauplan system with optional filtering for all users
* **`get_job`**: Get detailed information about a specific job by its ID
* **`cancel_job`**: Cancel a running job by its ID and get updated job status

#### User Management

* **`get_user_info`**: Get information about the current authenticated user (username and full name)

#### Instructions and Guidance

* **`get_instructions`**: Get detailed instructions for specific Bauplan use cases (pipeline, data, repair, wap, test, sdk)

---

## License

This project is provided with no guarantees under the attached MIT License.
