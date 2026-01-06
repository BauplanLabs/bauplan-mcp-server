# Bauplan MCP Server & Agent Skills

Build AI-powered data engineering workflows with the Bauplan MCP Server and Agent Skills.

> [!NOTE]
> This project is released in Beta under MIT license. APIs and features may change without notice as we continue development.

## Overview

This repository provides two complementary tools for AI-assisted data engineering with Bauplan:

1. **MCP Server** - A Model Context Protocol server that gives AI assistants (Claude Code, Claude Desktop, Cursor) access to Bauplan lakehouse operations: querying tables, schema inspection, branch management, and running pipelines. A [video walkthrough](https://www.loom.com/share/651e2bd7ad4442928f539859a621c562) demonstrates setup and usage.

2. **Agent Skills** - Reusable skill definitions for Claude Code that provide guided workflows for common code generation tasks like creating pipelines (`/new-pipeline`) and data ingestion (`/wap`).

The intended usage for this repo is to help with *local development* by providing AI assistants access to your Bauplan lakehouse: a blog post with some context and background is available [here](https://www.bauplanlabs.com/post/bauplans-mcp-server).

## MCP Quick Start

You can get started in one minute with your existing AI assistant: a video setup with Claude Desktop and Claude Code is also available [here](https://drive.google.com/file/d/1BIBBuxuCKrrHvxOfkut_8TuYy77HZ1rQ/view?usp=sharing) for reference. 

You need:

- a Bauplan [API key](https://app.bauplanlabs.com/sign-up) properly configured in your local config file (default profile) - the server will pick it up automatically (see below for alternative authentication methods);
- [uv](https://docs.astral.sh/uv/guides/install-python/) (or a standard `pip` managed virtual environment, see below);
- an AI platform able to leverage the MCP, as for example Claude Code, Cursor, Claude Desktop.

<aside>

> [!WARNING]
> do not use an Admin Bauplan API key: while the server will refuse to write on `main`, it is good practice to use a non-admin key for AI-assisted development (see our roadmap below for more details on upcoming security features).

</aside>

Start the server with:

```bash
uv sync
uv run python main.py --transport streamable-http

```

The MCP server is now available at `http://localhost:8000/mcp`. You can configure the server in Claude Code for example with:

```bash
claude mcp add -t http mcp-bauplan "http://localhost:8000/mcp"

```

Similar commands can be run on [Claude Desktop](https://modelcontextprotocol.io/quickstart/user) or [Cursor](https://docs.cursor.com/en/context/mcp) to enable the AI to access the server.

Et voilà! You can now start asking your AI questions about your data lakehouse (and much more!).

## Advanced Configurations

### CLAUDE.md for Guided Usage

We provide a `CLAUDE.md` file in `mcp_bauplan/CLAUDE.md` that instructs the model on how to best use the Bauplan MCP server and the Bauplan skills provided in the `skills/` folder.

**For Claude Code users**: Copy this file to your project root. Claude Code automatically picks up `CLAUDE.md` files and uses them as context for every interaction. This ensures the model knows:
- When to use skills (`/wap`, `/new-pipeline`) vs MCP tools
- How to retrieve detailed instructions via `get_instructions`
- How to verify SDK/CLI syntax

For other MCP clients, include the contents as a system prompt or initial context.

### Bauplan Credentials

The Beta release covers the local development use case. Authentication to your Bauplan lakehouse happens as follows:

- if you do not specify a Bauplan profile as a flag, the default one on the machine running the server will be used at every interaction with the lakehouse.
- if you specify a profile as a flag, this profile will be used instead when instantiating a Bauplan client.
- if you specify a header in your assistant - key=`Bauplan`, value=`your_api_key` (e.g. in Claude code `claude mcp add -H "Bauplan: <your-bauplan-api-key>" ...`) -, `your_api_key` will be used instead when instantiating a Bauplan client. This is convenient for quick tests, and opens up the possibility of hosting the catalog on a shared infrastructure, delegating to clients the Bauplan API key management.

### Server CLI Options

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

#### Automatic Configuration Generation

Use the provided script to generate the configuration with the correct paths:

```bash
uv run scripts/generate-config.py
```

This will output a JSON configuration with all paths properly set to your installation directory. Copy the output and add it to your `claude_desktop_config.json` file.

#### Manual Configuration

Alternatively, you can manually add this configuration (modify the paths as needed):

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

The beta release exposes the core Bauplan functionalities for data lakehouse and pipeline management: thanks to the API-first nature of the platform, a competent AI assistant properly prompted should already be a very effective co-pilot for your lakehouse, including data exploration, schema inspection, iterating on pipelines, etc.

The Bauplan platform is constantly evolving, with new agent-specific commands and fine-grained permissions coming soon. We are now actively improving the MCP server and adding new features, including:

- a server-side deployment option for existing Bauplan users;
- further iterations on MCP and best practices around it for improved code generation (both in co-pilot and in agentic use cases).

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
- **`code_run`**: Run a Bauplan project from code files provided as a dictionary (useful for clients that cannot submit paths), automatically creates temporary directory and validates project structure

#### Job Management

- **`list_jobs`**: List jobs in the Bauplan system with optional filtering for all users
- **`get_job`**: Get detailed information about a specific job by its ID
- **`cancel_job`**: Cancel a running job by its ID and get updated job status

#### User Management

- **`get_user_info`**: Get information about the current authenticated user (username and full name)

#### Instructions and Guidance

- **`get_instructions`**: Get detailed instructions for specific Bauplan use cases (pipeline, data, repair, wap, test, sdk)

## Skills

> [!WARNING]
> Skills are very experimental and subject to change at any time.

The `skills/` folder contains reusable skill definitions for Claude Code that provide guided workflows for common Bauplan tasks. These skills can be incorporated into your Claude Code projects to enable AI-assisted data engineering.

### Available Skills

| Skill | Description |
|-------|-------------|
| **new-pipeline** | Create a new bauplan data pipeline project from scratch, including SQL and Python models with proper project structure |
| **wap** | Implement the Write-Audit-Publish (WAP) pattern for safe data ingestion from S3 with quality checks before publishing to production |

### Using Skills

To incorporate these skills into your Claude Code projects, see the [official documentation on distributing and installing skills](https://code.claude.com/docs/en/skills#distribute-skills). Each skill folder contains the skill definitinon along with examples and usage patterns.

## Developer Notes

If you are actively developing within this repo, an experimental integration test suite is available in `tests/`. These integration tests treat Claude as a black box process: a prompt is fed to Claude in non-interactive mode, and the output is analyzed to verify appropriate skill and tool usage, as well as the presence of expected side effects in the lakehouse (i.e., did the system accomplish the goal?). 

While not perfect, this setup allows us to rapidly iterate on Bauplan-related affordances with some level of confidence and some degree of repeatability (i.e., even as models evolve and prompts change, we can verify that certain core LLM decisions remain intact, for example, using `wap` as a skill when prompts mention safe data ingestion). To run the tests from the root, you can use `pytest` and specify a real S3 file for testing:

```bash
BAUPLAN_TEST_S3_PATH="s3://public-read-bucket/my-file.parquet" uv run pytest -v
```

As best practices emerge, Bauplan skills multiply and AI-assisted workflows rise, this suite will evolve to provide more comprehensive coverage and guidance.

## License

This project is provided with no guarantees under the attached MIT License.
