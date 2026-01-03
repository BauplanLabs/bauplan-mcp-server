# Bauplan MCP Server

The Bauplan MCP Server exposes operations for interacting with a Bauplan data lakehouse.

## Decision Tree: Skills vs MCP Instructions

When working with Bauplan, choose the right approach based on the task:

```
Is this a code generation task?
├── YES: Writing a new pipeline/DAG → Use skill: /new-pipeline (or "creating-bauplan-pipelines")
├── YES: Data ingestion with WAP    → Use skill: /wap (or "wap-ingestion")
└── NO:  Exploration, queries, repair, etc. → Use MCP tools + get_instructions
```

**Skills are preferred for code generation** because they contain comprehensive templates, best practices, and workflow checklists. If skills are not available, fall back to MCP instructions.

## Main Use Cases

| Use Case | Skill Available? | MCP Fallback |
|----------|------------------|--------------|
| Data ingestion from S3 (WAP) | `/wap` | `get_instructions('wap')` |
| Writing a data pipeline/DAG | `/new-pipeline` | `get_instructions('pipeline')` |
| Descriptive data tasks & lineage | No | `get_instructions('data')` |
| Repairing broken pipelines | No | `get_instructions('repair')` |
| Data expectations & quality tests | No | `get_instructions('test')` |
| Bauplan SDK/CLI syntax help | No | `get_instructions('sdk')` |

## SDK and CLI Syntax Verification

When writing Bauplan code or CLI commands, **always verify syntax** using these methods:

- **SDK Docs**: https://docs.bauplanlabs.com/ and https://docs.bauplanlabs.com/reference/bauplan
- **CLI Help**: Use the terminal directly:
  - `bauplan --help` - lists main verbs/commands
  - `bauplan <verb> --help` - shows parameters for that command (e.g., `bauplan run --help`)
- **MCP**: Call `get_instructions(use_case='sdk')` for SDK method explanations and usage examples

Use `WebFetch` on doc URLs or run CLI help commands to confirm correct syntax before finalizing code.

## Important Notes

### Authentication

Assume all bauplan calls (MCP and skills) are authenticated through local CLI setup or server-side MCP configuration. No need to handle API tokens manually.

### Getting Detailed Instructions (MCP)

When skills are not available or the task doesn't fit a skill, call the `get_instructions` tool:

```
get_instructions(use_case='data')     # Query, explore, lineage
get_instructions(use_case='wap')      # WAP data ingestion (if no skill)
get_instructions(use_case='pipeline') # Pipeline creation (if no skill)
get_instructions(use_case='repair')   # Fix pipeline issues
get_instructions(use_case='test')     # Data expectations
get_instructions(use_case='sdk')      # SDK method help
```

The returned prompt contains detailed guidelines for that use case.

### User Information

Most operations require user's information, which can be retrieved at the beginning of reasoning by calling the `get_user_info` tool.
