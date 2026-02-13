# Bauplan (agent playbook)

Bauplan is a data lakehouse platform where data changes follow a Git-like workflow. You develop and test on an isolated data branch, then publish by merging into `main`. Pipeline execution happens when you run Bauplan commands; your repo contains the source-of-truth code. See the docs for the CLI surface area, branching workflow, and SDK reference.

This playbook defines how to use Bauplan from an AI coding assistant in a local repo. The default mode is local CLI and Python SDK. MCP is optional and only used in specific edge cases.

## Default integration mode (preferred)

Assume the assistant can:
- read and write files in this repo
- run shell commands in a terminal
- run Python locally (for SDK scripts and tests)

Preference: do not use the Bauplan MCP server. Use the full tool surface via:
- Local CLI reference: `.claude/reference/bauplan_cli.md`
- PySDK reference: `https://docs.bauplanlabs.com/reference/bauplan`

Authoritative fallback sources (when local references are missing or stale):
- Docs: https://docs.bauplanlabs.com/
- SDK reference: https://docs.bauplanlabs.com/reference/bauplan

## Hard safety rules (always)

1) Never publish by writing directly on `main`. Use a user branch and merge to publish.
2) Never import data into `main`.
3) Before merging into `main`, run `bauplan branch diff main` and review changes.
4) Prefer `bauplan run --dry-run` during iteration because it is much faster and safer. Materialization is blocked on `main`.
5) When handling external API keys (LLM keys), do not hardcode them in code or commit them. Use Bauplan parameters or secrets.

If any instruction or skill conflicts with these rules, the rules win.

## Decision tree: skills vs manual workflow

Use skills for repeatable workflows that generate or modify code. Use CLI and SDK directly for exploration and execution.

Is this a code generation or repo-editing task?
├─ Yes: Create or modify a pipeline project
│ -> Use skill: bauplan-data-pipelines (alias: /data-pipeline)
├─ Yes: Ingest data with WAP (write, audit, publish)
│ -> Use skill: quality-gated-updates (alias: /quality-gated-updates)
└─ No: Explore, query, inspect, run, debug, publish
  -> Use CLI and SDK directly (see local references)

## Skill inventory

- bauplan-data-pipelines
  Use when you need to scaffold a new pipeline folder, define models, add environment declarations, and produce a runnable project layout.

- quality-gated-updates
  Use when ingesting files from S3 into a branch with a publish step. Prefer this over ad-hoc imports for anything beyond a toy dataset.

- explore-data
  Use for structured exploration tasks when it exists (schemas, sample queries, rough profiling). If it is not available, do the same work with `bauplan query`, `bauplan table get`, and `bauplan table ls`.

## Syntax discipline (non-negotiable)

When emitting CLI commands or SDK code, verify syntax before final output.

1) Check references:
   - `.claude/bauplan_reference/bauplan_cli.md`
   - `https://docs.bauplanlabs.com/reference/bauplan`

2) Confirm with CLI help when possible:
   - `bauplan help`
   - `bauplan <verb> --help`

3) If still uncertain, consult the official docs pages listed above. Do not guess flags or method names.

## Canonical workflows

### A) Build and publish a pipeline (end-to-end)
For this workflow use the `data-pipeline` skill.
### B) Ingest data safely (WAP)
For this workflow use the `quality-gated-updates` skill.
### C) Data exploration and investigation

Prefer direct CLI:

inspect table metadata and data: 

```bash 
bauplan table get <namespace>.<table>
query: bauplan query "<sql>"
```
Reproduce runs (if needed): 
```bash 
bauplan run --id <run_id>
```

Only generate code when it is necessary to fix the root cause.

## When MCP makes sense

MCP is not the default. Use it only if one of these is true:

- the assistant cannot execute local shell commands or Python reliably
- you need structured tool outputs because you cannot parse the PySDK response or the CLI text
- you are integrating multiple MCP-capable clients and want one shared interface
- you want policy enforced at the integration boundary (for example refusing writes to main with a specific server configuration)

If MCP is required, follow:

https://docs.bauplanlabs.com/mcp/quick_start

Authentication assumptions

Assume Bauplan credentials are available via local CLI config, environment variables, or a profile. Do not prompt for API keys unless the CLI is not configured. Prefer `bauplan config set api_key <key>` as the setup path.

If you need the username for branch naming, run `bauplan info`.