---
name: "Write-Audit-Publish (WAP)"
description: "Ingest data from S3 into bauplan using the Write-Audit-Publish pattern for safe data loading"
allowed-tools:
  - Bash(bauplan:*)
  - Read
  - Write
  - Glob
  - Grep
  - WebFetch(domain:docs.bauplanlabs.com)
---

# Write-Audit-Publish (WAP) Pattern

The WAP pattern is a data ingestion methodology that ensures data quality before making data visible to downstream consumers. It prevents "dataset pollution in production" by sandboxing changes until they pass quality checks.

## The Three Steps

1. **Write**: Ingest data into a temporary branch (isolated from production)
2. **Audit**: Run data quality checks on the staged data
3. **Publish**: Merge validated data to the main branch

## CRITICAL: Branch Safety

> **All WAP operations happen on a temporary branch, NEVER on `main`.**

The temporary branch is:
- Created from `main` at the start
- Used for all write operations
- Merged back to `main` only after quality checks pass
- Deleted after successful merge (or on failure for cleanup)

## Prerequisites

Before starting WAP ingestion:

1. **Get your username**: `bauplan info` (look for "Username:")
2. **Verify S3 source**: Ensure the S3 path contains valid parquet/csv/jsonl files
3. **Know the target namespace**: Default is `bauplan`

## CLI Quick Reference

```bash
bauplan --help              # General help
bauplan branch --help       # Branch operations
bauplan table --help        # Table operations
bauplan import --help       # Data import operations
```

## WAP Flow Overview

```python
import bauplan
from datetime import datetime

def wap_ingest(
    table_name: str,
    s3_path: str,
    namespace: str = "bauplan",
    columns_to_check: list = None
):
    """
    Write-Audit-Publish flow for safe data ingestion.

    Args:
        table_name: Target table name
        s3_path: S3 URI pattern (e.g., 's3://bucket/path/*.parquet')
        namespace: Target namespace (default: 'bauplan')
        columns_to_check: Columns to validate for null values
    """
    client = bauplan.Client()

    # Generate unique branch name
    branch_name = f"{namespace}.wap_{table_name}_{int(datetime.now().timestamp())}"

    try:
        # === WRITE PHASE ===
        # 1. Create temporary branch from main
        if client.has_branch(branch_name):
            client.delete_branch(branch_name)
        client.create_branch(branch_name)

        # 2. Create table (schema inferred from S3 files)
        client.create_table(
            table=table_name,
            search_uri=s3_path,
            namespace=namespace,
            branch=branch_name,
            replace=True
        )

        # 3. Import data into table
        client.import_data(
            table=table_name,
            search_uri=s3_path,
            namespace=namespace,
            branch=branch_name
        )

        # === AUDIT PHASE ===
        # 4. Run quality checks
        if columns_to_check:
            data = client.scan(
                table=f"{namespace}.{table_name}",
                ref=branch_name,
                columns=columns_to_check
            )
            for col in columns_to_check:
                null_count = data[col].null_count
                assert null_count == 0, f"Column {col} has {null_count} null values"

        # === PUBLISH PHASE ===
        # 5. Merge to main
        client.merge_branch(
            source_ref=branch_name,
            into_branch="main"
        )
        print(f"Successfully published {table_name} to main")

    finally:
        # 6. Cleanup: delete temporary branch
        if client.has_branch(branch_name):
            client.delete_branch(branch_name)
```

## Key SDK Methods

| Method | Description |
|--------|-------------|
| `client.create_branch(name)` | Create a new branch from current HEAD |
| `client.has_branch(name)` | Check if branch exists |
| `client.delete_branch(name)` | Delete a branch |
| `client.create_table(table, search_uri, ...)` | Create table with schema inferred from S3 |
| `client.import_data(table, search_uri, ...)` | Import data from S3 into table |
| `client.scan(table, ref, columns)` | Read table data for validation |
| `client.merge_branch(source_ref, into_branch)` | Merge branch into target |
| `client.has_table(table, ref)` | Check if table exists on branch |

> **SDK Reference**: For detailed method signatures, check https://docs.bauplanlabs.com/reference/bauplan

## CLI-Based WAP Flow

If using the CLI instead of SDK:

```bash
# 1. Get username
bauplan info

# 2. Checkout to main first
bauplan branch checkout main

# 3. Create temporary ingestion branch
bauplan branch create <username>.wap_<table_name>_<timestamp>

# 4. Checkout to the new branch
bauplan branch checkout <username>.wap_<table_name>_<timestamp>

# 5. Create table (schema inferred from S3)
bauplan table create <table_name> --search-uri "s3://bucket/path/*.parquet" --namespace <namespace>

# 6. Import data
bauplan import <table_name> --search-uri "s3://bucket/path/*.parquet" --namespace <namespace>

# 7. Validate data (query to check for issues)
bauplan query "SELECT COUNT(*) as nulls FROM <namespace>.<table_name> WHERE <column> IS NULL"

# 8. If validation passes, merge to main
bauplan branch merge <username>.wap_<table_name>_<timestamp> --into main

# 9. Cleanup: delete temporary branch
bauplan branch delete <username>.wap_<table_name>_<timestamp>
```

## Workflow Summary

1. **Get username**: `bauplan info`
2. **Checkout main**: `bauplan branch checkout main`
3. **Create WAP branch**: `bauplan branch create <username>.wap_<table>_<ts>`
4. **Checkout WAP branch**: `bauplan branch checkout <username>.wap_<table>_<ts>`
5. **Create table**: Infer schema from S3 source
6. **Import data**: Load data from S3
7. **Audit**: Run quality checks (null checks, value ranges, etc.)
8. **Publish**: `bauplan branch merge <branch> --into main`
9. **Cleanup**: `bauplan branch delete <branch>`

## Advanced Examples

See [examples.md](examples.md) for:
- Multiple table ingestion
- Custom quality checks
- Handling failures and rollback
- Incremental/append ingestion
- Different file formats (parquet, csv, jsonl)
