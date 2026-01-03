# Data Ingestion with Write-Audit-Publish (WAP)

Data ingestion from S3 (parquet, csv, or JSONL) into Bauplan tables **must** follow the Write-Audit-Publish (WAP) pattern for safe, atomic data loading.

## WAP Overview

1. **Write**: Create a temporary branch from `main` and ingest data there
2. **Audit**: Run data quality checks on the branch
3. **Publish**: If checks pass, merge the branch to `main`; otherwise, preserve for debugging

**Key Benefits**:
- Atomic operations: all changes apply to `main` or none do
- Safe multi-table ingestion: create/modify multiple tables in one atomic merge
- Branch isolation: `main` is never modified until explicitly merged

## Required Parameters

Before writing a WAP script, gather:
- **S3 path** (required): Source URI (e.g., `s3://bucket/path/*.parquet`)
- **Table name** (required): Target table name
- **on_success** (optional): `"inspect"` (default) to keep branch for review, or `"merge"` to auto-merge
- **on_failure** (optional): `"keep"` (default) to preserve for debugging, or `"delete"` to cleanup

## Python Template

```python
import bauplan
from datetime import datetime

def wap_ingest(
    table_name: str,
    s3_path: str,
    namespace: str = "bauplan",
    on_success: str = "inspect",
    on_failure: str = "keep",
):
    """WAP flow: Write to temp branch → Audit → Publish to main."""
    client = bauplan.Client()
    username = client.info().user.username
    branch = f"{username}.wap_{table_name}_{int(datetime.now().timestamp())}"

    try:
        # === WRITE ===
        client.create_branch(branch, from_ref="main")

        # Create table (skip if appending to existing table)
        if not client.has_table(table=table_name, ref=branch, namespace=namespace):
            client.create_table(
                table=table_name, search_uri=s3_path,
                namespace=namespace, branch=branch
            )

        client.import_data(
            table=table_name, search_uri=s3_path,
            namespace=namespace, branch=branch
        )

        # === AUDIT ===
        result = client.query(
            f"SELECT COUNT(*) as cnt FROM {namespace}.{table_name}", ref=branch
        )
        row_count = result.column("cnt")[0].as_py()
        assert row_count > 0, "No data imported"
        print(f"Imported {row_count} rows")

        # === PUBLISH ===
        if on_success == "merge":
            client.merge_branch(source_ref=branch, into_branch="main")
            client.delete_branch(branch)
            print(f"Published to main, cleaned up {branch}")
        else:
            print(f"Branch '{branch}' ready for inspection")
            print(f"Merge: client.merge_branch('{branch}', 'main')")

        return branch, True

    except Exception as e:
        print(f"WAP failed: {e}")
        if on_failure == "delete" and client.has_branch(branch):
            client.delete_branch(branch)
        else:
            print(f"Branch '{branch}' preserved for debugging")
        raise

# Usage:
# branch, ok = wap_ingest("orders", "s3://bucket/orders/*.parquet")
```

## Appending to Existing Tables

If the table already exists on `main`, skip `create_table` and only call `import_data`. The new rows are sandboxed on the branch until merged.

## CLI Merge After Inspection

When `on_success="inspect"` (default), merge manually after review:

```bash
bauplan checkout main
bauplan branch merge <username>.wap_<table_name>_<timestamp>
bauplan branch rm <username>.wap_<table_name>_<timestamp>  # optional cleanup
```

## SDK Reference

| Method | Description |
|--------|-------------|
| `bauplan.Client()` | Initialize client |
| `client.info().user.username` | Get current username |
| `client.create_branch(name, from_ref)` | Create branch |
| `client.has_branch(name)` | Check if branch exists |
| `client.delete_branch(name)` | Delete branch |
| `client.create_table(table, search_uri, namespace, branch)` | Create table (schema inferred from S3) |
| `client.import_data(table, search_uri, namespace, branch)` | Import data from S3 |
| `client.query(sql, ref)` | Run SQL query, returns PyArrow Table |
| `client.merge_branch(source_ref, into_branch)` | Merge branch atomically |
| `client.has_table(table, ref, namespace)` | Check if table exists |

When in doubt on parameters and the latest SDK methods, you can use `get_instructions('sdk')` in the MCP server to get up-to-date information.