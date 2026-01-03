# Data Pipelines

A Bauplan pipeline is a **DAG of models** (SQL/Python functions) that transform data. Each model takes tables as inputs and produces exactly one output table.

> **CRITICAL**: Never run pipelines on `main` branch. Use a development branch: `<username>.<branch_name>`

## Project Structure

```
my-project/
  bauplan_project.yml    # Required: project config
  source.sql             # SQL model (first node only)
  models.py              # Python models
```

### bauplan_project.yml

```yaml
project:
  id: <unique-uuid>
  name: <project_name>
```

## Pipeline as a DAG

```
[lakehouse: raw_events] ──→ [staging.sql] ──→ [transform] ──→ [daily_summary]
                                                  ↑
[lakehouse: dim_zones] ────────────────────────────┘
```

- **Source tables**: Existing lakehouse tables (entry points)
- **SQL models**: Output name = filename (`staging.sql` → `staging`)
- **Python models**: Output name = function name (`def transform()` → `transform`)
- Multiple inputs allowed, one output per model

## When to Use SQL vs Python

| Model Type | Use For |
|------------|---------|
| **SQL** | First nodes only (reading from lakehouse tables) |
| **Python** | Any transformations (irrespective of the DAG position) |

## SQL Model (First Node)

```sql
-- staging.sql
SELECT event_id, event_type, price, event_time
FROM raw_ecommerce_events
WHERE event_time IS NOT NULL AND price > 0
```

## Python Model (can accept multiple inputs)

```python
import bauplan

@bauplan.model(
    columns=['trip_id', 'miles', 'borough', 'zone'],  # Output schema validation
    materialization_strategy='REPLACE'                 # Persist to lakehouse
)
@bauplan.python('3.11', pip={'polars': '1.15.0'})
def trips_with_zones(
    # Multiple inputs with I/O pushdown (columns + filter)
    trips=bauplan.Model(
        'staging',  # Reference the output of previous model, staging.sql
    ),
    zones=bauplan.Model('zones', columns=['zone_id', 'borough', 'zone'])
):
    """
    Joins trips with zone information.

    | trip_id | miles | borough   | zone    |
    |---------|-------|-----------|---------|
    | 1       | 5.2   | Manhattan | Midtown |
    """
    import polars as pl
    return pl.from_arrow(trips).join(
        pl.from_arrow(zones), on='zone_id'
    ).drop('zone_id').to_arrow()
```

## Key Decorators & Parameters

| Decorator/Param | Description |
|-----------------|-------------|
| `@bauplan.model(columns=[...])` | Output schema validation (recommended) |
| `@bauplan.model(materialization_strategy=)` | `'REPLACE'`, `'APPEND'`, or omit (in-memory) |
| `@bauplan.python('3.11', pip={...})` | Python version and dependencies |
| `bauplan.Model('table', columns=[...], filter="...")` | I/O pushdown at storage level |

## Data Quality Expectations

```python
import bauplan
from bauplan.standard_expectations import expect_column_no_nulls

@bauplan.expectation()
@bauplan.python('3.11')
def test_no_null_ids(data=bauplan.Model('transform')):
    result = expect_column_no_nulls(data, 'event_id')
    assert result, 'event_id must not contain null values'
    return result
```

## Running Pipelines

### CLI (Interactive Mode)

```bash
# Get your username
bauplan info

# Create and checkout dev branch
bauplan branch create <username>.<branch_name>
bauplan branch checkout <username>.<branch_name>

# Dry run (validate without executing)
bauplan run --dry-run [--strict]

# Execute pipeline
bauplan run [--strict]
```

Use `--strict` to fail immediately on column mismatches or expectation failures.

### SDK (Programmatic Mode)

```python
import bauplan

client = bauplan.Client()
username = client.info().user.username
branch = f"{username}.dev_pipeline"

# Create branch from main
client.create_branch(branch, from_ref="main")

# Run pipeline on branch
run = client.run(
    project_dir="./my-project",
    ref=branch,
)
print(f"Job {run.job_id}: {run.job_status}")
```

The `client.run()` method executes the pipeline transactionally—if errors occur, the target branch is preserved.

## Best Practices

1. **Whenever possible, specify `columns`** in `@bauplan.model()` for output validation, reasoning from the input schemas to the expected output
2. **Use I/O pushdown** with `columns` and `filter` in `bauplan.Model()`
3. **Add docstrings** with ASCII table showing expected output
4. **Only materialize final outputs** (intermediate models need no `materialization_strategy`)
