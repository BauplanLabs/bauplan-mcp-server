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
[lakehouse: raw_trips] ──→ [staging.sql] ──→ [trips_with_zones]
                                                  ↑
[lakehouse: zones] ───────────────────────────────┘
```

- **Source tables**: Existing lakehouse tables (entry points)
- **SQL models**: Output name = filename (`staging.sql` → `staging`)
- **Python models**: Output name = function name (`def trips_with_zones()` → `trips_with_zones`)
- Multiple inputs allowed, one output per model

## When to Use SQL vs Python

| Model Type | Use For |
|------------|---------|
| **SQL** | First nodes only (reading from lakehouse tables) |
| **Python** | Any transformations (irrespective of the DAG position) |

## SQL Model (First Node)

Every SQL model must declare its output schema with an `output_schema` directive that names a `TableSchema` class defined in the sibling `models.py`:

```sql
-- staging.sql
-- bauplan: output_schema = StagingSchema
SELECT trip_id, miles, zone_id
FROM raw_trips
WHERE miles > 0
```

```python
# models.py
import bauplan


class StagingSchema(bauplan.TableSchema):
    """Trips with positive mileage."""

    trip_id: bauplan.Int64 | None
    miles: bauplan.Float64 | None
    zone_id: bauplan.Int64 | None
```

## Python Model (can accept multiple inputs)

```python
from typing import Annotated

import bauplan
import pyarrow as pa


class ZoneColumns(bauplan.TableSchema):
    """Zone lookup key and labels."""

    zone_id: bauplan.Int64 | None
    borough: bauplan.String | None
    zone: bauplan.String | None


class TripsWithZonesSchema(bauplan.TableSchema):
    """Trips with zone labels, without the join key."""

    trip_id: bauplan.Int64 | None
    miles: bauplan.Float64 | None
    borough: bauplan.String | None
    zone: bauplan.String | None


@bauplan.model(materialization_strategy='REPLACE')  # Persist to lakehouse
@bauplan.python('3.13', pip={'polars': '2.0.0'})
def trips_with_zones(
    # Multiple inputs with I/O pushdown (projection_schema + filter)
    trips: Annotated[
        pa.Table,
        bauplan.Model('staging', projection_schema=StagingSchema),  # Output of staging.sql
    ],
    zones: Annotated[
        pa.Table,
        bauplan.Model('zones', projection_schema=ZoneColumns),
    ],
) -> Annotated[pa.Table, TripsWithZonesSchema]:  # Output schema validation (mandatory)
    """
    Joins trips with zone information.

    | trip_id | miles | borough   | zone    |
    |---------|-------|-----------|---------|
    | 1       | 5.2   | Manhattan | Midtown |
    """
    import polars as pl

    return (
        pl.DataFrame(trips)
        .join(pl.DataFrame(zones), on='zone_id')
        .drop('zone_id')
        .to_arrow()
    )
```

## Key Decorators & Parameters

| Decorator/Param | Description |
|-----------------|-------------|
| `-> Annotated[pa.Table, OutputSchema]` | Output schema validation (mandatory) |
| `class OutputSchema(bauplan.TableSchema)` | Column names and types |
| `@bauplan.model(materialization_strategy=)` | `'REPLACE'`, `'APPEND'`, or omit (in-memory) |
| `@bauplan.python('3.13', pip={...})` | Python version and dependencies |
| `bauplan.Model('table', projection_schema=..., filter="...")` | I/O pushdown at storage level |
| `Annotated[float, bauplan.Parameter('name')]` | Run parameter |

## Data Quality Expectations

```python
from typing import Annotated

import bauplan
import pyarrow as pa


@bauplan.expectation()
@bauplan.python('3.13')
def test_no_null_ids(
    data: Annotated[pa.Table, bauplan.Model('trips_with_zones')],
) -> bool:
    """trip_id must not be null."""
    from bauplan.standard_expectations import expect_column_no_nulls

    return expect_column_no_nulls(data, 'trip_id')
```

## Running Pipelines

### CLI (Interactive Mode)

```bash
# Get your username
bauplan info

# Create and checkout dev branch
bauplan branch create <username>.<branch_name>
bauplan branch checkout <username>.<branch_name>

# Dry run (validate without materializing)
bauplan run --dry-run

# Execute pipeline
bauplan run
```

Strict mode is on by default: a failing expectation fails the run. There is no `--strict` flag; `--no-strict` disables it for the whole run.

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

1. **Always declare the output schema** with `-> Annotated[pa.Table, OutputSchema]`, reasoning from the input schemas to the expected output
2. **Use I/O pushdown** with `projection_schema` and `filter` in `bauplan.Model()`. `filter` only works on lakehouse tables; when the input is another model, filter inside the function body
3. **Add docstrings** with ASCII table showing expected output
4. **Only materialize final outputs** (intermediate models need no `materialization_strategy`)
