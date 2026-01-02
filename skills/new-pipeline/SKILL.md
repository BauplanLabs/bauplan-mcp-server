---
name: "New Bauplan Pipeline"
description: "Create a new bauplan data pipeline project from scratch with SQL and Python models"
allowed-tools:
  - Bash(bauplan:*)
  - Read
  - Write
  - Glob
  - Grep
---

# Creating a New Bauplan Data Pipeline

This skill guides you through creating a new bauplan data pipeline project from scratch, including the project configuration and SQL/Python transformation models.

## CRITICAL: Branch Safety

> **NEVER run pipelines on `main` branch.** Always use a development branch.

Before ANY `bauplan run` command, you MUST:

1. **Check current branches**: `bauplan branch ls`
2. **Create a development branch** if one doesn't exist
3. **Run ONLY on your development branch**, never on `main`

### Branch Workflow

```bash
# 1. Get your username (required for branch naming)
bauplan info
# Look for "Username:" in the output

# 2. Checkout to main first
bauplan branch checkout main

# 3. Create a development branch (format: username.branch_name)
bauplan branch create <username>.<branch_name>

# 4. Checkout to your new branch
bauplan branch checkout <username>.<branch_name>

# 5. Now all commands run on this branch implicitly
bauplan run
```

> **Note**: The username from `bauplan info` is required for branch creation. Development branches follow the naming convention: `<username>.<branch_name>` (e.g., `john.feature-pipeline`). After checkout, all commands run on the current branch implicitly.

## Prerequisites

Before creating the pipeline, verify that:
1. **You have a development branch** (not `main`)
2. Source tables exist in the bauplan lakehouse
3. You understand the schema of the source tables

### CLI Quick Reference

The `bauplan` CLI is your primary tool. Use `--help` for guidance:

```bash
bauplan --help              # General help
bauplan run --help          # Help with running pipelines
bauplan query --help        # Help with querying data
bauplan table --help        # Help with table operations
bauplan branch --help       # Help with branch operations
...
```

### Verify Source Tables

Always check that source tables exist before writing models (when never specified, the default namespace is `bauplan`):

```bash
# Get table schema
bauplan table get <namespace>.<table_name>

# Query sample data
bauplan query "SELECT * FROM <namespace>.<table_name> LIMIT 5"
```

## Project Structure

A bauplan project is a folder containing:

```
my-project/
  bauplan_project.yml    # Required: project configuration
  models.sql             # Optional: SQL models
  models.py              # Optional: Python models
  expectations.py        # Optional: data quality tests
```

## bauplan_project.yml

Every project requires this configuration file:

```yaml
project:
  id: <unique-uuid>       # Generate a unique UUID
  name: <project_name>    # Descriptive name for the project
```

## SQL Models

SQL models are `.sql` files where:
- The **filename** becomes the output table name
- The **FROM clause** defines input tables
- Optional: Add materialization strategy as a comment

### Basic SQL Model

```sql
-- trips.sql
SELECT
    pickup_datetime,
    PULocationID,
    trip_miles
FROM taxi_fhvhv
WHERE pickup_datetime >= '2022-12-01'
```

Output table: `trips` (from filename)
Input table: `taxi_fhvhv` (from FROM clause)

### SQL Model with Materialization

```sql
-- bauplan: materialization_strategy=REPLACE

SELECT
  DATE_TRUNC('hour', event_time::TIMESTAMP) AS event_hour,
  event_type,
  product_id,
  brand,
  price
FROM public.ecommerce
```

## Python Models

Python models use decorators to define transformations:

```python
import bauplan

@bauplan.model()
@bauplan.python('3.11')
def my_model(
    data=bauplan.Model('source_table')
):
    # Transform data using Arrow tables
    import pyarrow as pa
    # ... transformation logic ...
    return result_table  # Must return Arrow table
```

### Key Decorators

- `@bauplan.model()` - Registers function as a model
- `@bauplan.model(materialization_strategy='REPLACE')` - Persist output to lakehouse
- `@bauplan.python('3.11', pip={'pandas': '1.5.3'})` - Specify Python version and packages

### I/O Pushdown with `columns` and `filter`

> **IMPORTANT**: Always use `columns` and `filter` parameters in `bauplan.Model()` to restrict the data read. This enables I/O pushdown, dramatically reducing the amount of data transferred and improving performance.

```python
bauplan.Model(
    'table_name',
    columns=['col1', 'col2', 'col3'],  # Only read these columns
    filter="date >= '2022-01-01'"       # Pre-filter at storage level
)
```

**Always specify:**
- `columns`: List only the columns your model actually needs
- `filter`: SQL-like filter expression to restrict rows at the storage level

### Basic Python Model

```python
import bauplan

@bauplan.model()
@bauplan.python('3.11', pip={'polars': '1.15.0'})
def clean_trips(
    # Use columns and filter for I/O pushdown
    data=bauplan.Model(
        'trips',
        columns=['pickup_datetime', 'PULocationID', 'trip_miles'],
        filter="trip_miles > 0"
    )
):
    import polars as pl

    df = pl.from_arrow(data)
    df = df.filter(pl.col('trip_miles') > 0.0)

    return df.to_arrow()
```

### Python Model with Multiple Inputs

```python
@bauplan.model()
@bauplan.python('3.11')
def trips_and_zones(
    # Always restrict columns and filter when possible
    trips=bauplan.Model(
        'taxi_fhvhv',
        columns=['pickup_datetime', 'PULocationID', 'trip_miles'],
        filter="pickup_datetime >= '2022-12-01'"
    ),
    zones=bauplan.Model(
        'taxi_zones',
        columns=['LocationID', 'Borough', 'Zone']
    ),
):
    result = trips.join(zones, 'PULocationID', 'LocationID')
    return result.combine_chunks()
```

## Running the Pipeline

> **REMINDER: NEVER run on `main`. Always checkout to your development branch first.**

```bash
# 1. Check current branch - confirm you're NOT on main
bauplan branch ls

# 2. If needed, checkout to main and create a new branch
bauplan branch checkout main
bauplan branch create <username>.<branch_name>

# 3. Checkout to your development branch
bauplan branch checkout <username>.<branch_name>

# 4. Dry run to validate
bauplan run --dry-run

# 5. Run pipeline (runs on current branch)
bauplan run
```

## Workflow Summary

1. **Get username**: Run `bauplan info` to get your username
2. **Checkout main**: `bauplan branch checkout main`
3. **Create development branch**: `bauplan branch create <username>.<branch_name>`
4. **Checkout branch**: `bauplan branch checkout <username>.<branch_name>`
5. **Verify sources**: Use `bauplan table` and `bauplan query` to check source tables
6. **Create project folder**: With `bauplan_project.yml`
7. **Write models**: SQL files and/or Python files
8. **Dry run**: Validate with `bauplan run --dry-run`
9. **Run pipeline**: Execute with `bauplan run`

## Advanced Examples

See [examples.md](examples.md) for:
- APPEND materialization strategy
- Complex SQL aggregations
- DuckDB queries in Python models
- Data quality expectations
- Multi-stage pipelines
