# Reading Data and Metadata, Reasoning About Lineage

Data in Bauplan is represented as tables, logically grouped by namespaces (e.g. `bauplan.titanic`).

> **IMPORTANT**: When not specified, the namespace is the default one: `bauplan`.

> **IMPORTANT**: When not specified, the branch is the default one: `main`.

Each table has a schema that defines its columns and their types, like a database.

## Data Evolution and Branching

Data evolution follows Git-like abstractions:
- A **main** data branch contains the production version of the tables
- Other data branches can be created for development and experimentation

> **IMPORTANT**: All branches aside from `main` have the structure `<user_name>.<branch_name>`, so you always need to remember or retrieve your Bauplan user name.

## Versioning and Time Travel

Every change to the lakehouse is versioned and identified by a commit on a branch. The hash of a commit can be used in all the methods supporting the `ref` parameter to time-travel to that specific commit.

For example:
- Use the commit hash in a query
- Query two versions of the same table across different branches using the branch name

## Available Metadata

On top of table data, all Bauplan metadata can be retrieved with the appropriate tool call. You can list:
- Namespaces
- Tables
- Branches
- Jobs
- Logs
- Commits

Check the tool list to find out all the options.

## Data Lineage

If asked to reason about data lineage (e.g., which tables are materialized by a specific job, or which table will change if a specific model in a pipeline is modified), you need a job ID to start from (ask if users don't have it).

### Steps for Lineage Analysis

1. First retrieve the job information with code
2. Then reason about the input and output tables of the job
3. If needed, retrieve further information about those tables

### Understanding Model Inputs and Outputs

**Python model example:**

```python
def clean_trips(
    data=bauplan.Model('trips')
):
    ...
```

This has:
- **Input**: the table `trips`
- **Output**: the table `clean_trips` (the name of the function)

**SQL model example (`trips.sql`):**

```sql
SELECT
    pickup_datetime,
    PULocationID,
    trip_miles
FROM taxi_fhvhv
...
```

This has:
- **Input**: the table `taxi_fhvhv`
- **Output**: the table `trips` (the name of the file)
