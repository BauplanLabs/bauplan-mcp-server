# Data Pipelines

Pipelines are logically organized by projects: each project is a folder with:
1. One `bauplan_project.yml`
2. One or more pipeline files (`.sql` or `.py`)

Pipelines are defined by chaining together **models** which are SQL queries or Python decorated functions, whose inputs are either source tables in the data lake or other models in the pipeline.

A function may have multiple models as input parents, but always returns one output model:
- In **SQL**, the inputs are the tables needed to execute the query (what needs to be queried as I/O), and output is the result of running the query itself
- In **Python**, inputs are expressed as function inputs, and output is the return value of the function

When the code is submitted to Bauplan with a `run`, the system automatically reconstructs the DAG of the models by parsing the SQL and Python files and understanding the connections between them.

## Sample Project Structure

A sample project folder contains the following three files, heavily commented - make sure you read and understand the comments and the syntax.

> **NOTE**: This is not to be copied and pasted in the final answer. It is just an example of a minimal project, to be used as reference when creating your own code to make sure the project structure is correct and that you know the basic syntax of a Bauplan file when deciding to write SQL or Python models.

### bauplan_project.yml

```yaml
project:
  # id should be a unique uuid
  id: 8048ce66-49c6-4f43-a487-afe22f6b05d3
  # name should be a descriptive name for the project
  name: project_name
```

### trips.sql

```sql
SELECT
    pickup_datetime,
    PULocationID,
    trip_miles
-- the FROM clause implicitly defines the input of this SQL model
-- which in this case is the taxi_fhvhv table in the data lake
FROM taxi_fhvhv
WHERE pickup_datetime >= '2022-12-01T00:00:00-05:00'
AND pickup_datetime < '2022-12-31T00:00:00-05:00'
```

### model.py

```python
import bauplan
from bauplan.standard_expectations import expect_column_no_nulls

# this decorator registers the function as a Bauplan model
# the materialization strategy determines how the model's output is stored
# if the argument is omitted, no materialization will occur, other options are
# 'REPLACE' or 'APPEND'
@bauplan.model(materialization_strategy='REPLACE')
# this decorator specifies the Python version and any required packages
# make sure to specify packages AND their versions inside the dictionary
@bauplan.python('3.11', pip={'polars': '1.15.0'})
def clean_trips(
    # the function defines the trips model - the SQL above -
    # as its input, so that Bauplan knows how to build the DAG
    data=bauplan.Model('trips')
):
    # the function body is arbitrary Python code, written by the user to transform input models
    # into one output model
    import polars as pl
    import math
    # every print statement gets logged and it's retrievable through the log-related APIs
    print(f'\n{round(data.nbytes / math.pow(1024, 3), 3)} GB, {data.num_rows} rows\n')
    df = pl.from_arrow(data)
    df = df.filter(pl.col('trip_miles') > 0.0)
    # model ALWAYS returns an Arrow table, pyarrow is available, no need to import it
    return df.to_arrow()


# this decorator registers the function as a Bauplan expectation
# the expectation can be associated to one or more models in the pipeline
# and will be executed after the model is run - it should return a boolean
# and can optionally, inside the body, raise an AssertionError
@bauplan.expectation()
@bauplan.python('3.11')
def test_null_values_location_id(data=bauplan.Model('clean_trips')):
    column_to_check = 'PULocationID'
    # re-use built-in expectation to check for null values in a column
    _is_expectation_correct = expect_column_no_nulls(data, column_to_check)
    # if the expectation is not met, raise an AssertionError with a descriptive message
    assert _is_expectation_correct, f'expectation test failed: we expected {column_to_check} to have no null values'
    # return a boolean to indicate if the expectation passed or failed
    return _is_expectation_correct
```
