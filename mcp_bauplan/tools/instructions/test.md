# Data Expectations and Quality Tests

Data expectations are a powerful way to ensure data quality and consistency in your pipelines. They allow you to define rules and checks that your data must pass before it can be considered valid.

This helps catch issues early and ensures that your data processing steps are working as intended.

## Defining Expectations

Expectations can be defined using:
- **Built-in functions** from the `bauplan.standard_expectations` module
- **Custom expectations** to suit your specific needs

Expectations are defined as Python functions decorated with `@bauplan.expectation()`, and can be associated with one or more models in your pipeline.

## Execution

Expectations are executed after the model is run, directly in memory, and should return a boolean indicating whether the expectation passed or failed.

If an expectation fails, it can raise an `AssertionError` with a descriptive message to help diagnose the issue.

## Built-in Expectations

The `bauplan.standard_expectations` module contains many built-in expectations:

- `expect_column_accepted_values`
- `expect_column_all_null`
- `expect_column_all_unique`
- `expect_column_equal_concatenation`
- `expect_column_mean_greater_or_equal_than`
- `expect_column_mean_greater_than`
- `expect_column_mean_smaller_or_equal_than`
- `expect_column_mean_smaller_than`
- `expect_column_no_nulls`
- `expect_column_not_unique`
- `expect_column_some_null`

## Best Practice: Separate Expectations File

Put all your expectations in a separate file named `expectations.py`:
- The file should be in the same folder as your `bauplan_project.yml` and other pipeline files
- It will be automatically detected and executed by Bauplan when you run your project

## Example: expectations.py

```python
import bauplan
from bauplan.standard_expectations import expect_column_no_nulls

@bauplan.expectation()
@bauplan.python('3.11')
def test_null_values_location_id(data=bauplan.Model('model_to_be_tested')):
    _is_expectation_correct = expect_column_no_nulls(data, 'PULocationID')
    # if the expectation is not met, raise an AssertionError with a descriptive message
    assert _is_expectation_correct, f'expectation test failed: we expected PULocationID to have no null values'
    # return a boolean to indicate if the expectation passed or failed
    return _is_expectation_correct
```
