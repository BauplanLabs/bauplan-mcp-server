# Advanced Pipeline Examples

This document contains advanced examples and edge cases for bauplan pipelines.

## Materialization Strategies

### REPLACE Strategy

Replaces the entire table on each run. Use for dimension tables or complete refreshes:

```sql
-- product_catalog.sql
-- bauplan: materialization_strategy=REPLACE

SELECT
    product_id,
    name,
    category,
    price,
    updated_at
FROM raw_products
WHERE is_active = true
```

### APPEND Strategy

Appends new data to existing table. Use for fact tables or incremental loads:

```sql
-- daily_events.sql
-- bauplan: materialization_strategy=APPEND

SELECT
    event_id,
    event_type,
    user_id,
    created_at
FROM raw_events
WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
```

Python equivalent:

```python
@bauplan.model(materialization_strategy='APPEND')
@bauplan.python('3.11')
def daily_metrics(
    # Use filter for I/O pushdown - only read recent data
    data=bauplan.Model(
        'events',
        columns=['event_id', 'event_type', 'user_id', 'amount', 'created_at'],
        filter="created_at >= CURRENT_DATE - INTERVAL '1 day'"
    )
):
    # Process only new data
    return data
```

## Complex SQL Aggregations

### Hourly Metrics with Window Functions

```sql
-- hourly_metrics.sql
-- bauplan: materialization_strategy=REPLACE

SELECT
    DATE_TRUNC('hour', event_time::TIMESTAMP) AS event_hour,
    event_type,
    COUNT(*) AS event_count,
    COUNT(DISTINCT user_id) AS unique_users,
    SUM(CASE WHEN event_type = 'purchase' THEN amount ELSE 0 END) AS revenue,
    LAG(COUNT(*)) OVER (ORDER BY DATE_TRUNC('hour', event_time::TIMESTAMP)) AS prev_hour_count
FROM events
GROUP BY 1, 2
ORDER BY 1 DESC
```

### Multi-Table Joins

```sql
-- order_details.sql

SELECT
    o.order_id,
    o.order_date,
    c.customer_name,
    p.product_name,
    oi.quantity,
    oi.unit_price,
    oi.quantity * oi.unit_price AS line_total
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN customers c ON o.customer_id = c.customer_id
JOIN products p ON oi.product_id = p.product_id
WHERE o.status = 'completed'
```

## DuckDB in Python Models

Use DuckDB for SQL-like transformations in Python. Always use `columns` and `filter` for I/O pushdown:

```python
@bauplan.python('3.11', pip={'duckdb': '1.0.0'})
@bauplan.model(materialization_strategy='REPLACE')
def purchase_analytics(
    # Use columns and filter for I/O pushdown
    events=bauplan.Model(
        'ecommerce_events',
        columns=['user_session', 'event_time', 'event_type', 'price'],
        filter="event_type = 'purchase'"
    )
):
    import duckdb

    con = duckdb.connect()
    query = """
        SELECT
            user_session AS purchase_session,
            DATE_TRUNC('hour', event_time) AS event_hour,
            COUNT(*) AS session_count,
            SUM(price) AS total_revenue,
            AVG(price) AS avg_order_value
        FROM events
        GROUP BY 1, 2
        ORDER BY 2 ASC
    """
    return con.execute(query).arrow()
```

### Multi-Input DuckDB Analysis

```python
@bauplan.python('3.11', pip={'duckdb': '1.0.0'})
@bauplan.model(materialization_strategy='REPLACE')
def brand_metrics(
    # Always specify columns needed for the analysis
    sessions=bauplan.Model(
        'purchase_sessions',
        columns=['purchase_session', 'event_hour']
    ),
    events=bauplan.Model(
        'ecommerce_events',
        columns=['brand', 'user_session', 'event_type', 'price']
    )
):
    import duckdb

    con = duckdb.connect()
    query = """
        SELECT
            e.brand,
            COUNT(DISTINCT e.user_session) AS unique_sessions,
            COUNT(DISTINCT s.purchase_session) AS orders,
            SUM(CASE WHEN e.event_type = 'purchase' THEN e.price ELSE 0 END) AS revenue,
            ROUND(revenue / NULLIF(orders, 0), 2) AS avg_order_value
        FROM events e
        LEFT JOIN sessions s
            ON e.user_session = s.purchase_session
        GROUP BY 1
        ORDER BY revenue DESC
    """
    return con.execute(query).arrow()
```

## I/O Pushdown with Column Selection and Filtering

> **CRITICAL**: Always use `columns` and `filter` parameters to enable I/O pushdown. This restricts data at the storage level, dramatically reducing data transfer and improving performance.

```python
@bauplan.model()
@bauplan.python('3.11')
def optimized_model(
    trips=bauplan.Model(
        'taxi_fhvhv',
        columns=[
            'pickup_datetime',
            'dropoff_datetime',
            'PULocationID',
            'DOLocationID',
            'trip_miles',
            'base_passenger_fare'
        ],
        filter="pickup_datetime >= '2022-12-01' AND pickup_datetime < '2023-01-01'"
    ),
    zones=bauplan.Model(
        'taxi_zones',
        columns=['LocationID', 'Borough', 'Zone']
    ),
):
    result = trips.join(zones, 'PULocationID', 'LocationID')
    return result.combine_chunks()
```

## Data Quality Expectations

### Basic Expectations File

Create `expectations.py` in your project folder:

```python
import bauplan
from bauplan.standard_expectations import (
    expect_column_no_nulls,
    expect_column_all_unique,
    expect_column_accepted_values
)

@bauplan.expectation()
@bauplan.python('3.11')
def test_no_null_ids(data=bauplan.Model('clean_orders')):
    result = expect_column_no_nulls(data, 'order_id')
    assert result, 'order_id must not contain null values'
    return result

@bauplan.expectation()
@bauplan.python('3.11')
def test_unique_order_ids(data=bauplan.Model('clean_orders')):
    result = expect_column_all_unique(data, 'order_id')
    assert result, 'order_id must be unique'
    return result

@bauplan.expectation()
@bauplan.python('3.11')
def test_valid_status(data=bauplan.Model('clean_orders')):
    valid_statuses = ['pending', 'processing', 'completed', 'cancelled']
    result = expect_column_accepted_values(data, 'status', valid_statuses)
    assert result, f'status must be one of {valid_statuses}'
    return result
```

### Custom Expectation

```python
@bauplan.expectation()
@bauplan.python('3.11', pip={'pyarrow': '14.0.0'})
def test_positive_revenue(data=bauplan.Model('sales_metrics')):
    import pyarrow.compute as pc

    revenue_col = data.column('revenue')
    min_revenue = pc.min(revenue_col).as_py()

    is_valid = min_revenue >= 0
    assert is_valid, f'Revenue must be non-negative, found min: {min_revenue}'
    return is_valid
```

### Statistical Expectations

```python
from bauplan.standard_expectations import (
    expect_column_mean_greater_than,
    expect_column_mean_smaller_than
)

@bauplan.expectation()
@bauplan.python('3.11')
def test_reasonable_trip_distance(data=bauplan.Model('clean_trips')):
    # Average trip should be between 1 and 50 miles
    lower_bound = expect_column_mean_greater_than(data, 'trip_miles', 1.0)
    upper_bound = expect_column_mean_smaller_than(data, 'trip_miles', 50.0)

    result = lower_bound and upper_bound
    assert result, 'Average trip distance out of expected range'
    return result
```

## Multi-Stage Pipeline Example

A complete e-commerce analytics pipeline:

```
ecommerce-pipeline/
  bauplan_project.yml
  staging.sql           # Stage 1: Clean raw data
  models.py             # Stage 2-3: Transform and aggregate
  expectations.py       # Data quality checks
```

### bauplan_project.yml

```yaml
project:
  id: 550e8400-e29b-41d4-a716-446655440000
  name: ecommerce_analytics
```

### staging.sql

```sql
-- bauplan: materialization_strategy=REPLACE

SELECT
    event_id,
    LOWER(event_type) AS event_type,
    product_id,
    COALESCE(brand, 'Unknown') AS brand,
    CAST(price AS DECIMAL(10,2)) AS price,
    user_id,
    user_session,
    event_time::TIMESTAMP AS event_time
FROM raw_ecommerce_events
WHERE event_time IS NOT NULL
  AND price > 0
```

### models.py

```python
import bauplan

@bauplan.python('3.11', pip={'duckdb': '1.0.0'})
@bauplan.model(materialization_strategy='REPLACE')
def session_metrics(
    # Use columns for I/O pushdown - only read needed columns
    staging=bauplan.Model(
        'staging',
        columns=['user_session', 'event_time', 'product_id', 'event_type', 'price']
    )
):
    import duckdb
    con = duckdb.connect()

    query = """
        SELECT
            user_session,
            MIN(event_time) AS session_start,
            MAX(event_time) AS session_end,
            COUNT(*) AS total_events,
            COUNT(DISTINCT product_id) AS products_viewed,
            SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases,
            SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) AS session_revenue
        FROM staging
        GROUP BY 1
    """
    return con.execute(query).arrow()


@bauplan.python('3.11', pip={'duckdb': '1.0.0'})
@bauplan.model(materialization_strategy='REPLACE')
def daily_summary(
    # Use columns and filter for I/O pushdown
    sessions=bauplan.Model(
        'session_metrics',
        columns=['session_start', 'purchases', 'session_revenue'],
        filter="purchases > 0"
    )
):
    import duckdb
    con = duckdb.connect()

    query = """
        SELECT
            DATE_TRUNC('day', session_start) AS date,
            COUNT(*) AS total_sessions,
            SUM(purchases) AS total_purchases,
            ROUND(SUM(purchases)::FLOAT / COUNT(*) * 100, 2) AS conversion_rate,
            SUM(session_revenue) AS total_revenue,
            ROUND(AVG(session_revenue), 2) AS avg_session_revenue
        FROM sessions
        GROUP BY 1
        ORDER BY 1
    """
    return con.execute(query).arrow()
```

### expectations.py

```python
import bauplan
from bauplan.standard_expectations import expect_column_no_nulls

@bauplan.expectation()
@bauplan.python('3.11')
def test_staging_completeness(data=bauplan.Model('staging')):
    for col in ['event_id', 'user_session', 'event_time']:
        result = expect_column_no_nulls(data, col)
        assert result, f'{col} contains null values'
    return True

@bauplan.expectation()
@bauplan.python('3.11')
def test_valid_conversion_rate(data=bauplan.Model('daily_summary')):
    import pyarrow.compute as pc

    conversion = data.column('conversion_rate')
    max_rate = pc.max(conversion).as_py()

    is_valid = max_rate <= 100
    assert is_valid, f'Conversion rate cannot exceed 100%, found: {max_rate}'
    return is_valid
```

## Available Built-in Expectations

Reference for `bauplan.standard_expectations`:

| Function | Description |
|----------|-------------|
| `expect_column_no_nulls` | Column has no null values |
| `expect_column_all_null` | Column is entirely null |
| `expect_column_some_null` | Column has at least one null |
| `expect_column_all_unique` | All values in column are unique |
| `expect_column_not_unique` | Column has duplicate values |
| `expect_column_accepted_values` | Values are within allowed set |
| `expect_column_mean_greater_than` | Mean exceeds threshold |
| `expect_column_mean_smaller_than` | Mean below threshold |
| `expect_column_mean_greater_or_equal_than` | Mean >= threshold |
| `expect_column_mean_smaller_or_equal_than` | Mean <= threshold |
| `expect_column_equal_concatenation` | Column equals concatenation of others |
