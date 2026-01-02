# Ingest Data

Data is ingested from S3 files (parquet, csv or JSONL) into Bauplan tables in a two-step process.

## Step 1: Create the Table

If the table does not exist already, create it with `create_table`:
- The schema is automatically inferred based on the parquet/csv schema of the files in the S3 URI provided at creation time

## Step 2: Import the Data

Load the data into the desired table with `import_data`, passing the same S3 URI:
- If the table already exists, importing data is equivalent to append

## Best Practice: Write-Audit-Publish (WAP) Pattern

As a best practice, all data ingestion operations (with or without table creation) should happen with the Write-Audit-Publish pattern (WAP):

1. **Write**: Create a temporary ingestion data branch to host all the data operations (e.g., `<user_name>.ingestion_<current_epoch>`)
2. **Audit**: Perform data quality checks on the branch
3. **Publish**: Only after data quality checks have passed, merge the branch back to `main`
