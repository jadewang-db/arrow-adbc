# Databricks Rust ADBC Driver Examples

This directory contains example applications demonstrating how to use the Databricks Rust ADBC driver.

## Prerequisites

1. **Databricks Workspace**: You need access to a Databricks workspace with a SQL Warehouse
2. **Personal Access Token (PAT)**: Generate a PAT from your Databricks workspace
   - Go to User Settings → Access Tokens → Generate New Token
3. **SQL Warehouse**: Note your SQL Warehouse ID
   - Go to SQL Warehouses → Your Warehouse → Connection Details

## Setting Up Environment Variables

The demo applications use environment variables for configuration. Set these before running:

```bash
# Required: Your Databricks workspace URL
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"

# Required: Your SQL Warehouse ID (found in warehouse connection details)
export DATABRICKS_WAREHOUSE_ID="1234567890abcdef"

# Required: Your Personal Access Token
export DATABRICKS_TOKEN="dapi_your_token_here_1234567890abcdef"

# Optional: Default catalog (e.g., "main", "hive_metastore")
export DATABRICKS_CATALOG="main"

# Optional: Default schema (e.g., "default", "information_schema")
export DATABRICKS_SCHEMA="default"
```

### Alternative: Using `.env` file

You can also create a `.env` file in the examples directory (this file is gitignored):

```bash
# Create .env file
cat > .env << 'EOF'
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_WAREHOUSE_ID=1234567890abcdef
DATABRICKS_TOKEN=dapi_your_token_here_1234567890abcdef
DATABRICKS_CATALOG=main
DATABRICKS_SCHEMA=default
EOF

# Load environment variables
source .env
```

## Running the Examples

### Demo Application

The main demo application (`demo_app.rs`) demonstrates:
- Connecting to Databricks
- Executing queries
- Reading Arrow RecordBatch results
- Working with various data types
- Schema introspection
- Error handling

Run it with:

```bash
cargo run --example demo_app
```

Expected output:
```
=== Databricks Rust ADBC Driver Demo ===

Step 1: Loading configuration from environment variables...
  Host: https://your-workspace.cloud.databricks.com
  Warehouse ID: 1234567890abcdef
  Token: dapi_your_...
  Default Catalog: main
  Default Schema: default

Step 2: Creating Databricks ADBC driver...
  Driver created successfully

Step 3: Creating database with connection options...
  Database created successfully

Step 4: Creating connection to Databricks...
  Connection established successfully
  Session ID: 01234567-89ab-cdef-0123-456789abcdef
  Current Catalog: main
  Current Schema: default

Step 5: Executing simple query...
  Query: SELECT 42 AS answer, 'Hello, Databricks!' AS message
  Schema:
    - answer: Int32
    - message: Utf8
  Results:
    Batch with 1 rows
    Row 0: answer: PrimitiveArray<Int32> [42] message: StringArray ["Hello, Databricks!"]

...
```

## Common Use Cases

### Simple Query

```rust
use adbc_core::{Driver, Database, Connection, Statement};
use adbc_databricks::DatabricksDriver;

let mut driver = DatabricksDriver::new();
let mut database = driver.new_database_with_opts([
    ("uri", "https://your-workspace.cloud.databricks.com"),
    ("databricks.warehouse_id", "your-warehouse-id"),
    ("databricks.token", "dapi_your_token"),
])?;

let mut connection = database.new_connection()?;
let mut statement = connection.new_statement()?;

statement.set_sql_query("SELECT * FROM my_table LIMIT 10")?;
let reader = statement.execute()?;

for batch in reader {
    let batch = batch?;
    println!("Got {} rows", batch.num_rows());
}
```

### Working with Catalog and Schema

```rust
let mut database = driver.new_database_with_opts([
    ("uri", "https://your-workspace.cloud.databricks.com"),
    ("databricks.warehouse_id", "your-warehouse-id"),
    ("databricks.token", "dapi_your_token"),
    ("databricks.catalog", "main"),
    ("databricks.schema", "default"),
])?;

let connection = database.new_connection()?;

// Now queries will use main.default by default
```

### Schema Introspection

```rust
use adbc_core::Connection;

let connection = database.new_connection()?;

// Get schema for a table
let schema = connection.get_table_schema(
    Some("main"),
    Some("default"),
    "my_table"
)?;

for field in schema.fields() {
    println!("Column: {} (type: {:?})", field.name(), field.data_type());
}
```

### Large Result Sets

The driver automatically handles large result sets using Databricks' external links (presigned URLs to cloud storage):

```rust
// This query returns 1 million rows but streams efficiently
statement.set_sql_query("SELECT * FROM range(1000000)")?;
let reader = statement.execute()?;

let mut total_rows = 0;
for batch in reader {
    let batch = batch?;
    total_rows += batch.num_rows();
    // Process batch
}
println!("Processed {} rows", total_rows);
```

## Troubleshooting

### Authentication Errors

If you get 401/403 errors:
- Verify your token is valid and hasn't expired
- Check that your token has the necessary permissions
- Ensure the workspace URL is correct

### Connection Errors

If you can't connect:
- Verify the SQL Warehouse is running (not stopped)
- Check that your warehouse ID is correct
- Ensure network connectivity to the Databricks workspace

### Query Errors

If queries fail:
- Check the SQL syntax is compatible with Databricks SQL/Spark SQL
- Verify the catalog/schema/table names exist and you have access
- Check the warehouse has sufficient permissions

## Additional Resources

- [Databricks SQL Documentation](https://docs.databricks.com/sql/index.html)
- [ADBC Specification](https://arrow.apache.org/adbc/)
- [Arrow Rust Documentation](https://docs.rs/arrow/)
