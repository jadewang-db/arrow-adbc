# Quick Start Guide: Databricks Rust ADBC Driver

Get started with the Databricks Rust ADBC driver in 5 minutes.

## Step 1: Prerequisites

You need:
- A Databricks workspace with an active SQL Warehouse
- A Personal Access Token (PAT) from Databricks
- Rust toolchain installed (1.75+)

### Getting Your Credentials

1. **Workspace URL**: Found in your browser URL when logged into Databricks
   - Format: `https://your-workspace.cloud.databricks.com`

2. **SQL Warehouse ID**:
   - Go to: SQL Warehouses → [Your Warehouse] → Connection Details
   - Copy the `http_path` value after `/sql/1.0/warehouses/`
   - Example: If `http_path = /sql/1.0/warehouses/abc123def456`, your warehouse ID is `abc123def456`

3. **Personal Access Token**:
   - Go to: User Settings → Access Tokens → Generate New Token
   - Save it securely (you won't see it again!)

## Step 2: Set Environment Variables

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_WAREHOUSE_ID="abc123def456"
export DATABRICKS_TOKEN="dapi_your_token_here"

# Optional: Set default catalog and schema
export DATABRICKS_CATALOG="main"
export DATABRICKS_SCHEMA="default"
```

Or use the provided template:

```bash
cd rust/driver/databricks/examples
cp .env.template .env
# Edit .env with your credentials
source .env
```

## Step 3: Run Your First Example

```bash
cd rust/driver/databricks
cargo run --example simple_query
```

You should see:
```
Connecting to Databricks...
Connected! Session: Some("01234567-89ab-cdef-0123-456789abcdef")

Executing query...

Schema:
  - message: Utf8
  - answer: Int32

Results:
  Batch: 1 rows × 2 columns
  Row 0: StringArray ["Hello from Databricks!"] PrimitiveArray<Int32> [42]

Done!
```

## Step 4: Run the Full Demo

```bash
cargo run --example demo_app
```

This comprehensive demo shows:
- Connection management
- Query execution
- Data type handling
- Schema introspection
- Error handling

## Step 5: Try Table Operations (Requires Catalog/Schema)

If you have catalog and schema configured:

```bash
cargo run --example table_operations
```

This shows:
- Creating tables
- Inserting data
- Updating and deleting rows
- Querying results

## Common Usage Patterns

### Basic Query

```rust
use adbc_core::{Driver, Database, Connection, Statement};
use adbc_databricks::DatabricksDriver;

// Create driver and connect
let mut driver = DatabricksDriver::new();
let db = driver.new_database_with_opts([
    ("uri", "https://your-workspace.cloud.databricks.com"),
    ("databricks.warehouse_id", "your-warehouse-id"),
    ("databricks.token", "dapi_your_token"),
])?;

let mut conn = db.new_connection()?;

// Execute query
let mut stmt = conn.new_statement()?;
stmt.set_sql_query("SELECT * FROM my_table LIMIT 10")?;

// Read results as Arrow RecordBatches
let reader = stmt.execute()?;
for batch in reader {
    let batch = batch?;
    println!("Got {} rows", batch.num_rows());
    // Process batch...
}
```

### With Catalog and Schema

```rust
let db = driver.new_database_with_opts([
    ("uri", "https://your-workspace.cloud.databricks.com"),
    ("databricks.warehouse_id", "your-warehouse-id"),
    ("databricks.token", "dapi_your_token"),
    ("databricks.catalog", "main"),
    ("databricks.schema", "default"),
])?;
```

### DML Operations (INSERT/UPDATE/DELETE)

```rust
let mut stmt = conn.new_statement()?;
stmt.set_sql_query("INSERT INTO my_table VALUES (1, 'test')")?;

match stmt.execute_update() {
    Ok(Some(rows)) => println!("Affected {} rows", rows),
    Ok(None) => println!("Statement executed, row count unknown"),
    Err(e) => eprintln!("Error: {}", e.message),
}
```

## Troubleshooting

### Error: "401 Unauthorized"
- Check your token is valid and not expired
- Verify you copied the complete token
- Try generating a new token

### Error: "Cannot find warehouse"
- Verify the warehouse ID is correct
- Check the warehouse is running (not stopped)
- Ensure you have permission to use the warehouse

### Error: "Table not found"
- Check the catalog/schema/table names
- Verify you have access permissions
- Try fully qualifying the table: `catalog.schema.table`

### Error: "Connection timeout"
- Check your network connectivity
- Verify the workspace URL is correct
- Ensure no firewall is blocking the connection

## Next Steps

- Explore the examples in `rust/driver/databricks/examples/`
- Read the full documentation at the top of each example
- Check `examples/README.md` for more detailed usage information
- Look at the E2E tests in `tests/e2e_tests.rs` for advanced patterns

## Using in Your Project

Add to your `Cargo.toml`:

```toml
[dependencies]
adbc_core = "0.22"
adbc_databricks = "0.22"
arrow-array = ">=53.1.0, <58"
arrow-schema = ">=53.1.0, <58"
```

Then import and use:

```rust
use adbc_core::{Driver, Database, Connection, Statement};
use adbc_databricks::DatabricksDriver;
```

## Additional Resources

- [Full Examples README](examples/README.md)
- [E2E Tests](tests/e2e_tests.rs)
- [Databricks SQL Documentation](https://docs.databricks.com/sql/)
- [Apache Arrow ADBC Specification](https://arrow.apache.org/adbc/)
