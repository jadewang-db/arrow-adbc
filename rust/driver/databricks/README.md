# ADBC Databricks Driver

This crate provides an [ADBC](https://arrow.apache.org/adbc/) driver for
[Databricks SQL Warehouses](https://docs.databricks.com/sql/index.html)
using the Statement Execution API (SEA).

## Features

- Native Rust implementation calling SEA REST API directly
- Arrow-native: Returns results as Arrow RecordBatches
- Parallel chunk fetching with LZ4 compression support
- ADBC 1.1.0 compliant

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
adbc_databricks = "0.22.0"
```

## Usage

```rust,ignore
use adbc_core::Driver;
use adbc_databricks::DatabricksDriver;

// Create driver and database
let mut driver = DatabricksDriver::new();
let mut database = driver.new_database_with_opts([
    ("uri", "https://my-workspace.cloud.databricks.com"),
    ("databricks.warehouse_id", "abc123"),
    ("databricks.token", "dapi..."),
])?;

// Create connection and statement
let mut connection = database.new_connection()?;
let mut statement = connection.new_statement()?;

// Execute query
statement.set_sql_query("SELECT * FROM my_table")?;
let reader = statement.execute()?;

for batch in reader {
    // Process Arrow RecordBatches
}
```

## Configuration Options

### Database Options

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `uri` | String | Yes | Workspace URL (e.g., `https://xxx.cloud.databricks.com`) |
| `databricks.warehouse_id` | String | Yes | SQL Warehouse ID |
| `databricks.token` | String | Yes | Personal Access Token |
| `databricks.catalog` | String | No | Default catalog |
| `databricks.schema` | String | No | Default schema |

### Environment Variables

| Variable | Maps To |
|----------|---------|
| `DATABRICKS_HOST` | `uri` |
| `DATABRICKS_WAREHOUSE_ID` | `databricks.warehouse_id` |
| `DATABRICKS_TOKEN` | `databricks.token` |
| `DATABRICKS_CATALOG` | `databricks.catalog` |
| `DATABRICKS_SCHEMA` | `databricks.schema` |

## License

Licensed under the Apache License, Version 2.0.
