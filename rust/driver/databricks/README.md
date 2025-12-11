# adbc_driver_databricks

ADBC (Arrow Database Connectivity) driver for Databricks SQL Warehouses.

## Overview

This driver provides Arrow-native access to Databricks SQL Warehouses using
the Statement Execution API (SEA). It enables high-performance data access
without unnecessary data copies by returning results as Arrow RecordBatches.

## Features

- **Native Rust Implementation**: Pure Rust driver calling SEA REST API directly
- **Arrow-Native**: Return results as Arrow RecordBatches for zero-copy integration
- **High Performance**: Parallel chunk fetching with LZ4 compression support
- **ADBC 1.1.0 Compliant**: Implements all required ADBC traits

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
adbc_driver_databricks = "0.22"
```

## Usage

```rust
use adbc_core::{Driver, Database, Connection, Statement};
use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_driver_databricks::DatabricksDriver;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut driver = DatabricksDriver::new();

    let database = driver.new_database_with_opts([
        (OptionDatabase::Uri, OptionValue::String("https://xxx.cloud.databricks.com".into())),
        (OptionDatabase::Other("databricks.warehouse_id".into()), OptionValue::String("abc123def456".into())),
        (OptionDatabase::Other("databricks.token".into()), OptionValue::String("dapi...".into())),
    ])?;

    let mut connection = database.new_connection()?;
    let mut statement = connection.new_statement()?;

    statement.set_sql_query("SELECT * FROM my_table LIMIT 10")?;
    let reader = statement.execute()?;

    // Process RecordBatches from the reader
    for batch in reader {
        let batch = batch?;
        println!("Got batch with {} rows", batch.num_rows());
    }

    Ok(())
}
```

## Configuration

### Required Options

| Option | Description |
|--------|-------------|
| `uri` | Workspace URL (e.g., `https://xxx.cloud.databricks.com`) |
| `databricks.warehouse_id` | SQL Warehouse ID |
| `databricks.token` | Personal Access Token |

### Optional Options

| Option | Default | Description |
|--------|---------|-------------|
| `databricks.catalog` | None | Default catalog |
| `databricks.schema` | None | Default schema |
| `databricks.http.connect_timeout` | 10000 | Connect timeout (ms) |
| `databricks.http.read_timeout` | 300000 | Read timeout (ms) |
| `databricks.fetch.concurrency` | 8 | Parallel chunk fetchers |
| `databricks.fetch.compression` | `LZ4_FRAME` | Result compression |

### Environment Variables

Configuration can also be provided via environment variables:

| Variable | Maps To |
|----------|---------|
| `DATABRICKS_HOST` | `uri` |
| `DATABRICKS_WAREHOUSE_ID` | `databricks.warehouse_id` |
| `DATABRICKS_TOKEN` | `databricks.token` |
| `DATABRICKS_CATALOG` | `databricks.catalog` |
| `DATABRICKS_SCHEMA` | `databricks.schema` |

## Features

### TLS Backend

By default, the driver uses `rustls` for TLS. To use native TLS instead:

```toml
[dependencies]
adbc_driver_databricks = { version = "0.22", default-features = false, features = ["native-tls"] }
```

### FFI Export

To export the driver as a C dynamic library:

```toml
[dependencies]
adbc_driver_databricks = { version = "0.22", features = ["ffi"] }
```

## License

Apache License, Version 2.0
