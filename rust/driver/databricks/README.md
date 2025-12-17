# ADBC Driver for Databricks

Arrow Database Connectivity (ADBC) driver for Databricks SQL Warehouses.

## Overview

This driver provides Arrow-native access to Databricks SQL Warehouses using the Statement Execution API (SEA). It enables high-performance data access with zero-copy integration.

## Features

- **Native Rust Implementation**: Pure Rust driver calling SEA REST API directly
- **Arrow-Native**: Returns results as Arrow RecordBatches
- **High Performance**: Parallel chunk fetching with LZ4 compression support
- **ADBC 1.1.0 Compliant**: Implements all required ADBC traits
- **Async I/O**: Uses Tokio runtime internally for efficient network operations

## Status

🚧 **Under Development** - This driver is currently being implemented.

## Planned Capabilities

### Phase 1 (MVP)
- PAT authentication
- Session management
- Query execution with ARROW_STREAM format
- Parallel chunk fetching for large results
- LZ4 decompression
- Metadata queries (GetInfo, GetObjects, GetTableSchema)

### Phase 2
- OAuth 2.0 / M2M token authentication
- Prepared statements with parameter binding
- Connection pooling

### Phase 3
- Bulk ingestion support
- Query progress tracking
- Advanced error handling and retries

## Configuration

Driver options:

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `uri` | String | Yes | Workspace URL (e.g., `https://xxx.cloud.databricks.com`) |
| `databricks.warehouse_id` | String | Yes | SQL Warehouse ID |
| `databricks.token` | String | Yes | Personal Access Token |
| `databricks.catalog` | String | No | Default catalog |
| `databricks.schema` | String | No | Default schema |

## Development

### Building

```bash
cargo build -p adbc-driver-databricks
```

### Testing

```bash
# Unit tests
cargo test -p adbc-driver-databricks

# Integration tests (requires Databricks credentials)
export DATABRICKS_TEST_CONFIG_FILE=/path/to/config.json
cargo test -p adbc-driver-databricks --ignored
```

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](../../../LICENSE) for details.
