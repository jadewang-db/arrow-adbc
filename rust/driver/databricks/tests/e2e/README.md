# E2E Tests for Databricks Rust ADBC Driver

This directory contains end-to-end tests that validate the Databricks ADBC driver against a real Databricks SQL Warehouse.

## Prerequisites

Before running E2E tests, you need:

1. **Databricks Workspace** with Unity Catalog enabled
2. **SQL Warehouse** (Serverless or Classic)
3. **Personal Access Token (PAT)** with appropriate permissions
4. **Test catalog and schema** (see [Test Data Setup](#test-data-setup))

## Configuration

### Configuration File Format

Create a JSON configuration file with your Databricks credentials. The format matches the C# ADBC driver for consistency:

```json
{
  "environment": "Databricks",
  "uri": "https://your-workspace.cloud.databricks.com/sql/1.0/warehouses/YOUR_WAREHOUSE_ID",
  "token": "dapi...",
  "query": "select count(*) from `main`.`your_schema`.`your_table`",
  "type": "databricks",
  "trace": "true",
  "expectedResults": 1,
  "metadata": {
    "catalog": "main",
    "schema": "your_schema",
    "table": "your_table",
    "expectedColumnCount": 3
  }
}
```

### Configuration Fields

| Field | Required | Description |
|-------|----------|-------------|
| `environment` | Yes | Environment name (e.g., "Databricks") |
| `uri` | Yes | Full URI including warehouse: `https://{host}/sql/1.0/warehouses/{warehouse_id}` |
| `token` | Yes | Personal Access Token (starts with `dapi`) |
| `query` | No | Default test query |
| `type` | Yes | Driver type (use "databricks") |
| `trace` | No | Enable trace logging ("true" or "false") |
| `expectedResults` | No | Expected row count for test query |
| `metadata.catalog` | No | Test catalog name |
| `metadata.schema` | No | Test schema name |
| `metadata.table` | No | Test table name |
| `metadata.expectedColumnCount` | No | Expected column count for metadata tests |

### Example Configuration

Copy `databricks.example.json` and fill in your values:

```bash
cp tests/e2e/databricks.example.json ~/.databricks/test_config.json
# Edit ~/.databricks/test_config.json with your values
```

## Test Data Setup

Before running E2E tests, create the test tables in your Databricks workspace:

1. Connect to your Databricks workspace (SQL Editor or Notebook)
2. Run the `setup.sql` script to create test catalog, schema, and tables
3. Update your configuration file with the catalog/schema names

```sql
-- Run in Databricks SQL Editor
-- See setup.sql for full script
CREATE CATALOG IF NOT EXISTS e2e_tests;
CREATE SCHEMA IF NOT EXISTS e2e_tests.rust_adbc_driver;
USE e2e_tests.rust_adbc_driver;
-- ... (see setup.sql for table creation)
```

## Running E2E Tests

### Set Configuration Path

```bash
export DATABRICKS_TEST_CONFIG_FILE=/path/to/your/config.json
```

### Run All E2E Tests

E2E tests are marked with `#[ignore]` and require the `--ignored` flag:

```bash
# From the rust directory
cd /path/to/arrow-adbc/rust

# Run all E2E tests
cargo test -p adbc_databricks --test e2e_tests -- --ignored

# Run with verbose output
cargo test -p adbc_databricks --test e2e_tests -- --ignored --nocapture

# Run tests sequentially (recommended for E2E tests)
cargo test -p adbc_databricks --test e2e_tests -- --ignored --test-threads=1
```

### Run Specific Tests

```bash
# Run a specific E2E test
cargo test -p adbc_databricks --test e2e_tests test_e2e_config_and_connect -- --ignored --nocapture

# Run tests matching a pattern
cargo test -p adbc_databricks --test e2e_tests e2e -- --ignored
```

### Run All Tests (Unit + E2E)

```bash
# Run unit tests (always run, no config needed)
cargo test -p adbc_databricks

# Run E2E unit tests only (no config needed, tests config parsing)
cargo test -p adbc_databricks --test e2e_tests

# Run all tests including ignored E2E tests (requires config)
cargo test -p adbc_databricks --test e2e_tests -- --include-ignored
```

## Test Categories

### Configuration Tests

Tests that verify configuration loading and parsing:

- `test_e2e_config_loads_successfully` - Verify config loads from JSON
- `test_e2e_config_uri_parsing` - Verify URI parsing extracts host/warehouse
- `test_e2e_config_validates` - Verify configuration validation

### Connection Tests (Future Work Items)

Tests that verify connection to Databricks:

- `e2e_connection_open_creates_session`
- `e2e_connection_close_terminates_session`
- `e2e_connection_set_catalog_changes_context`

### Query Tests (Future Work Items)

Tests that execute queries:

- `e2e_query_select_one_returns_result`
- `e2e_query_empty_result_returns_schema`
- `e2e_result_external_large_query`

## Troubleshooting

### Common Issues

#### "Environment variable not set"

```bash
# Make sure the environment variable is set
export DATABRICKS_TEST_CONFIG_FILE=/path/to/config.json
echo $DATABRICKS_TEST_CONFIG_FILE
```

#### "File not found"

```bash
# Verify the file exists
ls -la $DATABRICKS_TEST_CONFIG_FILE

# Verify the file is valid JSON
cat $DATABRICKS_TEST_CONFIG_FILE | jq .
```

#### "Failed to parse configuration"

Common causes:
- Invalid JSON syntax (missing commas, quotes)
- Missing required fields (`uri`, `token`, `type`)
- Invalid URI format

Validate your JSON:
```bash
cat $DATABRICKS_TEST_CONFIG_FILE | python -m json.tool
```

#### "Invalid URI format"

The URI must follow this format:
```
https://{workspace-host}/sql/1.0/warehouses/{warehouse-id}
```

Example:
```
https://my-workspace.cloud.databricks.com/sql/1.0/warehouses/abc123def456
```

#### Authentication Errors

- Verify your PAT token is valid and not expired
- Check token permissions for SQL Warehouse access
- Ensure the warehouse is running (not stopped)

### Debug Mode

Enable trace logging in your configuration:

```json
{
  "trace": "true"
}
```

Run tests with output capture disabled:
```bash
cargo test -p adbc_databricks -- --ignored --nocapture
```

### Warehouse Issues

If tests fail with "warehouse not found" or timeout:

1. Verify the warehouse exists and is running
2. Check the warehouse ID matches your configuration
3. Ensure your PAT has permission to access the warehouse
4. Try starting the warehouse manually before running tests

## CI/CD Integration

For automated testing in CI pipelines:

```yaml
# Example GitHub Actions workflow
- name: Create test configuration
  run: |
    cat > /tmp/databricks_test_config.json << EOF
    {
      "environment": "CI",
      "uri": "${{ secrets.DATABRICKS_URI }}",
      "token": "${{ secrets.DATABRICKS_TOKEN }}",
      "type": "databricks"
    }
    EOF

- name: Run E2E Tests
  env:
    DATABRICKS_TEST_CONFIG_FILE: /tmp/databricks_test_config.json
  run: |
    cargo test -p adbc_databricks --release -- --ignored --test-threads=1
```

## File Structure

```
tests/e2e/
├── README.md                    # This documentation
├── config.rs                    # E2EConfig struct and parsing
├── helpers.rs                   # Test helper functions and macros
├── mod.rs                       # Test module with E2E tests
├── databricks.example.json      # Example configuration file
└── setup.sql                    # SQL script to create test data
```

## Security Notes

- **Never commit** configuration files with real credentials
- Use environment variables or CI secrets for tokens
- The example configuration file uses placeholder values
- Add your configuration file path to `.gitignore`

```bash
# Add to .gitignore
echo "databricks_test_config.json" >> .gitignore
```
