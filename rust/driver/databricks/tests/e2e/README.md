# E2E Tests Setup

End-to-end tests for the Databricks Rust ADBC driver. These tests run against
a live Databricks SQL Warehouse to validate the entire driver stack.

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- SQL Warehouse (Serverless or Classic)
- Personal Access Token (PAT) with appropriate permissions
- (Optional) Test catalog and schema for data type tests

## Configuration File Setup (Recommended)

The E2E test infrastructure follows the C# driver test pattern, using a JSON
configuration file pointed to by an environment variable.

### 1. Copy the Example Configuration

```bash
cp tests/e2e/databricks.json.example tests/e2e/databricks.local.json
```

### 2. Edit the Configuration

Edit `databricks.local.json` with your actual Databricks credentials:

```json
{
    "hostName": "https://your-workspace.cloud.databricks.com",
    "path": "/sql/1.0/warehouses/YOUR_WAREHOUSE_ID",
    "token": "dapi_YOUR_PERSONAL_ACCESS_TOKEN",
    "auth_type": "token",
    "type": "databricks",
    "catalog": "e2e_tests",
    "dbSchema": "rust_adbc_driver"
}
```

**Required Fields:**
- `hostName`: Your Databricks workspace URL (including `https://`)
- `path`: Path to your SQL Warehouse (format: `/sql/1.0/warehouses/<warehouse_id>`)
- `token`: Your Personal Access Token

**Optional Fields:**
- `catalog`: Default catalog for tests
- `dbSchema`: Default schema for tests
- `metadata`: Test metadata for validation tests

### 3. Set the Environment Variable

```bash
export DATABRICKS_TEST_CONFIG_FILE="$(pwd)/tests/e2e/databricks.local.json"
```

### 4. Add to .gitignore

The `*.local.json` pattern should be in `.gitignore` to prevent committing
credentials. Verify this is set:

```bash
echo "*.local.json" >> .gitignore
```

## Alternative: Environment Variables (Legacy)

If you prefer individual environment variables:

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_WAREHOUSE_ID="abc123def456"
export DATABRICKS_TOKEN="dapi1234567890"
export DATABRICKS_E2E_CATALOG="e2e_tests"
export DATABRICKS_E2E_SCHEMA="rust_adbc_driver"
```

Note: The JSON configuration file approach is preferred as it matches the C#
driver pattern and supports more configuration options.

## Test Data Setup (Optional)

For comprehensive E2E tests, you may want to create test tables:

### Option 1: Using Databricks CLI

```bash
databricks sql exec -f tests/e2e/setup.sql
```

### Option 2: Manual Execution

Copy and execute the contents of `tests/e2e/setup.sql` in your SQL Warehouse
or Databricks notebook.

## Running Tests

### Run All E2E Tests

```bash
cargo test --release --ignored e2e
```

### Run Specific E2E Test

```bash
cargo test --release --ignored e2e_basic_connection
cargo test --release --ignored e2e_query_select_one
```

### Run with Verbose Output

```bash
cargo test --release --ignored e2e -- --nocapture --test-threads=1
```

### Run with Specific Thread Count

For tests that may conflict, run with a single thread:

```bash
cargo test --release --ignored e2e -- --test-threads=1
```

## Test Categories

| Category | Description |
|----------|-------------|
| `e2e_basic_*` | Basic connection and simple query tests |
| `e2e_query_*` | Query execution and result handling |
| `e2e_connection_*` | Connection lifecycle and session management |
| `e2e_metadata_*` | Metadata API tests (get_info, get_objects, etc.) |
| `e2e_types_*` | Data type coverage tests |
| `e2e_result_*` | Large result and external links tests |

## Troubleshooting

### Test Skipped: "DATABRICKS_TEST_CONFIG_FILE not set"

- Ensure the `DATABRICKS_TEST_CONFIG_FILE` environment variable is set
- Verify the file path is correct and the file exists
- Check that the JSON syntax is valid

### Configuration Validation Errors

- Ensure `hostName` includes the protocol (`https://`)
- Verify `path` contains a valid warehouse ID
- Check that `token` is a valid PAT (starts with `dapi`)

### Connection Errors

| Error | Possible Cause |
|-------|----------------|
| `Unauthenticated` | PAT token expired or invalid |
| `NotFound` | Warehouse ID incorrect |
| `IO/Timeout` | Warehouse stopped or network issues |
| `Unauthorized` | Insufficient permissions |

### Warehouse Issues

- Verify your warehouse is running (not stopped)
- Check that auto-stop is configured appropriately for testing
- Ensure your PAT has access to the warehouse

### Network Issues

- Verify firewall allows connections to Databricks workspace
- Check proxy settings if applicable
- Ensure workspace URL is correct

## CI/CD Integration

For CI/CD pipelines, set the configuration as secrets:

```yaml
# GitHub Actions example
env:
  DATABRICKS_TEST_CONFIG_FILE: ${{ runner.temp }}/databricks.json

steps:
  - name: Create test config
    run: |
      cat > ${{ runner.temp }}/databricks.json << 'EOF'
      {
        "hostName": "${{ secrets.DATABRICKS_HOST }}",
        "path": "/sql/1.0/warehouses/${{ secrets.DATABRICKS_WAREHOUSE_ID }}",
        "token": "${{ secrets.DATABRICKS_TOKEN }}",
        "catalog": "e2e_tests",
        "dbSchema": "rust_adbc_driver_ci"
      }
      EOF

  - name: Run E2E tests
    run: cargo test --release --ignored e2e
```

## Security Notes

- **Never commit credentials** to version control
- Use `*.local.json` pattern for local configuration files
- Rotate PAT tokens regularly
- Use least-privilege permissions for test tokens
- Consider using separate warehouses for CI/CD tests
