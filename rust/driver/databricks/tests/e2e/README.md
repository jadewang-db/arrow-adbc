# E2E Test Setup and Execution Guide

This directory contains end-to-end (E2E) tests for the Databricks Rust ADBC driver. These tests validate the complete driver stack against a real Databricks SQL Warehouse.

## Prerequisites

Before running E2E tests, you need:

1. **Databricks Workspace**: Access to a Databricks workspace (AWS, Azure, or GCP)
2. **SQL Warehouse**: A running SQL Warehouse (Serverless or Classic)
3. **Personal Access Token (PAT)**: Generate a PAT with appropriate permissions
4. **Unity Catalog Access**: Permissions to create catalogs, schemas, and tables

## Configuration Setup

### Step 1: Create Configuration File

Create a JSON configuration file based on the example template. You can copy the example and customize it:

```bash
cd tests/e2e
cp databricks.example.json databricks.local.json
```

Edit `databricks.local.json` with your actual values:

```json
{
  "environment": "Databricks",
  "uri": "https://your-workspace.cloud.databricks.com/sql/1.0/warehouses/YOUR_WAREHOUSE_ID",
  "token": "dapi1234567890abcdef...",
  "query": "select count(*) from `e2e_tests`.`rust_adbc_driver`.`simple_table`",
  "type": "databricks",
  "trace": "true",
  "expectedResults": 1,
  "metadata": {
    "catalog": "e2e_tests",
    "schema": "rust_adbc_driver",
    "table": "simple_table",
    "expectedColumnCount": 3
  }
}
```

**Configuration Fields:**

| Field | Required | Description |
|-------|----------|-------------|
| `environment` | Yes | Environment name (e.g., "Databricks") |
| `uri` | Yes | Full URI including warehouse: `https://{host}/sql/1.0/warehouses/{warehouse_id}` |
| `token` | Yes | Personal Access Token (PAT) starting with `dapi` |
| `query` | No | Optional test query for validation |
| `type` | Yes | Driver type (always "databricks") |
| `trace` | No | Enable trace logging ("true"/"false") |
| `expectedResults` | No | Expected result count for test query |
| `metadata.catalog` | No | Default catalog for tests |
| `metadata.schema` | No | Default schema for tests |
| `metadata.table` | No | Test table name |
| `metadata.expectedColumnCount` | No | Expected column count for metadata tests |

**Important Security Notes:**
- Never commit `*.local.json` files (they are in `.gitignore`)
- Keep your PAT token secure
- Rotate tokens regularly
- Use a dedicated test workspace if possible

### Step 2: Get Your Warehouse ID

You can find your warehouse ID in the Databricks UI:

1. Navigate to **SQL Warehouses** in your workspace
2. Click on your warehouse
3. The warehouse ID is in the URL: `https://your-workspace.cloud.databricks.com/sql/warehouses/{warehouse_id}`

Or extract it from the full warehouse URI.

### Step 3: Generate Personal Access Token

1. Go to **Settings** → **User Settings** → **Access Tokens**
2. Click **Generate New Token**
3. Set an appropriate lifetime and description
4. Copy the token (it starts with `dapi`)

### Step 4: Setup Test Data

Run the SQL setup script to create test catalog, schema, and tables:

```bash
# Option 1: Run via Databricks SQL Editor
# 1. Open the SQL Editor in your workspace
# 2. Copy and paste the contents of setup.sql
# 3. Execute the script

# Option 2: Run via databricks-sql CLI (if available)
databricks-sql execute -f tests/e2e/setup.sql
```

The script creates:
- Catalog: `e2e_tests`
- Schema: `e2e_tests.rust_adbc_driver`
- Tables:
  - `test_types` - All supported data types
  - `test_large` - Large dataset (1M rows) for external links testing
  - `simple_table` - Basic test table
- View: `simple_view` - For metadata testing

**Verify Setup:**

```sql
-- Check that tables exist
SHOW TABLES IN e2e_tests.rust_adbc_driver;

-- Verify row counts
SELECT COUNT(*) FROM e2e_tests.rust_adbc_driver.test_types;      -- Should return 3
SELECT COUNT(*) FROM e2e_tests.rust_adbc_driver.test_large;      -- Should return 1000000
SELECT COUNT(*) FROM e2e_tests.rust_adbc_driver.simple_table;    -- Should return 3
```

## Running E2E Tests

### Set Environment Variable

Point to your configuration file:

```bash
export DATABRICKS_TEST_CONFIG_FILE=/path/to/databricks.local.json

# Or use absolute path
export DATABRICKS_TEST_CONFIG_FILE=$(pwd)/tests/e2e/databricks.local.json
```

### Run All E2E Tests

E2E tests are marked with `#[ignore]` attribute and must be run explicitly:

```bash
# Run all E2E tests
cargo test --ignored

# Run with verbose output
cargo test --ignored -- --nocapture

# Run with single thread (recommended for E2E tests)
cargo test --ignored -- --test-threads=1 --nocapture
```

### Run Specific E2E Test

```bash
# Run specific test by name
cargo test --ignored test_e2e_config_and_connect

# Run with verbose output
cargo test --ignored test_e2e_config_and_connect -- --nocapture
```

### Run Unit Tests Only (Default)

Unit tests run by default without `--ignored`:

```bash
cargo test
```

## Test Categories

### Configuration Tests
- **test_e2e_config_and_connect**: Validates configuration loading and URI parsing

### Future Test Categories (Coming Soon)
- Connection lifecycle tests
- Basic query execution tests
- Data type coverage tests
- Large result handling tests
- Metadata query tests
- Error handling tests

## Troubleshooting

### Issue: "DATABRICKS_TEST_CONFIG_FILE not set"

**Solution:** Ensure the environment variable is set and points to a valid file:

```bash
# Check if variable is set
echo $DATABRICKS_TEST_CONFIG_FILE

# Set the variable
export DATABRICKS_TEST_CONFIG_FILE=/path/to/your/config.json

# Verify file exists
ls -l $DATABRICKS_TEST_CONFIG_FILE
```

### Issue: "Cannot load test configuration"

**Causes:**
1. File doesn't exist at the specified path
2. Invalid JSON format
3. Missing required fields

**Solution:**
```bash
# Validate JSON format
cat $DATABRICKS_TEST_CONFIG_FILE | jq .

# Check file permissions
ls -l $DATABRICKS_TEST_CONFIG_FILE
```

### Issue: Authentication Errors

**Symptoms:**
- `401 Unauthenticated` errors
- `403 Permission Denied` errors

**Solutions:**
1. Verify your PAT token is correct and not expired
2. Check token has appropriate permissions
3. Ensure warehouse is running
4. Verify you have access to the warehouse

```bash
# Test token validity with curl
curl -H "Authorization: Bearer YOUR_TOKEN" \
     https://your-workspace.cloud.databricks.com/api/2.0/sql/warehouses
```

### Issue: Warehouse Not Found

**Symptoms:**
- `404 Not Found` errors
- Connection timeout

**Solutions:**
1. Verify warehouse ID is correct
2. Check warehouse is running (not stopped)
3. Ensure you have access to the warehouse

### Issue: Schema/Table Not Found

**Symptoms:**
- `NOT_FOUND` errors when running tests
- Missing test data

**Solutions:**
1. Run the `setup.sql` script
2. Verify catalog/schema names in config match setup
3. Check permissions to create/access objects

```sql
-- Verify setup
SHOW CATALOGS LIKE 'e2e_tests';
SHOW SCHEMAS IN e2e_tests;
SHOW TABLES IN e2e_tests.rust_adbc_driver;
```

### Issue: Tests Timeout

**Symptoms:**
- Tests hang or timeout
- Slow execution

**Solutions:**
1. Check warehouse is running and not auto-stopped
2. Increase warehouse cluster size for better performance
3. Verify network connectivity
4. Run tests sequentially: `cargo test --ignored -- --test-threads=1`

## CI/CD Integration

For CI/CD pipelines, store sensitive configuration in secrets:

### GitHub Actions Example

```yaml
name: E2E Tests

on: [push, pull_request]

jobs:
  e2e-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Setup Rust
        uses: actions-rs/toolchain@v1
        with:
          toolchain: stable

      - name: Create test configuration
        run: |
          cat > /tmp/databricks_test_config.json << EOF
          {
            "environment": "Databricks",
            "uri": "${{ secrets.DATABRICKS_URI }}",
            "token": "${{ secrets.DATABRICKS_TOKEN }}",
            "type": "databricks",
            "metadata": {
              "catalog": "e2e_tests",
              "schema": "rust_adbc_driver_ci"
            }
          }
          EOF

      - name: Run E2E Tests
        env:
          DATABRICKS_TEST_CONFIG_FILE: /tmp/databricks_test_config.json
        run: |
          cd rust/driver/databricks
          cargo test --ignored -- --test-threads=1
```

## Test Data Cleanup

To clean up test data after running tests:

```sql
-- Drop all test objects
DROP TABLE IF EXISTS e2e_tests.rust_adbc_driver.test_types;
DROP TABLE IF EXISTS e2e_tests.rust_adbc_driver.test_large;
DROP TABLE IF EXISTS e2e_tests.rust_adbc_driver.simple_table;
DROP VIEW IF EXISTS e2e_tests.rust_adbc_driver.simple_view;
DROP SCHEMA IF EXISTS e2e_tests.rust_adbc_driver;
DROP CATALOG IF EXISTS e2e_tests;
```

Or run the cleanup section at the end of `setup.sql`.

## Best Practices

1. **Use Dedicated Test Environment**: Use a separate workspace or catalog for testing
2. **Rotate Tokens Regularly**: Update PAT tokens periodically
3. **Run Tests Serially**: Use `--test-threads=1` to avoid race conditions
4. **Clean Up Resources**: Drop test objects when done
5. **Monitor Costs**: Large result tests may incur compute costs
6. **Version Control**: Never commit `*.local.json` files
7. **Validate Setup**: Run verification queries after setup script

## Additional Resources

- [Databricks ADBC Driver Design](../../docs/databricks-rust-adbc-driver-design.md)
- [Detailed Implementation Plan](../../docs/detailed-implementation-plan.md)
- [ADBC Specification](https://arrow.apache.org/adbc/)
- [Databricks SQL Statement Execution API](https://docs.databricks.com/api/workspace/statementexecution)

## Contact

For issues or questions:
- File an issue on the Apache Arrow ADBC repository
- Refer to the design documentation in `docs/`
