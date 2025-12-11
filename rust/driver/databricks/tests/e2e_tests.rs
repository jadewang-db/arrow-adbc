// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! End-to-end test suite for the Databricks ADBC driver.
//!
//! These tests run against a live Databricks SQL Warehouse and validate
//! the entire driver stack in a production-like environment.
//!
//! # Prerequisites
//!
//! - Databricks workspace with Unity Catalog enabled
//! - SQL Warehouse (Serverless or Classic)
//! - Personal Access Token with appropriate permissions
//! - Test catalog and schema (optional, but recommended)
//!
//! # Configuration
//!
//! Set the `DATABRICKS_TEST_CONFIG_FILE` environment variable to point to
//! a JSON configuration file with the following format:
//!
//! ```json
//! {
//!     "hostName": "https://your-workspace.cloud.databricks.com",
//!     "path": "/sql/1.0/warehouses/abc123def456",
//!     "token": "dapi1234567890abcdef",
//!     "catalog": "e2e_tests",
//!     "dbSchema": "rust_adbc_driver"
//! }
//! ```
//!
//! # Running Tests
//!
//! ```bash
//! # Run all E2E tests
//! cargo test --release --ignored e2e
//!
//! # Run specific E2E test
//! cargo test --release --ignored e2e_basic_connection
//!
//! # Run with verbose output
//! cargo test --release --ignored -- --nocapture --test-threads=1
//! ```

mod e2e;

use adbc_core::{Connection, Statement};
use e2e::helpers::*;

// =============================================================================
// Basic Infrastructure Tests
// =============================================================================

/// Test that we can establish a connection to the Databricks warehouse.
///
/// This is the most basic E2E test - it validates that the configuration
/// is correct and we can successfully create a session.
#[test]
#[ignore]
fn e2e_basic_connection() {
    skip_if_no_config!();

    let conn = create_test_connection();
    // Connection created successfully - session is active
    drop(conn);
    // Connection dropped - session should be terminated
}

/// Test that we can execute a simple SELECT 1 query.
///
/// This validates the basic query execution path without any
/// complex data types or large results.
#[test]
#[ignore]
fn e2e_query_select_one() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let batches = execute_query_and_collect(&mut conn, "SELECT 1 AS one");

    assert!(!batches.is_empty(), "Expected at least one batch");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "Expected exactly one row");

    // Verify the schema
    let schema = batches[0].schema();
    assert_eq!(schema.fields().len(), 1, "Expected one column");
    assert_eq!(
        schema.field(0).name(),
        "one",
        "Expected column name 'one'"
    );
}

/// Test that we can execute a query returning multiple rows.
#[test]
#[ignore]
fn e2e_query_multiple_rows() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let batches = execute_query_and_collect(
        &mut conn,
        "SELECT id FROM (VALUES (1), (2), (3), (4), (5)) AS t(id)",
    );

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5, "Expected 5 rows");
}

/// Test that we can execute a query returning multiple columns.
#[test]
#[ignore]
fn e2e_query_multiple_columns() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let batches = execute_query_and_collect(&mut conn, "SELECT 1 AS a, 2 AS b, 3 AS c");

    assert!(!batches.is_empty(), "Expected at least one batch");

    let schema = batches[0].schema();
    assert_eq!(schema.fields().len(), 3, "Expected three columns");
    assert_eq!(schema.field(0).name(), "a");
    assert_eq!(schema.field(1).name(), "b");
    assert_eq!(schema.field(2).name(), "c");
}

/// Test that we can execute a query returning an empty result set.
#[test]
#[ignore]
fn e2e_query_empty_result() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let batches = execute_query_and_collect(&mut conn, "SELECT 1 AS one WHERE 1 = 0");

    // Should return schema even with no rows
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0, "Expected zero rows");
}

/// Test that we can create multiple statements on the same connection.
#[test]
#[ignore]
fn e2e_multiple_statements() {
    skip_if_no_config!();

    let mut conn = create_test_connection();

    // Execute first query
    let batches1 = execute_query_and_collect(&mut conn, "SELECT 1 AS first");
    assert_eq!(count_rows(&batches1), 1);

    // Execute second query on same connection
    let batches2 = execute_query_and_collect(&mut conn, "SELECT 2 AS second");
    assert_eq!(count_rows(&batches2), 1);

    // Execute third query
    let batches3 = execute_query_and_collect(&mut conn, "SELECT 3 AS third");
    assert_eq!(count_rows(&batches3), 1);
}

/// Test that we can reuse a statement with different queries.
#[test]
#[ignore]
fn e2e_statement_reuse() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().expect("Failed to create statement");

    // First query
    stmt.set_sql_query("SELECT 1 AS first")
        .expect("Failed to set query");
    let reader = stmt.execute().expect("Failed to execute");
    let batches: Vec<_> = reader.into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(count_rows(&batches), 1);

    // Second query - reusing same statement
    stmt.set_sql_query("SELECT 1 AS a, 2 AS b")
        .expect("Failed to set query");
    let reader = stmt.execute().expect("Failed to execute");
    let batches: Vec<_> = reader.into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(count_rows(&batches), 1);
    assert_eq!(batches[0].schema().fields().len(), 2);
}

/// Test that NULL values are handled correctly.
#[test]
#[ignore]
fn e2e_null_values() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let batches = execute_query_and_collect(&mut conn, "SELECT NULL AS null_col, 1 AS not_null");

    assert!(!batches.is_empty());
    assert_eq!(batches[0].num_rows(), 1);

    // The first column should be null
    let null_col = batches[0].column(0);
    assert_eq!(null_col.null_count(), 1, "Expected null value");

    // The second column should not be null
    let not_null_col = batches[0].column(1);
    assert_eq!(not_null_col.null_count(), 0, "Expected non-null value");
}

/// Test string values with special characters.
#[test]
#[ignore]
fn e2e_string_special_chars() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let batches = execute_query_and_collect(
        &mut conn,
        "SELECT 'Hello, World!' AS greeting, 'Line1\\nLine2' AS multiline",
    );

    assert!(!batches.is_empty());
    assert_eq!(batches[0].num_rows(), 1);
}

/// Test unicode string values.
#[test]
#[ignore]
fn e2e_unicode_strings() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    // Test various unicode characters including emoji
    let batches = execute_query_and_collect(&mut conn, "SELECT 'Hello World' AS emoji");

    assert!(!batches.is_empty());
    assert_eq!(batches[0].num_rows(), 1);
}

// =============================================================================
// Helper Functions
// =============================================================================

fn count_rows(batches: &[arrow_array::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}
