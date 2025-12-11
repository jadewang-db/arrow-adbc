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

//! E2E test helper functions.
//!
//! This module provides helper functions for creating test connections,
//! databases, and drivers. It follows the C# driver test pattern with
//! lazy-loaded configuration and conditional test execution.
//!
//! # Example
//!
//! ```ignore
//! use crate::e2e::helpers::*;
//!
//! #[test]
//! #[ignore]
//! fn test_query_execution() {
//!     skip_if_no_config!();
//!
//!     let mut conn = create_test_connection();
//!     let mut stmt = conn.new_statement().unwrap();
//!     stmt.set_sql_query("SELECT 1").unwrap();
//!     let reader = stmt.execute().unwrap();
//!     // ...
//! }
//! ```

use super::config::E2EConfig;
use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use adbc_driver_databricks::{
    DatabricksConnection, DatabricksDatabase, DatabricksDriver, DatabricksStatement,
};
use once_cell::sync::Lazy;
use std::sync::Mutex;

/// Lazy-loaded test configuration (matches C# pattern).
///
/// This allows the configuration to be loaded once and reused across tests.
/// If the configuration cannot be loaded, the value will be `None`.
static TEST_CONFIG: Lazy<Mutex<Option<E2EConfig>>> =
    Lazy::new(|| Mutex::new(E2EConfig::from_env().ok()));

/// Check if E2E tests can execute (matches C# Utils.CanExecuteTestConfig).
///
/// This returns `true` if the `DATABRICKS_TEST_CONFIG_FILE` environment
/// variable is set and points to a valid configuration file.
pub fn can_execute_test_config() -> bool {
    E2EConfig::can_execute()
}

/// Get test configuration or panic with helpful message.
///
/// # Panics
///
/// Panics if the configuration cannot be loaded. The panic message includes
/// instructions for setting up the configuration.
pub fn get_test_config() -> E2EConfig {
    TEST_CONFIG
        .lock()
        .unwrap()
        .clone()
        .expect(
            "Cannot load test configuration from environment variable \
             DATABRICKS_TEST_CONFIG_FILE. The execution of this test will be skipped. \
             Set DATABRICKS_TEST_CONFIG_FILE to point to a valid JSON configuration file.",
        )
}

/// Create a test driver.
///
/// Returns a new `DatabricksDriver` instance.
pub fn create_test_driver() -> DatabricksDriver {
    DatabricksDriver::new()
}

/// Create a test database with configuration from JSON file.
///
/// This function reads the configuration from the `DATABRICKS_TEST_CONFIG_FILE`
/// environment variable and creates a database with the configured options.
///
/// # Panics
///
/// Panics if:
/// - The configuration cannot be loaded
/// - Required configuration fields are missing (hostName, token, warehouse path)
/// - The database cannot be created
pub fn create_test_database() -> DatabricksDatabase {
    let config = get_test_config();
    let mut driver = create_test_driver();

    let host = config
        .host()
        .expect("hostName required in test config");
    let warehouse_id = config
        .warehouse_id()
        .expect("path with warehouse ID required in test config");
    let token = config
        .token
        .as_ref()
        .expect("token required in test config");

    // Note: Catalog and schema are not included here because the SEA API has a conflict
    // between session_id and catalog/schema fields. Tests that need specific catalog/schema
    // should use create_test_connection_with_catalog or set them via SQL commands.
    let options: Vec<(OptionDatabase, OptionValue)> = vec![
        (OptionDatabase::Uri, OptionValue::String(host)),
        (
            OptionDatabase::Other("databricks.warehouse_id".into()),
            OptionValue::String(warehouse_id),
        ),
        (
            OptionDatabase::Other("databricks.token".into()),
            OptionValue::String(token.clone()),
        ),
    ];

    driver
        .new_database_with_opts(options)
        .expect("Failed to create test database")
}

/// Create a test connection.
///
/// This function creates a database from configuration and then creates
/// a new connection.
///
/// # Panics
///
/// Panics if the database or connection cannot be created.
pub fn create_test_connection() -> DatabricksConnection {
    let db = create_test_database();
    db.new_connection().expect("Failed to create test connection")
}

/// Create a test connection with specific catalog and schema.
///
/// # Arguments
///
/// * `catalog` - The catalog to use for the connection
/// * `schema` - The schema to use for the connection
///
/// # Panics
///
/// Panics if the database or connection cannot be created.
pub fn create_test_connection_with_catalog(catalog: &str, schema: &str) -> DatabricksConnection {
    let db = create_test_database();
    db.new_connection_with_opts([
        (
            OptionConnection::CurrentCatalog,
            OptionValue::String(catalog.to_string()),
        ),
        (
            OptionConnection::CurrentSchema,
            OptionValue::String(schema.to_string()),
        ),
    ])
    .expect("Failed to create test connection with catalog")
}

/// Create a statement and set a SQL query.
///
/// # Arguments
///
/// * `conn` - The connection to create the statement on (mutable reference)
/// * `sql` - The SQL query to set
///
/// # Returns
///
/// A statement with the SQL query set.
pub fn create_statement_with_query(
    conn: &mut DatabricksConnection,
    sql: &str,
) -> DatabricksStatement {
    let mut stmt = conn.new_statement().expect("Failed to create statement");
    stmt.set_sql_query(sql).expect("Failed to set SQL query");
    stmt
}

/// Execute a SQL query and collect all batches.
///
/// # Arguments
///
/// * `conn` - The connection to use (mutable reference)
/// * `sql` - The SQL query to execute
///
/// # Returns
///
/// A vector of all record batches from the query.
pub fn execute_query_and_collect(
    conn: &mut DatabricksConnection,
    sql: &str,
) -> Vec<arrow_array::RecordBatch> {
    let mut stmt = create_statement_with_query(conn, sql);
    let reader = stmt.execute().expect("Failed to execute query");
    reader
        .into_iter()
        .map(|r| r.expect("Failed to read batch"))
        .collect()
}

/// Count total rows from a query.
///
/// # Arguments
///
/// * `conn` - The connection to use (mutable reference)
/// * `sql` - The SQL query to execute
///
/// # Returns
///
/// The total number of rows returned by the query.
pub fn count_query_rows(conn: &mut DatabricksConnection, sql: &str) -> usize {
    let batches = execute_query_and_collect(conn, sql);
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Count total rows from a RecordBatchReader.
///
/// This helper consumes the reader and counts all rows across all batches.
///
/// # Arguments
///
/// * `reader` - The RecordBatchReader to count rows from
///
/// # Returns
///
/// The total number of rows in all batches.
pub fn count_reader_rows(reader: impl arrow_array::RecordBatchReader) -> usize {
    reader.map(|batch_result| batch_result.unwrap().num_rows()).sum()
}

/// Macro for conditional test execution (like C# Skip.IfNot).
///
/// This macro checks if the test configuration is available and skips
/// the test if it is not. It prints a message explaining why the test
/// was skipped.
///
/// # Example
///
/// ```ignore
/// #[test]
/// #[ignore]
/// fn test_something() {
///     skip_if_no_config!();
///     // Test code here...
/// }
/// ```
#[macro_export]
macro_rules! skip_if_no_config {
    () => {
        if !$crate::e2e::helpers::can_execute_test_config() {
            println!("Skipping test: DATABRICKS_TEST_CONFIG_FILE not set or file not found");
            return;
        }
    };
}

/// Re-export the macro at module level.
pub use skip_if_no_config;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_can_execute_returns_false_when_not_configured() {
        // This test should pass regardless of whether configuration is set,
        // it's just testing that the function doesn't panic.
        let _result = can_execute_test_config();
    }

    #[test]
    fn test_create_test_driver() {
        // Should not panic
        let _driver = create_test_driver();
    }
}
