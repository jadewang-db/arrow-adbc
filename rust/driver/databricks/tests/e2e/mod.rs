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

//! End-to-end test infrastructure for the Databricks ADBC driver.
//!
//! This module provides the infrastructure for running E2E tests against
//! a live Databricks SQL Warehouse. Tests in this module validate the entire
//! driver stack in a production-like environment.
//!
//! # Configuration
//!
//! E2E tests require a configuration file pointed to by the
//! `DATABRICKS_TEST_CONFIG_FILE` environment variable. The configuration
//! follows the C# driver test pattern.
//!
//! See the [`config`] module for configuration file format.
//!
//! # Running E2E Tests
//!
//! E2E tests are marked with `#[ignore]` to prevent them from running
//! during normal test runs. To run E2E tests:
//!
//! ```bash
//! # Set the configuration file path
//! export DATABRICKS_TEST_CONFIG_FILE="/path/to/databricks.local.json"
//!
//! # Run all E2E tests
//! cargo test --release --ignored e2e
//!
//! # Run specific E2E test
//! cargo test --release --ignored e2e_basic_connection
//!
//! # Run with verbose output
//! cargo test --release --ignored e2e -- --nocapture --test-threads=1
//! ```
//!
//! # Test Categories
//!
//! - **basic_tests**: Basic connection and simple query tests
//! - **connection_tests**: Connection lifecycle and session management (Sprint 5.4)
//! - **query_tests**: Query execution and result handling (Sprint 5.5)
//!
//! # Example Test
//!
//! ```ignore
//! use crate::e2e::helpers::*;
//!
//! #[test]
//! #[ignore]
//! fn e2e_query_select_one() {
//!     skip_if_no_config!();
//!
//!     let mut conn = create_test_connection();
//!     let batches = execute_query_and_collect(&conn, "SELECT 1 AS one");
//!
//!     assert_eq!(batches.len(), 1);
//!     assert_eq!(batches[0].num_rows(), 1);
//! }
//! ```

pub mod config;
pub mod helpers;
pub mod connection_tests;
pub mod query_basic_tests;
pub mod query_types_tests;
pub mod query_large_tests;

// Re-export commonly used items for convenience
pub use config::E2EConfig;
pub use helpers::{
    can_execute_test_config, count_query_rows, count_reader_rows, create_statement_with_query,
    create_test_connection, create_test_connection_with_catalog, create_test_database,
    create_test_driver, execute_query_and_collect, get_test_config,
};
