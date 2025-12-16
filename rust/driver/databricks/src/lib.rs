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

//! ADBC driver for Databricks SQL Warehouses using the Statement Execution API (SEA).
//!
//! This driver provides Arrow-native access to Databricks SQL Warehouses,
//! enabling high-performance data access without unnecessary data copies.
//!
//! # Features
//!
//! - Native Rust implementation calling SEA REST API directly
//! - Arrow-native: Returns results as Arrow RecordBatches
//! - Parallel chunk fetching with LZ4 compression support
//! - ADBC 1.1.0 compliant
//!
//! # Example
//!
//! ```ignore
//! use adbc_core::Driver;
//! use adbc_databricks::DatabricksDriver;
//!
//! let mut driver = DatabricksDriver::new();
//! let mut database = driver.new_database_with_opts([
//!     ("uri", "https://my-workspace.cloud.databricks.com"),
//!     ("databricks.warehouse_id", "abc123"),
//!     ("databricks.token", "dapi..."),
//! ])?;
//!
//! let mut connection = database.new_connection()?;
//! let mut statement = connection.new_statement()?;
//!
//! statement.set_sql_query("SELECT * FROM my_table")?;
//! let reader = statement.execute()?;
//!
//! for batch in reader {
//!     // Process Arrow RecordBatches
//! }
//! ```

pub mod client;
mod connection;
mod database;
mod driver;
mod error;
mod fetch;
mod options;
pub mod session;
mod statement;

pub use connection::DatabricksConnection;
pub use database::DatabricksDatabase;
pub use driver::DatabricksDriver;
pub use error::{Error, Result};
pub use session::SessionManager;
pub use statement::DatabricksStatement;
