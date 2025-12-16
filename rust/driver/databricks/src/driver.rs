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

//! Databricks ADBC driver implementation.
//!
//! This module provides the entry point for creating database connections
//! to Databricks SQL Warehouses.

use adbc_core::error::Result;
use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::{Driver, Optionable};

use crate::database::DatabricksDatabase;

/// Databricks ADBC driver.
///
/// This is the entry point for creating database connections to Databricks
/// SQL Warehouses using the Statement Execution API (SEA).
///
/// # Example
///
/// ```ignore
/// use adbc_core::Driver;
/// use adbc_databricks::DatabricksDriver;
///
/// let mut driver = DatabricksDriver::new();
/// let mut database = driver.new_database_with_opts([
///     ("uri", "https://my-workspace.cloud.databricks.com"),
///     ("databricks.warehouse_id", "abc123"),
///     ("databricks.token", "dapi..."),
/// ])?;
/// ```
#[derive(Debug, Default)]
pub struct DatabricksDriver {}

impl DatabricksDriver {
    /// Create a new Databricks driver instance.
    pub fn new() -> Self {
        Self {}
    }
}

impl Driver for DatabricksDriver {
    type DatabaseType = DatabricksDatabase;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        Ok(DatabricksDatabase::new())
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        let mut database = DatabricksDatabase::new();
        for (key, value) in opts {
            database.set_option(key, value)?;
        }
        Ok(database)
    }
}
