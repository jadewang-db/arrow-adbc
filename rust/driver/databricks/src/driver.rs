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

//! ADBC Driver implementation for Databricks

use adbc_core::{Driver, Optionable};
use adbc_core::options::{OptionDatabase, OptionValue};

use crate::database::DatabricksDatabase;

/// ADBC driver for Databricks SQL Warehouses
///
/// This is a stateless unit struct that serves as the entry point
/// for creating database connections. It implements the ADBC Driver trait.
#[derive(Debug)]
pub struct DatabricksDriver;

impl DatabricksDriver {
    /// Create a new DatabricksDriver instance
    pub fn new() -> Self {
        Self
    }
}

impl Default for DatabricksDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl Driver for DatabricksDriver {
    type DatabaseType = DatabricksDatabase;

    fn new_database(&mut self) -> adbc_core::error::Result<Self::DatabaseType> {
        Ok(DatabricksDatabase::new())
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> adbc_core::error::Result<Self::DatabaseType> {
        let mut db = DatabricksDatabase::new();

        for (key, value) in opts {
            db.set_option(key, value)?;
        }

        Ok(db)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::Driver;

    #[test]
    fn test_driver_new() {
        let mut driver = DatabricksDriver::new();
        let db = driver.new_database();
        assert!(db.is_ok(), "Failed to create database: {:?}", db.err());
    }

    #[test]
    fn test_driver_default() {
        let mut driver = DatabricksDriver::default();
        let db = driver.new_database();
        assert!(db.is_ok(), "Failed to create database using default: {:?}", db.err());
    }

    #[test]
    fn test_driver_with_opts() {
        let mut driver = DatabricksDriver::new();
        let opts = vec![
            (OptionDatabase::Uri, OptionValue::String("https://test.databricks.com".into())),
        ];
        let db = driver.new_database_with_opts(opts);
        assert!(db.is_ok(), "Failed to create database with options: {:?}", db.err());
    }

    #[test]
    fn test_driver_multiple_databases() {
        let mut driver = DatabricksDriver::new();

        // Create first database
        let db1 = driver.new_database();
        assert!(db1.is_ok(), "Failed to create first database");

        // Create second database with different options
        let opts = vec![
            (OptionDatabase::Uri, OptionValue::String("https://test2.databricks.com".into())),
        ];
        let db2 = driver.new_database_with_opts(opts);
        assert!(db2.is_ok(), "Failed to create second database");
    }
}
