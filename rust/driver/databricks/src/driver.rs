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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::options::keys;
    use adbc_core::Optionable;

    #[test]
    fn test_driver_new() {
        // Verify that the driver can be created
        let _driver = DatabricksDriver::new();
        // If we reach here, the driver was successfully created
    }

    #[test]
    fn test_driver_default() {
        // Verify that the driver implements Default
        let _driver = DatabricksDriver::default();
        // If we reach here, the driver was successfully created via Default
    }

    #[test]
    fn test_driver_creates_database() {
        let mut driver = DatabricksDriver::new();
        let db = driver.new_database();
        assert!(db.is_ok(), "new_database should succeed");
    }

    #[test]
    fn test_driver_creates_database_with_uri_option() {
        let mut driver = DatabricksDriver::new();
        let db = driver.new_database_with_opts([(
            OptionDatabase::Uri,
            OptionValue::String("https://example.cloud.databricks.com".into()),
        )]);
        assert!(db.is_ok(), "new_database_with_opts should succeed");

        let db = db.unwrap();
        let uri = db.get_option_string(OptionDatabase::Uri);
        assert!(uri.is_ok(), "Should be able to get uri option");
        assert_eq!(
            uri.unwrap(),
            "https://example.cloud.databricks.com",
            "URI should match"
        );
    }

    #[test]
    fn test_driver_creates_database_with_password_option() {
        let mut driver = DatabricksDriver::new();
        let db = driver.new_database_with_opts([(
            OptionDatabase::Password,
            OptionValue::String("dapi_test_token".into()),
        )]);
        assert!(db.is_ok(), "new_database_with_opts should succeed with password");
    }

    #[test]
    fn test_driver_creates_database_with_multiple_opts() {
        let mut driver = DatabricksDriver::new();
        let db = driver.new_database_with_opts([
            (
                OptionDatabase::Uri,
                OptionValue::String("https://example.cloud.databricks.com".into()),
            ),
            (
                OptionDatabase::Password,
                OptionValue::String("dapi_token".into()),
            ),
            (
                OptionDatabase::Other(keys::WAREHOUSE_ID.into()),
                OptionValue::String("abc123def456".into()),
            ),
            (
                OptionDatabase::Other(keys::CATALOG.into()),
                OptionValue::String("main".into()),
            ),
            (
                OptionDatabase::Other(keys::SCHEMA.into()),
                OptionValue::String("default".into()),
            ),
        ]);
        assert!(
            db.is_ok(),
            "new_database_with_opts should succeed with multiple options"
        );

        let db = db.unwrap();

        // Verify uri was set
        let uri = db.get_option_string(OptionDatabase::Uri).unwrap();
        assert_eq!(uri, "https://example.cloud.databricks.com");

        // Verify warehouse_id was set
        let warehouse_id = db
            .get_option_string(OptionDatabase::Other(keys::WAREHOUSE_ID.into()))
            .unwrap();
        assert_eq!(warehouse_id, "abc123def456");

        // Verify catalog was set
        let catalog = db
            .get_option_string(OptionDatabase::Other(keys::CATALOG.into()))
            .unwrap();
        assert_eq!(catalog, "main");

        // Verify schema was set
        let schema = db
            .get_option_string(OptionDatabase::Other(keys::SCHEMA.into()))
            .unwrap();
        assert_eq!(schema, "default");
    }

    #[test]
    fn test_driver_creates_database_with_empty_opts() {
        let mut driver = DatabricksDriver::new();
        let db = driver.new_database_with_opts(std::iter::empty());
        assert!(
            db.is_ok(),
            "new_database_with_opts should succeed with empty options"
        );
    }

    #[test]
    fn test_driver_creates_database_with_invalid_option() {
        let mut driver = DatabricksDriver::new();
        let db = driver.new_database_with_opts([(
            OptionDatabase::Other("invalid.option.key".into()),
            OptionValue::String("some_value".into()),
        )]);
        assert!(
            db.is_err(),
            "new_database_with_opts should fail with unknown option"
        );
    }

    #[test]
    fn test_driver_creates_database_with_wrong_value_type() {
        let mut driver = DatabricksDriver::new();
        // URI expects a string, not an int
        let db = driver.new_database_with_opts([(OptionDatabase::Uri, OptionValue::Int(42))]);
        assert!(
            db.is_err(),
            "new_database_with_opts should fail with wrong value type"
        );
    }

    #[test]
    fn test_driver_is_debug() {
        let driver = DatabricksDriver::new();
        let debug_str = format!("{:?}", driver);
        assert!(
            debug_str.contains("DatabricksDriver"),
            "Debug output should contain struct name"
        );
    }

    #[test]
    fn test_driver_type_is_databricks_database() {
        // This is a compile-time check that DatabaseType is DatabricksDatabase
        let mut driver = DatabricksDriver::new();
        let _db: DatabricksDatabase = driver.new_database().unwrap();
    }
}
