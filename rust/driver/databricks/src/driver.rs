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

//! DatabricksDriver implementation.

use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::Driver;

use crate::database::DatabricksDatabase;

/// Entry point for creating Databricks database connections.
///
/// The driver is responsible for creating new database instances.
/// Each database instance holds configuration and a shared Tokio runtime.
#[derive(Debug, Default)]
pub struct DatabricksDriver {
    /// Optional Tokio runtime handle to use.
    /// If None, a new runtime will be created for each database.
    handle: Option<tokio::runtime::Handle>,
}

impl DatabricksDriver {
    /// Create a new DatabricksDriver instance.
    pub fn new() -> Self {
        Self { handle: None }
    }

    /// Create a new DatabricksDriver with a custom Tokio runtime handle.
    ///
    /// This allows sharing a runtime across multiple databases.
    pub fn with_runtime(handle: tokio::runtime::Handle) -> Self {
        Self {
            handle: Some(handle),
        }
    }
}

impl Driver for DatabricksDriver {
    type DatabaseType = DatabricksDatabase;

    fn new_database(&mut self) -> adbc_core::error::Result<Self::DatabaseType> {
        Ok(DatabricksDatabase::new(self.handle.clone()))
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> adbc_core::error::Result<Self::DatabaseType> {
        let mut database = DatabricksDatabase::new(self.handle.clone());
        for (key, value) in opts {
            database.set_option(key, value)?;
        }
        Ok(database)
    }
}

// Import Optionable for set_option method
use adbc_core::Optionable;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::options::database;

    #[test]
    fn test_driver_new() {
        let driver = DatabricksDriver::new();
        assert!(driver.handle.is_none());
    }

    #[test]
    fn test_driver_default() {
        let driver = DatabricksDriver::default();
        assert!(driver.handle.is_none());
    }

    #[test]
    fn test_driver_with_runtime() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let driver = DatabricksDriver::with_runtime(runtime.handle().clone());
        assert!(driver.handle.is_some());
    }

    #[test]
    fn test_new_database() {
        let mut driver = DatabricksDriver::new();
        let db = driver.new_database();
        assert!(db.is_ok());
    }

    #[test]
    fn test_new_database_with_opts_uri() {
        let mut driver = DatabricksDriver::new();
        let opts = vec![(
            OptionDatabase::Uri,
            OptionValue::String("https://test.cloud.databricks.com".into()),
        )];
        let db = driver.new_database_with_opts(opts);
        assert!(db.is_ok());

        let db = db.unwrap();
        let uri = db.get_option_string(OptionDatabase::Uri).unwrap();
        assert_eq!(uri, "https://test.cloud.databricks.com");
    }

    #[test]
    fn test_new_database_with_all_required_opts() {
        let mut driver = DatabricksDriver::new();
        let opts = vec![
            (
                OptionDatabase::Uri,
                OptionValue::String("https://test.cloud.databricks.com".into()),
            ),
            (
                OptionDatabase::Other(database::WAREHOUSE_ID.into()),
                OptionValue::String("abc123def456".into()),
            ),
            (
                OptionDatabase::Other(database::TOKEN.into()),
                OptionValue::String("dapi123456".into()),
            ),
        ];
        let db = driver.new_database_with_opts(opts);
        assert!(db.is_ok());

        let db = db.unwrap();
        assert_eq!(
            db.get_option_string(OptionDatabase::Uri).unwrap(),
            "https://test.cloud.databricks.com"
        );
        assert_eq!(
            db.get_option_string(OptionDatabase::Other(database::WAREHOUSE_ID.into()))
                .unwrap(),
            "abc123def456"
        );
    }

    #[test]
    fn test_new_database_with_optional_catalog_and_schema() {
        let mut driver = DatabricksDriver::new();
        let opts = vec![
            (
                OptionDatabase::Uri,
                OptionValue::String("https://test.cloud.databricks.com".into()),
            ),
            (
                OptionDatabase::Other(database::WAREHOUSE_ID.into()),
                OptionValue::String("abc123".into()),
            ),
            (
                OptionDatabase::Other(database::TOKEN.into()),
                OptionValue::String("dapi123".into()),
            ),
            (
                OptionDatabase::Other(database::CATALOG.into()),
                OptionValue::String("main".into()),
            ),
            (
                OptionDatabase::Other(database::SCHEMA.into()),
                OptionValue::String("default".into()),
            ),
        ];
        let db = driver.new_database_with_opts(opts);
        assert!(db.is_ok());

        let db = db.unwrap();
        assert_eq!(
            db.get_option_string(OptionDatabase::Other(database::CATALOG.into()))
                .unwrap(),
            "main"
        );
        assert_eq!(
            db.get_option_string(OptionDatabase::Other(database::SCHEMA.into()))
                .unwrap(),
            "default"
        );
    }

    #[test]
    fn test_new_database_with_invalid_option_type() {
        let mut driver = DatabricksDriver::new();
        // URI must be a string, not an int
        let opts = vec![(OptionDatabase::Uri, OptionValue::Int(123))];
        let result = driver.new_database_with_opts(opts);
        assert!(result.is_err());
    }

    #[test]
    fn test_new_database_with_unknown_option() {
        let mut driver = DatabricksDriver::new();
        let opts = vec![(
            OptionDatabase::Other("unknown.option".into()),
            OptionValue::String("value".into()),
        )];
        let result = driver.new_database_with_opts(opts);
        assert!(result.is_err());
    }

    #[test]
    fn test_multiple_databases_from_same_driver() {
        let mut driver = DatabricksDriver::new();

        let db1 = driver.new_database();
        let db2 = driver.new_database();

        assert!(db1.is_ok());
        assert!(db2.is_ok());
    }

    #[test]
    fn test_driver_with_shared_runtime() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let mut driver = DatabricksDriver::with_runtime(runtime.handle().clone());

        let db1 = driver.new_database();
        let db2 = driver.new_database();

        assert!(db1.is_ok());
        assert!(db2.is_ok());
    }
}
