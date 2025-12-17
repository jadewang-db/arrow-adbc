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

use super::config::E2EConfig;
use adbc_driver_databricks::{DatabricksConnection, DatabricksDatabase};
use std::sync::Mutex;

/// Lazy-loaded test configuration from JSON file
pub static TEST_CONFIG: once_cell::sync::Lazy<Mutex<Option<E2EConfig>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(E2EConfig::from_env().ok()));

/// Check if E2E tests can execute
pub fn can_execute_e2e_tests() -> bool {
    std::env::var("DATABRICKS_TEST_CONFIG_FILE")
        .map(|path| std::path::Path::new(&path).exists())
        .unwrap_or(false)
}

/// Get test configuration or panic with helpful message
pub fn get_test_config() -> E2EConfig {
    TEST_CONFIG
        .lock()
        .unwrap()
        .clone()
        .expect(
            "Cannot load test configuration from DATABRICKS_TEST_CONFIG_FILE. \
             Set this environment variable to point to a valid JSON configuration file.",
        )
}

/// Create a test database configured from E2E config
///
/// Reads configuration from DATABRICKS_TEST_CONFIG_FILE and creates a properly
/// configured DatabricksDatabase instance.
pub fn create_test_database() -> DatabricksDatabase {
    use adbc_core::options::{OptionDatabase, OptionValue};
    use adbc_core::Optionable;

    let config = get_test_config();
    let (host, warehouse_id) = config
        .parse_uri()
        .expect("Failed to parse URI from test config");

    let mut database = DatabricksDatabase::new();

    // Set required options
    database
        .set_option(OptionDatabase::Uri, OptionValue::String(host))
        .expect("Failed to set host");

    database
        .set_option(
            OptionDatabase::Other("databricks.warehouse_id".to_string()),
            OptionValue::String(warehouse_id),
        )
        .expect("Failed to set warehouse_id");

    database
        .set_option(OptionDatabase::Password, OptionValue::String(config.token))
        .expect("Failed to set token");

    // Set optional catalog and schema if present
    if !config.metadata.catalog.is_empty() {
        database
            .set_option(
                OptionDatabase::Other("databricks.catalog".to_string()),
                OptionValue::String(config.metadata.catalog),
            )
            .expect("Failed to set catalog");
    }

    if !config.metadata.schema.is_empty() {
        database
            .set_option(
                OptionDatabase::Other("databricks.schema".to_string()),
                OptionValue::String(config.metadata.schema),
            )
            .expect("Failed to set schema");
    }

    database
}

/// Create a test connection from a configured database
///
/// Creates a connection using the database's new_connection() method.
/// Panics if the connection cannot be created.
pub fn create_test_connection(database: &DatabricksDatabase) -> DatabricksConnection {
    use adbc_core::Database;
    database
        .new_connection()
        .expect("Failed to create test connection")
}

/// Macro for conditional test execution
/// Usage: skip_if_no_config!();
#[macro_export]
macro_rules! skip_if_no_config {
    () => {
        if !$crate::e2e::helpers::can_execute_e2e_tests() {
            println!("Skipping test: DATABRICKS_TEST_CONFIG_FILE not set or file not found");
            return;
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::fs;
    use std::io::Write;

    #[test]
    fn test_can_execute_e2e_tests_no_env() {
        // Temporarily clear the environment variable
        let original = env::var("DATABRICKS_TEST_CONFIG_FILE").ok();
        env::remove_var("DATABRICKS_TEST_CONFIG_FILE");

        assert!(!can_execute_e2e_tests());

        // Restore if it was set
        if let Some(val) = original {
            env::set_var("DATABRICKS_TEST_CONFIG_FILE", val);
        }
    }

    #[test]
    fn test_can_execute_e2e_tests_with_valid_file() {
        // Create a temporary config file
        let temp_dir = tempfile::tempdir().unwrap();
        let config_path = temp_dir.path().join("test_config.json");

        let config_json = r#"{
            "environment": "Databricks",
            "uri": "https://test.cloud.databricks.com/sql/1.0/warehouses/test123",
            "token": "dapi_test",
            "type": "databricks"
        }"#;

        let mut file = fs::File::create(&config_path).unwrap();
        file.write_all(config_json.as_bytes()).unwrap();

        // Set environment variable
        let original = env::var("DATABRICKS_TEST_CONFIG_FILE").ok();
        env::set_var(
            "DATABRICKS_TEST_CONFIG_FILE",
            config_path.to_str().unwrap(),
        );

        assert!(can_execute_e2e_tests());

        // Restore original value
        env::remove_var("DATABRICKS_TEST_CONFIG_FILE");
        if let Some(val) = original {
            env::set_var("DATABRICKS_TEST_CONFIG_FILE", val);
        }
    }

    #[test]
    fn test_can_execute_e2e_tests_with_nonexistent_file() {
        let original = env::var("DATABRICKS_TEST_CONFIG_FILE").ok();
        env::set_var(
            "DATABRICKS_TEST_CONFIG_FILE",
            "/nonexistent/path/to/config.json",
        );

        assert!(!can_execute_e2e_tests());

        // Restore
        env::remove_var("DATABRICKS_TEST_CONFIG_FILE");
        if let Some(val) = original {
            env::set_var("DATABRICKS_TEST_CONFIG_FILE", val);
        }
    }
}
