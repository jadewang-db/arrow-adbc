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

//! Test helper functions for E2E tests.
//!
//! This module provides utilities for setting up and running E2E tests against
//! a real Databricks SQL Warehouse.

use super::config::{ConfigError, E2EConfig, CONFIG_ENV_VAR};
use once_cell::sync::Lazy;
use std::sync::Mutex;

/// Lazy-loaded test configuration from JSON file.
///
/// The configuration is loaded once on first access and cached for subsequent uses.
/// This ensures consistent configuration across all tests in a test run.
static TEST_CONFIG: Lazy<Mutex<Option<E2EConfig>>> = Lazy::new(|| {
    let config = E2EConfig::from_env().ok();
    if let Some(ref cfg) = config {
        if cfg.is_trace_enabled() {
            eprintln!("[E2E] Configuration loaded from: {}",
                std::env::var(CONFIG_ENV_VAR).unwrap_or_default());
            eprintln!("[E2E] Environment: {}", cfg.environment);
            eprintln!("[E2E] Trace enabled");
        }
    }
    Mutex::new(config)
});

/// Check if E2E tests can execute based on configuration availability.
///
/// Returns `true` if:
/// - The `DATABRICKS_TEST_CONFIG_FILE` environment variable is set
/// - The file exists at the specified path
///
/// # Example
///
/// ```ignore
/// if !can_execute_e2e_tests() {
///     println!("Skipping E2E tests - no configuration available");
///     return;
/// }
/// ```
pub fn can_execute_e2e_tests() -> bool {
    std::env::var(CONFIG_ENV_VAR)
        .map(|path| std::path::Path::new(&path).exists())
        .unwrap_or(false)
}

/// Get the test configuration.
///
/// # Panics
///
/// Panics with a helpful error message if configuration cannot be loaded.
/// This should only be called after verifying `can_execute_e2e_tests()` returns true,
/// or within a test marked with `#[ignore]`.
///
/// # Example
///
/// ```ignore
/// #[test]
/// #[ignore]
/// fn my_e2e_test() {
///     skip_if_no_config!();
///     let config = get_test_config();
///     // Use config...
/// }
/// ```
pub fn get_test_config() -> E2EConfig {
    TEST_CONFIG
        .lock()
        .expect("Failed to acquire config lock")
        .clone()
        .expect(
            "Cannot load test configuration.\n\
             \n\
             To run E2E tests, set the DATABRICKS_TEST_CONFIG_FILE environment variable\n\
             to point to a valid JSON configuration file.\n\
             \n\
             Example:\n\
             export DATABRICKS_TEST_CONFIG_FILE=/path/to/databricks_test_config.json\n\
             \n\
             See tests/e2e/README.md for configuration format and setup instructions.",
        )
}

/// Try to get the test configuration without panicking.
///
/// Returns `Ok(E2EConfig)` if configuration is available,
/// or `Err(ConfigError)` with details about why configuration couldn't be loaded.
pub fn try_get_test_config() -> Result<E2EConfig, ConfigError> {
    TEST_CONFIG
        .lock()
        .expect("Failed to acquire config lock")
        .clone()
        .ok_or_else(|| {
            // Try to determine the specific error
            match std::env::var(CONFIG_ENV_VAR) {
                Err(_) => ConfigError::EnvVarNotSet(CONFIG_ENV_VAR.to_string()),
                Ok(path) => {
                    if !std::path::Path::new(&path).exists() {
                        ConfigError::FileNotFound(path)
                    } else {
                        // File exists but couldn't be parsed - try to get more info
                        match E2EConfig::from_file(&path) {
                            Err(e) => e,
                            Ok(_) => ConfigError::ParseError("Unknown error".to_string()),
                        }
                    }
                }
            }
        })
}

/// Print a trace message if tracing is enabled in the configuration.
///
/// # Arguments
///
/// * `config` - The E2E configuration
/// * `message` - The message to print
pub fn trace_message(config: &E2EConfig, message: &str) {
    if config.is_trace_enabled() {
        eprintln!("[E2E TRACE] {}", message);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_can_execute_without_env_var() {
        // Save original value
        let original = std::env::var(CONFIG_ENV_VAR).ok();

        // Remove the env var
        std::env::remove_var(CONFIG_ENV_VAR);

        // Should return false when env var is not set
        assert!(!can_execute_e2e_tests());

        // Restore original value
        if let Some(value) = original {
            std::env::set_var(CONFIG_ENV_VAR, value);
        }
    }

    #[test]
    fn test_try_get_config_returns_error_when_not_set() {
        // This test verifies error handling when config is not available
        // In CI without config, this should return an error type
        let result = try_get_test_config();

        // If config IS available, that's fine too - just verify we get a result
        match result {
            Ok(_) => {
                // Config is available, which is fine
                assert!(can_execute_e2e_tests());
            }
            Err(e) => {
                // Verify we get a meaningful error
                let error_msg = e.to_string();
                assert!(
                    error_msg.contains("not set")
                        || error_msg.contains("not found")
                        || error_msg.contains("parse"),
                    "Expected meaningful error message, got: {}",
                    error_msg
                );
            }
        }
    }

    #[test]
    fn test_trace_message() {
        let json = r#"{
            "environment": "Test",
            "uri": "https://example.com/sql/1.0/warehouses/abc",
            "token": "test",
            "type": "databricks",
            "trace": "true"
        }"#;

        let config = E2EConfig::from_json(json).unwrap();
        // This just verifies trace_message doesn't panic
        trace_message(&config, "Test message");
    }
}
