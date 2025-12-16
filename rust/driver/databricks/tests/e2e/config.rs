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

//! E2E test configuration for Databricks ADBC driver.
//!
//! Configuration is loaded from a JSON file specified by the `DATABRICKS_TEST_CONFIG_FILE`
//! environment variable. The format matches the C# ADBC driver test configuration for
//! consistency across language implementations.

use serde::{Deserialize, Serialize};
use std::error::Error as StdError;
use std::fmt;

/// Environment variable name for the test configuration file path.
pub const CONFIG_ENV_VAR: &str = "DATABRICKS_TEST_CONFIG_FILE";

/// Error type for configuration loading and parsing.
#[derive(Debug)]
pub enum ConfigError {
    /// Environment variable not set
    EnvVarNotSet(String),
    /// File not found
    FileNotFound(String),
    /// Failed to read file
    FileReadError(String),
    /// Failed to parse JSON
    ParseError(String),
    /// Invalid URI format
    InvalidUri(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::EnvVarNotSet(var) => {
                write!(f, "Environment variable '{}' is not set", var)
            }
            ConfigError::FileNotFound(path) => {
                write!(f, "Configuration file not found: {}", path)
            }
            ConfigError::FileReadError(msg) => {
                write!(f, "Failed to read configuration file: {}", msg)
            }
            ConfigError::ParseError(msg) => {
                write!(f, "Failed to parse configuration JSON: {}", msg)
            }
            ConfigError::InvalidUri(msg) => {
                write!(f, "Invalid URI format: {}", msg)
            }
        }
    }
}

impl StdError for ConfigError {}

/// E2E test configuration matching C# ADBC driver format.
///
/// Loaded from JSON file via `DATABRICKS_TEST_CONFIG_FILE` environment variable.
///
/// # Example JSON Configuration
///
/// ```json
/// {
///   "environment": "Databricks",
///   "uri": "https://your-workspace.cloud.databricks.com/sql/1.0/warehouses/YOUR_WAREHOUSE_ID",
///   "token": "dapi...",
///   "query": "select count(*) from `main`.`your_schema`.`your_table`",
///   "type": "databricks",
///   "trace": "true",
///   "expectedResults": 1,
///   "metadata": {
///     "catalog": "main",
///     "schema": "your_schema",
///     "table": "your_table",
///     "expectedColumnCount": 3
///   }
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct E2EConfig {
    /// Environment name (e.g., "Databricks")
    pub environment: String,

    /// Full URI including warehouse path.
    /// Format: `https://{host}/sql/1.0/warehouses/{warehouse_id}`
    pub uri: String,

    /// Personal Access Token
    pub token: String,

    /// Optional test query
    #[serde(default)]
    pub query: String,

    /// Driver type (e.g., "databricks")
    #[serde(rename = "type")]
    pub driver_type: String,

    /// Trace logging flag
    #[serde(default)]
    pub trace: String,

    /// Expected number of results for test query
    #[serde(rename = "expectedResults", default)]
    pub expected_results: i64,

    /// Test metadata
    #[serde(default)]
    pub metadata: TestMetadata,
}

/// Metadata for test tables and expected values.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TestMetadata {
    /// Catalog name for tests
    #[serde(default)]
    pub catalog: String,

    /// Schema name for tests
    #[serde(default)]
    pub schema: String,

    /// Table name for tests
    #[serde(default)]
    pub table: String,

    /// Expected column count for metadata tests
    #[serde(rename = "expectedColumnCount", default)]
    pub expected_column_count: i32,
}

impl E2EConfig {
    /// Load configuration from file specified by `DATABRICKS_TEST_CONFIG_FILE` environment variable.
    ///
    /// # Returns
    ///
    /// Returns the parsed configuration or an error if:
    /// - The environment variable is not set
    /// - The file does not exist
    /// - The file cannot be read
    /// - The JSON is invalid
    pub fn from_env() -> Result<Self, ConfigError> {
        let config_path = std::env::var(CONFIG_ENV_VAR)
            .map_err(|_| ConfigError::EnvVarNotSet(CONFIG_ENV_VAR.to_string()))?;

        Self::from_file(&config_path)
    }

    /// Load configuration from a specific file path.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the JSON configuration file
    pub fn from_file(path: &str) -> Result<Self, ConfigError> {
        if !std::path::Path::new(path).exists() {
            return Err(ConfigError::FileNotFound(path.to_string()));
        }

        let content = std::fs::read_to_string(path)
            .map_err(|e| ConfigError::FileReadError(e.to_string()))?;

        Self::from_json(&content)
    }

    /// Parse configuration from a JSON string.
    ///
    /// # Arguments
    ///
    /// * `json` - JSON string containing the configuration
    pub fn from_json(json: &str) -> Result<Self, ConfigError> {
        serde_json::from_str(json).map_err(|e| ConfigError::ParseError(e.to_string()))
    }

    /// Parse host and warehouse_id from the URI.
    ///
    /// The URI format is: `https://{host}/sql/1.0/warehouses/{warehouse_id}`
    ///
    /// # Returns
    ///
    /// Returns a tuple of (host_url, warehouse_id) where:
    /// - host_url: The full host URL (e.g., `https://my-workspace.cloud.databricks.com`)
    /// - warehouse_id: The warehouse ID extracted from the path
    pub fn parse_uri(&self) -> Result<(String, String), ConfigError> {
        let url = url::Url::parse(&self.uri)
            .map_err(|e| ConfigError::InvalidUri(format!("Failed to parse URL: {}", e)))?;

        let host = url
            .host_str()
            .ok_or_else(|| ConfigError::InvalidUri("No host in URI".to_string()))?
            .to_string();

        let warehouse_id = url
            .path_segments()
            .and_then(|segments| segments.last())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| ConfigError::InvalidUri("No warehouse_id in URI path".to_string()))?
            .to_string();

        Ok((format!("https://{}", host), warehouse_id))
    }

    /// Check if trace logging is enabled.
    pub fn is_trace_enabled(&self) -> bool {
        self.trace.to_lowercase() == "true"
    }

    /// Get the fully qualified table name for tests.
    ///
    /// Returns format: `catalog.schema.table`
    pub fn get_full_table_name(&self) -> String {
        format!(
            "`{}`.`{}`.`{}`",
            self.metadata.catalog, self.metadata.schema, self.metadata.table
        )
    }

    /// Validate the configuration has all required fields.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.uri.is_empty() {
            return Err(ConfigError::ParseError("uri is required".to_string()));
        }
        if self.token.is_empty() {
            return Err(ConfigError::ParseError("token is required".to_string()));
        }
        // Validate URI can be parsed
        self.parse_uri()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_config_from_json() {
        let json = r#"{
            "environment": "Databricks",
            "uri": "https://my-workspace.cloud.databricks.com/sql/1.0/warehouses/abc123",
            "token": "dapi_test_token",
            "query": "SELECT 1",
            "type": "databricks",
            "trace": "true",
            "expectedResults": 1,
            "metadata": {
                "catalog": "main",
                "schema": "test_schema",
                "table": "test_table",
                "expectedColumnCount": 5
            }
        }"#;

        let config = E2EConfig::from_json(json).expect("Failed to parse config");

        assert_eq!(config.environment, "Databricks");
        assert_eq!(config.token, "dapi_test_token");
        assert_eq!(config.driver_type, "databricks");
        assert!(config.is_trace_enabled());
        assert_eq!(config.expected_results, 1);
        assert_eq!(config.metadata.catalog, "main");
        assert_eq!(config.metadata.schema, "test_schema");
        assert_eq!(config.metadata.table, "test_table");
        assert_eq!(config.metadata.expected_column_count, 5);
    }

    #[test]
    fn test_parse_uri() {
        let json = r#"{
            "environment": "Databricks",
            "uri": "https://my-workspace.cloud.databricks.com/sql/1.0/warehouses/abc123def456",
            "token": "dapi_test",
            "type": "databricks"
        }"#;

        let config = E2EConfig::from_json(json).expect("Failed to parse config");
        let (host, warehouse_id) = config.parse_uri().expect("Failed to parse URI");

        assert_eq!(host, "https://my-workspace.cloud.databricks.com");
        assert_eq!(warehouse_id, "abc123def456");
    }

    #[test]
    fn test_parse_uri_invalid() {
        let json = r#"{
            "environment": "Databricks",
            "uri": "not-a-valid-url",
            "token": "dapi_test",
            "type": "databricks"
        }"#;

        let config = E2EConfig::from_json(json).expect("Failed to parse config");
        let result = config.parse_uri();

        assert!(result.is_err());
    }

    #[test]
    fn test_default_values() {
        let json = r#"{
            "environment": "Databricks",
            "uri": "https://example.com/sql/1.0/warehouses/abc",
            "token": "dapi_test",
            "type": "databricks"
        }"#;

        let config = E2EConfig::from_json(json).expect("Failed to parse config");

        assert_eq!(config.query, "");
        assert_eq!(config.trace, "");
        assert_eq!(config.expected_results, 0);
        assert_eq!(config.metadata.catalog, "");
        assert!(!config.is_trace_enabled());
    }

    #[test]
    fn test_get_full_table_name() {
        let json = r#"{
            "environment": "Databricks",
            "uri": "https://example.com/sql/1.0/warehouses/abc",
            "token": "dapi_test",
            "type": "databricks",
            "metadata": {
                "catalog": "main",
                "schema": "test_schema",
                "table": "test_table"
            }
        }"#;

        let config = E2EConfig::from_json(json).expect("Failed to parse config");

        assert_eq!(
            config.get_full_table_name(),
            "`main`.`test_schema`.`test_table`"
        );
    }

    #[test]
    fn test_validate_missing_uri() {
        let json = r#"{
            "environment": "Databricks",
            "uri": "",
            "token": "dapi_test",
            "type": "databricks"
        }"#;

        let config = E2EConfig::from_json(json).expect("Failed to parse config");
        let result = config.validate();

        assert!(result.is_err());
    }

    #[test]
    fn test_validate_missing_token() {
        let json = r#"{
            "environment": "Databricks",
            "uri": "https://example.com/sql/1.0/warehouses/abc",
            "token": "",
            "type": "databricks"
        }"#;

        let config = E2EConfig::from_json(json).expect("Failed to parse config");
        let result = config.validate();

        assert!(result.is_err());
    }

    #[test]
    fn test_config_error_display() {
        let err = ConfigError::EnvVarNotSet("TEST_VAR".to_string());
        assert!(err.to_string().contains("TEST_VAR"));

        let err = ConfigError::FileNotFound("/path/to/file".to_string());
        assert!(err.to_string().contains("/path/to/file"));

        let err = ConfigError::ParseError("invalid json".to_string());
        assert!(err.to_string().contains("invalid json"));
    }
}
