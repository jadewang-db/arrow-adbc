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

//! E2E test configuration module.
//!
//! This module provides configuration loading for end-to-end tests against
//! a live Databricks SQL Warehouse. The configuration follows the C# driver
//! test pattern, using a JSON configuration file pointed to by an environment
//! variable.
//!
//! # Configuration File Format
//!
//! ```json
//! {
//!     "hostName": "https://your-workspace.cloud.databricks.com",
//!     "path": "/sql/1.0/warehouses/abc123def456",
//!     "token": "dapi1234567890abcdef",
//!     "auth_type": "token",
//!     "type": "databricks",
//!     "catalog": "e2e_tests",
//!     "dbSchema": "rust_adbc_driver"
//! }
//! ```
//!
//! # Environment Variable
//!
//! Set `DATABRICKS_TEST_CONFIG_FILE` to point to your JSON configuration file:
//!
//! ```bash
//! export DATABRICKS_TEST_CONFIG_FILE="/path/to/databricks.local.json"
//! ```

use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

/// Test configuration for E2E tests (matches C# DatabricksTestConfiguration).
///
/// This struct mirrors the configuration format used by the C# Databricks driver
/// tests, allowing for consistent test configuration across driver implementations.
///
/// Supports two formats:
/// 1. Separate fields: `hostName` + `path` (e.g., hostName="https://workspace.databricks.com", path="/sql/1.0/warehouses/abc123")
/// 2. Combined URI: `uri` (e.g., "https://workspace.databricks.com/sql/1.0/warehouses/abc123")
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct E2EConfig {
    /// Hostname (e.g., "https://my-workspace.cloud.databricks.com")
    #[serde(rename = "hostName", default)]
    pub host_name: Option<String>,

    /// Combined URI containing host and path (alternative to hostName + path)
    #[serde(default)]
    pub uri: Option<String>,

    /// Warehouse path (e.g., "/sql/1.0/warehouses/abc123")
    #[serde(default)]
    pub path: Option<String>,

    /// Personal Access Token
    #[serde(default)]
    pub token: Option<String>,

    /// Authentication type (e.g., "token")
    #[serde(rename = "auth_type", default)]
    pub auth_type: Option<String>,

    /// Driver type
    #[serde(rename = "type", default)]
    pub driver_type: Option<String>,

    /// Catalog name
    #[serde(default)]
    pub catalog: Option<String>,

    /// Schema/database name
    #[serde(rename = "dbSchema", default)]
    pub schema: Option<String>,

    /// Test query
    #[serde(default)]
    pub query: String,

    /// Expected results count
    #[serde(rename = "expectedResults", default)]
    pub expected_results: i64,

    /// Metadata for tests
    #[serde(default)]
    pub metadata: TestMetadata,

    /// HTTP options (TLS, proxy, etc.)
    #[serde(rename = "http_options", default)]
    pub http_options: Option<HttpOptions>,

    /// OAuth grant type
    #[serde(rename = "grant_type", skip_serializing_if = "Option::is_none")]
    pub oauth_grant_type: Option<String>,

    /// OAuth client ID
    #[serde(rename = "client_id", skip_serializing_if = "Option::is_none")]
    pub oauth_client_id: Option<String>,

    /// OAuth client secret
    #[serde(rename = "client_secret", skip_serializing_if = "Option::is_none")]
    pub oauth_client_secret: Option<String>,

    /// OAuth scope
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
}

/// Metadata for test tables and expected results.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TestMetadata {
    /// Test catalog name
    #[serde(default)]
    pub catalog: String,

    /// Test schema name
    #[serde(default)]
    pub schema: String,

    /// Test table name
    #[serde(default)]
    pub table: String,

    /// Expected column count for metadata tests
    #[serde(rename = "expectedColumnCount", default)]
    pub expected_column_count: i32,
}

/// HTTP configuration options.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpOptions {
    /// TLS options
    #[serde(default)]
    pub tls: Option<TlsOptions>,

    /// Proxy options
    #[serde(default)]
    pub proxy: Option<ProxyOptions>,
}

/// TLS configuration options.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsOptions {
    /// Whether TLS is enabled
    pub enabled: Option<bool>,

    /// Disable server certificate validation
    pub disable_server_certificate_validation: Option<bool>,

    /// Allow self-signed certificates
    pub allow_self_signed: Option<bool>,

    /// Allow hostname mismatch
    pub allow_hostname_mismatch: Option<bool>,

    /// Path to trusted certificate
    pub trusted_certificate_path: Option<String>,
}

/// Proxy configuration options.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyOptions {
    /// Use proxy
    pub use_proxy: Option<String>,

    /// Proxy host
    pub proxy_host: Option<String>,

    /// Proxy port
    pub proxy_port: Option<u16>,

    /// Proxy authentication type
    pub proxy_auth: Option<String>,

    /// Proxy user ID
    pub proxy_uid: Option<String>,

    /// Proxy password
    pub proxy_pwd: Option<String>,

    /// Proxy ignore list
    pub proxy_ignore_list: Option<String>,
}

/// Error type for configuration loading.
#[derive(Debug)]
pub struct ConfigError {
    pub message: String,
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ConfigError {}

impl From<String> for ConfigError {
    fn from(message: String) -> Self {
        ConfigError { message }
    }
}

impl From<&str> for ConfigError {
    fn from(message: &str) -> Self {
        ConfigError {
            message: message.to_string(),
        }
    }
}

impl E2EConfig {
    /// Load configuration from file path specified in environment variable.
    ///
    /// Following the C# pattern: `DATABRICKS_TEST_CONFIG_FILE`
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The `DATABRICKS_TEST_CONFIG_FILE` environment variable is not set
    /// - The configuration file does not exist
    /// - The configuration file cannot be parsed as JSON
    /// - Required configuration fields are missing
    pub fn from_env() -> Result<Self, ConfigError> {
        let config_path = std::env::var("DATABRICKS_TEST_CONFIG_FILE").map_err(|_| {
            ConfigError::from("DATABRICKS_TEST_CONFIG_FILE environment variable not set")
        })?;

        Self::from_file(&config_path)
    }

    /// Load configuration from a JSON file.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the JSON configuration file
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The configuration file does not exist
    /// - The configuration file cannot be read
    /// - The configuration file cannot be parsed as JSON
    /// - Required configuration fields are missing
    pub fn from_file(path: &str) -> Result<Self, ConfigError> {
        if !Path::new(path).exists() {
            return Err(ConfigError::from(format!(
                "Configuration file not found: {}",
                path
            )));
        }

        let content = fs::read_to_string(path)
            .map_err(|e| ConfigError::from(format!("Failed to read configuration file: {}", e)))?;

        let config: E2EConfig = serde_json::from_str(&content)
            .map_err(|e| ConfigError::from(format!("Failed to parse configuration JSON: {}", e)))?;

        // Validate required fields
        config.validate()?;

        Ok(config)
    }

    /// Validate required configuration fields.
    fn validate(&self) -> Result<(), ConfigError> {
        // Either hostName or uri must be provided
        if self.host_name.is_none() && self.uri.is_none() {
            return Err(ConfigError::from(
                "Either hostName or uri is required in configuration",
            ));
        }
        if self.token.is_none() {
            return Err(ConfigError::from("token is required in configuration"));
        }
        Ok(())
    }

    /// Parse the URI to extract host and path components.
    /// URI format: "https://workspace.databricks.com/sql/1.0/warehouses/abc123"
    fn parse_uri(&self) -> Option<(String, String)> {
        self.uri.as_ref().and_then(|uri| {
            // Find the path starting with /sql/
            if let Some(idx) = uri.find("/sql/") {
                let host = uri[..idx].to_string();
                let path = uri[idx..].to_string();
                Some((host, path))
            } else {
                // No path found, use the whole URI as host
                Some((uri.clone(), String::new()))
            }
        })
    }

    /// Check if configuration is available (for conditional test execution).
    ///
    /// This can be used to skip tests when no configuration is available,
    /// similar to the C# `Utils.CanExecuteTestConfig()` pattern.
    pub fn can_execute() -> bool {
        if let Ok(config_path) = std::env::var("DATABRICKS_TEST_CONFIG_FILE") {
            Path::new(&config_path).exists()
        } else {
            false
        }
    }

    /// Extract warehouse ID from path.
    ///
    /// The path is expected to be in the format "/sql/1.0/warehouses/abc123"
    /// and this method extracts "abc123".
    ///
    /// # Returns
    ///
    /// The warehouse ID if the path is set and contains a valid warehouse ID,
    /// or `None` otherwise.
    pub fn warehouse_id(&self) -> Option<String> {
        // First try the explicit path field
        if let Some(path) = &self.path {
            return path.split('/').last().map(|s| s.to_string());
        }
        // Fall back to parsing the URI
        self.parse_uri()
            .and_then(|(_, path)| path.split('/').last().map(|s| s.to_string()))
    }

    /// Get the host URL without a trailing slash.
    pub fn host(&self) -> Option<String> {
        // First try the explicit hostName field
        if let Some(host) = &self.host_name {
            return Some(host.trim_end_matches('/').to_string());
        }
        // Fall back to parsing the URI
        self.parse_uri()
            .map(|(host, _)| host.trim_end_matches('/').to_string())
    }

    /// Get the catalog name for tests.
    ///
    /// Falls back to the metadata catalog if the top-level catalog is not set.
    pub fn test_catalog(&self) -> Option<String> {
        self.catalog.clone().or_else(|| {
            if self.metadata.catalog.is_empty() {
                None
            } else {
                Some(self.metadata.catalog.clone())
            }
        })
    }

    /// Get the schema name for tests.
    ///
    /// Falls back to the metadata schema if the top-level schema is not set.
    pub fn test_schema(&self) -> Option<String> {
        self.schema.clone().or_else(|| {
            if self.metadata.schema.is_empty() {
                None
            } else {
                Some(self.metadata.schema.clone())
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn create_test_config_file(content: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().expect("Failed to create temp file");
        file.write_all(content.as_bytes())
            .expect("Failed to write to temp file");
        file
    }

    #[test]
    fn test_load_valid_config() {
        let config_json = r#"{
            "hostName": "https://test-workspace.cloud.databricks.com",
            "path": "/sql/1.0/warehouses/abc123",
            "token": "dapi_test_token",
            "auth_type": "token",
            "type": "databricks",
            "catalog": "test_catalog",
            "dbSchema": "test_schema"
        }"#;

        let file = create_test_config_file(config_json);
        let config =
            E2EConfig::from_file(file.path().to_str().unwrap()).expect("Failed to load config");

        assert_eq!(
            config.host_name,
            Some("https://test-workspace.cloud.databricks.com".to_string())
        );
        assert_eq!(
            config.path,
            Some("/sql/1.0/warehouses/abc123".to_string())
        );
        assert_eq!(config.token, Some("dapi_test_token".to_string()));
        assert_eq!(config.catalog, Some("test_catalog".to_string()));
        assert_eq!(config.schema, Some("test_schema".to_string()));
    }

    #[test]
    fn test_extract_warehouse_id() {
        let config_json = r#"{
            "hostName": "https://test.cloud.databricks.com",
            "path": "/sql/1.0/warehouses/warehouse123",
            "token": "dapi_test_token"
        }"#;

        let file = create_test_config_file(config_json);
        let config = E2EConfig::from_file(file.path().to_str().unwrap()).unwrap();

        assert_eq!(config.warehouse_id(), Some("warehouse123".to_string()));
    }

    #[test]
    fn test_missing_host_name_and_uri() {
        let config_json = r#"{
            "path": "/sql/1.0/warehouses/abc123",
            "token": "dapi_test_token"
        }"#;

        let file = create_test_config_file(config_json);
        let result = E2EConfig::from_file(file.path().to_str().unwrap());

        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("hostName or uri"));
    }

    #[test]
    fn test_missing_token() {
        let config_json = r#"{
            "hostName": "https://test.cloud.databricks.com",
            "path": "/sql/1.0/warehouses/abc123"
        }"#;

        let file = create_test_config_file(config_json);
        let result = E2EConfig::from_file(file.path().to_str().unwrap());

        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("token"));
    }

    #[test]
    fn test_file_not_found() {
        let result = E2EConfig::from_file("/nonexistent/path/config.json");
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("not found"));
    }

    #[test]
    fn test_invalid_json() {
        let file = create_test_config_file("{ invalid json }");
        let result = E2EConfig::from_file(file.path().to_str().unwrap());
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("parse"));
    }

    #[test]
    fn test_host_without_trailing_slash() {
        let config_json = r#"{
            "hostName": "https://test.cloud.databricks.com/",
            "path": "/sql/1.0/warehouses/abc123",
            "token": "dapi_test_token"
        }"#;

        let file = create_test_config_file(config_json);
        let config = E2EConfig::from_file(file.path().to_str().unwrap()).unwrap();

        assert_eq!(
            config.host(),
            Some("https://test.cloud.databricks.com".to_string())
        );
    }

    #[test]
    fn test_metadata_fallback() {
        let config_json = r#"{
            "hostName": "https://test.cloud.databricks.com",
            "token": "dapi_test_token",
            "metadata": {
                "catalog": "fallback_catalog",
                "schema": "fallback_schema"
            }
        }"#;

        let file = create_test_config_file(config_json);
        let config = E2EConfig::from_file(file.path().to_str().unwrap()).unwrap();

        assert_eq!(config.test_catalog(), Some("fallback_catalog".to_string()));
        assert_eq!(config.test_schema(), Some("fallback_schema".to_string()));
    }

    #[test]
    fn test_uri_format_parsing() {
        let config_json = r#"{
            "uri": "https://test-workspace.cloud.databricks.com/sql/1.0/warehouses/abc123def",
            "token": "dapi_test_token"
        }"#;

        let file = create_test_config_file(config_json);
        let config = E2EConfig::from_file(file.path().to_str().unwrap()).unwrap();

        assert_eq!(
            config.host(),
            Some("https://test-workspace.cloud.databricks.com".to_string())
        );
        assert_eq!(config.warehouse_id(), Some("abc123def".to_string()));
    }

    #[test]
    fn test_uri_format_with_metadata() {
        let config_json = r#"{
            "uri": "https://benchmarking-prod.cloud.databricks.com/sql/1.0/warehouses/warehouse123",
            "token": "dapi_test_token",
            "metadata": {
                "catalog": "main",
                "schema": "test_schema"
            }
        }"#;

        let file = create_test_config_file(config_json);
        let config = E2EConfig::from_file(file.path().to_str().unwrap()).unwrap();

        assert_eq!(
            config.host(),
            Some("https://benchmarking-prod.cloud.databricks.com".to_string())
        );
        assert_eq!(config.warehouse_id(), Some("warehouse123".to_string()));
        assert_eq!(config.test_catalog(), Some("main".to_string()));
        assert_eq!(config.test_schema(), Some("test_schema".to_string()));
    }

    #[test]
    fn test_hostname_takes_precedence_over_uri() {
        let config_json = r#"{
            "hostName": "https://explicit-host.cloud.databricks.com",
            "uri": "https://uri-host.cloud.databricks.com/sql/1.0/warehouses/uri123",
            "path": "/sql/1.0/warehouses/path123",
            "token": "dapi_test_token"
        }"#;

        let file = create_test_config_file(config_json);
        let config = E2EConfig::from_file(file.path().to_str().unwrap()).unwrap();

        // hostName should take precedence over uri
        assert_eq!(
            config.host(),
            Some("https://explicit-host.cloud.databricks.com".to_string())
        );
        // path should take precedence over uri path
        assert_eq!(config.warehouse_id(), Some("path123".to_string()));
    }
}
