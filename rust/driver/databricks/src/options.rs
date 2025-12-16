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

//! Driver-specific options for the Databricks ADBC driver.
//!
//! This module defines the configuration options for connecting to
//! Databricks SQL Warehouses.

use std::time::Duration;

/// Databricks-specific option keys.
pub mod keys {
    /// SQL Warehouse ID.
    pub const WAREHOUSE_ID: &str = "databricks.warehouse_id";
    /// Personal Access Token.
    pub const TOKEN: &str = "databricks.token";
    /// Default catalog.
    pub const CATALOG: &str = "databricks.catalog";
    /// Default schema.
    pub const SCHEMA: &str = "databricks.schema";
    /// HTTP connect timeout in milliseconds.
    pub const HTTP_CONNECT_TIMEOUT: &str = "databricks.http.connect_timeout";
    /// HTTP read timeout in milliseconds.
    pub const HTTP_READ_TIMEOUT: &str = "databricks.http.read_timeout";
    /// Parallel chunk fetch concurrency.
    pub const FETCH_CONCURRENCY: &str = "databricks.fetch.concurrency";
    /// Result compression format.
    pub const FETCH_COMPRESSION: &str = "databricks.fetch.compression";
}

/// HTTP configuration for the driver.
#[derive(Debug, Clone)]
pub struct HttpConfig {
    /// Connection timeout.
    pub connect_timeout: Duration,
    /// Read timeout.
    pub read_timeout: Duration,
    /// Maximum number of retries.
    pub max_retries: u32,
    /// Base delay for exponential backoff.
    pub retry_backoff_base: Duration,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(300),
            max_retries: 3,
            retry_backoff_base: Duration::from_secs(1),
        }
    }
}

/// Database configuration.
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    /// Workspace host URL.
    pub host: String,
    /// SQL Warehouse ID.
    pub warehouse_id: String,
    /// Personal Access Token.
    pub token: String,
    /// Default catalog.
    pub default_catalog: Option<String>,
    /// Default schema.
    pub default_schema: Option<String>,
    /// HTTP configuration.
    pub http_config: HttpConfig,
    /// Chunk fetch concurrency.
    pub fetch_concurrency: usize,
}

impl DatabaseConfig {
    /// Create a new database configuration builder.
    pub fn builder() -> DatabaseConfigBuilder {
        DatabaseConfigBuilder::default()
    }
}

/// Builder for database configuration.
#[derive(Debug, Default)]
pub struct DatabaseConfigBuilder {
    host: Option<String>,
    warehouse_id: Option<String>,
    token: Option<String>,
    default_catalog: Option<String>,
    default_schema: Option<String>,
    http_config: HttpConfig,
    fetch_concurrency: usize,
}

impl DatabaseConfigBuilder {
    /// Set the workspace host URL.
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    /// Set the SQL Warehouse ID.
    pub fn warehouse_id(mut self, warehouse_id: impl Into<String>) -> Self {
        self.warehouse_id = Some(warehouse_id.into());
        self
    }

    /// Set the Personal Access Token.
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// Set the default catalog.
    pub fn default_catalog(mut self, catalog: impl Into<String>) -> Self {
        self.default_catalog = Some(catalog.into());
        self
    }

    /// Set the default schema.
    pub fn default_schema(mut self, schema: impl Into<String>) -> Self {
        self.default_schema = Some(schema.into());
        self
    }

    /// Set the HTTP configuration.
    pub fn http_config(mut self, config: HttpConfig) -> Self {
        self.http_config = config;
        self
    }

    /// Set the chunk fetch concurrency.
    pub fn fetch_concurrency(mut self, concurrency: usize) -> Self {
        self.fetch_concurrency = concurrency;
        self
    }

    /// Build the database configuration.
    pub fn build(self) -> crate::error::Result<DatabaseConfig> {
        let host = self
            .host
            .ok_or_else(|| crate::error::Error::config("host is required"))?;
        let warehouse_id = self
            .warehouse_id
            .ok_or_else(|| crate::error::Error::config("warehouse_id is required"))?;
        let token = self
            .token
            .ok_or_else(|| crate::error::Error::config("token is required"))?;

        Ok(DatabaseConfig {
            host,
            warehouse_id,
            token,
            default_catalog: self.default_catalog,
            default_schema: self.default_schema,
            http_config: self.http_config,
            fetch_concurrency: if self.fetch_concurrency == 0 {
                8
            } else {
                self.fetch_concurrency
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // Option Keys Tests
    // ========================================================================

    #[test]
    fn test_option_keys_are_defined() {
        // Verify all expected option keys are defined
        assert_eq!(keys::WAREHOUSE_ID, "databricks.warehouse_id");
        assert_eq!(keys::TOKEN, "databricks.token");
        assert_eq!(keys::CATALOG, "databricks.catalog");
        assert_eq!(keys::SCHEMA, "databricks.schema");
        assert_eq!(keys::HTTP_CONNECT_TIMEOUT, "databricks.http.connect_timeout");
        assert_eq!(keys::HTTP_READ_TIMEOUT, "databricks.http.read_timeout");
        assert_eq!(keys::FETCH_CONCURRENCY, "databricks.fetch.concurrency");
        assert_eq!(keys::FETCH_COMPRESSION, "databricks.fetch.compression");
    }

    #[test]
    fn test_option_keys_have_consistent_prefix() {
        // All keys should start with "databricks."
        assert!(keys::WAREHOUSE_ID.starts_with("databricks."));
        assert!(keys::TOKEN.starts_with("databricks."));
        assert!(keys::CATALOG.starts_with("databricks."));
        assert!(keys::SCHEMA.starts_with("databricks."));
        assert!(keys::HTTP_CONNECT_TIMEOUT.starts_with("databricks."));
        assert!(keys::HTTP_READ_TIMEOUT.starts_with("databricks."));
        assert!(keys::FETCH_CONCURRENCY.starts_with("databricks."));
        assert!(keys::FETCH_COMPRESSION.starts_with("databricks."));
    }

    // ========================================================================
    // HttpConfig Tests
    // ========================================================================

    #[test]
    fn test_http_config_default() {
        let config = HttpConfig::default();

        assert_eq!(config.connect_timeout, Duration::from_secs(10));
        assert_eq!(config.read_timeout, Duration::from_secs(300));
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.retry_backoff_base, Duration::from_secs(1));
    }

    #[test]
    fn test_http_config_custom() {
        let config = HttpConfig {
            connect_timeout: Duration::from_secs(30),
            read_timeout: Duration::from_secs(600),
            max_retries: 5,
            retry_backoff_base: Duration::from_millis(500),
        };

        assert_eq!(config.connect_timeout, Duration::from_secs(30));
        assert_eq!(config.read_timeout, Duration::from_secs(600));
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.retry_backoff_base, Duration::from_millis(500));
    }

    #[test]
    fn test_http_config_clone() {
        let config = HttpConfig::default();
        let cloned = config.clone();

        assert_eq!(config.connect_timeout, cloned.connect_timeout);
        assert_eq!(config.read_timeout, cloned.read_timeout);
        assert_eq!(config.max_retries, cloned.max_retries);
        assert_eq!(config.retry_backoff_base, cloned.retry_backoff_base);
    }

    // ========================================================================
    // DatabaseConfigBuilder Tests - Success Cases
    // ========================================================================

    #[test]
    fn test_database_config_builder_minimal() {
        let config = DatabaseConfig::builder()
            .host("https://example.databricks.com")
            .warehouse_id("abc123")
            .token("dapi_token_12345")
            .build()
            .expect("Should build successfully");

        assert_eq!(config.host, "https://example.databricks.com");
        assert_eq!(config.warehouse_id, "abc123");
        assert_eq!(config.token, "dapi_token_12345");
        assert!(config.default_catalog.is_none());
        assert!(config.default_schema.is_none());
        // Default fetch concurrency is 8
        assert_eq!(config.fetch_concurrency, 8);
    }

    #[test]
    fn test_database_config_builder_all_options() {
        let http_config = HttpConfig {
            connect_timeout: Duration::from_secs(60),
            read_timeout: Duration::from_secs(120),
            max_retries: 5,
            retry_backoff_base: Duration::from_millis(200),
        };

        let config = DatabaseConfig::builder()
            .host("https://workspace.cloud.databricks.com")
            .warehouse_id("warehouse-xyz")
            .token("dapi_secret_token")
            .default_catalog("main")
            .default_schema("default")
            .http_config(http_config.clone())
            .fetch_concurrency(16)
            .build()
            .expect("Should build successfully");

        assert_eq!(config.host, "https://workspace.cloud.databricks.com");
        assert_eq!(config.warehouse_id, "warehouse-xyz");
        assert_eq!(config.token, "dapi_secret_token");
        assert_eq!(config.default_catalog, Some("main".to_string()));
        assert_eq!(config.default_schema, Some("default".to_string()));
        assert_eq!(config.fetch_concurrency, 16);
        assert_eq!(config.http_config.connect_timeout, Duration::from_secs(60));
        assert_eq!(config.http_config.read_timeout, Duration::from_secs(120));
        assert_eq!(config.http_config.max_retries, 5);
    }

    #[test]
    fn test_database_config_builder_zero_concurrency_defaults_to_8() {
        let config = DatabaseConfig::builder()
            .host("https://example.com")
            .warehouse_id("wh123")
            .token("token123")
            .fetch_concurrency(0) // Explicitly set to 0
            .build()
            .expect("Should build successfully");

        // Should default to 8 when 0 is provided
        assert_eq!(config.fetch_concurrency, 8);
    }

    #[test]
    fn test_database_config_builder_custom_concurrency() {
        let config = DatabaseConfig::builder()
            .host("https://example.com")
            .warehouse_id("wh123")
            .token("token123")
            .fetch_concurrency(32)
            .build()
            .expect("Should build successfully");

        assert_eq!(config.fetch_concurrency, 32);
    }

    #[test]
    fn test_database_config_builder_method_chaining() {
        // Verify that all builder methods return Self and can be chained
        let config = DatabaseConfig::builder()
            .host("host")
            .warehouse_id("wh")
            .token("token")
            .default_catalog("cat")
            .default_schema("schema")
            .http_config(HttpConfig::default())
            .fetch_concurrency(4)
            .build()
            .expect("Should build successfully");

        assert_eq!(config.host, "host");
        assert_eq!(config.default_catalog, Some("cat".to_string()));
        assert_eq!(config.default_schema, Some("schema".to_string()));
    }

    // ========================================================================
    // DatabaseConfigBuilder Tests - Error Cases
    // ========================================================================

    #[test]
    fn test_database_config_builder_missing_host() {
        let result = DatabaseConfig::builder()
            .warehouse_id("wh123")
            .token("token123")
            .build();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("host is required"));
    }

    #[test]
    fn test_database_config_builder_missing_warehouse_id() {
        let result = DatabaseConfig::builder()
            .host("https://example.com")
            .token("token123")
            .build();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("warehouse_id is required"));
    }

    #[test]
    fn test_database_config_builder_missing_token() {
        let result = DatabaseConfig::builder()
            .host("https://example.com")
            .warehouse_id("wh123")
            .build();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("token is required"));
    }

    #[test]
    fn test_database_config_builder_missing_all_required() {
        let result = DatabaseConfig::builder().build();

        // Should fail on the first missing required field (host)
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("host is required"));
    }

    // ========================================================================
    // DatabaseConfig Tests
    // ========================================================================

    #[test]
    fn test_database_config_builder_factory() {
        // Verify that DatabaseConfig::builder() returns a builder
        let builder = DatabaseConfig::builder();
        let config = builder
            .host("test")
            .warehouse_id("wh")
            .token("tok")
            .build()
            .expect("Should build");

        assert_eq!(config.host, "test");
    }

    #[test]
    fn test_database_config_clone() {
        let config = DatabaseConfig::builder()
            .host("https://example.com")
            .warehouse_id("wh123")
            .token("token123")
            .default_catalog("catalog")
            .build()
            .expect("Should build");

        let cloned = config.clone();

        assert_eq!(config.host, cloned.host);
        assert_eq!(config.warehouse_id, cloned.warehouse_id);
        assert_eq!(config.token, cloned.token);
        assert_eq!(config.default_catalog, cloned.default_catalog);
        assert_eq!(config.default_schema, cloned.default_schema);
        assert_eq!(config.fetch_concurrency, cloned.fetch_concurrency);
    }

    #[test]
    fn test_database_config_uses_default_http_config() {
        let config = DatabaseConfig::builder()
            .host("https://example.com")
            .warehouse_id("wh123")
            .token("token123")
            .build()
            .expect("Should build");

        // Should use default HttpConfig values
        assert_eq!(config.http_config.connect_timeout, Duration::from_secs(10));
        assert_eq!(config.http_config.read_timeout, Duration::from_secs(300));
        assert_eq!(config.http_config.max_retries, 3);
    }

    // ========================================================================
    // Edge Case Tests
    // ========================================================================

    #[test]
    fn test_builder_with_empty_strings() {
        // Empty strings are technically valid at the builder level
        // (validation of actual values should happen at a higher level)
        let config = DatabaseConfig::builder()
            .host("")
            .warehouse_id("")
            .token("")
            .build()
            .expect("Should build with empty strings");

        assert_eq!(config.host, "");
        assert_eq!(config.warehouse_id, "");
        assert_eq!(config.token, "");
    }

    #[test]
    fn test_builder_with_string_types() {
        // Test that builder accepts both &str and String
        let config = DatabaseConfig::builder()
            .host(String::from("https://example.com"))
            .warehouse_id("wh123".to_string())
            .token("token")
            .default_catalog(String::from("cat"))
            .default_schema("schema".to_owned())
            .build()
            .expect("Should build");

        assert_eq!(config.host, "https://example.com");
        assert_eq!(config.warehouse_id, "wh123");
        assert_eq!(config.default_catalog, Some("cat".to_string()));
    }

    #[test]
    fn test_builder_overwrite_values() {
        // Later calls should overwrite earlier ones
        let config = DatabaseConfig::builder()
            .host("first")
            .host("second")
            .warehouse_id("wh1")
            .warehouse_id("wh2")
            .token("token1")
            .token("token2")
            .build()
            .expect("Should build");

        assert_eq!(config.host, "second");
        assert_eq!(config.warehouse_id, "wh2");
        assert_eq!(config.token, "token2");
    }
}
