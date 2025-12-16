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
