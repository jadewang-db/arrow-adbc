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

use std::time::Duration;

/// Databricks-specific option keys for Database configuration.
pub mod database {
    /// SQL Warehouse ID (required).
    pub const WAREHOUSE_ID: &str = "databricks.warehouse_id";

    /// Personal Access Token (required).
    pub const TOKEN: &str = "databricks.token";

    /// Default catalog.
    pub const CATALOG: &str = "databricks.catalog";

    /// Default schema.
    pub const SCHEMA: &str = "databricks.schema";

    /// HTTP connect timeout in milliseconds (default: 10000).
    pub const HTTP_CONNECT_TIMEOUT: &str = "databricks.http.connect_timeout";

    /// HTTP read timeout in milliseconds (default: 300000).
    pub const HTTP_READ_TIMEOUT: &str = "databricks.http.read_timeout";

    /// Number of parallel chunk fetchers (default: 8).
    pub const FETCH_CONCURRENCY: &str = "databricks.fetch.concurrency";

    /// Result compression format (default: "LZ4_FRAME").
    pub const FETCH_COMPRESSION: &str = "databricks.fetch.compression";
}

/// Databricks-specific option keys for Statement configuration.
pub mod statement {
    /// Wait timeout for statement execution (default: "10s").
    pub const WAIT_TIMEOUT: &str = "databricks.statement.wait_timeout";

    /// Maximum rows to return.
    pub const ROW_LIMIT: &str = "databricks.statement.row_limit";

    /// Maximum bytes to return.
    pub const BYTE_LIMIT: &str = "databricks.statement.byte_limit";
}

/// Environment variable names for configuration.
pub mod env {
    /// Databricks workspace host.
    pub const HOST: &str = "DATABRICKS_HOST";

    /// SQL Warehouse ID.
    pub const WAREHOUSE_ID: &str = "DATABRICKS_WAREHOUSE_ID";

    /// Personal Access Token.
    pub const TOKEN: &str = "DATABRICKS_TOKEN";

    /// Default catalog.
    pub const CATALOG: &str = "DATABRICKS_CATALOG";

    /// Default schema.
    pub const SCHEMA: &str = "DATABRICKS_SCHEMA";
}

/// HTTP configuration for the driver.
#[derive(Debug, Clone)]
pub struct HttpConfig {
    /// Connect timeout.
    pub connect_timeout: Duration,
    /// Read timeout.
    pub read_timeout: Duration,
    /// Maximum retry attempts.
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
    /// Number of parallel chunk fetchers.
    pub fetch_concurrency: usize,
    /// Result compression format.
    pub fetch_compression: String,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            host: String::new(),
            warehouse_id: String::new(),
            token: String::new(),
            default_catalog: None,
            default_schema: None,
            http_config: HttpConfig::default(),
            fetch_concurrency: 8,
            fetch_compression: "LZ4_FRAME".to_string(),
        }
    }
}
