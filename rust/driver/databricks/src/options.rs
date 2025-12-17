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

//! Configuration options for the Databricks driver

use std::time::Duration;

/// Database-level configuration
#[derive(Clone, Debug)]
pub struct DatabaseConfig {
    pub host: Option<String>,
    pub warehouse_id: Option<String>,
    pub token: Option<String>,
    pub default_catalog: Option<String>,
    pub default_schema: Option<String>,
    pub http_config: HttpConfig,
    pub fetch_config: FetchConfig,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            host: None,
            warehouse_id: None,
            token: None,
            default_catalog: None,
            default_schema: None,
            http_config: HttpConfig::default(),
            fetch_config: FetchConfig::default(),
        }
    }
}

/// HTTP client configuration
#[derive(Clone, Debug)]
pub struct HttpConfig {
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    pub max_retries: u32,
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

/// Result fetching configuration
#[derive(Clone, Debug)]
pub struct FetchConfig {
    pub concurrency: usize,
    pub compression: String,
}

impl Default for FetchConfig {
    fn default() -> Self {
        Self {
            concurrency: 8,
            compression: "LZ4_FRAME".to_string(),
        }
    }
}

/// Custom Databricks option keys
pub mod keys {
    pub const WAREHOUSE_ID: &str = "databricks.warehouse_id";
    pub const TOKEN: &str = "databricks.token";
    pub const CATALOG: &str = "databricks.catalog";
    pub const SCHEMA: &str = "databricks.schema";
    pub const HTTP_CONNECT_TIMEOUT: &str = "databricks.http.connect_timeout";
    pub const HTTP_READ_TIMEOUT: &str = "databricks.http.read_timeout";
    pub const FETCH_CONCURRENCY: &str = "databricks.fetch.concurrency";
    pub const FETCH_COMPRESSION: &str = "databricks.fetch.compression";
}
