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

//! Databricks database implementation.
//!
//! This module provides the database type that holds configuration
//! and creates connections to Databricks SQL Warehouses.

use std::sync::{Arc, RwLock};

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
use adbc_core::{Database, Optionable};
use tokio::runtime::Runtime;

use crate::connection::DatabricksConnection;
use crate::options::{keys, DatabaseConfig, HttpConfig};

/// Databricks database.
///
/// Holds shared configuration and the Tokio runtime for creating connections
/// to Databricks SQL Warehouses.
///
/// # Contract
///
/// - Creating a Database does NOT establish a network connection
/// - Configuration is validated at Database creation time
/// - Runtime is shared across all connections from this Database
#[derive(Debug)]
pub struct DatabricksDatabase {
    /// Workspace host URL.
    host: RwLock<Option<String>>,
    /// SQL Warehouse ID.
    warehouse_id: RwLock<Option<String>>,
    /// Personal Access Token.
    token: RwLock<Option<String>>,
    /// Default catalog.
    default_catalog: RwLock<Option<String>>,
    /// Default schema.
    default_schema: RwLock<Option<String>>,
    /// HTTP configuration.
    http_config: RwLock<HttpConfig>,
    /// Chunk fetch concurrency.
    fetch_concurrency: RwLock<usize>,
    /// Shared Tokio runtime.
    runtime: RwLock<Option<Arc<Runtime>>>,
}

impl DatabricksDatabase {
    /// Create a new database with default configuration.
    pub fn new() -> Self {
        Self {
            host: RwLock::new(None),
            warehouse_id: RwLock::new(None),
            token: RwLock::new(None),
            default_catalog: RwLock::new(None),
            default_schema: RwLock::new(None),
            http_config: RwLock::new(HttpConfig::default()),
            fetch_concurrency: RwLock::new(8),
            runtime: RwLock::new(None),
        }
    }

    /// Build the configuration from the current options.
    fn build_config(&self) -> Result<DatabaseConfig> {
        let host = self
            .host
            .read()
            .unwrap()
            .clone()
            .ok_or_else(|| Error::with_message_and_status("uri is required", Status::InvalidArguments))?;
        let warehouse_id = self
            .warehouse_id
            .read()
            .unwrap()
            .clone()
            .ok_or_else(|| {
                Error::with_message_and_status(
                    "databricks.warehouse_id is required",
                    Status::InvalidArguments,
                )
            })?;
        let token = self.token.read().unwrap().clone().ok_or_else(|| {
            Error::with_message_and_status("databricks.token is required", Status::InvalidArguments)
        })?;

        let default_catalog = self.default_catalog.read().unwrap().clone();
        let default_schema = self.default_schema.read().unwrap().clone();
        let http_config = self.http_config.read().unwrap().clone();
        let fetch_concurrency = *self.fetch_concurrency.read().unwrap();

        Ok(DatabaseConfig {
            host,
            warehouse_id,
            token,
            default_catalog,
            default_schema,
            http_config,
            fetch_concurrency,
        })
    }

    /// Get or create the shared Tokio runtime.
    fn get_or_create_runtime(&self) -> Result<Arc<Runtime>> {
        // First try to read existing runtime
        {
            let guard = self.runtime.read().unwrap();
            if let Some(ref runtime) = *guard {
                return Ok(runtime.clone());
            }
        }

        // Need to create new runtime
        let mut guard = self.runtime.write().unwrap();
        // Check again in case another thread created it
        if let Some(ref runtime) = *guard {
            return Ok(runtime.clone());
        }

        let runtime = Runtime::new().map_err(|e| {
            Error::with_message_and_status(
                format!("Failed to create Tokio runtime: {}", e),
                Status::Internal,
            )
        })?;
        let runtime = Arc::new(runtime);
        *guard = Some(runtime.clone());
        Ok(runtime)
    }
}

impl Default for DatabricksDatabase {
    fn default() -> Self {
        Self::new()
    }
}

impl Optionable for DatabricksDatabase {
    type Option = OptionDatabase;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        // Helper to extract string from OptionValue
        fn extract_string(value: OptionValue, option_name: &str) -> Result<String> {
            if let OptionValue::String(s) = value {
                Ok(s)
            } else {
                Err(Error::with_message_and_status(
                    format!("{} must be a string", option_name),
                    Status::InvalidArguments,
                ))
            }
        }

        match key {
            OptionDatabase::Uri => {
                let mut guard = self.host.write().unwrap();
                *guard = Some(extract_string(value, "uri")?);
            }
            OptionDatabase::Password => {
                let mut guard = self.token.write().unwrap();
                *guard = Some(extract_string(value, "password")?);
            }
            OptionDatabase::Other(key) => {
                let string_value = extract_string(value, &key)?;

                match key.as_str() {
                    keys::WAREHOUSE_ID => {
                        let mut guard = self.warehouse_id.write().unwrap();
                        *guard = Some(string_value);
                    }
                    keys::TOKEN => {
                        let mut guard = self.token.write().unwrap();
                        *guard = Some(string_value);
                    }
                    keys::CATALOG => {
                        let mut guard = self.default_catalog.write().unwrap();
                        *guard = Some(string_value);
                    }
                    keys::SCHEMA => {
                        let mut guard = self.default_schema.write().unwrap();
                        *guard = Some(string_value);
                    }
                    keys::FETCH_CONCURRENCY => {
                        let mut guard = self.fetch_concurrency.write().unwrap();
                        *guard = string_value.parse().map_err(|_| {
                            Error::with_message_and_status(
                                format!("{} must be a positive integer", keys::FETCH_CONCURRENCY),
                                Status::InvalidArguments,
                            )
                        })?;
                    }
                    _ => {
                        return Err(Error::with_message_and_status(
                            format!("Unknown option: {}", key),
                            Status::InvalidArguments,
                        ));
                    }
                }
            }
            _ => {
                return Err(Error::with_message_and_status(
                    format!("Unknown option: {:?}", key),
                    Status::InvalidArguments,
                ));
            }
        }
        Ok(())
    }

    fn get_option_bytes(&self, _key: Self::Option) -> Result<Vec<u8>> {
        Err(Error::with_message_and_status(
            "get_option_bytes not implemented",
            Status::NotImplemented,
        ))
    }

    fn get_option_double(&self, _key: Self::Option) -> Result<f64> {
        Err(Error::with_message_and_status(
            "get_option_double not implemented",
            Status::NotImplemented,
        ))
    }

    fn get_option_int(&self, _key: Self::Option) -> Result<i64> {
        Err(Error::with_message_and_status(
            "get_option_int not implemented",
            Status::NotImplemented,
        ))
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match key {
            OptionDatabase::Uri => self.host.read().unwrap().clone().ok_or_else(|| {
                Error::with_message_and_status("uri not set", Status::InvalidArguments)
            }),
            OptionDatabase::Other(key) => match key.as_str() {
                keys::WAREHOUSE_ID => self.warehouse_id.read().unwrap().clone().ok_or_else(|| {
                    Error::with_message_and_status("warehouse_id not set", Status::InvalidArguments)
                }),
                keys::CATALOG => self.default_catalog.read().unwrap().clone().ok_or_else(|| {
                    Error::with_message_and_status("catalog not set", Status::InvalidArguments)
                }),
                keys::SCHEMA => self.default_schema.read().unwrap().clone().ok_or_else(|| {
                    Error::with_message_and_status("schema not set", Status::InvalidArguments)
                }),
                _ => Err(Error::with_message_and_status(
                    format!("Unknown option: {}", key),
                    Status::InvalidArguments,
                )),
            },
            _ => Err(Error::with_message_and_status(
                format!("Unknown option: {:?}", key),
                Status::InvalidArguments,
            )),
        }
    }
}

impl Database for DatabricksDatabase {
    type ConnectionType = DatabricksConnection;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        let config = Arc::new(self.build_config()?);
        let runtime = self.get_or_create_runtime()?;
        DatabricksConnection::new(config, runtime)
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        let config = Arc::new(self.build_config()?);
        let runtime = self.get_or_create_runtime()?;
        let mut connection = DatabricksConnection::new(config, runtime)?;
        for (key, value) in opts {
            connection.set_option(key, value)?;
        }
        Ok(connection)
    }
}
