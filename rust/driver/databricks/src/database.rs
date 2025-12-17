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

//! Database implementation for Databricks

use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
use adbc_core::{Database, Optionable};

use crate::connection::DatabricksConnection;
use crate::options::{keys, DatabaseConfig};

/// Database handle for Databricks connections
///
/// Holds shared configuration for creating connections.
/// Creating a Database does NOT establish a network connection.
pub struct DatabricksDatabase {
    config: DatabaseConfig,
    runtime: Arc<Mutex<Option<Arc<Runtime>>>>,
}

impl DatabricksDatabase {
    /// Create a new DatabricksDatabase with default configuration
    pub fn new() -> Self {
        Self {
            config: DatabaseConfig::default(),
            runtime: Arc::new(Mutex::new(None)),
        }
    }

    /// Get or create the Tokio runtime
    ///
    /// The runtime is created lazily on first connection creation and shared
    /// across all connections from this database.
    fn get_runtime(&self) -> Arc<Runtime> {
        let mut guard = self.runtime.lock().expect("Failed to lock runtime mutex");
        if guard.is_none() {
            let rt = Runtime::new().expect("Failed to create Tokio runtime");
            *guard = Some(Arc::new(rt));
        }
        guard.as_ref().unwrap().clone()
    }

    /// Validate configuration before creating connection
    ///
    /// Ensures that all required options are set before attempting to
    /// create a connection.
    fn validate_config(&self) -> Result<()> {
        if self.config.host.is_none() {
            return Err(Error::with_message_and_status(
                "Missing required option: uri",
                Status::InvalidArguments,
            ));
        }
        if self.config.warehouse_id.is_none() {
            return Err(Error::with_message_and_status(
                "Missing required option: databricks.warehouse_id",
                Status::InvalidArguments,
            ));
        }
        if self.config.token.is_none() {
            return Err(Error::with_message_and_status(
                "Missing required option: databricks.token",
                Status::InvalidArguments,
            ));
        }
        Ok(())
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
        match key {
            OptionDatabase::Uri => {
                let uri = match value {
                    OptionValue::String(s) => s,
                    _ => return Err(Error::with_message_and_status("URI must be a string", Status::InvalidArguments)),
                };
                // Extract host from URI (remove protocol if present)
                let host = uri
                    .strip_prefix("https://")
                    .or_else(|| uri.strip_prefix("http://"))
                    .unwrap_or(&uri);
                self.config.host = Some(format!("https://{}", host.trim_end_matches('/')));
                Ok(())
            }
            OptionDatabase::Password => {
                // Password field is used for token
                let token = match value {
                    OptionValue::String(s) => s,
                    _ => return Err(Error::with_message_and_status("Token must be a string", Status::InvalidArguments)),
                };
                self.config.token = Some(token);
                Ok(())
            }
            OptionDatabase::Other(ref key_str) => {
                match key_str.as_str() {
                    keys::WAREHOUSE_ID => {
                        let warehouse_id = match value {
                            OptionValue::String(s) => s,
                            _ => return Err(Error::with_message_and_status(
                                "Warehouse ID must be a string",
                                Status::InvalidArguments,
                            )),
                        };
                        self.config.warehouse_id = Some(warehouse_id);
                        Ok(())
                    }
                    keys::TOKEN => {
                        let token = match value {
                            OptionValue::String(s) => s,
                            _ => return Err(Error::with_message_and_status(
                                "Token must be a string",
                                Status::InvalidArguments,
                            )),
                        };
                        self.config.token = Some(token);
                        Ok(())
                    }
                    keys::CATALOG => {
                        let catalog = match value {
                            OptionValue::String(s) => s,
                            _ => return Err(Error::with_message_and_status(
                                "Catalog must be a string",
                                Status::InvalidArguments,
                            )),
                        };
                        self.config.default_catalog = Some(catalog);
                        Ok(())
                    }
                    keys::SCHEMA => {
                        let schema = match value {
                            OptionValue::String(s) => s,
                            _ => return Err(Error::with_message_and_status(
                                "Schema must be a string",
                                Status::InvalidArguments,
                            )),
                        };
                        self.config.default_schema = Some(schema);
                        Ok(())
                    }
                    keys::HTTP_CONNECT_TIMEOUT => {
                        let timeout_ms = match value {
                            OptionValue::Int(i) => i,
                            _ => return Err(Error::with_message_and_status(
                                "Connect timeout must be an integer (milliseconds)",
                                Status::InvalidArguments,
                            )),
                        };
                        self.config.http_config.connect_timeout = std::time::Duration::from_millis(timeout_ms as u64);
                        Ok(())
                    }
                    keys::HTTP_READ_TIMEOUT => {
                        let timeout_ms = match value {
                            OptionValue::Int(i) => i,
                            _ => return Err(Error::with_message_and_status(
                                "Read timeout must be an integer (milliseconds)",
                                Status::InvalidArguments,
                            )),
                        };
                        self.config.http_config.read_timeout = std::time::Duration::from_millis(timeout_ms as u64);
                        Ok(())
                    }
                    keys::FETCH_CONCURRENCY => {
                        let concurrency = match value {
                            OptionValue::Int(i) => i,
                            _ => return Err(Error::with_message_and_status(
                                "Fetch concurrency must be an integer",
                                Status::InvalidArguments,
                            )),
                        };
                        self.config.fetch_config.concurrency = concurrency as usize;
                        Ok(())
                    }
                    keys::FETCH_COMPRESSION => {
                        let compression = match value {
                            OptionValue::String(s) => s,
                            _ => return Err(Error::with_message_and_status(
                                "Fetch compression must be a string",
                                Status::InvalidArguments,
                            )),
                        };
                        self.config.fetch_config.compression = compression;
                        Ok(())
                    }
                    _ => Err(Error::with_message_and_status(
                        format!("Unrecognized database option: {}", key_str),
                        Status::NotFound,
                    )),
                }
            }
            _ => Err(Error::with_message_and_status(
                format!("Unsupported database option: {:?}", key),
                Status::NotImplemented,
            )),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match key {
            OptionDatabase::Uri => self.config.host.clone().ok_or_else(|| {
                Error::with_message_and_status("URI not set", Status::NotFound)
            }),
            OptionDatabase::Password => self.config.token.clone().ok_or_else(|| {
                Error::with_message_and_status("Token not set", Status::NotFound)
            }),
            OptionDatabase::Other(ref key_str) => match key_str.as_str() {
                keys::WAREHOUSE_ID => self.config.warehouse_id.clone().ok_or_else(|| {
                    Error::with_message_and_status("Warehouse ID not set", Status::NotFound)
                }),
                keys::TOKEN => self.config.token.clone().ok_or_else(|| {
                    Error::with_message_and_status("Token not set", Status::NotFound)
                }),
                keys::CATALOG => self.config.default_catalog.clone().ok_or_else(|| {
                    Error::with_message_and_status("Catalog not set", Status::NotFound)
                }),
                keys::SCHEMA => self.config.default_schema.clone().ok_or_else(|| {
                    Error::with_message_and_status("Schema not set", Status::NotFound)
                }),
                keys::FETCH_COMPRESSION => Ok(self.config.fetch_config.compression.clone()),
                _ => Err(Error::with_message_and_status(
                    format!("Unrecognized database option: {}", key_str),
                    Status::NotFound,
                )),
            },
            _ => Err(Error::with_message_and_status(
                "Option type not supported as string",
                Status::InvalidData,
            )),
        }
    }

    fn get_option_bytes(&self, _key: Self::Option) -> Result<Vec<u8>> {
        Err(Error::with_message_and_status(
            "Bytes options not supported for database",
            Status::NotImplemented,
        ))
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        match key {
            OptionDatabase::Other(ref key_str) => match key_str.as_str() {
                keys::HTTP_CONNECT_TIMEOUT => {
                    Ok(self.config.http_config.connect_timeout.as_millis() as i64)
                }
                keys::HTTP_READ_TIMEOUT => {
                    Ok(self.config.http_config.read_timeout.as_millis() as i64)
                }
                keys::FETCH_CONCURRENCY => Ok(self.config.fetch_config.concurrency as i64),
                _ => Err(Error::with_message_and_status(
                    format!("Option {} is not an integer option", key_str),
                    Status::InvalidData,
                )),
            },
            _ => Err(Error::with_message_and_status(
                "Option type not supported as integer",
                Status::InvalidData,
            )),
        }
    }

    fn get_option_double(&self, _key: Self::Option) -> Result<f64> {
        Err(Error::with_message_and_status(
            "Double options not supported for database",
            Status::NotImplemented,
        ))
    }
}

impl Database for DatabricksDatabase {
    type ConnectionType = DatabricksConnection;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        self.validate_config()?;

        let runtime = self.get_runtime();
        DatabricksConnection::new(self.config.clone(), runtime)
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        self.validate_config()?;

        let runtime = self.get_runtime();
        let mut conn = DatabricksConnection::new(self.config.clone(), runtime)?;

        for (key, value) in opts {
            conn.set_option(key, value)?;
        }

        Ok(conn)
    }
}
