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

//! DatabricksDatabase implementation.

use std::sync::Arc;
use std::time::Duration;

use adbc_core::error::{Error, Status};
use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
use adbc_core::{Database, Optionable};

use crate::connection::DatabricksConnection;
use crate::options::{database, DatabaseConfig, HttpConfig};

/// Runtime wrapper for Tokio.
pub enum Runtime {
    /// External runtime handle.
    Handle(tokio::runtime::Handle),
    /// Owned runtime.
    Tokio(tokio::runtime::Runtime),
}

impl Runtime {
    /// Create a new runtime.
    pub fn new(handle: Option<tokio::runtime::Handle>) -> std::io::Result<Self> {
        if let Some(handle) = handle {
            Ok(Self::Handle(handle))
        } else {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;
            Ok(Self::Tokio(runtime))
        }
    }

    /// Block on a future.
    pub fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        match self {
            Runtime::Handle(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
            Runtime::Tokio(runtime) => runtime.block_on(future),
        }
    }
}

/// Holds shared configuration and the Tokio runtime.
///
/// Creating a Database does NOT establish a network connection.
/// Configuration is validated at Database creation time.
/// The runtime is shared across all connections from this Database.
pub struct DatabricksDatabase {
    /// Database configuration.
    config: DatabaseConfig,
    /// Tokio runtime handle.
    handle: Option<tokio::runtime::Handle>,
    /// Shared runtime (created lazily on first connection).
    runtime: Option<Arc<Runtime>>,
}

impl DatabricksDatabase {
    /// Create a new DatabricksDatabase with an optional runtime handle.
    pub(crate) fn new(handle: Option<tokio::runtime::Handle>) -> Self {
        Self {
            config: DatabaseConfig::default(),
            handle,
            runtime: None,
        }
    }

    /// Get or create the shared runtime.
    fn get_runtime(&mut self) -> adbc_core::error::Result<Arc<Runtime>> {
        if let Some(ref runtime) = self.runtime {
            return Ok(runtime.clone());
        }

        let runtime = Runtime::new(self.handle.clone()).map_err(|e| {
            Error::with_message_and_status(format!("Failed to create runtime: {}", e), Status::IO)
        })?;
        let runtime = Arc::new(runtime);
        self.runtime = Some(runtime.clone());
        Ok(runtime)
    }

    /// Validate the configuration.
    fn validate_config(&self) -> adbc_core::error::Result<()> {
        if self.config.host.is_empty() {
            return Err(Error::with_message_and_status(
                "Missing required option: uri (host)",
                Status::InvalidArguments,
            ));
        }
        if self.config.warehouse_id.is_empty() {
            return Err(Error::with_message_and_status(
                format!("Missing required option: {}", database::WAREHOUSE_ID),
                Status::InvalidArguments,
            ));
        }
        if self.config.token.is_empty() {
            return Err(Error::with_message_and_status(
                format!("Missing required option: {}", database::TOKEN),
                Status::InvalidArguments,
            ));
        }
        Ok(())
    }
}

impl Optionable for DatabricksDatabase {
    type Option = OptionDatabase;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> adbc_core::error::Result<()> {
        match key.as_ref() {
            "uri" => {
                if let OptionValue::String(v) = value {
                    // Parse the URI and extract the host
                    let uri = v.trim_end_matches('/');
                    self.config.host = uri.to_string();
                    Ok(())
                } else {
                    Err(Error::with_message_and_status(
                        "uri must be a string",
                        Status::InvalidArguments,
                    ))
                }
            }
            key if key == database::WAREHOUSE_ID => {
                if let OptionValue::String(v) = value {
                    self.config.warehouse_id = v;
                    Ok(())
                } else {
                    Err(Error::with_message_and_status(
                        format!("{} must be a string", database::WAREHOUSE_ID),
                        Status::InvalidArguments,
                    ))
                }
            }
            key if key == database::TOKEN => {
                if let OptionValue::String(v) = value {
                    self.config.token = v;
                    Ok(())
                } else {
                    Err(Error::with_message_and_status(
                        format!("{} must be a string", database::TOKEN),
                        Status::InvalidArguments,
                    ))
                }
            }
            key if key == database::CATALOG => {
                if let OptionValue::String(v) = value {
                    self.config.default_catalog = Some(v);
                    Ok(())
                } else {
                    Err(Error::with_message_and_status(
                        format!("{} must be a string", database::CATALOG),
                        Status::InvalidArguments,
                    ))
                }
            }
            key if key == database::SCHEMA => {
                if let OptionValue::String(v) = value {
                    self.config.default_schema = Some(v);
                    Ok(())
                } else {
                    Err(Error::with_message_and_status(
                        format!("{} must be a string", database::SCHEMA),
                        Status::InvalidArguments,
                    ))
                }
            }
            key if key == database::HTTP_CONNECT_TIMEOUT => {
                if let OptionValue::Int(v) = value {
                    self.config.http_config.connect_timeout = Duration::from_millis(v as u64);
                    Ok(())
                } else {
                    Err(Error::with_message_and_status(
                        format!("{} must be an integer", database::HTTP_CONNECT_TIMEOUT),
                        Status::InvalidArguments,
                    ))
                }
            }
            key if key == database::HTTP_READ_TIMEOUT => {
                if let OptionValue::Int(v) = value {
                    self.config.http_config.read_timeout = Duration::from_millis(v as u64);
                    Ok(())
                } else {
                    Err(Error::with_message_and_status(
                        format!("{} must be an integer", database::HTTP_READ_TIMEOUT),
                        Status::InvalidArguments,
                    ))
                }
            }
            key if key == database::FETCH_CONCURRENCY => {
                if let OptionValue::Int(v) = value {
                    self.config.fetch_concurrency = v as usize;
                    Ok(())
                } else {
                    Err(Error::with_message_and_status(
                        format!("{} must be an integer", database::FETCH_CONCURRENCY),
                        Status::InvalidArguments,
                    ))
                }
            }
            key if key == database::FETCH_COMPRESSION => {
                if let OptionValue::String(v) = value {
                    self.config.fetch_compression = v;
                    Ok(())
                } else {
                    Err(Error::with_message_and_status(
                        format!("{} must be a string", database::FETCH_COMPRESSION),
                        Status::InvalidArguments,
                    ))
                }
            }
            _ => Err(Error::with_message_and_status(
                format!("Unrecognized option: {:?}", key),
                Status::NotFound,
            )),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> adbc_core::error::Result<String> {
        match key.as_ref() {
            "uri" => Ok(self.config.host.clone()),
            key if key == database::WAREHOUSE_ID => Ok(self.config.warehouse_id.clone()),
            key if key == database::CATALOG => self.config.default_catalog.clone().ok_or_else(|| {
                Error::with_message_and_status(
                    format!("{} has not been set", database::CATALOG),
                    Status::NotFound,
                )
            }),
            key if key == database::SCHEMA => self.config.default_schema.clone().ok_or_else(|| {
                Error::with_message_and_status(
                    format!("{} has not been set", database::SCHEMA),
                    Status::NotFound,
                )
            }),
            key if key == database::FETCH_COMPRESSION => Ok(self.config.fetch_compression.clone()),
            _ => Err(Error::with_message_and_status(
                format!("Unrecognized option: {:?}", key),
                Status::NotFound,
            )),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> adbc_core::error::Result<Vec<u8>> {
        Err(Error::with_message_and_status(
            format!("Unrecognized option: {:?}", key),
            Status::NotFound,
        ))
    }

    fn get_option_int(&self, key: Self::Option) -> adbc_core::error::Result<i64> {
        match key.as_ref() {
            key if key == database::HTTP_CONNECT_TIMEOUT => {
                Ok(self.config.http_config.connect_timeout.as_millis() as i64)
            }
            key if key == database::HTTP_READ_TIMEOUT => {
                Ok(self.config.http_config.read_timeout.as_millis() as i64)
            }
            key if key == database::FETCH_CONCURRENCY => Ok(self.config.fetch_concurrency as i64),
            _ => Err(Error::with_message_and_status(
                format!("Unrecognized option: {:?}", key),
                Status::NotFound,
            )),
        }
    }

    fn get_option_double(&self, key: Self::Option) -> adbc_core::error::Result<f64> {
        Err(Error::with_message_and_status(
            format!("Unrecognized option: {:?}", key),
            Status::NotFound,
        ))
    }
}

impl Database for DatabricksDatabase {
    type ConnectionType = DatabricksConnection;

    fn new_connection(&self) -> adbc_core::error::Result<Self::ConnectionType> {
        self.validate_config()?;
        // For now, create a placeholder connection
        // Full implementation will be in a later work item
        DatabricksConnection::new(Arc::new(self.config.clone()))
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
    ) -> adbc_core::error::Result<Self::ConnectionType> {
        self.validate_config()?;
        let mut connection = DatabricksConnection::new(Arc::new(self.config.clone()))?;
        for (key, value) in opts {
            connection.set_option(key, value)?;
        }
        Ok(connection)
    }
}
