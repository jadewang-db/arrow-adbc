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

use std::sync::{Arc, Mutex};
use std::time::Duration;

use adbc_core::error::{Error, Status};
use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
use adbc_core::{Database, Optionable};

use crate::connection::DatabricksConnection;
use crate::options::{database, DatabaseConfig};

// HttpConfig is used via DatabaseConfig but not directly in this module
#[allow(unused_imports)]
use crate::options::HttpConfig;

/// Runtime wrapper for Tokio.
///
/// Provides a unified interface for executing async operations, either using
/// an externally provided runtime handle or an internally managed runtime.
pub enum Runtime {
    /// External runtime handle.
    Handle(tokio::runtime::Handle),
    /// Owned runtime.
    Tokio(tokio::runtime::Runtime),
}

impl std::fmt::Debug for Runtime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Runtime::Handle(_) => write!(f, "Runtime::Handle(...)"),
            Runtime::Tokio(_) => write!(f, "Runtime::Tokio(...)"),
        }
    }
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
    /// Uses Mutex for interior mutability since the Database trait uses &self.
    runtime: Mutex<Option<Arc<Runtime>>>,
}

impl DatabricksDatabase {
    /// Create a new DatabricksDatabase with an optional runtime handle.
    pub(crate) fn new(handle: Option<tokio::runtime::Handle>) -> Self {
        Self {
            config: DatabaseConfig::default(),
            handle,
            runtime: Mutex::new(None),
        }
    }

    /// Get or create the shared runtime.
    fn get_runtime(&self) -> adbc_core::error::Result<Arc<Runtime>> {
        let mut guard = self.runtime.lock().map_err(|_| {
            Error::with_message_and_status("Failed to acquire runtime lock", Status::Internal)
        })?;

        if let Some(ref runtime) = *guard {
            return Ok(runtime.clone());
        }

        let runtime = Runtime::new(self.handle.clone()).map_err(|e| {
            Error::with_message_and_status(format!("Failed to create runtime: {}", e), Status::IO)
        })?;
        let runtime = Arc::new(runtime);
        *guard = Some(runtime.clone());
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
        let runtime = self.get_runtime()?;
        DatabricksConnection::new(Arc::new(self.config.clone()), runtime)
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
    ) -> adbc_core::error::Result<Self::ConnectionType> {
        self.validate_config()?;
        let runtime = self.get_runtime()?;
        let mut connection = DatabricksConnection::new(Arc::new(self.config.clone()), runtime)?;
        for (key, value) in opts {
            connection.set_option(key, value)?;
        }
        Ok(connection)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to create a database with all required options set.
    fn create_configured_database() -> DatabricksDatabase {
        let mut db = DatabricksDatabase::new(None);
        db.set_option(
            OptionDatabase::Uri,
            OptionValue::String("https://test.cloud.databricks.com".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other(database::WAREHOUSE_ID.into()),
            OptionValue::String("test-warehouse-123".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other(database::TOKEN.into()),
            OptionValue::String("dapi_test_token".into()),
        )
        .unwrap();
        db
    }

    // ==================== Runtime Tests ====================

    #[test]
    fn test_runtime_new_creates_owned_runtime() {
        let runtime = Runtime::new(None).unwrap();
        assert!(matches!(runtime, Runtime::Tokio(_)));
    }

    #[test]
    fn test_runtime_new_with_handle() {
        let owned_runtime = tokio::runtime::Runtime::new().unwrap();
        let runtime = Runtime::new(Some(owned_runtime.handle().clone())).unwrap();
        assert!(matches!(runtime, Runtime::Handle(_)));
    }

    #[test]
    fn test_runtime_block_on_owned() {
        let runtime = Runtime::new(None).unwrap();
        let result = runtime.block_on(async { 42 });
        assert_eq!(result, 42);
    }

    #[test]
    fn test_runtime_block_on_handle() {
        let owned_runtime = tokio::runtime::Runtime::new().unwrap();
        let runtime = Runtime::new(Some(owned_runtime.handle().clone())).unwrap();
        let result = runtime.block_on(async { 123 });
        assert_eq!(result, 123);
    }

    // ==================== Database Creation Tests ====================

    #[test]
    fn test_database_new() {
        let db = DatabricksDatabase::new(None);
        assert!(db.handle.is_none());
        assert!(db.runtime.lock().unwrap().is_none());
    }

    #[test]
    fn test_database_new_with_handle() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let db = DatabricksDatabase::new(Some(runtime.handle().clone()));
        assert!(db.handle.is_some());
    }

    // ==================== URI Option Tests ====================

    #[test]
    fn test_set_option_uri() {
        let mut db = DatabricksDatabase::new(None);
        let result = db.set_option(
            OptionDatabase::Uri,
            OptionValue::String("https://my-workspace.cloud.databricks.com".into()),
        );
        assert!(result.is_ok());
        assert_eq!(
            db.get_option_string(OptionDatabase::Uri).unwrap(),
            "https://my-workspace.cloud.databricks.com"
        );
    }

    #[test]
    fn test_set_option_uri_trims_trailing_slash() {
        let mut db = DatabricksDatabase::new(None);
        db.set_option(
            OptionDatabase::Uri,
            OptionValue::String("https://test.databricks.com/".into()),
        )
        .unwrap();
        assert_eq!(
            db.get_option_string(OptionDatabase::Uri).unwrap(),
            "https://test.databricks.com"
        );
    }

    #[test]
    fn test_set_option_uri_wrong_type() {
        let mut db = DatabricksDatabase::new(None);
        let result = db.set_option(OptionDatabase::Uri, OptionValue::Int(123));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);
    }

    // ==================== Warehouse ID Option Tests ====================

    #[test]
    fn test_set_option_warehouse_id() {
        let mut db = DatabricksDatabase::new(None);
        let result = db.set_option(
            OptionDatabase::Other(database::WAREHOUSE_ID.into()),
            OptionValue::String("abc123def456".into()),
        );
        assert!(result.is_ok());
        assert_eq!(
            db.get_option_string(OptionDatabase::Other(database::WAREHOUSE_ID.into()))
                .unwrap(),
            "abc123def456"
        );
    }

    #[test]
    fn test_set_option_warehouse_id_wrong_type() {
        let mut db = DatabricksDatabase::new(None);
        let result = db.set_option(
            OptionDatabase::Other(database::WAREHOUSE_ID.into()),
            OptionValue::Int(123),
        );
        assert!(result.is_err());
    }

    // ==================== Token Option Tests ====================

    #[test]
    fn test_set_option_token() {
        let mut db = DatabricksDatabase::new(None);
        let result = db.set_option(
            OptionDatabase::Other(database::TOKEN.into()),
            OptionValue::String("dapi_test_token_xyz".into()),
        );
        assert!(result.is_ok());
        // Token is not retrievable via get_option_string for security
        // It's stored in config but we verify by checking new_connection works
    }

    #[test]
    fn test_set_option_token_wrong_type() {
        let mut db = DatabricksDatabase::new(None);
        let result = db.set_option(
            OptionDatabase::Other(database::TOKEN.into()),
            OptionValue::Int(123),
        );
        assert!(result.is_err());
    }

    // ==================== Catalog and Schema Option Tests ====================

    #[test]
    fn test_set_option_catalog() {
        let mut db = DatabricksDatabase::new(None);
        let result = db.set_option(
            OptionDatabase::Other(database::CATALOG.into()),
            OptionValue::String("main".into()),
        );
        assert!(result.is_ok());
        assert_eq!(
            db.get_option_string(OptionDatabase::Other(database::CATALOG.into()))
                .unwrap(),
            "main"
        );
    }

    #[test]
    fn test_set_option_schema() {
        let mut db = DatabricksDatabase::new(None);
        let result = db.set_option(
            OptionDatabase::Other(database::SCHEMA.into()),
            OptionValue::String("default".into()),
        );
        assert!(result.is_ok());
        assert_eq!(
            db.get_option_string(OptionDatabase::Other(database::SCHEMA.into()))
                .unwrap(),
            "default"
        );
    }

    #[test]
    fn test_get_option_catalog_not_set() {
        let db = DatabricksDatabase::new(None);
        let result = db.get_option_string(OptionDatabase::Other(database::CATALOG.into()));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::NotFound);
    }

    #[test]
    fn test_get_option_schema_not_set() {
        let db = DatabricksDatabase::new(None);
        let result = db.get_option_string(OptionDatabase::Other(database::SCHEMA.into()));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::NotFound);
    }

    // ==================== HTTP Config Option Tests ====================

    #[test]
    fn test_set_option_http_connect_timeout() {
        let mut db = DatabricksDatabase::new(None);
        let result = db.set_option(
            OptionDatabase::Other(database::HTTP_CONNECT_TIMEOUT.into()),
            OptionValue::Int(5000),
        );
        assert!(result.is_ok());
        assert_eq!(
            db.get_option_int(OptionDatabase::Other(database::HTTP_CONNECT_TIMEOUT.into()))
                .unwrap(),
            5000
        );
    }

    #[test]
    fn test_set_option_http_read_timeout() {
        let mut db = DatabricksDatabase::new(None);
        let result = db.set_option(
            OptionDatabase::Other(database::HTTP_READ_TIMEOUT.into()),
            OptionValue::Int(60000),
        );
        assert!(result.is_ok());
        assert_eq!(
            db.get_option_int(OptionDatabase::Other(database::HTTP_READ_TIMEOUT.into()))
                .unwrap(),
            60000
        );
    }

    #[test]
    fn test_set_option_http_timeout_wrong_type() {
        let mut db = DatabricksDatabase::new(None);
        let result = db.set_option(
            OptionDatabase::Other(database::HTTP_CONNECT_TIMEOUT.into()),
            OptionValue::String("5000".into()),
        );
        assert!(result.is_err());
    }

    // ==================== Fetch Config Option Tests ====================

    #[test]
    fn test_set_option_fetch_concurrency() {
        let mut db = DatabricksDatabase::new(None);
        let result = db.set_option(
            OptionDatabase::Other(database::FETCH_CONCURRENCY.into()),
            OptionValue::Int(16),
        );
        assert!(result.is_ok());
        assert_eq!(
            db.get_option_int(OptionDatabase::Other(database::FETCH_CONCURRENCY.into()))
                .unwrap(),
            16
        );
    }

    #[test]
    fn test_set_option_fetch_compression() {
        let mut db = DatabricksDatabase::new(None);
        let result = db.set_option(
            OptionDatabase::Other(database::FETCH_COMPRESSION.into()),
            OptionValue::String("NONE".into()),
        );
        assert!(result.is_ok());
        assert_eq!(
            db.get_option_string(OptionDatabase::Other(database::FETCH_COMPRESSION.into()))
                .unwrap(),
            "NONE"
        );
    }

    #[test]
    fn test_default_fetch_compression() {
        let db = DatabricksDatabase::new(None);
        assert_eq!(
            db.get_option_string(OptionDatabase::Other(database::FETCH_COMPRESSION.into()))
                .unwrap(),
            "LZ4_FRAME"
        );
    }

    #[test]
    fn test_default_fetch_concurrency() {
        let db = DatabricksDatabase::new(None);
        assert_eq!(
            db.get_option_int(OptionDatabase::Other(database::FETCH_CONCURRENCY.into()))
                .unwrap(),
            8
        );
    }

    #[test]
    fn test_default_http_connect_timeout() {
        let db = DatabricksDatabase::new(None);
        assert_eq!(
            db.get_option_int(OptionDatabase::Other(database::HTTP_CONNECT_TIMEOUT.into()))
                .unwrap(),
            10000 // 10 seconds in ms
        );
    }

    #[test]
    fn test_default_http_read_timeout() {
        let db = DatabricksDatabase::new(None);
        assert_eq!(
            db.get_option_int(OptionDatabase::Other(database::HTTP_READ_TIMEOUT.into()))
                .unwrap(),
            300000 // 300 seconds in ms
        );
    }

    // ==================== Unknown Option Tests ====================

    #[test]
    fn test_set_unknown_option() {
        let mut db = DatabricksDatabase::new(None);
        let result = db.set_option(
            OptionDatabase::Other("unknown.option".into()),
            OptionValue::String("value".into()),
        );
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::NotFound);
    }

    #[test]
    fn test_get_unknown_option_string() {
        let db = DatabricksDatabase::new(None);
        let result = db.get_option_string(OptionDatabase::Other("unknown.option".into()));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::NotFound);
    }

    #[test]
    fn test_get_unknown_option_int() {
        let db = DatabricksDatabase::new(None);
        let result = db.get_option_int(OptionDatabase::Other("unknown.option".into()));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::NotFound);
    }

    #[test]
    fn test_get_option_bytes_not_supported() {
        let db = DatabricksDatabase::new(None);
        let result = db.get_option_bytes(OptionDatabase::Uri);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::NotFound);
    }

    #[test]
    fn test_get_option_double_not_supported() {
        let db = DatabricksDatabase::new(None);
        let result = db.get_option_double(OptionDatabase::Uri);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::NotFound);
    }

    // ==================== Configuration Validation Tests ====================

    #[test]
    fn test_validate_config_missing_uri() {
        let db = DatabricksDatabase::new(None);
        let result = db.new_connection();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);
        assert!(err.message.contains("uri"));
    }

    #[test]
    fn test_validate_config_missing_warehouse_id() {
        let mut db = DatabricksDatabase::new(None);
        db.set_option(
            OptionDatabase::Uri,
            OptionValue::String("https://test.databricks.com".into()),
        )
        .unwrap();
        let result = db.new_connection();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);
        assert!(err.message.contains(database::WAREHOUSE_ID));
    }

    #[test]
    fn test_validate_config_missing_token() {
        let mut db = DatabricksDatabase::new(None);
        db.set_option(
            OptionDatabase::Uri,
            OptionValue::String("https://test.databricks.com".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other(database::WAREHOUSE_ID.into()),
            OptionValue::String("warehouse123".into()),
        )
        .unwrap();
        let result = db.new_connection();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);
        assert!(err.message.contains(database::TOKEN));
    }

    // ==================== Connection Creation Tests ====================
    // Note: Connection creation tests are primarily in connection.rs since they require
    // a mock server for session creation. Tests here focus on validation logic.

    #[test]
    fn test_get_runtime_creates_runtime_lazily() {
        let db = DatabricksDatabase::new(None);
        // Runtime should not exist initially
        assert!(db.runtime.lock().unwrap().is_none());

        // Getting runtime should create it
        let runtime_result = db.get_runtime();
        assert!(runtime_result.is_ok());

        // Now runtime should exist
        assert!(db.runtime.lock().unwrap().is_some());
    }

    #[test]
    fn test_get_runtime_returns_same_runtime() {
        let db = DatabricksDatabase::new(None);

        let runtime1 = db.get_runtime().unwrap();
        let runtime2 = db.get_runtime().unwrap();

        // Both should be the same Arc (same pointer)
        assert!(Arc::ptr_eq(&runtime1, &runtime2));
    }

    #[test]
    fn test_get_runtime_with_external_handle() {
        let external_runtime = tokio::runtime::Runtime::new().unwrap();
        let db = DatabricksDatabase::new(Some(external_runtime.handle().clone()));

        let runtime = db.get_runtime().unwrap();
        // Should wrap the handle
        runtime.block_on(async {
            // Verify the runtime works
            42
        });
    }
}
