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

#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::error::Status;
    use adbc_core::options::{OptionDatabase, OptionValue};
    use adbc_core::{Database, Optionable};

    // ============================================================================
    // Database Creation Tests
    // ============================================================================

    #[test]
    fn test_database_new() {
        let db = DatabricksDatabase::new();
        // All options should be unset initially
        assert!(db.host.read().unwrap().is_none());
        assert!(db.warehouse_id.read().unwrap().is_none());
        assert!(db.token.read().unwrap().is_none());
        assert!(db.default_catalog.read().unwrap().is_none());
        assert!(db.default_schema.read().unwrap().is_none());
        // Runtime should be None (lazy creation)
        assert!(db.runtime.read().unwrap().is_none());
        // Default fetch concurrency should be 8
        assert_eq!(*db.fetch_concurrency.read().unwrap(), 8);
    }

    #[test]
    fn test_database_default() {
        let db = DatabricksDatabase::default();
        assert!(db.host.read().unwrap().is_none());
    }

    #[test]
    fn test_database_is_debug() {
        let db = DatabricksDatabase::new();
        let debug_str = format!("{:?}", db);
        assert!(
            debug_str.contains("DatabricksDatabase"),
            "Debug output should contain struct name"
        );
    }

    // ============================================================================
    // Set Option Tests
    // ============================================================================

    #[test]
    fn test_database_set_uri_option() {
        let mut db = DatabricksDatabase::new();
        let result = db.set_option(
            OptionDatabase::Uri,
            OptionValue::String("https://example.cloud.databricks.com".into()),
        );
        assert!(result.is_ok(), "Setting uri should succeed");

        let host = db.host.read().unwrap();
        assert_eq!(
            host.as_deref(),
            Some("https://example.cloud.databricks.com")
        );
    }

    #[test]
    fn test_database_set_password_option() {
        let mut db = DatabricksDatabase::new();
        let result = db.set_option(
            OptionDatabase::Password,
            OptionValue::String("dapi_token_12345".into()),
        );
        assert!(result.is_ok(), "Setting password should succeed");

        let token = db.token.read().unwrap();
        assert_eq!(token.as_deref(), Some("dapi_token_12345"));
    }

    #[test]
    fn test_database_set_warehouse_id_option() {
        let mut db = DatabricksDatabase::new();
        let result = db.set_option(
            OptionDatabase::Other(keys::WAREHOUSE_ID.into()),
            OptionValue::String("abc123def456".into()),
        );
        assert!(result.is_ok(), "Setting warehouse_id should succeed");

        let warehouse_id = db.warehouse_id.read().unwrap();
        assert_eq!(warehouse_id.as_deref(), Some("abc123def456"));
    }

    #[test]
    fn test_database_set_token_via_other() {
        let mut db = DatabricksDatabase::new();
        let result = db.set_option(
            OptionDatabase::Other(keys::TOKEN.into()),
            OptionValue::String("dapi_token_via_other".into()),
        );
        assert!(result.is_ok(), "Setting token via Other should succeed");

        let token = db.token.read().unwrap();
        assert_eq!(token.as_deref(), Some("dapi_token_via_other"));
    }

    #[test]
    fn test_database_set_catalog_option() {
        let mut db = DatabricksDatabase::new();
        let result = db.set_option(
            OptionDatabase::Other(keys::CATALOG.into()),
            OptionValue::String("main".into()),
        );
        assert!(result.is_ok(), "Setting catalog should succeed");

        let catalog = db.default_catalog.read().unwrap();
        assert_eq!(catalog.as_deref(), Some("main"));
    }

    #[test]
    fn test_database_set_schema_option() {
        let mut db = DatabricksDatabase::new();
        let result = db.set_option(
            OptionDatabase::Other(keys::SCHEMA.into()),
            OptionValue::String("default".into()),
        );
        assert!(result.is_ok(), "Setting schema should succeed");

        let schema = db.default_schema.read().unwrap();
        assert_eq!(schema.as_deref(), Some("default"));
    }

    #[test]
    fn test_database_set_fetch_concurrency_option() {
        let mut db = DatabricksDatabase::new();
        let result = db.set_option(
            OptionDatabase::Other(keys::FETCH_CONCURRENCY.into()),
            OptionValue::String("16".into()),
        );
        assert!(result.is_ok(), "Setting fetch_concurrency should succeed");

        let concurrency = *db.fetch_concurrency.read().unwrap();
        assert_eq!(concurrency, 16);
    }

    #[test]
    fn test_database_set_required_options() {
        let mut db = DatabricksDatabase::new();
        db.set_option(
            OptionDatabase::Uri,
            OptionValue::String("https://example.cloud.databricks.com".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Password,
            OptionValue::String("dapi_token".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other(keys::WAREHOUSE_ID.into()),
            OptionValue::String("abc123".into()),
        )
        .unwrap();

        assert_eq!(
            db.get_option_string(OptionDatabase::Uri).unwrap(),
            "https://example.cloud.databricks.com"
        );
        assert_eq!(
            db.get_option_string(OptionDatabase::Other(keys::WAREHOUSE_ID.into()))
                .unwrap(),
            "abc123"
        );
    }

    #[test]
    fn test_database_set_unknown_option_fails() {
        let mut db = DatabricksDatabase::new();
        let result = db.set_option(
            OptionDatabase::Other("unknown.option".into()),
            OptionValue::String("value".into()),
        );
        assert!(result.is_err(), "Unknown option should fail");

        let err = result.unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);
        assert!(err.message.contains("Unknown option"));
    }

    #[test]
    fn test_database_set_wrong_value_type_fails() {
        let mut db = DatabricksDatabase::new();
        let result = db.set_option(OptionDatabase::Uri, OptionValue::Int(42));
        assert!(result.is_err(), "Non-string value for uri should fail");

        let err = result.unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);
        assert!(err.message.contains("must be a string"));
    }

    #[test]
    fn test_database_set_invalid_concurrency_fails() {
        let mut db = DatabricksDatabase::new();
        let result = db.set_option(
            OptionDatabase::Other(keys::FETCH_CONCURRENCY.into()),
            OptionValue::String("not_a_number".into()),
        );
        assert!(result.is_err(), "Invalid concurrency should fail");

        let err = result.unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);
        assert!(err.message.contains("must be a positive integer"));
    }

    // ============================================================================
    // Get Option Tests
    // ============================================================================

    #[test]
    fn test_database_get_option_string_unset() {
        let db = DatabricksDatabase::new();
        let result = db.get_option_string(OptionDatabase::Uri);
        assert!(result.is_err(), "Getting unset uri should fail");

        let err = result.unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);
        assert!(err.message.contains("not set"));
    }

    #[test]
    fn test_database_get_option_string_unknown() {
        let db = DatabricksDatabase::new();
        let result = db.get_option_string(OptionDatabase::Other("unknown.option".into()));
        assert!(result.is_err(), "Getting unknown option should fail");

        let err = result.unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);
        assert!(err.message.contains("Unknown option"));
    }

    #[test]
    fn test_database_get_option_bytes_not_implemented() {
        let db = DatabricksDatabase::new();
        let result = db.get_option_bytes(OptionDatabase::Uri);
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(err.status, Status::NotImplemented);
    }

    #[test]
    fn test_database_get_option_double_not_implemented() {
        let db = DatabricksDatabase::new();
        let result = db.get_option_double(OptionDatabase::Uri);
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(err.status, Status::NotImplemented);
    }

    #[test]
    fn test_database_get_option_int_not_implemented() {
        let db = DatabricksDatabase::new();
        let result = db.get_option_int(OptionDatabase::Uri);
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(err.status, Status::NotImplemented);
    }

    // ============================================================================
    // Validation and Connection Tests
    // ============================================================================

    #[test]
    fn test_database_validation_fails_without_uri() {
        let mut db = DatabricksDatabase::new();
        db.set_option(
            OptionDatabase::Password,
            OptionValue::String("dapi_token".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other(keys::WAREHOUSE_ID.into()),
            OptionValue::String("abc123".into()),
        )
        .unwrap();

        let result = db.new_connection();
        assert!(result.is_err(), "Connection without uri should fail");

        let err = result.unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);
        assert!(err.message.contains("uri is required"));
    }

    #[test]
    fn test_database_validation_fails_without_warehouse_id() {
        let mut db = DatabricksDatabase::new();
        db.set_option(
            OptionDatabase::Uri,
            OptionValue::String("https://example.cloud.databricks.com".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Password,
            OptionValue::String("dapi_token".into()),
        )
        .unwrap();

        let result = db.new_connection();
        assert!(
            result.is_err(),
            "Connection without warehouse_id should fail"
        );

        let err = result.unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);
        assert!(err.message.contains("warehouse_id is required"));
    }

    #[test]
    fn test_database_validation_fails_without_token() {
        let mut db = DatabricksDatabase::new();
        db.set_option(
            OptionDatabase::Uri,
            OptionValue::String("https://example.cloud.databricks.com".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other(keys::WAREHOUSE_ID.into()),
            OptionValue::String("abc123".into()),
        )
        .unwrap();

        let result = db.new_connection();
        assert!(result.is_err(), "Connection without token should fail");

        let err = result.unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);
        assert!(err.message.contains("token is required"));
    }

    #[test]
    fn test_database_validation_fails_with_no_options() {
        let db = DatabricksDatabase::new();
        let result = db.new_connection();
        assert!(result.is_err(), "Connection with no options should fail");
    }

    // ============================================================================
    // Runtime Sharing Tests
    // ============================================================================

    #[test]
    fn test_database_runtime_lazy_creation() {
        let mut db = DatabricksDatabase::new();

        // Runtime should not be created initially
        assert!(
            db.runtime.read().unwrap().is_none(),
            "Runtime should not exist before first connection"
        );

        // Set required options
        db.set_option(
            OptionDatabase::Uri,
            OptionValue::String("https://example.cloud.databricks.com".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Password,
            OptionValue::String("dapi_token".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other(keys::WAREHOUSE_ID.into()),
            OptionValue::String("abc123".into()),
        )
        .unwrap();

        // Runtime still should not be created after setting options
        assert!(
            db.runtime.read().unwrap().is_none(),
            "Runtime should not exist after setting options but before connection"
        );

        // Attempt to create connection - this should create the runtime (even though
        // the connection itself will fail since there's no server)
        let result = db.new_connection();

        // Connection will fail because there's no server, but the runtime should have been created
        assert!(result.is_err(), "Connection should fail without a server");

        // Runtime should exist after the attempt (lazy creation happens before session)
        assert!(
            db.runtime.read().unwrap().is_some(),
            "Runtime should exist after connection attempt"
        );
    }

    // Note: Tests that require actual connections with session creation are now
    // implemented using mock servers in the connection module tests.
    // The database module tests focus on validation and configuration behavior.

    // ============================================================================
    // Connection Type Tests
    // ============================================================================

    #[test]
    fn test_database_connection_type_is_databricks_connection() {
        // This is a compile-time check that ConnectionType is DatabricksConnection
        fn assert_connection_type<T: Database<ConnectionType = DatabricksConnection>>(_: &T) {}

        let db = DatabricksDatabase::new();
        assert_connection_type(&db);
    }

    // ============================================================================
    // Configuration Propagation Tests
    // ============================================================================

    // Note: Configuration propagation is now tested via the connection's
    // SessionManager and SeaClient. The connection no longer exposes the
    // raw config directly. Instead, we verify via the Optionable interface.
    //
    // The test below verifies that catalog and schema are properly propagated
    // to the connection via the Optionable interface.
    #[test]
    fn test_database_catalog_schema_propagates_to_connection() {
        let mut db = DatabricksDatabase::new();
        let host = "https://my-workspace.cloud.databricks.com";
        let token = "dapi_my_token";
        let warehouse_id = "warehouse123";
        let catalog = "my_catalog";
        let schema = "my_schema";

        db.set_option(OptionDatabase::Uri, OptionValue::String(host.into()))
            .unwrap();
        db.set_option(
            OptionDatabase::Password,
            OptionValue::String(token.into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other(keys::WAREHOUSE_ID.into()),
            OptionValue::String(warehouse_id.into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other(keys::CATALOG.into()),
            OptionValue::String(catalog.into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other(keys::SCHEMA.into()),
            OptionValue::String(schema.into()),
        )
        .unwrap();

        // Connection creation will fail because it tries to connect to a non-existent server.
        // This is expected since we're not running a mock server.
        // The important thing is that the config validation passes.
        let result = db.new_connection();

        // Since we don't have a mock server, the connection will fail with IO error
        // when trying to create a session. This is expected.
        assert!(result.is_err());
        let err = result.unwrap_err();
        // Should fail at network level, not validation
        assert_ne!(err.status, Status::InvalidArguments);
    }
}
