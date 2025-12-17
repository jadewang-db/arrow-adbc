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

//! Connection implementation for Databricks

use std::collections::HashSet;
use std::sync::Arc;

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{InfoCode, ObjectDepth, OptionConnection, OptionValue};
use adbc_core::{Connection, Optionable};
use arrow_array::RecordBatchReader;
use arrow_schema::Schema;
use tokio::runtime::Runtime;

use crate::client::{SeaClient, SeaClientConfig};
use crate::options::DatabaseConfig;
use crate::session::SessionManager;
use crate::statement::DatabricksStatement;

/// Empty reader for unimplemented methods that return RecordBatchReader
/// This is used as a placeholder until the methods are fully implemented
struct UnimplementedBatchReader {
    schema: std::sync::Arc<Schema>,
}

impl UnimplementedBatchReader {
    fn new() -> Self {
        Self {
            schema: std::sync::Arc::new(Schema::empty()),
        }
    }
}

impl Iterator for UnimplementedBatchReader {
    type Item = std::result::Result<arrow_array::RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        // This should never be called since methods that return this type use todo!()
        unreachable!("UnimplementedBatchReader should not be used")
    }
}

impl RecordBatchReader for UnimplementedBatchReader {
    fn schema(&self) -> std::sync::Arc<Schema> {
        self.schema.clone()
    }
}

/// Connection to a Databricks SQL Warehouse
///
/// Holds the client, session manager, and runtime needed for executing
/// SQL statements against a Databricks SQL Warehouse.
#[derive(Debug)]
pub struct DatabricksConnection {
    #[allow(dead_code)]
    client: Arc<SeaClient>,
    session_manager: Arc<SessionManager>,
    runtime: Arc<Runtime>,
    #[allow(dead_code)]
    config: DatabaseConfig,
    current_catalog: Option<String>,
    current_schema: Option<String>,
}

impl DatabricksConnection {
    /// Create a new connection with the given configuration and runtime
    ///
    /// This will create a SEA client and session manager, and immediately
    /// establish a session with the SQL Warehouse.
    ///
    /// # Arguments
    /// * `config` - Database configuration containing connection details
    /// * `runtime` - Tokio runtime to use for async operations
    ///
    /// # Errors
    /// Returns an error if the client cannot be created or the initial
    /// session cannot be established.
    pub fn new(config: DatabaseConfig, runtime: Arc<Runtime>) -> Result<Self> {
        // Create SEA client
        let client_config = SeaClientConfig {
            host: config.host.clone().unwrap(),
            token: config.token.clone().unwrap(),
            warehouse_id: config.warehouse_id.clone().unwrap(),
            connect_timeout: config.http_config.connect_timeout,
            read_timeout: config.http_config.read_timeout,
        };

        let client = Arc::new(
            SeaClient::new(client_config)
                .map_err(|e| Error::with_message_and_status(e.to_string(), Status::Internal))?,
        );

        // Create session manager
        let session_manager = Arc::new(SessionManager::new(
            client.clone(),
            config.default_catalog.clone(),
            config.default_schema.clone(),
        ));

        // Create session immediately to validate connectivity
        let _session_id = runtime
            .block_on(session_manager.get_session_id())
            .map_err(|e| Error::with_message_and_status(e.to_string(), Status::Internal))?;

        Ok(Self {
            client,
            session_manager,
            runtime,
            current_catalog: config.default_catalog.clone(),
            current_schema: config.default_schema.clone(),
            config,
        })
    }

    /// Get the session ID for this connection
    ///
    /// This is primarily used for testing and debugging purposes.
    ///
    /// # Errors
    /// Returns an error if the session ID cannot be retrieved.
    pub fn session_id(&self) -> Result<String> {
        self.runtime
            .block_on(self.session_manager.get_session_id())
            .map_err(|e| Error::with_message_and_status(e.to_string(), Status::Internal))
    }
}

impl Optionable for DatabricksConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match key {
            OptionConnection::AutoCommit => {
                let val = match value {
                    OptionValue::String(s) => s,
                    _ => {
                        return Err(Error::with_message_and_status(
                            "AutoCommit option must be a string",
                            Status::InvalidArguments,
                        ))
                    }
                };
                if val != "true" {
                    return Err(Error::with_message_and_status(
                        "Databricks only supports autocommit mode",
                        Status::InvalidArguments,
                    ));
                }
                Ok(())
            }
            OptionConnection::CurrentCatalog => {
                let val = match value {
                    OptionValue::String(s) => s,
                    _ => {
                        return Err(Error::with_message_and_status(
                            "CurrentCatalog option must be a string",
                            Status::InvalidArguments,
                        ))
                    }
                };
                self.current_catalog = Some(val);
                Ok(())
            }
            OptionConnection::CurrentSchema => {
                let val = match value {
                    OptionValue::String(s) => s,
                    _ => {
                        return Err(Error::with_message_and_status(
                            "CurrentSchema option must be a string",
                            Status::InvalidArguments,
                        ))
                    }
                };
                self.current_schema = Some(val);
                Ok(())
            }
            OptionConnection::Other(ref k) => Err(Error::with_message_and_status(
                format!("Unknown connection option: {}", k),
                Status::NotImplemented,
            )),
            _ => Err(Error::with_message_and_status(
                format!("Unsupported connection option: {:?}", key),
                Status::NotImplemented,
            )),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match key {
            OptionConnection::AutoCommit => Ok("true".to_string()),
            OptionConnection::CurrentCatalog => self.current_catalog.clone().ok_or_else(|| {
                Error::with_message_and_status("Current catalog not set", Status::NotFound)
            }),
            OptionConnection::CurrentSchema => self.current_schema.clone().ok_or_else(|| {
                Error::with_message_and_status("Current schema not set", Status::NotFound)
            }),
            _ => Err(Error::with_message_and_status(
                format!("Unknown option: {:?}", key),
                Status::NotFound,
            )),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        Err(Error::with_message_and_status(
            format!("Option {:?} is not a byte array", key),
            Status::NotFound,
        ))
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        Err(Error::with_message_and_status(
            format!("Option {:?} is not an integer", key),
            Status::NotFound,
        ))
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        Err(Error::with_message_and_status(
            format!("Option {:?} is not a double", key),
            Status::NotFound,
        ))
    }
}

impl Connection for DatabricksConnection {
    type StatementType = DatabricksStatement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        DatabricksStatement::new(
            self.client.clone(),
            self.session_manager.clone(),
            self.runtime.clone(),
            &self.config,
        )
    }

    fn cancel(&mut self) -> Result<()> {
        // Cancel any active operations on this connection
        // Since statements are independent, there's nothing to cancel at the connection level
        Ok(())
    }

    fn get_info(&self, _codes: Option<HashSet<InfoCode>>) -> Result<impl RecordBatchReader> {
        // Metadata methods will be implemented in Sprint 4
        #[allow(unreachable_code)]
        {
            todo!("Implemented in Sprint 4");
            Ok(UnimplementedBatchReader::new())
        }
    }

    fn get_objects(
        &self,
        _depth: ObjectDepth,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _table_type: Option<Vec<&str>>,
        _column_name: Option<&str>,
    ) -> Result<impl RecordBatchReader> {
        // Metadata methods will be implemented in Sprint 4
        #[allow(unreachable_code)]
        {
            todo!("Implemented in Sprint 4");
            Ok(UnimplementedBatchReader::new())
        }
    }

    fn get_table_schema(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: &str,
    ) -> Result<Schema> {
        // Metadata methods will be implemented in Sprint 4
        todo!("Implemented in Sprint 4")
    }

    fn get_table_types(&self) -> Result<impl RecordBatchReader> {
        // Metadata methods will be implemented in Sprint 4
        #[allow(unreachable_code)]
        {
            todo!("Implemented in Sprint 4");
            Ok(UnimplementedBatchReader::new())
        }
    }

    fn commit(&mut self) -> Result<()> {
        Err(Error::with_message_and_status(
            "Transactions not supported by Databricks",
            Status::NotImplemented,
        ))
    }

    fn rollback(&mut self) -> Result<()> {
        Err(Error::with_message_and_status(
            "Transactions not supported by Databricks",
            Status::NotImplemented,
        ))
    }

    fn read_partition(&self, _partition: impl AsRef<[u8]>) -> Result<impl RecordBatchReader> {
        #[allow(unreachable_code)]
        {
            return Err(Error::with_message_and_status(
                "Partitioned reads not supported",
                Status::NotImplemented,
            ));
            Ok(UnimplementedBatchReader::new())
        }
    }

    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> Result<impl RecordBatchReader> {
        #[allow(unreachable_code)]
        {
            return Err(Error::with_message_and_status(
                "Statistics not supported",
                Status::NotImplemented,
            ));
            Ok(UnimplementedBatchReader::new())
        }
    }

    fn get_statistic_names(&self) -> Result<impl RecordBatchReader> {
        #[allow(unreachable_code)]
        {
            return Err(Error::with_message_and_status(
                "Statistics not supported",
                Status::NotImplemented,
            ));
            Ok(UnimplementedBatchReader::new())
        }
    }
}

impl Drop for DatabricksConnection {
    /// Clean up the connection by terminating the session
    ///
    /// This ensures that server-side resources are released when the
    /// connection is dropped.
    fn drop(&mut self) {
        // Attempt to terminate the session
        // We ignore errors here since drop handlers should not panic
        let _ = self.runtime.block_on(self.session_manager.terminate());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::options::DatabaseConfig;

    /// Helper to create a test connection for unit tests
    /// Note: This does not actually connect to Databricks, it just creates the struct
    fn create_mock_connection() -> DatabricksConnection {
        // Create minimal config for testing
        let config = DatabaseConfig {
            host: Some("https://test.databricks.com".to_string()),
            warehouse_id: Some("test-warehouse".to_string()),
            token: Some("test-token".to_string()),
            default_catalog: Some("test_catalog".to_string()),
            default_schema: Some("test_schema".to_string()),
            http_config: crate::options::HttpConfig::default(),
            fetch_config: crate::options::FetchConfig::default(),
        };

        let runtime = Arc::new(
            tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime"),
        );

        // Create client config
        let client_config = SeaClientConfig {
            host: config.host.clone().unwrap(),
            token: config.token.clone().unwrap(),
            warehouse_id: config.warehouse_id.clone().unwrap(),
            connect_timeout: config.http_config.connect_timeout,
            read_timeout: config.http_config.read_timeout,
        };

        let client = Arc::new(
            SeaClient::new(client_config).expect("Failed to create test client"),
        );

        let session_manager = Arc::new(SessionManager::new(
            client.clone(),
            config.default_catalog.clone(),
            config.default_schema.clone(),
        ));

        DatabricksConnection {
            client,
            session_manager,
            runtime,
            current_catalog: config.default_catalog.clone(),
            current_schema: config.default_schema.clone(),
            config,
        }
    }

    #[test]
    fn test_connection_autocommit_always_true() {
        let conn = create_mock_connection();

        // AutoCommit should always return "true"
        let result = conn.get_option_string(OptionConnection::AutoCommit);
        assert!(
            result.is_ok(),
            "Should be able to get AutoCommit option: {:?}",
            result.err()
        );
        assert_eq!(result.unwrap(), "true");
    }

    #[test]
    fn test_connection_autocommit_cannot_disable() {
        let mut conn = create_mock_connection();

        // Cannot set autocommit to false
        let result = conn.set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("false".into()),
        );
        assert!(result.is_err(), "Should not be able to disable autocommit");

        let err = result.unwrap_err();
        assert_eq!(
            err.status,
            Status::InvalidArguments,
            "Should return InvalidArguments status"
        );
        assert!(
            err.message.contains("autocommit"),
            "Error message should mention autocommit: {}",
            err.message
        );
    }

    #[test]
    fn test_connection_autocommit_set_true_succeeds() {
        let mut conn = create_mock_connection();

        // Setting autocommit to true should succeed
        let result = conn.set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("true".into()),
        );
        assert!(
            result.is_ok(),
            "Should be able to set autocommit to true: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_connection_current_catalog_get_set() {
        let mut conn = create_mock_connection();

        // Should be able to get initial catalog
        let result = conn.get_option_string(OptionConnection::CurrentCatalog);
        assert!(
            result.is_ok(),
            "Should be able to get current catalog: {:?}",
            result.err()
        );
        assert_eq!(result.unwrap(), "test_catalog");

        // Should be able to set new catalog
        let result = conn.set_option(
            OptionConnection::CurrentCatalog,
            OptionValue::String("new_catalog".into()),
        );
        assert!(
            result.is_ok(),
            "Should be able to set catalog: {:?}",
            result.err()
        );

        // Verify new value
        let result = conn.get_option_string(OptionConnection::CurrentCatalog);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "new_catalog");
    }

    #[test]
    fn test_connection_current_schema_get_set() {
        let mut conn = create_mock_connection();

        // Should be able to get initial schema
        let result = conn.get_option_string(OptionConnection::CurrentSchema);
        assert!(
            result.is_ok(),
            "Should be able to get current schema: {:?}",
            result.err()
        );
        assert_eq!(result.unwrap(), "test_schema");

        // Should be able to set new schema
        let result = conn.set_option(
            OptionConnection::CurrentSchema,
            OptionValue::String("new_schema".into()),
        );
        assert!(
            result.is_ok(),
            "Should be able to set schema: {:?}",
            result.err()
        );

        // Verify new value
        let result = conn.get_option_string(OptionConnection::CurrentSchema);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "new_schema");
    }

    #[test]
    fn test_connection_catalog_not_set_returns_not_found() {
        let mut conn = create_mock_connection();
        conn.current_catalog = None;

        let result = conn.get_option_string(OptionConnection::CurrentCatalog);
        assert!(result.is_err(), "Should return error when catalog not set");

        let err = result.unwrap_err();
        assert_eq!(
            err.status,
            Status::NotFound,
            "Should return NotFound status"
        );
        assert!(
            err.message.contains("not set"),
            "Error message should mention not set: {}",
            err.message
        );
    }

    #[test]
    fn test_connection_schema_not_set_returns_not_found() {
        let mut conn = create_mock_connection();
        conn.current_schema = None;

        let result = conn.get_option_string(OptionConnection::CurrentSchema);
        assert!(result.is_err(), "Should return error when schema not set");

        let err = result.unwrap_err();
        assert_eq!(
            err.status,
            Status::NotFound,
            "Should return NotFound status"
        );
        assert!(
            err.message.contains("not set"),
            "Error message should mention not set: {}",
            err.message
        );
    }

    #[test]
    fn test_connection_unknown_option_returns_not_implemented() {
        let mut conn = create_mock_connection();

        // Try to set an unknown option
        let result = conn.set_option(
            OptionConnection::Other("unknown_option".to_string()),
            OptionValue::String("value".into()),
        );
        assert!(result.is_err(), "Should return error for unknown option");

        let err = result.unwrap_err();
        assert_eq!(
            err.status,
            Status::NotImplemented,
            "Should return NotImplemented status"
        );
        assert!(
            err.message.contains("Unknown"),
            "Error message should mention unknown option: {}",
            err.message
        );
    }

    #[test]
    fn test_connection_get_option_bytes_not_supported() {
        let conn = create_mock_connection();

        let result = conn.get_option_bytes(OptionConnection::AutoCommit);
        assert!(result.is_err(), "get_option_bytes should not be supported");

        let err = result.unwrap_err();
        assert_eq!(err.status, Status::NotFound);
        assert!(err.message.contains("not a byte array"));
    }

    #[test]
    fn test_connection_get_option_int_not_supported() {
        let conn = create_mock_connection();

        let result = conn.get_option_int(OptionConnection::AutoCommit);
        assert!(result.is_err(), "get_option_int should not be supported");

        let err = result.unwrap_err();
        assert_eq!(err.status, Status::NotFound);
        assert!(err.message.contains("not an integer"));
    }

    #[test]
    fn test_connection_get_option_double_not_supported() {
        let conn = create_mock_connection();

        let result = conn.get_option_double(OptionConnection::AutoCommit);
        assert!(
            result.is_err(),
            "get_option_double should not be supported"
        );

        let err = result.unwrap_err();
        assert_eq!(err.status, Status::NotFound);
        assert!(err.message.contains("not a double"));
    }
}
