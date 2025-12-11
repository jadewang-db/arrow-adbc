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

//! DatabricksConnection implementation.
//!
//! Represents an active session with the SQL Warehouse.
//!
//! # Session Lifecycle
//!
//! - Session created on `new_connection()`
//! - Session kept alive automatically (statements refresh the idle timeout)
//! - Session terminated on Connection drop
//!
//! # Example
//!
//! ```ignore
//! use adbc_core::{Database, Connection, Optionable};
//! use adbc_driver_databricks::DatabricksDatabase;
//!
//! let db = DatabricksDatabase::new(None);
//! // ... configure database ...
//! let mut connection = db.new_connection()?;
//!
//! // Get autocommit (always true for Databricks)
//! let autocommit = connection.get_option_string(OptionConnection::AutoCommit)?;
//! assert_eq!(autocommit, "true");
//!
//! // Set current catalog/schema
//! connection.set_option(
//!     OptionConnection::CurrentCatalog,
//!     OptionValue::String("main".into())
//! )?;
//! ```

use std::collections::HashSet;
use std::sync::Arc;

use adbc_core::error::{Error, Status};
use adbc_core::options::{InfoCode, ObjectDepth, OptionConnection, OptionValue};
use adbc_core::{Connection, Optionable};
use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, Schema, SchemaRef};

use crate::client::{SeaClient, SeaClientConfig};
use crate::database::Runtime;
use crate::options::DatabaseConfig;
use crate::session::SessionManager;
use crate::statement::DatabricksStatement;

/// A single batch reader for returning metadata results.
#[derive(Debug)]
pub struct SingleBatchReader {
    batch: Option<RecordBatch>,
    schema: SchemaRef,
}

impl SingleBatchReader {
    /// Create a new single batch reader.
    pub fn new(batch: RecordBatch) -> Self {
        let schema = batch.schema();
        Self {
            batch: Some(batch),
            schema,
        }
    }

    /// Create an empty reader with the given schema.
    pub fn empty(schema: SchemaRef) -> Self {
        Self {
            batch: None,
            schema,
        }
    }
}

impl Iterator for SingleBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        Ok(self.batch.take()).transpose()
    }
}

impl arrow_array::RecordBatchReader for SingleBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Represents an active session with the SQL Warehouse.
///
/// Session lifecycle:
/// - Session created on `new_connection()`
/// - Session kept alive automatically (statements refresh the idle timeout)
/// - Session terminated on Connection drop
///
/// # Thread Safety
///
/// `DatabricksConnection` is `Send` but not `Sync` - it should be owned
/// by a single thread at a time. The underlying `SeaClient` and `SessionManager`
/// are thread-safe and can be shared.
#[derive(Debug)]
pub struct DatabricksConnection {
    /// SEA client for API calls.
    client: Arc<SeaClient>,
    /// Session manager for session lifecycle.
    session_manager: Arc<SessionManager>,
    /// Tokio runtime for async operations.
    runtime: Arc<Runtime>,
    /// Database configuration.
    config: Arc<DatabaseConfig>,
    /// Current catalog.
    current_catalog: Option<String>,
    /// Current schema.
    current_schema: Option<String>,
}

impl DatabricksConnection {
    /// Create a new connection with session management.
    ///
    /// This constructor creates a SEA client and session manager, and immediately
    /// creates a session with the SQL Warehouse.
    ///
    /// # Arguments
    ///
    /// * `config` - Database configuration with host, token, warehouse_id, etc.
    /// * `runtime` - Tokio runtime for async operations
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The SEA client cannot be created
    /// - Session creation fails (network error, authentication error, etc.)
    pub(crate) fn new(
        config: Arc<DatabaseConfig>,
        runtime: Arc<Runtime>,
    ) -> adbc_core::error::Result<Self> {
        // Create SEA client configuration
        let client_config = SeaClientConfig::new(
            &config.host,
            &config.token,
            &config.warehouse_id,
        )
        .with_connect_timeout(config.http_config.connect_timeout)
        .with_read_timeout(config.http_config.read_timeout);

        // Create the SEA client
        let client = Arc::new(
            SeaClient::new(client_config).map_err(|e| {
                Error::with_message_and_status(
                    format!("Failed to create SEA client: {}", e),
                    Status::IO,
                )
            })?,
        );

        // Create session manager
        let session_manager = Arc::new(SessionManager::new(
            client.clone(),
            config.default_catalog.clone(),
            config.default_schema.clone(),
        ));

        // Create session immediately (blocks until complete)
        runtime.block_on(session_manager.get_session_id()).map_err(|e| {
            Error::with_message_and_status(
                format!("Failed to create session: {}", e),
                Status::IO,
            )
        })?;

        let current_catalog = config.default_catalog.clone();
        let current_schema = config.default_schema.clone();

        Ok(Self {
            client,
            session_manager,
            runtime,
            config,
            current_catalog,
            current_schema,
        })
    }

    /// Get the session ID for this connection.
    ///
    /// # Returns
    ///
    /// The session ID string.
    ///
    /// # Errors
    ///
    /// Returns an error if the session is not active or cannot be retrieved.
    pub fn session_id(&self) -> adbc_core::error::Result<String> {
        self.runtime
            .block_on(self.session_manager.get_session_id())
            .map_err(|e| {
                Error::with_message_and_status(
                    format!("Failed to get session ID: {}", e),
                    Status::InvalidState,
                )
            })
    }

    /// Get the SEA client.
    ///
    /// This is used internally by statements to execute queries.
    pub(crate) fn client(&self) -> Arc<SeaClient> {
        self.client.clone()
    }

    /// Get the session manager.
    ///
    /// This is used internally by statements to get the session ID.
    pub(crate) fn session_manager(&self) -> Arc<SessionManager> {
        self.session_manager.clone()
    }

    /// Get the runtime.
    ///
    /// This is used internally by statements for async operations.
    pub(crate) fn runtime(&self) -> Arc<Runtime> {
        self.runtime.clone()
    }

    /// Get the database configuration.
    pub(crate) fn config(&self) -> &DatabaseConfig {
        &self.config
    }
}

impl Drop for DatabricksConnection {
    fn drop(&mut self) {
        // Attempt to terminate the session
        // Note: We can't propagate errors from Drop, so we log failures
        let result = self.runtime.block_on(self.session_manager.terminate());
        if let Err(e) = result {
            // In production, you might want to use a proper logging framework
            eprintln!("Warning: Failed to terminate session on connection drop: {}", e);
        }
    }
}

impl Optionable for DatabricksConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> adbc_core::error::Result<()> {
        match key.as_ref() {
            adbc_core::constants::ADBC_CONNECTION_OPTION_AUTOCOMMIT => {
                // Databricks doesn't support transactions, autocommit is always on
                if let OptionValue::String(v) = value {
                    if v == "true" {
                        Ok(())
                    } else {
                        Err(Error::with_message_and_status(
                            "Databricks does not support transactions; autocommit must be true",
                            Status::NotImplemented,
                        ))
                    }
                } else {
                    Err(Error::with_message_and_status(
                        "AutoCommit value must be of type String",
                        Status::InvalidArguments,
                    ))
                }
            }
            adbc_core::constants::ADBC_CONNECTION_OPTION_CURRENT_CATALOG => {
                if let OptionValue::String(v) = value {
                    self.current_catalog = Some(v);
                    Ok(())
                } else {
                    Err(Error::with_message_and_status(
                        "CurrentCatalog value must be of type String",
                        Status::InvalidArguments,
                    ))
                }
            }
            adbc_core::constants::ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA => {
                if let OptionValue::String(v) = value {
                    self.current_schema = Some(v);
                    Ok(())
                } else {
                    Err(Error::with_message_and_status(
                        "CurrentSchema value must be of type String",
                        Status::InvalidArguments,
                    ))
                }
            }
            _ => Err(Error::with_message_and_status(
                format!("Unknown connection option: {:?}", key),
                Status::NotImplemented,
            )),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> adbc_core::error::Result<String> {
        match key.as_ref() {
            adbc_core::constants::ADBC_CONNECTION_OPTION_AUTOCOMMIT => {
                // Databricks is always in autocommit mode
                Ok("true".to_string())
            }
            adbc_core::constants::ADBC_CONNECTION_OPTION_CURRENT_CATALOG => {
                self.current_catalog.clone().ok_or_else(|| {
                    Error::with_message_and_status("CurrentCatalog has not been set", Status::NotFound)
                })
            }
            adbc_core::constants::ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA => {
                self.current_schema.clone().ok_or_else(|| {
                    Error::with_message_and_status("CurrentSchema has not been set", Status::NotFound)
                })
            }
            _ => Err(Error::with_message_and_status(
                format!("Unknown connection option: {:?}", key),
                Status::NotFound,
            )),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> adbc_core::error::Result<Vec<u8>> {
        Err(Error::with_message_and_status(
            format!("Option {:?} is not a byte array", key),
            Status::NotFound,
        ))
    }

    fn get_option_int(&self, key: Self::Option) -> adbc_core::error::Result<i64> {
        Err(Error::with_message_and_status(
            format!("Option {:?} is not an integer", key),
            Status::NotFound,
        ))
    }

    fn get_option_double(&self, key: Self::Option) -> adbc_core::error::Result<f64> {
        Err(Error::with_message_and_status(
            format!("Option {:?} is not a double", key),
            Status::NotFound,
        ))
    }
}

impl Connection for DatabricksConnection {
    type StatementType = DatabricksStatement;

    fn new_statement(&mut self) -> adbc_core::error::Result<Self::StatementType> {
        Ok(DatabricksStatement::new(
            self.client.clone(),
            self.session_manager.clone(),
            self.runtime.clone(),
            self.config.clone(),
            self.current_catalog.clone(),
            self.current_schema.clone(),
        ))
    }

    fn cancel(&mut self) -> adbc_core::error::Result<()> {
        // Connection-level cancel is a no-op for Databricks
        // Individual statement cancellation should be done via Statement::cancel()
        Ok(())
    }

    fn get_info(
        &self,
        _codes: Option<HashSet<InfoCode>>,
    ) -> adbc_core::error::Result<impl arrow_array::RecordBatchReader + Send> {
        // TODO: Implement get_info in Sprint 4
        Err::<SingleBatchReader, _>(Error::with_message_and_status(
            "get_info not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn get_objects(
        &self,
        _depth: ObjectDepth,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _table_type: Option<Vec<&str>>,
        _column_name: Option<&str>,
    ) -> adbc_core::error::Result<impl arrow_array::RecordBatchReader + Send> {
        // TODO: Implement get_objects in Sprint 4
        Err::<SingleBatchReader, _>(Error::with_message_and_status(
            "get_objects not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn get_table_schema(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: &str,
    ) -> adbc_core::error::Result<Schema> {
        // TODO: Implement get_table_schema in Sprint 4
        Err(Error::with_message_and_status(
            "get_table_schema not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn get_table_types(
        &self,
    ) -> adbc_core::error::Result<impl arrow_array::RecordBatchReader + Send> {
        // TODO: Implement get_table_types in Sprint 4
        Err::<SingleBatchReader, _>(Error::with_message_and_status(
            "get_table_types not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn get_statistic_names(
        &self,
    ) -> adbc_core::error::Result<impl arrow_array::RecordBatchReader + Send> {
        // Statistics not supported by Databricks SEA
        Err::<SingleBatchReader, _>(Error::with_message_and_status(
            "Statistics not supported",
            Status::NotImplemented,
        ))
    }

    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> adbc_core::error::Result<impl arrow_array::RecordBatchReader + Send> {
        // Statistics not supported by Databricks SEA
        Err::<SingleBatchReader, _>(Error::with_message_and_status(
            "Statistics not supported",
            Status::NotImplemented,
        ))
    }

    fn commit(&mut self) -> adbc_core::error::Result<()> {
        // Databricks doesn't support transactions
        Err(Error::with_message_and_status(
            "Databricks does not support transactions",
            Status::NotImplemented,
        ))
    }

    fn rollback(&mut self) -> adbc_core::error::Result<()> {
        // Databricks doesn't support transactions
        Err(Error::with_message_and_status(
            "Databricks does not support transactions",
            Status::NotImplemented,
        ))
    }

    fn read_partition(
        &self,
        _partition: impl AsRef<[u8]>,
    ) -> adbc_core::error::Result<impl arrow_array::RecordBatchReader + Send> {
        // Partitioned reads not supported
        Err::<SingleBatchReader, _>(Error::with_message_and_status(
            "Partitioned reads not supported",
            Status::NotImplemented,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::SessionResponse;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Helper to create a test DatabaseConfig pointing to a mock server.
    fn create_test_config(server_uri: &str) -> Arc<DatabaseConfig> {
        Arc::new(DatabaseConfig {
            host: server_uri.to_string(),
            warehouse_id: "test-warehouse".to_string(),
            token: "test-token".to_string(),
            default_catalog: Some("main".to_string()),
            default_schema: Some("default".to_string()),
            ..Default::default()
        })
    }

    /// Helper to create a test runtime that uses the current tokio runtime handle.
    /// This is needed because tests run inside a tokio runtime context, and we
    /// cannot create a nested runtime.
    fn create_test_runtime() -> Arc<Runtime> {
        Arc::new(Runtime::new(Some(tokio::runtime::Handle::current())).unwrap())
    }

    // ==================== Optionable Trait Tests ====================

    #[tokio::test(flavor = "multi_thread")]
    async fn test_connection_autocommit_always_true() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(SessionResponse {
                session_id: "test-session".to_string(),
            }))
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/test-session"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri());
        let runtime = create_test_runtime();
        let conn = DatabricksConnection::new(config, runtime).unwrap();

        // AutoCommit should always return "true"
        let autocommit = conn.get_option_string(OptionConnection::AutoCommit).unwrap();
        assert_eq!(autocommit, "true");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_connection_autocommit_cannot_be_disabled() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(SessionResponse {
                session_id: "test-session".to_string(),
            }))
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/test-session"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri());
        let runtime = create_test_runtime();
        let mut conn = DatabricksConnection::new(config, runtime).unwrap();

        // Setting autocommit to "true" should succeed
        let result = conn.set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("true".to_string()),
        );
        assert!(result.is_ok());

        // Setting autocommit to "false" should fail
        let result = conn.set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("false".to_string()),
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::NotImplemented);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_connection_current_catalog() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(SessionResponse {
                session_id: "test-session".to_string(),
            }))
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/test-session"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri());
        let runtime = create_test_runtime();
        let mut conn = DatabricksConnection::new(config, runtime).unwrap();

        // Default catalog should be set from config
        let catalog = conn.get_option_string(OptionConnection::CurrentCatalog).unwrap();
        assert_eq!(catalog, "main");

        // Set a new catalog
        conn.set_option(
            OptionConnection::CurrentCatalog,
            OptionValue::String("new_catalog".to_string()),
        )
        .unwrap();

        let catalog = conn.get_option_string(OptionConnection::CurrentCatalog).unwrap();
        assert_eq!(catalog, "new_catalog");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_connection_current_schema() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(SessionResponse {
                session_id: "test-session".to_string(),
            }))
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/test-session"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri());
        let runtime = create_test_runtime();
        let mut conn = DatabricksConnection::new(config, runtime).unwrap();

        // Default schema should be set from config
        let schema = conn.get_option_string(OptionConnection::CurrentSchema).unwrap();
        assert_eq!(schema, "default");

        // Set a new schema
        conn.set_option(
            OptionConnection::CurrentSchema,
            OptionValue::String("new_schema".to_string()),
        )
        .unwrap();

        let schema = conn.get_option_string(OptionConnection::CurrentSchema).unwrap();
        assert_eq!(schema, "new_schema");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_connection_catalog_not_set() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(SessionResponse {
                session_id: "test-session".to_string(),
            }))
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/test-session"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        // Create config without default catalog
        let config = Arc::new(DatabaseConfig {
            host: mock_server.uri(),
            warehouse_id: "test-warehouse".to_string(),
            token: "test-token".to_string(),
            default_catalog: None,
            default_schema: None,
            ..Default::default()
        });
        let runtime = create_test_runtime();
        let conn = DatabricksConnection::new(config, runtime).unwrap();

        // Getting catalog when not set should return NotFound
        let result = conn.get_option_string(OptionConnection::CurrentCatalog);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::NotFound);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_connection_unknown_option() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(SessionResponse {
                session_id: "test-session".to_string(),
            }))
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/test-session"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri());
        let runtime = create_test_runtime();
        let mut conn = DatabricksConnection::new(config, runtime).unwrap();

        // Setting unknown option should fail
        let result = conn.set_option(
            OptionConnection::Other("unknown.option".to_string()),
            OptionValue::String("value".to_string()),
        );
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::NotImplemented);

        // Getting unknown option should fail
        let result = conn.get_option_string(OptionConnection::Other("unknown.option".to_string()));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::NotFound);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_connection_option_type_validation() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(SessionResponse {
                session_id: "test-session".to_string(),
            }))
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/test-session"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri());
        let runtime = create_test_runtime();
        let mut conn = DatabricksConnection::new(config, runtime).unwrap();

        // Setting catalog with non-string value should fail
        let result = conn.set_option(OptionConnection::CurrentCatalog, OptionValue::Int(123));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::InvalidArguments);

        // Setting autocommit with non-string value should fail
        let result = conn.set_option(OptionConnection::AutoCommit, OptionValue::Int(1));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::InvalidArguments);
    }

    // ==================== Connection Lifecycle Tests ====================

    #[tokio::test(flavor = "multi_thread")]
    async fn test_connection_creates_session() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(SessionResponse {
                session_id: "lifecycle-session-123".to_string(),
            }))
            .expect(1)
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/lifecycle-session-123"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri());
        let runtime = create_test_runtime();
        let conn = DatabricksConnection::new(config, runtime).unwrap();

        // Session should be created
        let session_id = conn.session_id().unwrap();
        assert_eq!(session_id, "lifecycle-session-123");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_connection_terminates_session_on_drop() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(SessionResponse {
                session_id: "drop-session-456".to_string(),
            }))
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/drop-session-456"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1) // Should be called exactly once when connection is dropped
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri());
        let runtime = create_test_runtime();

        {
            let _conn = DatabricksConnection::new(config, runtime).unwrap();
            // Connection dropped here
        }

        // The mock expectation will verify the DELETE was called
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_connection_session_id_consistent() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(SessionResponse {
                session_id: "consistent-session".to_string(),
            }))
            .expect(1) // Should only be called once
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/consistent-session"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri());
        let runtime = create_test_runtime();
        let conn = DatabricksConnection::new(config, runtime).unwrap();

        // Multiple calls should return the same session ID
        let session_id_1 = conn.session_id().unwrap();
        let session_id_2 = conn.session_id().unwrap();
        let session_id_3 = conn.session_id().unwrap();

        assert_eq!(session_id_1, "consistent-session");
        assert_eq!(session_id_2, "consistent-session");
        assert_eq!(session_id_3, "consistent-session");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_connection_creation_fails_on_session_error() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
                "error_code": "UNAUTHENTICATED",
                "message": "Invalid or expired token"
            })))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri());
        let runtime = create_test_runtime();
        let result = DatabricksConnection::new(config, runtime);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::IO);
        assert!(err.message.contains("Failed to create session"));
    }

    // ==================== Connection Trait Tests ====================

    #[tokio::test(flavor = "multi_thread")]
    async fn test_connection_new_statement() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(SessionResponse {
                session_id: "stmt-session".to_string(),
            }))
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/stmt-session"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri());
        let runtime = create_test_runtime();
        let mut conn = DatabricksConnection::new(config, runtime).unwrap();

        let result = conn.new_statement();
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_connection_cancel_is_noop() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(SessionResponse {
                session_id: "cancel-session".to_string(),
            }))
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/cancel-session"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri());
        let runtime = create_test_runtime();
        let mut conn = DatabricksConnection::new(config, runtime).unwrap();

        // Cancel should succeed (no-op for connection level)
        let result = conn.cancel();
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_connection_commit_not_supported() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(SessionResponse {
                session_id: "commit-session".to_string(),
            }))
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/commit-session"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri());
        let runtime = create_test_runtime();
        let mut conn = DatabricksConnection::new(config, runtime).unwrap();

        let result = conn.commit();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::NotImplemented);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_connection_rollback_not_supported() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(SessionResponse {
                session_id: "rollback-session".to_string(),
            }))
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/rollback-session"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri());
        let runtime = create_test_runtime();
        let mut conn = DatabricksConnection::new(config, runtime).unwrap();

        let result = conn.rollback();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::NotImplemented);
    }

    // ==================== SingleBatchReader Tests ====================

    #[test]
    fn test_single_batch_reader_new() {
        use arrow_array::{Int32Array, StringArray};
        use arrow_schema::{DataType, Field};

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
            ],
        )
        .unwrap();

        let mut reader = SingleBatchReader::new(batch);

        // First call should return the batch
        let first = reader.next();
        assert!(first.is_some());
        let first_batch = first.unwrap().unwrap();
        assert_eq!(first_batch.num_rows(), 3);

        // Second call should return None
        let second = reader.next();
        assert!(second.is_none());
    }

    #[test]
    fn test_single_batch_reader_empty() {
        use arrow_array::RecordBatchReader as _;
        use arrow_schema::{DataType, Field};

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let mut reader = SingleBatchReader::empty(schema.clone());

        // Should return None immediately
        let first = reader.next();
        assert!(first.is_none());

        // Schema should still be available
        assert_eq!(reader.schema().fields().len(), 1);
    }

    #[test]
    fn test_single_batch_reader_schema() {
        use arrow_array::Int32Array;
        use arrow_schema::{DataType, Field};

        let schema = Arc::new(Schema::new(vec![
            Field::new("col1", DataType::Int32, false),
            Field::new("col2", DataType::Int32, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![Some(10), None])),
            ],
        )
        .unwrap();

        let reader = SingleBatchReader::new(batch);

        // Verify schema is correctly returned
        let reader_schema = arrow_array::RecordBatchReader::schema(&reader);
        assert_eq!(reader_schema.fields().len(), 2);
        assert_eq!(reader_schema.field(0).name(), "col1");
        assert_eq!(reader_schema.field(1).name(), "col2");
    }
}
