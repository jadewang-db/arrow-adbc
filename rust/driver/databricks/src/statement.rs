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

//! Databricks statement implementation.
//!
//! This module provides the statement type that executes SQL statements
//! and returns results as Arrow RecordBatches.
//!
//! # Example
//!
//! ```ignore
//! // Get a statement from a connection
//! let mut stmt = conn.new_statement()?;
//!
//! // Set the SQL query
//! stmt.set_sql_query("SELECT * FROM my_table LIMIT 10")?;
//!
//! // Execute and get results
//! let reader = stmt.execute()?;
//! for batch in reader {
//!     let batch = batch?;
//!     println!("Got {} rows", batch.num_rows());
//! }
//! ```

use std::sync::Arc;

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{OptionStatement, OptionValue};
use adbc_core::{Optionable, PartitionedResult, Statement};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::Schema;
use tokio::runtime::Runtime;

use crate::client::SeaClient;
use crate::session::SessionManager;

/// Statement option keys.
pub mod option_keys {
    /// Wait timeout for statement execution.
    pub const WAIT_TIMEOUT: &str = "databricks.statement.wait_timeout";
    /// Maximum rows to return.
    pub const ROW_LIMIT: &str = "databricks.statement.row_limit";
    /// Maximum bytes to return.
    pub const BYTE_LIMIT: &str = "databricks.statement.byte_limit";
}

/// Databricks statement.
///
/// Executes SQL statements and returns results as Arrow RecordBatches.
///
/// # Session Management
///
/// Each statement holds a reference to the connection's `SessionManager`. When
/// a statement is executed, it gets the session ID from the manager, which
/// refreshes the session's idle timeout.
#[derive(Debug)]
pub struct DatabricksStatement {
    /// SEA client for API calls.
    client: Arc<SeaClient>,
    /// Session manager (shared with connection).
    session_manager: Arc<SessionManager>,
    /// Tokio runtime (shared with connection).
    runtime: Arc<Runtime>,
    /// SQL query to execute.
    sql_query: Option<String>,
    /// Statement ID from the last execution.
    #[allow(dead_code)]
    statement_id: Option<String>,
    /// Wait timeout for statement execution.
    wait_timeout: Option<String>,
    /// Maximum rows to return.
    #[allow(dead_code)]
    row_limit: Option<i64>,
    /// Maximum bytes to return.
    #[allow(dead_code)]
    byte_limit: Option<i64>,
}

impl DatabricksStatement {
    /// Create a new statement.
    ///
    /// # Arguments
    ///
    /// * `client` - SEA client for API calls
    /// * `session_manager` - Session manager for session lifecycle
    /// * `runtime` - Shared Tokio runtime for async operations
    pub(crate) fn new(
        client: Arc<SeaClient>,
        session_manager: Arc<SessionManager>,
        runtime: Arc<Runtime>,
    ) -> Result<Self> {
        Ok(Self {
            client,
            session_manager,
            runtime,
            sql_query: None,
            statement_id: None,
            wait_timeout: None,
            row_limit: None,
            byte_limit: None,
        })
    }

    /// Get the SQL query.
    pub fn sql_query(&self) -> Option<&str> {
        self.sql_query.as_deref()
    }

    /// Get the statement ID.
    pub fn statement_id(&self) -> Option<&str> {
        self.statement_id.as_deref()
    }

    /// Get the SEA client.
    pub fn client(&self) -> &Arc<SeaClient> {
        &self.client
    }

    /// Get the session manager.
    pub fn session_manager(&self) -> &Arc<SessionManager> {
        &self.session_manager
    }

    /// Get the runtime.
    pub fn runtime(&self) -> &Arc<Runtime> {
        &self.runtime
    }
}

/// Empty record batch reader for placeholder implementations.
struct EmptyRecordBatchReader {
    schema: Arc<Schema>,
}

impl EmptyRecordBatchReader {
    fn new(schema: Schema) -> Self {
        Self {
            schema: Arc::new(schema),
        }
    }
}

impl Iterator for EmptyRecordBatchReader {
    type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

impl RecordBatchReader for EmptyRecordBatchReader {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

impl Optionable for DatabricksStatement {
    type Option = OptionStatement;

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
            OptionStatement::Other(key) => match key.as_str() {
                option_keys::WAIT_TIMEOUT => {
                    self.wait_timeout = Some(extract_string(value, "wait_timeout")?);
                }
                option_keys::ROW_LIMIT => {
                    let string_value = extract_string(value, "row_limit")?;
                    self.row_limit = Some(string_value.parse().map_err(|_| {
                        Error::with_message_and_status(
                            "row_limit must be a positive integer",
                            Status::InvalidArguments,
                        )
                    })?);
                }
                option_keys::BYTE_LIMIT => {
                    let string_value = extract_string(value, "byte_limit")?;
                    self.byte_limit = Some(string_value.parse().map_err(|_| {
                        Error::with_message_and_status(
                            "byte_limit must be a positive integer",
                            Status::InvalidArguments,
                        )
                    })?);
                }
                _ => {
                    return Err(Error::with_message_and_status(
                        format!("Unknown option: {}", key),
                        Status::InvalidArguments,
                    ));
                }
            },
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
            OptionStatement::Other(key) => match key.as_str() {
                option_keys::WAIT_TIMEOUT => self.wait_timeout.clone().ok_or_else(|| {
                    Error::with_message_and_status("wait_timeout not set", Status::InvalidArguments)
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

impl Statement for DatabricksStatement {
    fn bind(&mut self, _batch: RecordBatch) -> Result<()> {
        // TODO: Implement parameter binding
        Err(Error::with_message_and_status(
            "Parameter binding not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn bind_stream(&mut self, _stream: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        // TODO: Implement stream binding
        Err(Error::with_message_and_status(
            "Stream binding not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn cancel(&mut self) -> Result<()> {
        // TODO: Implement statement cancellation
        if let Some(_statement_id) = &self.statement_id {
            // Cancel the statement via SEA API
        }
        Ok(())
    }

    fn execute(&mut self) -> Result<impl RecordBatchReader + Send> {
        let _sql = self.sql_query.as_ref().ok_or_else(|| {
            Error::with_message_and_status("SQL query not set", Status::InvalidState)
        })?;

        // TODO: Implement statement execution via SEA API
        // 1. Get session ID from session manager
        // 2. Call execute_statement API
        // 3. Poll until complete if needed
        // 4. Fetch results (inline or external links)
        // 5. Return as RecordBatchReader

        Ok(EmptyRecordBatchReader::new(Schema::empty()))
    }

    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        Err(Error::with_message_and_status(
            "Partitioned execution not implemented",
            Status::NotImplemented,
        ))
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        // TODO: Implement schema-only execution
        Err(Error::with_message_and_status(
            "Schema-only execution not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        let _sql = self.sql_query.as_ref().ok_or_else(|| {
            Error::with_message_and_status("SQL query not set", Status::InvalidState)
        })?;

        // TODO: Implement update execution via SEA API
        // Return affected row count if available

        Ok(None)
    }

    fn get_parameter_schema(&self) -> Result<Schema> {
        Err(Error::with_message_and_status(
            "Parameter schema not implemented",
            Status::NotImplemented,
        ))
    }

    fn prepare(&mut self) -> Result<()> {
        // TODO: Implement statement preparation
        Err(Error::with_message_and_status(
            "Statement preparation not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()> {
        self.sql_query = Some(query.as_ref().to_string());
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> Result<()> {
        Err(Error::with_message_and_status(
            "Substrait plans not supported",
            Status::NotImplemented,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::SeaClientConfig;

    /// Create a multi-threaded runtime that supports block_on operations.
    fn create_mt_runtime() -> Arc<Runtime> {
        Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("Failed to create multi-threaded runtime"),
        )
    }

    /// Create a test SEA client and session manager (no session created yet).
    fn create_test_client_and_session(mock_uri: &str) -> (Arc<SeaClient>, Arc<SessionManager>) {
        let config = SeaClientConfig::new(mock_uri, "test_token", "test_warehouse");
        let client = Arc::new(SeaClient::new(config).expect("Failed to create test client"));
        let session_manager = Arc::new(SessionManager::new(client.clone(), None, None));
        (client, session_manager)
    }

    // Statement tests don't need mock servers because DatabricksStatement::new()
    // doesn't create any sessions - it just stores references to client and session_manager.
    // The session is only accessed during execute().

    #[test]
    fn test_statement_creation() {
        let runtime = create_mt_runtime();
        let mock_uri = "http://localhost:1234"; // Mock URI (not used since we don't create sessions)
        let (client, session_manager) = create_test_client_and_session(mock_uri);

        let stmt = DatabricksStatement::new(client, session_manager, runtime);
        assert!(stmt.is_ok(), "Statement creation should succeed");

        let stmt = stmt.unwrap();
        assert!(stmt.sql_query().is_none(), "SQL query should be None initially");
        assert!(stmt.statement_id().is_none(), "Statement ID should be None initially");
    }

    #[test]
    fn test_statement_set_sql_query() {
        let runtime = create_mt_runtime();
        let mock_uri = "http://localhost:1234";
        let (client, session_manager) = create_test_client_and_session(mock_uri);

        let mut stmt = DatabricksStatement::new(client, session_manager, runtime).unwrap();

        stmt.set_sql_query("SELECT 1").unwrap();
        assert_eq!(stmt.sql_query(), Some("SELECT 1"));

        stmt.set_sql_query("SELECT * FROM test_table").unwrap();
        assert_eq!(stmt.sql_query(), Some("SELECT * FROM test_table"));
    }

    #[test]
    fn test_statement_set_wait_timeout() {
        let runtime = create_mt_runtime();
        let mock_uri = "http://localhost:1234";
        let (client, session_manager) = create_test_client_and_session(mock_uri);

        let mut stmt = DatabricksStatement::new(client, session_manager, runtime).unwrap();

        stmt.set_option(
            OptionStatement::Other(option_keys::WAIT_TIMEOUT.into()),
            OptionValue::String("60s".into()),
        )
        .unwrap();

        let timeout = stmt
            .get_option_string(OptionStatement::Other(option_keys::WAIT_TIMEOUT.into()))
            .unwrap();
        assert_eq!(timeout, "60s");
    }

    #[test]
    fn test_statement_set_row_limit() {
        let runtime = create_mt_runtime();
        let mock_uri = "http://localhost:1234";
        let (client, session_manager) = create_test_client_and_session(mock_uri);

        let mut stmt = DatabricksStatement::new(client, session_manager, runtime).unwrap();

        stmt.set_option(
            OptionStatement::Other(option_keys::ROW_LIMIT.into()),
            OptionValue::String("1000".into()),
        )
        .unwrap();

        assert_eq!(stmt.row_limit, Some(1000));
    }

    #[test]
    fn test_statement_invalid_row_limit() {
        let runtime = create_mt_runtime();
        let mock_uri = "http://localhost:1234";
        let (client, session_manager) = create_test_client_and_session(mock_uri);

        let mut stmt = DatabricksStatement::new(client, session_manager, runtime).unwrap();

        let result = stmt.set_option(
            OptionStatement::Other(option_keys::ROW_LIMIT.into()),
            OptionValue::String("not_a_number".into()),
        );

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);
        assert!(err.message.contains("must be a positive integer"));
    }

    #[test]
    fn test_statement_execute_requires_sql() {
        let runtime = create_mt_runtime();
        let mock_uri = "http://localhost:1234";
        let (client, session_manager) = create_test_client_and_session(mock_uri);

        let mut stmt = DatabricksStatement::new(client, session_manager, runtime).unwrap();

        let result = stmt.execute();
        assert!(result.is_err());
        let err = result.err().expect("Expected an error");
        assert_eq!(err.status, Status::InvalidState);
        assert!(err.message.contains("SQL query not set"));
    }

    #[test]
    fn test_statement_bind_not_implemented() {
        let runtime = create_mt_runtime();
        let mock_uri = "http://localhost:1234";
        let (client, session_manager) = create_test_client_and_session(mock_uri);

        let mut stmt = DatabricksStatement::new(client, session_manager, runtime).unwrap();

        // Create an empty record batch for testing
        let schema = Arc::new(Schema::empty());
        let batch = RecordBatch::new_empty(schema);

        let result = stmt.bind(batch);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::NotImplemented);
    }

    #[test]
    fn test_statement_prepare_not_implemented() {
        let runtime = create_mt_runtime();
        let mock_uri = "http://localhost:1234";
        let (client, session_manager) = create_test_client_and_session(mock_uri);

        let mut stmt = DatabricksStatement::new(client, session_manager, runtime).unwrap();

        let result = stmt.prepare();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::NotImplemented);
    }

    #[test]
    fn test_statement_substrait_not_supported() {
        let runtime = create_mt_runtime();
        let mock_uri = "http://localhost:1234";
        let (client, session_manager) = create_test_client_and_session(mock_uri);

        let mut stmt = DatabricksStatement::new(client, session_manager, runtime).unwrap();

        let result = stmt.set_substrait_plan(&[1, 2, 3]);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::NotImplemented);
        assert!(err.message.contains("Substrait"));
    }

    #[test]
    fn test_statement_cancel_succeeds() {
        let runtime = create_mt_runtime();
        let mock_uri = "http://localhost:1234";
        let (client, session_manager) = create_test_client_and_session(mock_uri);

        let mut stmt = DatabricksStatement::new(client, session_manager, runtime).unwrap();

        let result = stmt.cancel();
        assert!(result.is_ok(), "cancel() should succeed");
    }
}
