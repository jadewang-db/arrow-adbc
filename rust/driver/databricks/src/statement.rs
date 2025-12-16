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
use std::time::Duration;

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{OptionStatement, OptionValue};
use adbc_core::{Optionable, PartitionedResult, Statement};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::Schema;
use tokio::runtime::Runtime;

use crate::client::{SeaClient, StatementResponse};
use crate::error::Error as DatabricksError;
use crate::fetch::ArrowResultReader;
use crate::session::SessionManager;

/// Statement option keys.
pub mod option_keys {
    /// Wait timeout for statement execution (in seconds).
    pub const WAIT_TIMEOUT: &str = "databricks.statement.wait_timeout";
    /// Maximum rows to return.
    pub const ROW_LIMIT: &str = "databricks.statement.row_limit";
    /// Maximum bytes to return.
    pub const BYTE_LIMIT: &str = "databricks.statement.byte_limit";
    /// Maximum time to wait for statement completion (in seconds).
    pub const MAX_WAIT: &str = "databricks.statement.max_wait";
}

/// Default maximum wait time for statement completion (5 minutes).
const DEFAULT_MAX_WAIT_SECS: u64 = 300;

/// Databricks statement.
///
/// Executes SQL statements and returns results as Arrow RecordBatches.
///
/// # Session Management
///
/// Each statement holds a reference to the connection's `SessionManager`. When
/// a statement is executed, it gets the session ID from the manager, which
/// refreshes the session's idle timeout.
///
/// # Statement Execution
///
/// Statements are executed synchronously by:
/// 1. Getting or creating a session via the `SessionManager`
/// 2. Calling `execute_and_wait` on the `SeaClient`, which handles polling
/// 3. Converting the results to Arrow RecordBatches via `ArrowResultReader`
///
/// The statement tracks its ID so it can be cancelled if needed.
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
    statement_id: Option<String>,
    /// Wait timeout for statement execution (e.g., "10s").
    wait_timeout: Option<String>,
    /// Maximum rows to return.
    row_limit: Option<i64>,
    /// Maximum bytes to return.
    byte_limit: Option<i64>,
    /// Maximum time to wait for statement completion.
    max_wait: Option<Duration>,
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
            max_wait: None,
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

    /// Convert a statement response to an ArrowResultReader.
    ///
    /// This handles both inline results (for small result sets) and external links
    /// (for larger result sets that need to be fetched from cloud storage).
    fn response_to_reader(&self, response: StatementResponse) -> Result<ArrowResultReader> {
        // Build schema from manifest
        let schema = self.build_schema_from_response(&response)?;

        // Check if we have results
        let result = match response.result {
            Some(r) => r,
            None => {
                // No results - return empty reader with schema
                return Ok(ArrowResultReader::empty(schema));
            }
        };

        // Check for external links (cloud fetch)
        if let Some(ref _external_links) = result.external_links {
            // For now, external links are handled in a future work item
            // Return empty reader - chunk fetching is Work Item 2.6
            return Ok(ArrowResultReader::empty(schema));
        }

        // Check for inline data (JSON array format)
        if result.data_array.is_some() {
            // Inline JSON results - convert to Arrow
            // For now, return empty reader - JSON parsing is Work Item 3.x
            return Ok(ArrowResultReader::empty(schema));
        }

        // No data available
        Ok(ArrowResultReader::empty(schema))
    }

    /// Build an Arrow schema from the statement response manifest.
    fn build_schema_from_response(&self, response: &StatementResponse) -> Result<Schema> {
        use arrow_schema::{DataType, Field};

        let manifest = match &response.manifest {
            Some(m) => m,
            None => return Ok(Schema::empty()),
        };

        let result_schema = match &manifest.schema {
            Some(s) => s,
            None => return Ok(Schema::empty()),
        };

        let columns = match &result_schema.columns {
            Some(c) => c,
            None => return Ok(Schema::empty()),
        };

        let fields: Vec<Field> = columns
            .iter()
            .map(|col| {
                // Map Databricks type names to Arrow types
                // This is a simplified mapping - full type support in Work Item 3.x
                let data_type = match col.type_name.as_deref() {
                    Some("INT") | Some("INTEGER") => DataType::Int32,
                    Some("BIGINT") | Some("LONG") => DataType::Int64,
                    Some("SMALLINT") | Some("SHORT") => DataType::Int16,
                    Some("TINYINT") | Some("BYTE") => DataType::Int8,
                    Some("FLOAT") | Some("REAL") => DataType::Float32,
                    Some("DOUBLE") => DataType::Float64,
                    Some("BOOLEAN") => DataType::Boolean,
                    Some("STRING") | Some("VARCHAR") => DataType::Utf8,
                    Some("BINARY") => DataType::Binary,
                    Some("DATE") => DataType::Date32,
                    Some("TIMESTAMP") | Some("TIMESTAMP_NTZ") => {
                        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
                    }
                    Some("DECIMAL") => {
                        // Default decimal precision/scale - actual values from type_text
                        DataType::Decimal128(38, 18)
                    }
                    _ => DataType::Utf8, // Default to string for unknown types
                };
                Field::new(&col.name, data_type, true)
            })
            .collect();

        Ok(Schema::new(fields))
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
                option_keys::MAX_WAIT => {
                    let string_value = extract_string(value, "max_wait")?;
                    let secs: u64 = string_value.parse().map_err(|_| {
                        Error::with_message_and_status(
                            "max_wait must be a positive integer (seconds)",
                            Status::InvalidArguments,
                        )
                    })?;
                    self.max_wait = Some(Duration::from_secs(secs));
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
        if let Some(ref statement_id) = self.statement_id {
            // Cancel the statement via SEA API
            let statement_id = statement_id.clone();
            self.runtime
                .block_on(async { self.client.cancel_statement(&statement_id).await })
                .map_err(|e| {
                    let db_err: DatabricksError = e;
                    Error::with_message_and_status(db_err.to_string(), db_err.to_adbc_status())
                })?;
        }
        // Clear the statement ID after cancellation
        self.statement_id = None;
        Ok(())
    }

    fn execute(&mut self) -> Result<impl RecordBatchReader + Send> {
        let sql = self.sql_query.as_ref().ok_or_else(|| {
            Error::with_message_and_status("SQL query not set", Status::InvalidState)
        })?;

        // Get session ID from session manager (creates session if needed)
        let session_id = self
            .runtime
            .block_on(async { self.session_manager.get_session_id().await })
            .map_err(|e| {
                let db_err: DatabricksError = e;
                Error::with_message_and_status(db_err.to_string(), db_err.to_adbc_status())
            })?;

        // Determine max wait time
        let max_wait = self
            .max_wait
            .unwrap_or(Duration::from_secs(DEFAULT_MAX_WAIT_SECS));

        // Execute statement and wait for completion
        let response = self
            .runtime
            .block_on(async {
                self.client
                    .execute_and_wait(
                        &session_id,
                        sql,
                        Some(max_wait),
                        self.row_limit,
                        self.byte_limit,
                    )
                    .await
            })
            .map_err(|e| {
                let db_err: DatabricksError = e;
                Error::with_message_and_status(db_err.to_string(), db_err.to_adbc_status())
            })?;

        // Store statement ID for potential cancellation
        self.statement_id = Some(response.statement_id.clone());

        // Convert response to record batch reader
        self.response_to_reader(response)
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
        let sql = self.sql_query.as_ref().ok_or_else(|| {
            Error::with_message_and_status("SQL query not set", Status::InvalidState)
        })?;

        // Get session ID from session manager
        let session_id = self
            .runtime
            .block_on(async { self.session_manager.get_session_id().await })
            .map_err(|e| {
                let db_err: DatabricksError = e;
                Error::with_message_and_status(db_err.to_string(), db_err.to_adbc_status())
            })?;

        // Determine max wait time
        let max_wait = self
            .max_wait
            .unwrap_or(Duration::from_secs(DEFAULT_MAX_WAIT_SECS));

        // Execute statement and wait for completion
        let response = self
            .runtime
            .block_on(async {
                self.client
                    .execute_and_wait(
                        &session_id,
                        sql,
                        Some(max_wait),
                        self.row_limit,
                        self.byte_limit,
                    )
                    .await
            })
            .map_err(|e| {
                let db_err: DatabricksError = e;
                Error::with_message_and_status(db_err.to_string(), db_err.to_adbc_status())
            })?;

        // Store statement ID
        self.statement_id = Some(response.statement_id.clone());

        // Return affected row count from manifest if available
        // For DDL/DML, the manifest may contain the row count
        let row_count = response
            .manifest
            .and_then(|m| m.total_row_count);

        Ok(row_count)
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

    #[test]
    fn test_statement_set_max_wait() {
        let runtime = create_mt_runtime();
        let mock_uri = "http://localhost:1234";
        let (client, session_manager) = create_test_client_and_session(mock_uri);

        let mut stmt = DatabricksStatement::new(client, session_manager, runtime).unwrap();

        // Set max wait to 120 seconds
        stmt.set_option(
            OptionStatement::Other(option_keys::MAX_WAIT.into()),
            OptionValue::String("120".into()),
        )
        .unwrap();

        assert_eq!(stmt.max_wait, Some(Duration::from_secs(120)));
    }

    #[test]
    fn test_statement_invalid_max_wait() {
        let runtime = create_mt_runtime();
        let mock_uri = "http://localhost:1234";
        let (client, session_manager) = create_test_client_and_session(mock_uri);

        let mut stmt = DatabricksStatement::new(client, session_manager, runtime).unwrap();

        let result = stmt.set_option(
            OptionStatement::Other(option_keys::MAX_WAIT.into()),
            OptionValue::String("not_a_number".into()),
        );

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);
        assert!(err.message.contains("max_wait"));
    }

    #[test]
    fn test_statement_set_byte_limit() {
        let runtime = create_mt_runtime();
        let mock_uri = "http://localhost:1234";
        let (client, session_manager) = create_test_client_and_session(mock_uri);

        let mut stmt = DatabricksStatement::new(client, session_manager, runtime).unwrap();

        stmt.set_option(
            OptionStatement::Other(option_keys::BYTE_LIMIT.into()),
            OptionValue::String("10000000".into()),
        )
        .unwrap();

        assert_eq!(stmt.byte_limit, Some(10_000_000));
    }

    #[test]
    fn test_statement_unknown_option_fails() {
        let runtime = create_mt_runtime();
        let mock_uri = "http://localhost:1234";
        let (client, session_manager) = create_test_client_and_session(mock_uri);

        let mut stmt = DatabricksStatement::new(client, session_manager, runtime).unwrap();

        let result = stmt.set_option(
            OptionStatement::Other("databricks.statement.unknown".into()),
            OptionValue::String("value".into()),
        );

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);
        assert!(err.message.contains("Unknown option"));
    }

    #[test]
    fn test_statement_execute_update_requires_sql() {
        let runtime = create_mt_runtime();
        let mock_uri = "http://localhost:1234";
        let (client, session_manager) = create_test_client_and_session(mock_uri);

        let mut stmt = DatabricksStatement::new(client, session_manager, runtime).unwrap();

        let result = stmt.execute_update();
        assert!(result.is_err());
        let err = result.err().expect("Expected an error");
        assert_eq!(err.status, Status::InvalidState);
        assert!(err.message.contains("SQL query not set"));
    }

    #[test]
    fn test_statement_execute_partitions_not_implemented() {
        let runtime = create_mt_runtime();
        let mock_uri = "http://localhost:1234";
        let (client, session_manager) = create_test_client_and_session(mock_uri);

        let mut stmt = DatabricksStatement::new(client, session_manager, runtime).unwrap();

        let result = stmt.execute_partitions();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::NotImplemented);
    }

    #[test]
    fn test_statement_execute_schema_not_implemented() {
        let runtime = create_mt_runtime();
        let mock_uri = "http://localhost:1234";
        let (client, session_manager) = create_test_client_and_session(mock_uri);

        let mut stmt = DatabricksStatement::new(client, session_manager, runtime).unwrap();

        let result = stmt.execute_schema();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::NotImplemented);
    }

    #[test]
    fn test_statement_get_parameter_schema_not_implemented() {
        let runtime = create_mt_runtime();
        let mock_uri = "http://localhost:1234";
        let (client, session_manager) = create_test_client_and_session(mock_uri);

        let stmt = DatabricksStatement::new(client, session_manager, runtime).unwrap();

        let result = stmt.get_parameter_schema();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::NotImplemented);
    }

    #[test]
    fn test_statement_bind_stream_not_implemented() {
        let runtime = create_mt_runtime();
        let mock_uri = "http://localhost:1234";
        let (client, session_manager) = create_test_client_and_session(mock_uri);

        let mut stmt = DatabricksStatement::new(client, session_manager, runtime).unwrap();

        // Create an empty reader for testing
        let schema = Arc::new(Schema::empty());
        let reader = Box::new(ArrowResultReader::empty((*schema).clone()));

        let result = stmt.bind_stream(reader);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::NotImplemented);
    }

    #[test]
    fn test_build_schema_from_response_with_columns() {
        let runtime = create_mt_runtime();
        let mock_uri = "http://localhost:1234";
        let (client, session_manager) = create_test_client_and_session(mock_uri);

        let stmt = DatabricksStatement::new(client, session_manager, runtime).unwrap();

        // Create a response with schema columns
        let response = crate::client::StatementResponse {
            statement_id: "test".to_string(),
            status: crate::client::StatementStatus {
                state: crate::client::StatementState::Succeeded,
                error: None,
            },
            manifest: Some(crate::client::ResultManifest {
                format: Some("ARROW_STREAM".to_string()),
                schema: Some(crate::client::ResultSchema {
                    column_count: Some(3),
                    columns: Some(vec![
                        crate::client::ColumnInfo {
                            name: "id".to_string(),
                            type_text: Some("INT".to_string()),
                            type_name: Some("INT".to_string()),
                            position: Some(0),
                        },
                        crate::client::ColumnInfo {
                            name: "name".to_string(),
                            type_text: Some("STRING".to_string()),
                            type_name: Some("STRING".to_string()),
                            position: Some(1),
                        },
                        crate::client::ColumnInfo {
                            name: "amount".to_string(),
                            type_text: Some("DOUBLE".to_string()),
                            type_name: Some("DOUBLE".to_string()),
                            position: Some(2),
                        },
                    ]),
                }),
                total_chunk_count: Some(1),
                total_row_count: Some(100),
                total_byte_count: Some(5000),
                truncated: Some(false),
                chunks: None,
            }),
            result: None,
        };

        let schema = stmt.build_schema_from_response(&response).unwrap();

        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(*schema.field(0).data_type(), arrow_schema::DataType::Int32);
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(*schema.field(1).data_type(), arrow_schema::DataType::Utf8);
        assert_eq!(schema.field(2).name(), "amount");
        assert_eq!(*schema.field(2).data_type(), arrow_schema::DataType::Float64);
    }

    #[test]
    fn test_build_schema_from_response_no_manifest() {
        let runtime = create_mt_runtime();
        let mock_uri = "http://localhost:1234";
        let (client, session_manager) = create_test_client_and_session(mock_uri);

        let stmt = DatabricksStatement::new(client, session_manager, runtime).unwrap();

        let response = crate::client::StatementResponse {
            statement_id: "test".to_string(),
            status: crate::client::StatementStatus {
                state: crate::client::StatementState::Succeeded,
                error: None,
            },
            manifest: None,
            result: None,
        };

        let schema = stmt.build_schema_from_response(&response).unwrap();
        assert_eq!(schema.fields().len(), 0);
    }

    #[test]
    fn test_response_to_reader_empty_result() {
        let runtime = create_mt_runtime();
        let mock_uri = "http://localhost:1234";
        let (client, session_manager) = create_test_client_and_session(mock_uri);

        let stmt = DatabricksStatement::new(client, session_manager, runtime).unwrap();

        let response = crate::client::StatementResponse {
            statement_id: "test".to_string(),
            status: crate::client::StatementStatus {
                state: crate::client::StatementState::Succeeded,
                error: None,
            },
            manifest: Some(crate::client::ResultManifest {
                format: Some("ARROW_STREAM".to_string()),
                schema: Some(crate::client::ResultSchema {
                    column_count: Some(1),
                    columns: Some(vec![crate::client::ColumnInfo {
                        name: "value".to_string(),
                        type_text: Some("BIGINT".to_string()),
                        type_name: Some("BIGINT".to_string()),
                        position: Some(0),
                    }]),
                }),
                total_chunk_count: Some(0),
                total_row_count: Some(0),
                total_byte_count: Some(0),
                truncated: Some(false),
                chunks: None,
            }),
            result: None,
        };

        let reader = stmt.response_to_reader(response).unwrap();
        let schema = reader.schema();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "value");
        assert_eq!(*schema.field(0).data_type(), arrow_schema::DataType::Int64);
    }
}
