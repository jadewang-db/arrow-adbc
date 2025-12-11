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

//! DatabricksStatement implementation.

use std::sync::Arc;
use std::time::Duration;

use adbc_core::error::{Error, Status};
use adbc_core::options::{OptionStatement, OptionValue};
use adbc_core::{Optionable, PartitionedResult, Statement};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, Schema, SchemaRef};

use crate::client::{
    poll_until_complete, Disposition, ExecuteStatementRequest, Format, PollingConfig, SeaClient,
};
use crate::database::Runtime;
use crate::error;
use crate::fetch::ArrowResultReader;
use crate::options::DatabaseConfig;
use crate::session::SessionManager;

/// Default wait timeout for synchronous waits (in seconds).
const DEFAULT_WAIT_TIMEOUT_SECS: &str = "10s";

/// Default polling interval initial delay.
const DEFAULT_POLL_INITIAL_DELAY: Duration = Duration::from_secs(1);

/// Default polling max delay.
const DEFAULT_POLL_MAX_DELAY: Duration = Duration::from_secs(10);

/// A single batch reader for returning statement results.
#[derive(Debug)]
pub struct StatementResultReader {
    batch: Option<RecordBatch>,
    schema: SchemaRef,
}

impl StatementResultReader {
    /// Create a new single batch reader.
    pub fn new(batch: RecordBatch) -> Self {
        let schema = batch.schema();
        Self {
            batch: Some(batch),
            schema,
        }
    }

    /// Create an empty reader with the given schema.
    #[allow(dead_code)]
    pub fn empty(schema: SchemaRef) -> Self {
        Self {
            batch: None,
            schema,
        }
    }
}

impl Iterator for StatementResultReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        Ok(self.batch.take()).transpose()
    }
}

impl RecordBatchReader for StatementResultReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Statement options that can be configured.
#[derive(Debug, Clone)]
pub struct StatementOptions {
    /// Wait timeout for initial execution.
    pub wait_timeout: String,
    /// Maximum rows to return.
    pub row_limit: Option<i64>,
    /// Maximum bytes to return.
    pub byte_limit: Option<i64>,
    /// Polling configuration.
    pub polling_config: PollingConfig,
}

impl Default for StatementOptions {
    fn default() -> Self {
        Self {
            wait_timeout: DEFAULT_WAIT_TIMEOUT_SECS.to_string(),
            row_limit: None,
            byte_limit: None,
            polling_config: PollingConfig::new(DEFAULT_POLL_INITIAL_DELAY)
                .with_max_delay(DEFAULT_POLL_MAX_DELAY),
        }
    }
}

/// Executes SQL statements and returns results.
pub struct DatabricksStatement {
    /// SEA client for API calls.
    client: Arc<SeaClient>,
    /// Session manager for session ID.
    session_manager: Arc<SessionManager>,
    /// Tokio runtime for async operations.
    runtime: Arc<Runtime>,
    /// Database configuration.
    #[allow(dead_code)]
    config: Arc<DatabaseConfig>,
    /// Current catalog.
    current_catalog: Option<String>,
    /// Current schema.
    current_schema: Option<String>,
    /// SQL query to execute.
    sql_query: Option<String>,
    /// Bound parameters (for prepared statements).
    #[allow(dead_code)]
    bound_batch: Option<RecordBatch>,
    /// Statement ID (set after execution).
    statement_id: Option<String>,
    /// Statement-specific options.
    options: StatementOptions,
}

impl DatabricksStatement {
    /// Create a new statement.
    pub(crate) fn new(
        client: Arc<SeaClient>,
        session_manager: Arc<SessionManager>,
        runtime: Arc<Runtime>,
        config: Arc<DatabaseConfig>,
        current_catalog: Option<String>,
        current_schema: Option<String>,
    ) -> Self {
        Self {
            client,
            session_manager,
            runtime,
            config,
            current_catalog,
            current_schema,
            sql_query: None,
            bound_batch: None,
            statement_id: None,
            options: StatementOptions::default(),
        }
    }

    /// Get the SEA client.
    #[allow(dead_code)]
    pub(crate) fn client(&self) -> Arc<SeaClient> {
        self.client.clone()
    }

    /// Get the session manager.
    #[allow(dead_code)]
    pub(crate) fn session_manager(&self) -> Arc<SessionManager> {
        self.session_manager.clone()
    }

    /// Get the runtime.
    #[allow(dead_code)]
    pub(crate) fn runtime(&self) -> Arc<Runtime> {
        self.runtime.clone()
    }

    /// Execute the statement and return an ArrowResultReader for inline results.
    fn execute_internal(&mut self) -> adbc_core::error::Result<ArrowResultReader> {
        let sql = self.sql_query.clone().ok_or_else(|| {
            Error::with_message_and_status(
                "SQL query not set. Call set_sql_query first.".to_string(),
                Status::InvalidArguments,
            )
        })?;

        // Clone the values we need for the async block
        let client = self.client.clone();
        let session_manager = self.session_manager.clone();
        let current_catalog = self.current_catalog.clone();
        let current_schema = self.current_schema.clone();
        let wait_timeout = self.options.wait_timeout.clone();
        let row_limit = self.options.row_limit;
        let byte_limit = self.options.byte_limit;
        let polling_config = self.options.polling_config.clone();

        // Execute async using block_on
        let response = self.runtime.block_on(async {
            // Get session ID
            let session_id = session_manager.get_session_id().await?;

            // Build the request
            let request = ExecuteStatementRequest {
                warehouse_id: client.warehouse_id().to_string(),
                statement: sql,
                session_id: Some(session_id),
                catalog: current_catalog,
                schema: current_schema,
                wait_timeout: Some(wait_timeout),
                row_limit,
                byte_limit,
                disposition: Some(Disposition::Inline),
                format: Some(Format::ArrowStream),
                compression: None,
            };

            // Execute the statement
            let response = client.execute_statement_with_retry(request).await?;

            // Check if we need to poll for completion
            if response.status.is_succeeded() {
                // Statement completed immediately
                Ok(response)
            } else if response.status.is_failed() {
                let error_msg = response
                    .status
                    .error_message()
                    .unwrap_or_else(|| "Statement execution failed".to_string());
                Err(error::Error::statement_failed(error_msg))
            } else if response.status.is_cancelled() {
                Err(error::Error::cancelled("Statement was cancelled"))
            } else {
                // Need to poll for completion
                poll_until_complete(&client, &response.statement_id, &polling_config).await
            }
        }).map_err(|e: error::Error| {
            Error::with_message_and_status(format!("Execute failed: {}", e), e.status())
        })?;

        // Store the statement ID
        self.statement_id = Some(response.statement_id.clone());

        // Check if this is an inline result
        if let Some(result) = &response.result {
            if result.data_array.is_some() {
                // Parse inline Arrow IPC data
                let reader =
                    ArrowResultReader::from_inline_response(&response).map_err(|e| {
                        Error::with_message_and_status(
                            format!("Failed to parse inline results: {}", e),
                            Status::InvalidData,
                        )
                    })?;
                return Ok(reader);
            }

            if result.external_links.is_some() {
                // External links result - not yet implemented
                return Err(Error::with_message_and_status(
                    "External links results not yet implemented. Use smaller result sets or enable INLINE disposition.",
                    Status::NotImplemented,
                ));
            }
        }

        // No result data - return empty reader with schema from manifest
        let schema = if let Some(manifest) = &response.manifest {
            if let Some(manifest_schema) = &manifest.schema {
                let fields: Vec<arrow_schema::Field> = manifest_schema
                    .columns
                    .iter()
                    .map(|col| {
                        arrow_schema::Field::new(
                            &col.name,
                            arrow_schema::DataType::Utf8, // Default to string
                            col.nullable,
                        )
                    })
                    .collect();
                Arc::new(Schema::new(fields))
            } else {
                Arc::new(Schema::empty())
            }
        } else {
            Arc::new(Schema::empty())
        };

        Ok(ArrowResultReader::empty(schema))
    }

    /// Execute update and return row count.
    fn execute_update_internal(&mut self) -> adbc_core::error::Result<Option<i64>> {
        let sql = self.sql_query.clone().ok_or_else(|| {
            Error::with_message_and_status(
                "SQL query not set. Call set_sql_query first.".to_string(),
                Status::InvalidArguments,
            )
        })?;

        // Clone the values we need for the async block
        let client = self.client.clone();
        let session_manager = self.session_manager.clone();
        let current_catalog = self.current_catalog.clone();
        let current_schema = self.current_schema.clone();
        let wait_timeout = self.options.wait_timeout.clone();
        let row_limit = self.options.row_limit;
        let byte_limit = self.options.byte_limit;
        let polling_config = self.options.polling_config.clone();

        // Execute async using block_on
        let response = self.runtime.block_on(async {
            // Get session ID
            let session_id = session_manager.get_session_id().await?;

            // Build the request
            let request = ExecuteStatementRequest {
                warehouse_id: client.warehouse_id().to_string(),
                statement: sql,
                session_id: Some(session_id),
                catalog: current_catalog,
                schema: current_schema,
                wait_timeout: Some(wait_timeout),
                row_limit,
                byte_limit,
                disposition: Some(Disposition::Inline),
                format: Some(Format::ArrowStream),
                compression: None,
            };

            // Execute the statement
            let response = client.execute_statement_with_retry(request).await?;

            // Check if we need to poll for completion
            if response.status.is_succeeded() {
                Ok(response)
            } else if response.status.is_failed() {
                let error_msg = response
                    .status
                    .error_message()
                    .unwrap_or_else(|| "Statement execution failed".to_string());
                Err(error::Error::statement_failed(error_msg))
            } else if response.status.is_cancelled() {
                Err(error::Error::cancelled("Statement was cancelled"))
            } else {
                poll_until_complete(&client, &response.statement_id, &polling_config).await
            }
        }).map_err(|e: error::Error| {
            Error::with_message_and_status(format!("Execute failed: {}", e), e.status())
        })?;

        // Store the statement ID
        self.statement_id = Some(response.statement_id.clone());

        // Get row count from manifest or result
        let row_count = response
            .manifest
            .as_ref()
            .and_then(|m| m.total_row_count)
            .or_else(|| response.result.as_ref().and_then(|r| r.row_count));

        Ok(row_count)
    }

    /// Cancel the statement.
    fn cancel_internal(&mut self) -> adbc_core::error::Result<()> {
        if let Some(statement_id) = self.statement_id.clone() {
            let client = self.client.clone();

            self.runtime.block_on(async {
                client.cancel_statement_with_retry(&statement_id).await
            }).map_err(|e| {
                Error::with_message_and_status(format!("Cancel failed: {}", e), e.status())
            })?;
        }
        Ok(())
    }
}

impl Optionable for DatabricksStatement {
    type Option = OptionStatement;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> adbc_core::error::Result<()> {
        match key.as_ref() {
            // Databricks-specific options
            "databricks.statement.wait_timeout" => {
                if let OptionValue::String(v) = value {
                    self.options.wait_timeout = v;
                    Ok(())
                } else {
                    Err(Error::with_message_and_status(
                        "wait_timeout must be a string",
                        Status::InvalidArguments,
                    ))
                }
            }
            "databricks.statement.row_limit" => {
                if let OptionValue::Int(v) = value {
                    self.options.row_limit = Some(v);
                    Ok(())
                } else {
                    Err(Error::with_message_and_status(
                        "row_limit must be an integer",
                        Status::InvalidArguments,
                    ))
                }
            }
            "databricks.statement.byte_limit" => {
                if let OptionValue::Int(v) = value {
                    self.options.byte_limit = Some(v);
                    Ok(())
                } else {
                    Err(Error::with_message_and_status(
                        "byte_limit must be an integer",
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
            "databricks.statement.wait_timeout" => Ok(self.options.wait_timeout.clone()),
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
            "databricks.statement.row_limit" => self.options.row_limit.ok_or_else(|| {
                Error::with_message_and_status("row_limit not set", Status::NotFound)
            }),
            "databricks.statement.byte_limit" => self.options.byte_limit.ok_or_else(|| {
                Error::with_message_and_status("byte_limit not set", Status::NotFound)
            }),
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

impl Statement for DatabricksStatement {
    fn bind(&mut self, batch: RecordBatch) -> adbc_core::error::Result<()> {
        self.bound_batch = Some(batch);
        Ok(())
    }

    fn bind_stream(
        &mut self,
        _reader: Box<dyn RecordBatchReader + Send>,
    ) -> adbc_core::error::Result<()> {
        Err(Error::with_message_and_status(
            "bind_stream not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn execute(&mut self) -> adbc_core::error::Result<impl RecordBatchReader + Send> {
        self.execute_internal()
    }

    fn execute_update(&mut self) -> adbc_core::error::Result<Option<i64>> {
        self.execute_update_internal()
    }

    fn execute_schema(&mut self) -> adbc_core::error::Result<Schema> {
        // Execute the statement to get the schema
        let reader = self.execute_internal()?;
        Ok((*reader.schema()).clone())
    }

    fn execute_partitions(&mut self) -> adbc_core::error::Result<PartitionedResult> {
        Err(Error::with_message_and_status(
            "Partitioned execution not supported",
            Status::NotImplemented,
        ))
    }

    fn get_parameter_schema(&self) -> adbc_core::error::Result<Schema> {
        Err(Error::with_message_and_status(
            "get_parameter_schema not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn prepare(&mut self) -> adbc_core::error::Result<()> {
        Err(Error::with_message_and_status(
            "Prepared statements not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn set_sql_query(&mut self, query: impl AsRef<str>) -> adbc_core::error::Result<()> {
        self.sql_query = Some(query.as_ref().to_string());
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> adbc_core::error::Result<()> {
        Err(Error::with_message_and_status(
            "Substrait plans are not supported by Databricks",
            Status::NotImplemented,
        ))
    }

    fn cancel(&mut self) -> adbc_core::error::Result<()> {
        self.cancel_internal()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{ResultData, SessionResponse};
    use arrow_array::{Int32Array, StringArray};
    use arrow_ipc::writer::StreamWriter;
    use base64::prelude::*;
    use std::io::Cursor;
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

    /// Helper to create a test runtime.
    fn create_test_runtime() -> Arc<Runtime> {
        Arc::new(Runtime::new(Some(tokio::runtime::Handle::current())).unwrap())
    }

    /// Create a test record batch and encode to base64 Arrow IPC.
    fn create_test_arrow_data() -> String {
        let schema = Arc::new(Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int32, false),
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
            ],
        )
        .unwrap();

        let mut buffer = Cursor::new(Vec::new());
        {
            let mut writer = StreamWriter::try_new(&mut buffer, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        BASE64_STANDARD.encode(buffer.into_inner())
    }

    // ==================== Statement Creation Tests ====================

    #[tokio::test(flavor = "multi_thread")]
    async fn test_statement_set_sql_query() {
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
        let client = Arc::new(
            SeaClient::new(crate::client::SeaClientConfig::new(
                &config.host,
                &config.token,
                &config.warehouse_id,
            ))
            .unwrap(),
        );
        let session_manager = Arc::new(SessionManager::new(client.clone(), None, None));

        // Create session
        runtime
            .block_on(session_manager.get_session_id())
            .unwrap();

        let mut stmt = DatabricksStatement::new(
            client,
            session_manager,
            runtime,
            config,
            Some("main".to_string()),
            Some("default".to_string()),
        );

        // Set SQL query
        stmt.set_sql_query("SELECT 1").unwrap();
        assert_eq!(stmt.sql_query, Some("SELECT 1".to_string()));

        // Set another query
        stmt.set_sql_query("SELECT * FROM test").unwrap();
        assert_eq!(stmt.sql_query, Some("SELECT * FROM test".to_string()));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_statement_options() {
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
        let client = Arc::new(
            SeaClient::new(crate::client::SeaClientConfig::new(
                &config.host,
                &config.token,
                &config.warehouse_id,
            ))
            .unwrap(),
        );
        let session_manager = Arc::new(SessionManager::new(client.clone(), None, None));

        runtime
            .block_on(session_manager.get_session_id())
            .unwrap();

        let mut stmt = DatabricksStatement::new(
            client,
            session_manager,
            runtime,
            config,
            None,
            None,
        );

        // Test setting wait_timeout
        stmt.set_option(
            OptionStatement::Other("databricks.statement.wait_timeout".to_string()),
            OptionValue::String("30s".to_string()),
        )
        .unwrap();
        assert_eq!(stmt.options.wait_timeout, "30s");

        // Test setting row_limit
        stmt.set_option(
            OptionStatement::Other("databricks.statement.row_limit".to_string()),
            OptionValue::Int(1000),
        )
        .unwrap();
        assert_eq!(stmt.options.row_limit, Some(1000));

        // Test getting options
        let wait_timeout = stmt
            .get_option_string(OptionStatement::Other(
                "databricks.statement.wait_timeout".to_string(),
            ))
            .unwrap();
        assert_eq!(wait_timeout, "30s");

        let row_limit = stmt
            .get_option_int(OptionStatement::Other(
                "databricks.statement.row_limit".to_string(),
            ))
            .unwrap();
        assert_eq!(row_limit, 1000);
    }

    // ==================== Execute Tests ====================

    #[tokio::test(flavor = "multi_thread")]
    async fn test_statement_execute_inline_success() {
        let mock_server = MockServer::start().await;

        // Session create
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(SessionResponse {
                session_id: "test-session".to_string(),
            }))
            .mount(&mock_server)
            .await;

        // Session delete
        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/test-session"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        // Statement execute
        let arrow_data = create_test_arrow_data();
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "statement_id": "stmt-123",
                "status": {
                    "state": "SUCCEEDED"
                },
                "result": {
                    "data_array": arrow_data,
                    "row_count": 3
                }
            })))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri());
        let runtime = create_test_runtime();
        let client = Arc::new(
            SeaClient::new(crate::client::SeaClientConfig::new(
                &config.host,
                &config.token,
                &config.warehouse_id,
            ))
            .unwrap(),
        );
        let session_manager = Arc::new(SessionManager::new(client.clone(), None, None));

        runtime
            .block_on(session_manager.get_session_id())
            .unwrap();

        let mut stmt = DatabricksStatement::new(
            client,
            session_manager,
            runtime,
            config,
            Some("main".to_string()),
            Some("default".to_string()),
        );

        stmt.set_sql_query("SELECT * FROM test").unwrap();
        let mut reader = stmt.execute().unwrap();

        // Read the results
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);

        // No more batches
        assert!(reader.next().is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_statement_execute_with_polling() {
        let mock_server = MockServer::start().await;

        // Session create
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(SessionResponse {
                session_id: "test-session".to_string(),
            }))
            .mount(&mock_server)
            .await;

        // Session delete
        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/test-session"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        // Statement execute - returns PENDING
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "statement_id": "stmt-poll",
                "status": {
                    "state": "PENDING"
                }
            })))
            .mount(&mock_server)
            .await;

        // First poll - still RUNNING
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-poll"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "statement_id": "stmt-poll",
                "status": {
                    "state": "RUNNING"
                }
            })))
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;

        // Second poll - SUCCEEDED with results
        let arrow_data = create_test_arrow_data();
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-poll"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "statement_id": "stmt-poll",
                "status": {
                    "state": "SUCCEEDED"
                },
                "result": {
                    "data_array": arrow_data,
                    "row_count": 3
                }
            })))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri());
        let runtime = create_test_runtime();
        let client = Arc::new(
            SeaClient::new(crate::client::SeaClientConfig::new(
                &config.host,
                &config.token,
                &config.warehouse_id,
            ))
            .unwrap(),
        );
        let session_manager = Arc::new(SessionManager::new(client.clone(), None, None));

        runtime
            .block_on(session_manager.get_session_id())
            .unwrap();

        let mut stmt = DatabricksStatement::new(
            client,
            session_manager,
            runtime,
            config,
            Some("main".to_string()),
            Some("default".to_string()),
        );

        // Use very short polling delays for testing
        stmt.options.polling_config = PollingConfig::new(Duration::from_millis(10))
            .with_max_delay(Duration::from_millis(50));

        stmt.set_sql_query("SELECT * FROM test").unwrap();
        let mut reader = stmt.execute().unwrap();

        // Read the results
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_statement_execute_no_query_error() {
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
        let client = Arc::new(
            SeaClient::new(crate::client::SeaClientConfig::new(
                &config.host,
                &config.token,
                &config.warehouse_id,
            ))
            .unwrap(),
        );
        let session_manager = Arc::new(SessionManager::new(client.clone(), None, None));

        runtime
            .block_on(session_manager.get_session_id())
            .unwrap();

        let mut stmt = DatabricksStatement::new(
            client,
            session_manager,
            runtime,
            config,
            None,
            None,
        );

        // Try to execute without setting query
        let result = stmt.execute();
        assert!(result.is_err());
        match result {
            Err(err) => assert!(err.message.contains("SQL query not set")),
            Ok(_) => panic!("Expected error but got Ok"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_statement_execute_failed() {
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

        // Statement execute returns FAILED
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "statement_id": "stmt-fail",
                "status": {
                    "state": "FAILED",
                    "error": {
                        "error_code": "SYNTAX_ERROR",
                        "message": "Invalid SQL syntax"
                    }
                }
            })))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri());
        let runtime = create_test_runtime();
        let client = Arc::new(
            SeaClient::new(crate::client::SeaClientConfig::new(
                &config.host,
                &config.token,
                &config.warehouse_id,
            ))
            .unwrap(),
        );
        let session_manager = Arc::new(SessionManager::new(client.clone(), None, None));

        runtime
            .block_on(session_manager.get_session_id())
            .unwrap();

        let mut stmt = DatabricksStatement::new(
            client,
            session_manager,
            runtime,
            config,
            None,
            None,
        );

        stmt.set_sql_query("INVALID SQL").unwrap();
        let result = stmt.execute();

        assert!(result.is_err());
        match result {
            Err(err) => assert!(
                err.message.contains("failed") || err.message.contains("Invalid SQL syntax")
            ),
            Ok(_) => panic!("Expected error but got Ok"),
        }
    }

    // ==================== Execute Update Tests ====================

    #[tokio::test(flavor = "multi_thread")]
    async fn test_statement_execute_update() {
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

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "statement_id": "stmt-update",
                "status": {
                    "state": "SUCCEEDED"
                },
                "manifest": {
                    "total_row_count": 5,
                    "total_chunk_count": 0
                }
            })))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri());
        let runtime = create_test_runtime();
        let client = Arc::new(
            SeaClient::new(crate::client::SeaClientConfig::new(
                &config.host,
                &config.token,
                &config.warehouse_id,
            ))
            .unwrap(),
        );
        let session_manager = Arc::new(SessionManager::new(client.clone(), None, None));

        runtime
            .block_on(session_manager.get_session_id())
            .unwrap();

        let mut stmt = DatabricksStatement::new(
            client,
            session_manager,
            runtime,
            config,
            None,
            None,
        );

        stmt.set_sql_query("INSERT INTO test VALUES (1, 2, 3, 4, 5)")
            .unwrap();
        let row_count = stmt.execute_update().unwrap();

        assert_eq!(row_count, Some(5));
    }

    // ==================== Cancel Tests ====================

    #[tokio::test(flavor = "multi_thread")]
    async fn test_statement_cancel() {
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

        // Cancel endpoint
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements/stmt-cancel/cancel"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri());
        let runtime = create_test_runtime();
        let client = Arc::new(
            SeaClient::new(crate::client::SeaClientConfig::new(
                &config.host,
                &config.token,
                &config.warehouse_id,
            ))
            .unwrap(),
        );
        let session_manager = Arc::new(SessionManager::new(client.clone(), None, None));

        runtime
            .block_on(session_manager.get_session_id())
            .unwrap();

        let mut stmt = DatabricksStatement::new(
            client,
            session_manager.clone(),
            runtime.clone(),
            config,
            None,
            None,
        );

        // Set the statement ID as if execute was called
        stmt.statement_id = Some("stmt-cancel".to_string());

        // Cancel should succeed
        let result = stmt.cancel();
        assert!(result.is_ok());
    }

    // ==================== StatementResultReader Tests ====================

    #[test]
    fn test_statement_result_reader() {
        let schema = Arc::new(Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let mut reader = StatementResultReader::new(batch);

        // First call returns the batch
        let first = reader.next();
        assert!(first.is_some());
        assert_eq!(first.unwrap().unwrap().num_rows(), 3);

        // Second call returns None
        assert!(reader.next().is_none());
    }

    #[test]
    fn test_statement_result_reader_empty() {
        let schema = Arc::new(Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int32, false),
        ]));

        let mut reader = StatementResultReader::empty(schema.clone());

        // Should return None immediately
        assert!(reader.next().is_none());

        // Schema should still be available
        assert_eq!(reader.schema(), schema);
    }
}
