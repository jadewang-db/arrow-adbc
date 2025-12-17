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

//! Statement implementation for Databricks

use std::sync::Arc;

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{OptionStatement, OptionValue};
use adbc_core::{Optionable, PartitionedResult, Statement};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, Schema, SchemaRef};
use tokio::runtime::Runtime;

use crate::client::SeaClient;
use crate::options::DatabaseConfig;
use crate::session::SessionManager;

/// Statement-level configuration
#[derive(Clone, Debug)]
pub struct StatementConfig {
    /// Timeout for statement execution (e.g., "10s", "5m")
    pub wait_timeout: String,
    /// Maximum number of rows to return
    pub row_limit: Option<i64>,
    /// Maximum number of bytes to return
    pub byte_limit: Option<i64>,
    /// Default catalog for the statement
    pub catalog: Option<String>,
    /// Default schema for the statement
    pub schema: Option<String>,
    /// Number of concurrent fetches for result data
    pub fetch_concurrency: usize,
}

/// SQL statement handle for executing queries
pub struct DatabricksStatement {
    #[allow(dead_code)]
    client: Arc<SeaClient>,
    #[allow(dead_code)]
    session_manager: Arc<SessionManager>,
    #[allow(dead_code)]
    runtime: Arc<Runtime>,
    /// Statement configuration
    config: StatementConfig,
    /// SQL query to execute
    sql_query: Option<String>,
    /// Statement ID from server (for prepared statements)
    #[allow(dead_code)]
    statement_id: Option<String>,
}

impl DatabricksStatement {
    /// Create a new statement with the given client, session manager, runtime, and database config
    ///
    /// # Arguments
    /// * `client` - SEA client for making API calls
    /// * `session_manager` - Session manager for maintaining session state
    /// * `runtime` - Tokio runtime for async operations
    /// * `db_config` - Database configuration to inherit defaults from
    pub fn new(
        client: Arc<SeaClient>,
        session_manager: Arc<SessionManager>,
        runtime: Arc<Runtime>,
        db_config: &DatabaseConfig,
    ) -> Result<Self> {
        Ok(Self {
            client,
            session_manager,
            runtime,
            config: StatementConfig {
                wait_timeout: "10s".to_string(),
                row_limit: None,
                byte_limit: None,
                catalog: db_config.default_catalog.clone(),
                schema: db_config.default_schema.clone(),
                fetch_concurrency: db_config.fetch_config.concurrency,
            },
            sql_query: None,
            statement_id: None,
        })
    }
}

/// Empty reader for stub implementations
struct EmptyBatchReader {
    schema: SchemaRef,
}

impl EmptyBatchReader {
    fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

impl Iterator for EmptyBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

impl RecordBatchReader for EmptyBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Optionable for DatabricksStatement {
    type Option = OptionStatement;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match key {
            OptionStatement::Other(ref k) => match k.as_str() {
                "databricks.statement.wait_timeout" => {
                    let timeout = match value {
                        OptionValue::String(s) => s,
                        _ => {
                            return Err(Error::with_message_and_status(
                                "wait_timeout must be a string",
                                Status::InvalidArguments,
                            ))
                        }
                    };
                    self.config.wait_timeout = timeout;
                    Ok(())
                }
                "databricks.statement.row_limit" => {
                    let limit = match value {
                        OptionValue::Int(i) => i,
                        _ => {
                            return Err(Error::with_message_and_status(
                                "row_limit must be an integer",
                                Status::InvalidArguments,
                            ))
                        }
                    };
                    self.config.row_limit = Some(limit);
                    Ok(())
                }
                "databricks.statement.byte_limit" => {
                    let limit = match value {
                        OptionValue::Int(i) => i,
                        _ => {
                            return Err(Error::with_message_and_status(
                                "byte_limit must be an integer",
                                Status::InvalidArguments,
                            ))
                        }
                    };
                    self.config.byte_limit = Some(limit);
                    Ok(())
                }
                _ => Err(Error::with_message_and_status(
                    format!("Unknown option: {}", k),
                    Status::NotImplemented,
                )),
            },
            _ => Err(Error::with_message_and_status(
                format!("Unsupported option: {:?}", key),
                Status::NotImplemented,
            )),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match key {
            OptionStatement::Other(ref k) if k == "databricks.statement.wait_timeout" => {
                Ok(self.config.wait_timeout.clone())
            }
            _ => Err(Error::with_message_and_status(
                format!("Unknown option: {:?}", key),
                Status::NotFound,
            )),
        }
    }

    fn get_option_bytes(&self, _key: Self::Option) -> Result<Vec<u8>> {
        Err(Error::with_message_and_status(
            "No byte options supported",
            Status::NotFound,
        ))
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        match key {
            OptionStatement::Other(ref k) if k == "databricks.statement.row_limit" => {
                self.config.row_limit.ok_or_else(|| {
                    Error::with_message_and_status("row_limit not set", Status::NotFound)
                })
            }
            OptionStatement::Other(ref k) if k == "databricks.statement.byte_limit" => {
                self.config.byte_limit.ok_or_else(|| {
                    Error::with_message_and_status("byte_limit not set", Status::NotFound)
                })
            }
            _ => Err(Error::with_message_and_status(
                format!("Unknown option: {:?}", key),
                Status::NotFound,
            )),
        }
    }

    fn get_option_double(&self, _key: Self::Option) -> Result<f64> {
        Err(Error::with_message_and_status(
            "No double options supported",
            Status::NotFound,
        ))
    }
}

impl Statement for DatabricksStatement {
    fn bind(&mut self, _batch: RecordBatch) -> Result<()> {
        Err(Error::with_message_and_status(
            "Parameter binding not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn bind_stream(&mut self, _reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        Err(Error::with_message_and_status(
            "Stream binding not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn cancel(&mut self) -> Result<()> {
        Err(Error::with_message_and_status(
            "Statement cancellation not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn execute(&mut self) -> Result<impl RecordBatchReader> {
        // Stub implementation - will be completed in later work items
        Ok(EmptyBatchReader::new(std::sync::Arc::new(Schema::empty())))
    }

    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        Err(Error::with_message_and_status(
            "Partitioned execution not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        Err(Error::with_message_and_status(
            "Schema execution not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        Err(Error::with_message_and_status(
            "Update execution not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn get_parameter_schema(&self) -> Result<Schema> {
        Err(Error::with_message_and_status(
            "Parameter schema not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn prepare(&mut self) -> Result<()> {
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
    use crate::options::{DatabaseConfig, FetchConfig, HttpConfig};
    use std::time::Duration;

    fn create_test_statement() -> DatabricksStatement {
        let db_config = DatabaseConfig {
            host: Some("https://test.databricks.com".to_string()),
            warehouse_id: Some("test-warehouse".to_string()),
            token: Some("test-token".to_string()),
            default_catalog: Some("test_catalog".to_string()),
            default_schema: Some("test_schema".to_string()),
            http_config: HttpConfig {
                connect_timeout: Duration::from_secs(10),
                read_timeout: Duration::from_secs(300),
                max_retries: 3,
                retry_backoff_base: Duration::from_secs(1),
            },
            fetch_config: FetchConfig {
                concurrency: 4,
                compression: "LZ4_FRAME".to_string(),
            },
        };

        // Create minimal components for statement
        let client_config = crate::client::SeaClientConfig {
            host: "https://test.databricks.com".to_string(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(300),
        };
        let client = Arc::new(SeaClient::new(client_config).unwrap());
        let session_manager = Arc::new(SessionManager::new(
            client.clone(),
            db_config.default_catalog.clone(),
            db_config.default_schema.clone(),
        ));
        let runtime = Arc::new(Runtime::new().unwrap());

        DatabricksStatement::new(client, session_manager, runtime, &db_config).unwrap()
    }

    #[test]
    fn test_statement_new_inherits_database_config() {
        let db_config = DatabaseConfig {
            host: Some("https://test.databricks.com".to_string()),
            warehouse_id: Some("test-warehouse".to_string()),
            token: Some("test-token".to_string()),
            default_catalog: Some("my_catalog".to_string()),
            default_schema: Some("my_schema".to_string()),
            http_config: HttpConfig::default(),
            fetch_config: FetchConfig {
                concurrency: 16,
                compression: "LZ4_FRAME".to_string(),
            },
        };

        let client_config = crate::client::SeaClientConfig {
            host: "https://test.databricks.com".to_string(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(300),
        };
        let client = Arc::new(SeaClient::new(client_config).unwrap());
        let session_manager = Arc::new(SessionManager::new(
            client.clone(),
            db_config.default_catalog.clone(),
            db_config.default_schema.clone(),
        ));
        let runtime = Arc::new(Runtime::new().unwrap());

        let stmt = DatabricksStatement::new(client, session_manager, runtime, &db_config).unwrap();

        // Verify config inheritance
        assert_eq!(stmt.config.wait_timeout, "10s");
        assert_eq!(stmt.config.catalog, Some("my_catalog".to_string()));
        assert_eq!(stmt.config.schema, Some("my_schema".to_string()));
        assert_eq!(stmt.config.fetch_concurrency, 16);
        assert!(stmt.config.row_limit.is_none());
        assert!(stmt.config.byte_limit.is_none());
    }

    #[test]
    fn test_set_option_wait_timeout() {
        let mut stmt = create_test_statement();

        // Set wait_timeout option
        let result = stmt.set_option(
            OptionStatement::Other("databricks.statement.wait_timeout".to_string()),
            OptionValue::String("30s".to_string()),
        );
        assert!(result.is_ok());
        assert_eq!(stmt.config.wait_timeout, "30s");
    }

    #[test]
    fn test_set_option_wait_timeout_wrong_type() {
        let mut stmt = create_test_statement();

        // Try to set wait_timeout with wrong type
        let result = stmt.set_option(
            OptionStatement::Other("databricks.statement.wait_timeout".to_string()),
            OptionValue::Int(30),
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("must be a string"));
    }

    #[test]
    fn test_set_option_row_limit() {
        let mut stmt = create_test_statement();

        // Set row_limit option
        let result = stmt.set_option(
            OptionStatement::Other("databricks.statement.row_limit".to_string()),
            OptionValue::Int(1000),
        );
        assert!(result.is_ok());
        assert_eq!(stmt.config.row_limit, Some(1000));
    }

    #[test]
    fn test_set_option_row_limit_wrong_type() {
        let mut stmt = create_test_statement();

        // Try to set row_limit with wrong type
        let result = stmt.set_option(
            OptionStatement::Other("databricks.statement.row_limit".to_string()),
            OptionValue::String("1000".to_string()),
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("must be an integer"));
    }

    #[test]
    fn test_set_option_byte_limit() {
        let mut stmt = create_test_statement();

        // Set byte_limit option
        let result = stmt.set_option(
            OptionStatement::Other("databricks.statement.byte_limit".to_string()),
            OptionValue::Int(1048576),
        );
        assert!(result.is_ok());
        assert_eq!(stmt.config.byte_limit, Some(1048576));
    }

    #[test]
    fn test_set_option_byte_limit_wrong_type() {
        let mut stmt = create_test_statement();

        // Try to set byte_limit with wrong type
        let result = stmt.set_option(
            OptionStatement::Other("databricks.statement.byte_limit".to_string()),
            OptionValue::Double(1048576.0),
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("must be an integer"));
    }

    #[test]
    fn test_set_option_unknown_option() {
        let mut stmt = create_test_statement();

        // Try to set unknown option
        let result = stmt.set_option(
            OptionStatement::Other("databricks.statement.unknown".to_string()),
            OptionValue::String("value".to_string()),
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Unknown option"));
    }

    #[test]
    fn test_get_option_string_wait_timeout() {
        let mut stmt = create_test_statement();

        // Default value
        let result = stmt.get_option_string(OptionStatement::Other(
            "databricks.statement.wait_timeout".to_string(),
        ));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "10s");

        // After setting
        stmt.set_option(
            OptionStatement::Other("databricks.statement.wait_timeout".to_string()),
            OptionValue::String("45s".to_string()),
        )
        .unwrap();
        let result = stmt.get_option_string(OptionStatement::Other(
            "databricks.statement.wait_timeout".to_string(),
        ));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "45s");
    }

    #[test]
    fn test_get_option_string_unknown_option() {
        let stmt = create_test_statement();

        let result = stmt.get_option_string(OptionStatement::Other(
            "databricks.statement.unknown".to_string(),
        ));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Unknown option"));
    }

    #[test]
    fn test_get_option_int_row_limit() {
        let mut stmt = create_test_statement();

        // Before setting - should be not found
        let result = stmt.get_option_int(OptionStatement::Other(
            "databricks.statement.row_limit".to_string(),
        ));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("not set"));

        // After setting
        stmt.set_option(
            OptionStatement::Other("databricks.statement.row_limit".to_string()),
            OptionValue::Int(500),
        )
        .unwrap();
        let result = stmt.get_option_int(OptionStatement::Other(
            "databricks.statement.row_limit".to_string(),
        ));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 500);
    }

    #[test]
    fn test_get_option_int_byte_limit() {
        let mut stmt = create_test_statement();

        // Before setting - should be not found
        let result = stmt.get_option_int(OptionStatement::Other(
            "databricks.statement.byte_limit".to_string(),
        ));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("not set"));

        // After setting
        stmt.set_option(
            OptionStatement::Other("databricks.statement.byte_limit".to_string()),
            OptionValue::Int(2097152),
        )
        .unwrap();
        let result = stmt.get_option_int(OptionStatement::Other(
            "databricks.statement.byte_limit".to_string(),
        ));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 2097152);
    }

    #[test]
    fn test_get_option_int_unknown_option() {
        let stmt = create_test_statement();

        let result =
            stmt.get_option_int(OptionStatement::Other("databricks.statement.unknown".to_string()));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Unknown option"));
    }

    #[test]
    fn test_get_option_bytes_not_supported() {
        let stmt = create_test_statement();

        let result = stmt.get_option_bytes(OptionStatement::Other(
            "databricks.statement.wait_timeout".to_string(),
        ));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("No byte options"));
    }

    #[test]
    fn test_get_option_double_not_supported() {
        let stmt = create_test_statement();

        let result = stmt.get_option_double(OptionStatement::Other(
            "databricks.statement.wait_timeout".to_string(),
        ));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("No double options"));
    }

    #[test]
    fn test_set_sql_query() {
        let mut stmt = create_test_statement();

        // Initially no query
        assert!(stmt.sql_query.is_none());

        // Set a query
        let result = stmt.set_sql_query("SELECT * FROM table");
        assert!(result.is_ok());
        assert_eq!(stmt.sql_query, Some("SELECT * FROM table".to_string()));

        // Update the query
        let result = stmt.set_sql_query("SELECT id, name FROM users");
        assert!(result.is_ok());
        assert_eq!(
            stmt.sql_query,
            Some("SELECT id, name FROM users".to_string())
        );
    }

    #[test]
    fn test_multiple_option_settings() {
        let mut stmt = create_test_statement();

        // Set multiple options
        stmt.set_option(
            OptionStatement::Other("databricks.statement.wait_timeout".to_string()),
            OptionValue::String("60s".to_string()),
        )
        .unwrap();
        stmt.set_option(
            OptionStatement::Other("databricks.statement.row_limit".to_string()),
            OptionValue::Int(10000),
        )
        .unwrap();
        stmt.set_option(
            OptionStatement::Other("databricks.statement.byte_limit".to_string()),
            OptionValue::Int(10485760),
        )
        .unwrap();

        // Verify all settings
        assert_eq!(stmt.config.wait_timeout, "60s");
        assert_eq!(stmt.config.row_limit, Some(10000));
        assert_eq!(stmt.config.byte_limit, Some(10485760));
    }
}
