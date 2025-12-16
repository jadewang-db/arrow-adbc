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

use std::sync::Arc;

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{OptionStatement, OptionValue};
use adbc_core::{Optionable, PartitionedResult, Statement};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::Schema;
use tokio::runtime::Runtime;

use crate::options::DatabaseConfig;

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
#[derive(Debug)]
pub struct DatabricksStatement {
    /// Database configuration.
    #[allow(dead_code)]
    config: Arc<DatabaseConfig>,
    /// Tokio runtime.
    #[allow(dead_code)]
    runtime: Arc<Runtime>,
    /// Session ID.
    #[allow(dead_code)]
    session_id: Option<String>,
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
    pub(crate) fn new(
        config: Arc<DatabaseConfig>,
        runtime: Arc<Runtime>,
        session_id: Option<String>,
    ) -> Result<Self> {
        Ok(Self {
            config,
            runtime,
            session_id,
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
        // 1. Call execute_statement API
        // 2. Poll until complete if needed
        // 3. Fetch results (inline or external links)
        // 4. Return as RecordBatchReader

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
