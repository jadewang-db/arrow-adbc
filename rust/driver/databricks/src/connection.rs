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

use std::collections::HashSet;
use std::sync::Arc;

use adbc_core::error::{Error, Status};
use adbc_core::options::{InfoCode, ObjectDepth, OptionConnection, OptionValue};
use adbc_core::{Connection, Optionable};
use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, Schema, SchemaRef};

use crate::options::DatabaseConfig;
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
pub struct DatabricksConnection {
    /// Database configuration.
    config: Arc<DatabaseConfig>,
    /// Current catalog.
    current_catalog: Option<String>,
    /// Current schema.
    current_schema: Option<String>,
    /// Session ID (will be populated when session management is implemented).
    #[allow(dead_code)]
    session_id: Option<String>,
}

impl DatabricksConnection {
    /// Create a new connection.
    pub(crate) fn new(config: Arc<DatabaseConfig>) -> adbc_core::error::Result<Self> {
        let current_catalog = config.default_catalog.clone();
        let current_schema = config.default_schema.clone();

        Ok(Self {
            config,
            current_catalog,
            current_schema,
            session_id: None,
        })
    }

    /// Get the database configuration.
    pub(crate) fn config(&self) -> &DatabaseConfig {
        &self.config
    }
}

impl Optionable for DatabricksConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> adbc_core::error::Result<()> {
        match key.as_ref() {
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
            _ => Err(Error::with_message_and_status(
                format!("Unrecognized option: {:?}", key),
                Status::NotFound,
            )),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> adbc_core::error::Result<String> {
        match key.as_ref() {
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
            adbc_core::constants::ADBC_CONNECTION_OPTION_AUTOCOMMIT => {
                // Databricks is always in autocommit mode
                Ok("true".to_string())
            }
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
        Err(Error::with_message_and_status(
            format!("Unrecognized option: {:?}", key),
            Status::NotFound,
        ))
    }

    fn get_option_double(&self, key: Self::Option) -> adbc_core::error::Result<f64> {
        Err(Error::with_message_and_status(
            format!("Unrecognized option: {:?}", key),
            Status::NotFound,
        ))
    }
}

impl Connection for DatabricksConnection {
    type StatementType = DatabricksStatement;

    fn new_statement(&mut self) -> adbc_core::error::Result<Self::StatementType> {
        Ok(DatabricksStatement::new(
            self.config.clone(),
            self.current_catalog.clone(),
            self.current_schema.clone(),
        ))
    }

    fn cancel(&mut self) -> adbc_core::error::Result<()> {
        // TODO: Implement connection cancellation
        Err(Error::with_message_and_status(
            "Connection cancellation not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn get_info(
        &self,
        _codes: Option<HashSet<InfoCode>>,
    ) -> adbc_core::error::Result<impl arrow_array::RecordBatchReader + Send> {
        // TODO: Implement get_info
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
        // TODO: Implement get_objects
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
        // TODO: Implement get_table_schema
        Err(Error::with_message_and_status(
            "get_table_schema not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn get_table_types(
        &self,
    ) -> adbc_core::error::Result<impl arrow_array::RecordBatchReader + Send> {
        // TODO: Implement get_table_types
        Err::<SingleBatchReader, _>(Error::with_message_and_status(
            "get_table_types not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn get_statistic_names(
        &self,
    ) -> adbc_core::error::Result<impl arrow_array::RecordBatchReader + Send> {
        // TODO: Implement get_statistic_names
        Err::<SingleBatchReader, _>(Error::with_message_and_status(
            "get_statistic_names not yet implemented",
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
        // TODO: Implement get_statistics
        Err::<SingleBatchReader, _>(Error::with_message_and_status(
            "get_statistics not yet implemented",
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
        // TODO: Implement read_partition
        Err::<SingleBatchReader, _>(Error::with_message_and_status(
            "read_partition not yet implemented",
            Status::NotImplemented,
        ))
    }
}
