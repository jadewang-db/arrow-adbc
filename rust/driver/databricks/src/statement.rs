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

use adbc_core::error::{Error, Status};
use adbc_core::options::{OptionStatement, OptionValue};
use adbc_core::{Optionable, PartitionedResult, Statement};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, Schema, SchemaRef};

use crate::client::SeaClient;
use crate::database::Runtime;
use crate::options::DatabaseConfig;
use crate::session::SessionManager;

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

/// Executes SQL statements and returns results.
pub struct DatabricksStatement {
    /// SEA client for API calls.
    #[allow(dead_code)]
    client: Arc<SeaClient>,
    /// Session manager for session ID.
    #[allow(dead_code)]
    session_manager: Arc<SessionManager>,
    /// Tokio runtime for async operations.
    #[allow(dead_code)]
    runtime: Arc<Runtime>,
    /// Database configuration.
    #[allow(dead_code)]
    config: Arc<DatabaseConfig>,
    /// Current catalog.
    #[allow(dead_code)]
    current_catalog: Option<String>,
    /// Current schema.
    #[allow(dead_code)]
    current_schema: Option<String>,
    /// SQL query to execute.
    sql_query: Option<String>,
    /// Bound parameters (for prepared statements).
    #[allow(dead_code)]
    bound_batch: Option<RecordBatch>,
    /// Statement ID (set after execution).
    #[allow(dead_code)]
    statement_id: Option<String>,
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
}

impl Optionable for DatabricksStatement {
    type Option = OptionStatement;

    fn set_option(&mut self, key: Self::Option, _value: OptionValue) -> adbc_core::error::Result<()> {
        // TODO: Implement statement options in Sprint 2.3
        Err(Error::with_message_and_status(
            format!("Unrecognized option: {:?}", key),
            Status::NotFound,
        ))
    }

    fn get_option_string(&self, key: Self::Option) -> adbc_core::error::Result<String> {
        Err(Error::with_message_and_status(
            format!("Unrecognized option: {:?}", key),
            Status::NotFound,
        ))
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

impl Statement for DatabricksStatement {
    fn bind(&mut self, batch: RecordBatch) -> adbc_core::error::Result<()> {
        self.bound_batch = Some(batch);
        Ok(())
    }

    fn bind_stream(
        &mut self,
        _reader: Box<dyn RecordBatchReader + Send>,
    ) -> adbc_core::error::Result<()> {
        // TODO: Implement bind_stream
        Err(Error::with_message_and_status(
            "bind_stream not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn execute(&mut self) -> adbc_core::error::Result<impl RecordBatchReader + Send> {
        // TODO: Implement execute in Sprint 2.3
        Err::<StatementResultReader, _>(Error::with_message_and_status(
            "execute not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn execute_update(&mut self) -> adbc_core::error::Result<Option<i64>> {
        // TODO: Implement execute_update in Sprint 4
        Err(Error::with_message_and_status(
            "execute_update not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn execute_schema(&mut self) -> adbc_core::error::Result<Schema> {
        // TODO: Implement execute_schema
        Err(Error::with_message_and_status(
            "execute_schema not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn execute_partitions(&mut self) -> adbc_core::error::Result<PartitionedResult> {
        // Partitioned execution not supported
        Err(Error::with_message_and_status(
            "Partitioned execution not supported",
            Status::NotImplemented,
        ))
    }

    fn get_parameter_schema(&self) -> adbc_core::error::Result<Schema> {
        // TODO: Implement get_parameter_schema
        Err(Error::with_message_and_status(
            "get_parameter_schema not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn prepare(&mut self) -> adbc_core::error::Result<()> {
        // TODO: Implement prepare in Phase 2
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
        // Substrait is not supported by Databricks
        Err(Error::with_message_and_status(
            "Substrait plans are not supported by Databricks",
            Status::NotImplemented,
        ))
    }

    fn cancel(&mut self) -> adbc_core::error::Result<()> {
        // TODO: Implement cancel in Sprint 2.3
        Err(Error::with_message_and_status(
            "cancel not yet implemented",
            Status::NotImplemented,
        ))
    }
}
