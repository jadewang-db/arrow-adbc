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
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, Schema, SchemaRef};
use tokio::runtime::Runtime;

use crate::client::{SeaClient, SeaClientConfig};
use crate::options::DatabaseConfig;
use crate::session::SessionManager;
use crate::statement::DatabricksStatement;

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
    #[allow(dead_code)]
    current_catalog: Option<String>,
    #[allow(dead_code)]
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
}

impl Optionable for DatabricksConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, _key: Self::Option, _value: OptionValue) -> Result<()> {
        // Stub implementation for now - will be completed in work item 1.7
        Ok(())
    }

    fn get_option_string(&self, _key: Self::Option) -> Result<String> {
        Err(Error::with_message_and_status(
            "Connection options not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn get_option_bytes(&self, _key: Self::Option) -> Result<Vec<u8>> {
        Err(Error::with_message_and_status(
            "Connection options not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn get_option_int(&self, _key: Self::Option) -> Result<i64> {
        Err(Error::with_message_and_status(
            "Connection options not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn get_option_double(&self, _key: Self::Option) -> Result<f64> {
        Err(Error::with_message_and_status(
            "Connection options not yet implemented",
            Status::NotImplemented,
        ))
    }
}

impl Connection for DatabricksConnection {
    type StatementType = DatabricksStatement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        // Stub implementation - will be completed in work item 1.7
        Ok(DatabricksStatement::new())
    }

    fn cancel(&mut self) -> Result<()> {
        Err(Error::with_message_and_status(
            "Connection methods not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn get_info(&self, _codes: Option<HashSet<InfoCode>>) -> Result<impl RecordBatchReader> {
        // Stub implementation - will be completed in work item 1.7
        Ok(EmptyBatchReader::new(std::sync::Arc::new(Schema::empty())))
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
        // Stub implementation - will be completed in work item 1.7
        Ok(EmptyBatchReader::new(std::sync::Arc::new(Schema::empty())))
    }

    fn get_table_schema(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: &str,
    ) -> Result<Schema> {
        // Stub implementation - will be completed in work item 1.7
        Ok(Schema::empty())
    }

    fn get_table_types(&self) -> Result<impl RecordBatchReader> {
        // Stub implementation - will be completed in work item 1.7
        Ok(EmptyBatchReader::new(std::sync::Arc::new(Schema::empty())))
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
        // Stub implementation - will be completed in work item 1.7
        Ok(EmptyBatchReader::new(std::sync::Arc::new(Schema::empty())))
    }

    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> Result<impl RecordBatchReader> {
        // Stub implementation - will be completed in work item 1.7
        Ok(EmptyBatchReader::new(std::sync::Arc::new(Schema::empty())))
    }

    fn get_statistic_names(&self) -> Result<impl RecordBatchReader> {
        // Stub implementation - will be completed in work item 1.7
        Ok(EmptyBatchReader::new(std::sync::Arc::new(Schema::empty())))
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
