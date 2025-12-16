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

//! SEA (Statement Execution API) client for Databricks.
//!
//! This module provides the HTTP client for communicating with the
//! Databricks SQL Statement Execution API.

pub mod error;
pub mod models;

use std::time::Duration;

use reqwest::Client;

use crate::error::Result;
use crate::options::HttpConfig;

pub use error::ApiError;
pub use models::*;

/// SEA REST API client for Databricks.
///
/// Handles all HTTP communication with the Databricks SQL Statement
/// Execution API.
#[derive(Debug, Clone)]
pub struct SeaClient {
    /// HTTP client.
    http_client: Client,
    /// Workspace host URL.
    host: String,
    /// Personal Access Token.
    token: String,
    /// SQL Warehouse ID.
    warehouse_id: String,
}

impl SeaClient {
    /// Create a new SEA client.
    pub fn new(
        host: impl Into<String>,
        token: impl Into<String>,
        warehouse_id: impl Into<String>,
        http_config: &HttpConfig,
    ) -> Result<Self> {
        let http_client = Client::builder()
            .connect_timeout(http_config.connect_timeout)
            .timeout(http_config.read_timeout)
            .build()
            .map_err(|e| crate::error::Error::Http(e))?;

        Ok(Self {
            http_client,
            host: host.into(),
            token: token.into(),
            warehouse_id: warehouse_id.into(),
        })
    }

    /// Get the base URL for the SEA API.
    fn base_url(&self) -> String {
        format!("{}/api/2.0/sql", self.host)
    }

    /// Execute a SQL statement.
    pub async fn execute_statement(
        &self,
        _request: &ExecuteStatementRequest,
    ) -> Result<StatementResponse> {
        // TODO: Implement execute statement API call
        // POST /api/2.0/sql/statements/
        unimplemented!("execute_statement not yet implemented")
    }

    /// Get the status and result of a statement.
    pub async fn get_statement(&self, _statement_id: &str) -> Result<StatementResponse> {
        // TODO: Implement get statement API call
        // GET /api/2.0/sql/statements/{statement_id}
        unimplemented!("get_statement not yet implemented")
    }

    /// Get a result chunk.
    pub async fn get_chunk(&self, _statement_id: &str, _chunk_index: usize) -> Result<ChunkResponse> {
        // TODO: Implement get chunk API call
        // GET /api/2.0/sql/statements/{statement_id}/result/chunks/{chunk_index}
        unimplemented!("get_chunk not yet implemented")
    }

    /// Cancel a statement.
    pub async fn cancel_statement(&self, _statement_id: &str) -> Result<()> {
        // TODO: Implement cancel statement API call
        // POST /api/2.0/sql/statements/{statement_id}/cancel
        unimplemented!("cancel_statement not yet implemented")
    }

    /// Close a statement.
    pub async fn close_statement(&self, _statement_id: &str) -> Result<()> {
        // TODO: Implement close statement API call
        // DELETE /api/2.0/sql/statements/{statement_id}
        unimplemented!("close_statement not yet implemented")
    }

    /// Create a new session.
    pub async fn create_session(
        &self,
        _request: &CreateSessionRequest,
    ) -> Result<SessionResponse> {
        // TODO: Implement create session API call
        // POST /api/2.0/sql/sessions/
        unimplemented!("create_session not yet implemented")
    }

    /// Delete a session.
    pub async fn delete_session(&self, _session_id: &str) -> Result<()> {
        // TODO: Implement delete session API call
        // DELETE /api/2.0/sql/sessions/{session_id}
        unimplemented!("delete_session not yet implemented")
    }

    /// Get the warehouse ID.
    pub fn warehouse_id(&self) -> &str {
        &self.warehouse_id
    }

    /// Get the host URL.
    pub fn host(&self) -> &str {
        &self.host
    }
}
