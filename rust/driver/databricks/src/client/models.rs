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

//! Request and response types for the SEA API.
//!
//! These types model the JSON structures used by the Databricks
//! Statement Execution API.

use serde::{Deserialize, Serialize};

/// Statement execution state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StatementState {
    /// Statement is pending execution.
    Pending,
    /// Statement is currently running.
    Running,
    /// Statement succeeded.
    Succeeded,
    /// Statement failed.
    Failed,
    /// Statement was cancelled.
    Cancelled,
    /// Statement was closed.
    Closed,
}

/// Result disposition mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Disposition {
    /// Results are returned inline in the response.
    Inline,
    /// Results are stored externally and links are provided.
    /// The API will automatically use this for large results.
    ExternalLinks,
}

impl Default for Disposition {
    fn default() -> Self {
        // Use EXTERNAL_LINKS by default. The API will return inline results
        // if they're small enough, or external links if they're large.
        Self::ExternalLinks
    }
}

/// Result format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Format {
    /// Arrow stream format.
    ArrowStream,
    /// JSON array format (not supported).
    JsonArray,
    /// CSV format (not supported).
    Csv,
}

impl Default for Format {
    fn default() -> Self {
        Self::ArrowStream
    }
}

/// Compression format for results.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Compression {
    /// No compression.
    None,
    /// LZ4 frame compression.
    Lz4Frame,
}

impl Default for Compression {
    fn default() -> Self {
        Self::Lz4Frame
    }
}

/// Request to execute a statement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteStatementRequest {
    /// SQL statement to execute.
    pub statement: String,
    /// Warehouse ID.
    pub warehouse_id: String,
    /// Optional session ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// Optional catalog.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,
    /// Optional schema.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    /// Result disposition.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disposition: Option<Disposition>,
    /// Result format.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<Format>,
    /// Compression format.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression: Option<Compression>,
    /// Wait timeout (e.g., "10s").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wait_timeout: Option<String>,
    /// Row limit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_limit: Option<i64>,
    /// Byte limit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub byte_limit: Option<i64>,
}

/// Statement status information.
#[derive(Debug, Clone, Deserialize)]
pub struct StatementStatus {
    /// Current state.
    pub state: StatementState,
    /// Error information if failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<StatementError>,
}

impl StatementStatus {
    /// Check if the statement has succeeded.
    pub fn is_succeeded(&self) -> bool {
        self.state == StatementState::Succeeded
    }

    /// Check if the statement has failed.
    pub fn is_failed(&self) -> bool {
        self.state == StatementState::Failed
    }

    /// Check if the statement was cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.state == StatementState::Cancelled
    }

    /// Check if the statement is still running (pending or running).
    pub fn is_running(&self) -> bool {
        matches!(self.state, StatementState::Pending | StatementState::Running)
    }

    /// Check if the statement is in a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            StatementState::Succeeded
                | StatementState::Failed
                | StatementState::Cancelled
                | StatementState::Closed
        )
    }

    /// Get the error message if the statement failed.
    pub fn error_message(&self) -> Option<String> {
        self.error.as_ref().and_then(|e| e.message.clone())
    }
}

/// Statement error information.
#[derive(Debug, Clone, Deserialize)]
pub struct StatementError {
    /// Error code.
    pub error_code: Option<String>,
    /// Error message.
    pub message: Option<String>,
}

/// Result manifest for large results.
#[derive(Debug, Clone, Deserialize)]
pub struct ResultManifest {
    /// Result format (e.g., "ARROW_STREAM").
    pub format: Option<String>,
    /// Schema information.
    pub schema: Option<ManifestSchema>,
    /// Total number of chunks.
    pub total_chunk_count: i32,
    /// Total number of rows.
    pub total_row_count: Option<i64>,
    /// Total number of bytes.
    pub total_byte_count: Option<i64>,
    /// Whether the result is truncated.
    pub truncated: Option<bool>,
}

/// Schema information from the manifest.
#[derive(Debug, Clone, Deserialize)]
pub struct ManifestSchema {
    /// Column information.
    pub columns: Vec<ColumnInfo>,
}

/// Column information from the manifest schema.
#[derive(Debug, Clone, Deserialize)]
pub struct ColumnInfo {
    /// Column name.
    pub name: String,
    /// Spark SQL type name (e.g., "INT", "STRING").
    pub type_name: String,
    /// Full type text.
    pub type_text: String,
    /// Column position (0-indexed).
    pub position: i32,
    /// Whether the column is nullable.
    #[serde(default = "default_true")]
    pub nullable: bool,
}

fn default_true() -> bool {
    true
}

/// External link to result data.
#[derive(Debug, Clone, Deserialize)]
pub struct ExternalLink {
    /// Chunk index.
    pub chunk_index: i32,
    /// Row offset.
    pub row_offset: i64,
    /// Row count.
    pub row_count: i64,
    /// Byte count.
    pub byte_count: i64,
    /// Presigned URL.
    pub external_link: String,
    /// Link expiration time.
    pub expiration: String,
}

/// Result data.
#[derive(Debug, Clone, Deserialize)]
pub struct ResultData {
    /// External links for large results.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_links: Option<Vec<ExternalLink>>,
    /// Inline data (base64 encoded Arrow IPC).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_array: Option<String>,
    /// Row count.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_count: Option<i64>,
    /// Byte count.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub byte_count: Option<i64>,
}

/// Response from execute statement.
#[derive(Debug, Clone, Deserialize)]
pub struct StatementResponse {
    /// Statement ID.
    pub statement_id: String,
    /// Statement status.
    pub status: StatementStatus,
    /// Result manifest (for large results).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest: Option<ResultManifest>,
    /// Result data.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<ResultData>,
}

/// Request to create a session.
#[derive(Debug, Clone, Serialize)]
pub struct CreateSessionRequest {
    /// Warehouse ID.
    pub warehouse_id: String,
    /// Optional catalog.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,
    /// Optional schema.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
}

/// Response from create session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionResponse {
    /// Session ID.
    pub session_id: String,
}

/// Response from get chunk.
#[derive(Debug, Clone, Deserialize)]
pub struct ChunkResponse {
    /// External links.
    pub external_links: Vec<ExternalLink>,
}
