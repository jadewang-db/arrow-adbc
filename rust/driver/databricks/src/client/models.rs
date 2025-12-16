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

//! Request and response models for the SEA API.
//!
//! This module defines the data structures used in communication with
//! the Databricks SQL Statement Execution API.

use serde::{Deserialize, Serialize};

/// Statement execution state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StatementState {
    /// Statement is pending execution.
    Pending,
    /// Statement is currently running.
    Running,
    /// Statement execution succeeded.
    Succeeded,
    /// Statement execution failed.
    Failed,
    /// Statement was cancelled.
    Canceled,
    /// Statement was closed.
    Closed,
}

/// Result disposition (how results are returned).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Disposition {
    /// Results are returned inline in the response.
    Inline,
    /// Results are returned via external links (presigned URLs).
    ExternalLinks,
}

/// Result format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResultFormat {
    /// Arrow IPC stream format.
    ArrowStream,
    /// JSON array format.
    JsonArray,
    /// CSV format.
    Csv,
}

/// Compression type for external links.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CompressionType {
    /// No compression.
    None,
    /// LZ4 frame compression.
    Lz4Frame,
}

/// Request to execute a SQL statement.
#[derive(Debug, Clone, Serialize)]
pub struct ExecuteStatementRequest {
    /// The SQL statement to execute.
    pub statement: String,
    /// SQL Warehouse ID.
    pub warehouse_id: String,
    /// Session ID (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// Catalog to use.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,
    /// Schema to use.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    /// Wait timeout for synchronous execution.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wait_timeout: Option<String>,
    /// Result disposition preference.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disposition: Option<String>,
    /// Result format preference.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    /// Row limit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_limit: Option<i64>,
    /// Byte limit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub byte_limit: Option<i64>,
}

/// Statement execution status.
#[derive(Debug, Clone, Deserialize)]
pub struct StatementStatus {
    /// Current state of the statement.
    pub state: StatementState,
    /// Error information if the statement failed.
    #[serde(default)]
    pub error: Option<StatementError>,
}

/// Statement error information.
#[derive(Debug, Clone, Deserialize)]
pub struct StatementError {
    /// Error code.
    pub error_code: Option<String>,
    /// Error message.
    pub message: Option<String>,
}

/// Result manifest for external links.
#[derive(Debug, Clone, Deserialize)]
pub struct ResultManifest {
    /// Result format.
    pub format: Option<String>,
    /// Result schema.
    pub schema: Option<ResultSchema>,
    /// Total chunk count.
    pub total_chunk_count: Option<i64>,
    /// Total row count.
    pub total_row_count: Option<i64>,
    /// Total byte count.
    pub total_byte_count: Option<i64>,
    /// Whether results are truncated.
    pub truncated: Option<bool>,
    /// Compression type for chunks.
    pub chunks: Option<Vec<ChunkInfo>>,
}

/// Result schema information.
#[derive(Debug, Clone, Deserialize)]
pub struct ResultSchema {
    /// Number of columns.
    pub column_count: Option<i64>,
    /// Column information.
    pub columns: Option<Vec<ColumnInfo>>,
}

/// Column information in the result schema.
#[derive(Debug, Clone, Deserialize)]
pub struct ColumnInfo {
    /// Column name.
    pub name: String,
    /// Column type string.
    pub type_text: Option<String>,
    /// Column type name.
    pub type_name: Option<String>,
    /// Column position.
    pub position: Option<i64>,
}

/// Chunk information in the manifest.
#[derive(Debug, Clone, Deserialize)]
pub struct ChunkInfo {
    /// Chunk index.
    pub chunk_index: i64,
    /// Row offset.
    pub row_offset: i64,
    /// Row count.
    pub row_count: i64,
    /// Byte count.
    pub byte_count: Option<i64>,
}

/// Result data (for inline results).
#[derive(Debug, Clone, Deserialize)]
pub struct ResultData {
    /// Inline data (for small results).
    pub data_array: Option<Vec<Vec<serde_json::Value>>>,
    /// External links (for large results).
    pub external_links: Option<Vec<ExternalLink>>,
    /// Row count.
    pub row_count: Option<i64>,
    /// Byte count.
    pub byte_count: Option<i64>,
}

/// External link to result data.
#[derive(Debug, Clone, Deserialize)]
pub struct ExternalLink {
    /// Chunk index.
    pub chunk_index: i64,
    /// Presigned URL to download the chunk.
    pub external_link: String,
    /// URL expiration timestamp.
    pub expiration: Option<String>,
    /// Row offset.
    pub row_offset: Option<i64>,
    /// Row count.
    pub row_count: Option<i64>,
    /// Byte count.
    pub byte_count: Option<i64>,
    /// HTTP headers for the request.
    pub http_headers: Option<std::collections::HashMap<String, String>>,
}

/// Response from statement execution or status check.
#[derive(Debug, Clone, Deserialize)]
pub struct StatementResponse {
    /// Statement ID.
    pub statement_id: String,
    /// Statement status.
    pub status: StatementStatus,
    /// Result manifest (for external links).
    pub manifest: Option<ResultManifest>,
    /// Result data.
    pub result: Option<ResultData>,
}

/// Response from get chunk request.
#[derive(Debug, Clone, Deserialize)]
pub struct ChunkResponse {
    /// External links for the chunk.
    pub external_links: Option<Vec<ExternalLink>>,
}

/// Request to create a session.
#[derive(Debug, Clone, Serialize)]
pub struct CreateSessionRequest {
    /// SQL Warehouse ID.
    pub warehouse_id: String,
    /// Session alias (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_alias: Option<String>,
    /// Catalog to use.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,
    /// Schema to use.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
}

/// Response from session creation.
#[derive(Debug, Clone, Deserialize)]
pub struct SessionResponse {
    /// Session ID.
    pub session_id: String,
}
