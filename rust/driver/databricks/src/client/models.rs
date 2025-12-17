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

//! Request and response models for the SEA API

use serde::{Deserialize, Serialize};

/// Request to create a new session
#[derive(Debug, Serialize)]
pub struct CreateSessionRequest {
    pub warehouse_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
}

/// Response from creating a session
#[derive(Debug, Deserialize)]
pub struct CreateSessionResponse {
    pub session_id: String,
}

/// Request to execute a SQL statement
#[derive(Debug, Serialize)]
pub struct ExecuteStatementRequest {
    pub statement: String,
    pub warehouse_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    pub wait_timeout: String,
    pub disposition: String,
    pub format: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_limit: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub byte_limit: Option<i64>,
}

impl Default for ExecuteStatementRequest {
    fn default() -> Self {
        Self {
            statement: String::new(),
            warehouse_id: String::new(),
            session_id: None,
            catalog: None,
            schema: None,
            wait_timeout: "10s".to_string(),
            disposition: "INLINE_OR_EXTERNAL_LINKS".to_string(),
            format: "ARROW_STREAM".to_string(),
            row_limit: None,
            byte_limit: None,
        }
    }
}

/// Response from executing a statement
#[derive(Debug, Deserialize)]
pub struct ExecuteStatementResponse {
    pub statement_id: String,
    pub status: StatementStatus,
    #[serde(default)]
    pub manifest: Option<ResultManifest>,
    #[serde(default)]
    pub result: Option<StatementResult>,
}

/// Statement execution status
#[derive(Debug, Deserialize)]
pub struct StatementStatus {
    pub state: StatementState,
    #[serde(default)]
    pub error: Option<StatementError>,
}

/// Statement execution state
#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StatementState {
    Pending,
    Running,
    Succeeded,
    Failed,
    Canceled,
    Closed,
}

/// Statement error information
#[derive(Debug, Deserialize)]
pub struct StatementError {
    pub error_code: Option<String>,
    pub message: Option<String>,
}

/// Result manifest containing schema and metadata
#[derive(Debug, Deserialize)]
pub struct ResultManifest {
    pub format: String,
    pub schema: ManifestSchema,
    pub total_chunk_count: i32,
    pub total_row_count: Option<i64>,
    pub total_byte_count: Option<i64>,
    pub truncated: Option<bool>,
}

/// Schema information from manifest
#[derive(Debug, Deserialize)]
pub struct ManifestSchema {
    pub columns: Vec<ColumnInfo>,
}

/// Column information
#[derive(Debug, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub type_name: String,
    pub type_text: String,
    pub position: i32,
    #[serde(default)]
    pub nullable: bool,
}

/// Statement result data
#[derive(Debug, Deserialize)]
pub struct StatementResult {
    #[serde(default)]
    pub data_array: Option<Vec<Vec<serde_json::Value>>>,
    #[serde(default)]
    pub arrow_batches: Option<Vec<ArrowBatch>>,
    #[serde(default)]
    pub chunk_index: Option<i32>,
    #[serde(default)]
    pub row_offset: Option<i64>,
    #[serde(default)]
    pub row_count: Option<i64>,
    #[serde(default)]
    pub external_links: Option<Vec<ExternalLink>>,
}

/// Arrow batch data for inline results
#[derive(Debug, Deserialize)]
pub struct ArrowBatch {
    /// Base64-encoded Arrow IPC stream data
    pub bytes: String,
    /// Number of rows in this batch
    pub row_count: i64,
    /// Start row offset
    pub start_row_offset: i64,
}

/// External link to cloud storage for large results
#[derive(Debug, Deserialize, Clone)]
pub struct ExternalLink {
    pub external_link: String,
    pub chunk_index: i32,
    pub row_offset: i64,
    pub row_count: i64,
    pub byte_count: i64,
    pub expiration: String,
}

/// Response from getting a chunk
#[derive(Debug, Deserialize)]
pub struct GetChunkResponse {
    pub external_links: Vec<ExternalLink>,
}
