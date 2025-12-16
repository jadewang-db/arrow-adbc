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
    /// Wait timeout for synchronous execution (e.g., "10s").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wait_timeout: Option<String>,
    /// Behavior when wait timeout is reached: "CONTINUE" or "CANCEL".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_wait_timeout: Option<String>,
    /// Result disposition preference: "INLINE" or "EXTERNAL_LINKS".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disposition: Option<String>,
    /// Result format preference: "ARROW_STREAM", "JSON_ARRAY", or "CSV".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    /// Row limit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_limit: Option<i64>,
    /// Byte limit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub byte_limit: Option<i64>,
}

impl ExecuteStatementRequest {
    /// Create a new execute statement request with required fields and sensible defaults.
    ///
    /// The default settings are:
    /// - wait_timeout: "10s"
    /// - on_wait_timeout: "CONTINUE"
    /// - disposition: "EXTERNAL_LINKS" (supports all formats and large results)
    /// - format: "ARROW_STREAM"
    ///
    /// Note: EXTERNAL_LINKS disposition is preferred because:
    /// - It works with all formats (ARROW_STREAM, JSON_ARRAY, CSV)
    /// - It supports larger result sets (INLINE is limited to 25 MiB)
    /// - It offers better throughput via Cloud Fetch technology
    pub fn new(warehouse_id: impl Into<String>, statement: impl Into<String>) -> Self {
        Self {
            warehouse_id: warehouse_id.into(),
            statement: statement.into(),
            session_id: None,
            catalog: None,
            schema: None,
            wait_timeout: Some("10s".to_string()),
            on_wait_timeout: Some("CONTINUE".to_string()),
            disposition: Some("EXTERNAL_LINKS".to_string()),
            format: Some("ARROW_STREAM".to_string()),
            row_limit: None,
            byte_limit: None,
        }
    }

    /// Set the session ID.
    pub fn with_session_id(mut self, session_id: impl Into<String>) -> Self {
        self.session_id = Some(session_id.into());
        self
    }

    /// Set the catalog.
    pub fn with_catalog(mut self, catalog: impl Into<String>) -> Self {
        self.catalog = Some(catalog.into());
        self
    }

    /// Set the schema.
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Set the wait timeout (e.g., "10s", "30s").
    pub fn with_wait_timeout(mut self, timeout: impl Into<String>) -> Self {
        self.wait_timeout = Some(timeout.into());
        self
    }

    /// Set the on_wait_timeout behavior ("CONTINUE" or "CANCEL").
    pub fn with_on_wait_timeout(mut self, behavior: impl Into<String>) -> Self {
        self.on_wait_timeout = Some(behavior.into());
        self
    }

    /// Set the row limit.
    pub fn with_row_limit(mut self, limit: i64) -> Self {
        self.row_limit = Some(limit);
        self
    }

    /// Set the byte limit.
    pub fn with_byte_limit(mut self, limit: i64) -> Self {
        self.byte_limit = Some(limit);
        self
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // ExecuteStatementRequest Tests
    // ========================================================================

    #[test]
    fn test_execute_statement_request_new() {
        let request = ExecuteStatementRequest::new("wh123", "SELECT 1");

        assert_eq!(request.warehouse_id, "wh123");
        assert_eq!(request.statement, "SELECT 1");
        assert!(request.session_id.is_none());
        assert!(request.catalog.is_none());
        assert!(request.schema.is_none());
        assert_eq!(request.wait_timeout, Some("10s".to_string()));
        assert_eq!(request.on_wait_timeout, Some("CONTINUE".to_string()));
        assert_eq!(
            request.disposition,
            Some("EXTERNAL_LINKS".to_string())
        );
        assert_eq!(request.format, Some("ARROW_STREAM".to_string()));
        assert!(request.row_limit.is_none());
        assert!(request.byte_limit.is_none());
    }

    #[test]
    fn test_execute_statement_request_builder() {
        let request = ExecuteStatementRequest::new("wh123", "SELECT * FROM table")
            .with_session_id("session456")
            .with_catalog("main")
            .with_schema("default")
            .with_wait_timeout("30s")
            .with_on_wait_timeout("CANCEL")
            .with_row_limit(1000)
            .with_byte_limit(1_000_000);

        assert_eq!(request.session_id, Some("session456".to_string()));
        assert_eq!(request.catalog, Some("main".to_string()));
        assert_eq!(request.schema, Some("default".to_string()));
        assert_eq!(request.wait_timeout, Some("30s".to_string()));
        assert_eq!(request.on_wait_timeout, Some("CANCEL".to_string()));
        assert_eq!(request.row_limit, Some(1000));
        assert_eq!(request.byte_limit, Some(1_000_000));
    }

    #[test]
    fn test_execute_statement_request_serialization() {
        let request = ExecuteStatementRequest::new("abc123", "SELECT 1")
            .with_session_id("session456");

        let json = serde_json::to_string(&request).expect("Failed to serialize");

        // Verify required fields are present
        assert!(json.contains("\"statement\":\"SELECT 1\""));
        assert!(json.contains("\"warehouse_id\":\"abc123\""));
        assert!(json.contains("\"session_id\":\"session456\""));
        assert!(json.contains("\"wait_timeout\":\"10s\""));
        assert!(json.contains("\"on_wait_timeout\":\"CONTINUE\""));
        assert!(json.contains("\"disposition\":\"EXTERNAL_LINKS\""));
        assert!(json.contains("\"format\":\"ARROW_STREAM\""));

        // Verify optional None fields are skipped
        assert!(!json.contains("\"row_limit\""));
        assert!(!json.contains("\"byte_limit\""));
        assert!(!json.contains("\"catalog\""));
        assert!(!json.contains("\"schema\""));
    }

    #[test]
    fn test_execute_statement_request_serialization_with_all_fields() {
        let request = ExecuteStatementRequest::new("wh123", "SELECT 1")
            .with_session_id("session456")
            .with_catalog("main")
            .with_schema("default")
            .with_row_limit(100)
            .with_byte_limit(10000);

        let json = serde_json::to_string(&request).expect("Failed to serialize");

        // All fields should be present now
        assert!(json.contains("\"catalog\":\"main\""));
        assert!(json.contains("\"schema\":\"default\""));
        assert!(json.contains("\"row_limit\":100"));
        assert!(json.contains("\"byte_limit\":10000"));
    }

    // ========================================================================
    // StatementResponse Deserialization Tests
    // ========================================================================

    #[test]
    fn test_statement_response_deserialization_succeeded() {
        let json = r#"{
            "statement_id": "stmt-123",
            "status": {
                "state": "SUCCEEDED"
            }
        }"#;

        let response: StatementResponse =
            serde_json::from_str(json).expect("Failed to deserialize");

        assert_eq!(response.statement_id, "stmt-123");
        assert_eq!(response.status.state, StatementState::Succeeded);
        assert!(response.status.error.is_none());
        assert!(response.manifest.is_none());
        assert!(response.result.is_none());
    }

    #[test]
    fn test_statement_response_deserialization_pending() {
        let json = r#"{
            "statement_id": "stmt-456",
            "status": {
                "state": "PENDING"
            }
        }"#;

        let response: StatementResponse =
            serde_json::from_str(json).expect("Failed to deserialize");

        assert_eq!(response.statement_id, "stmt-456");
        assert_eq!(response.status.state, StatementState::Pending);
    }

    #[test]
    fn test_statement_response_deserialization_running() {
        let json = r#"{
            "statement_id": "stmt-789",
            "status": {
                "state": "RUNNING"
            }
        }"#;

        let response: StatementResponse =
            serde_json::from_str(json).expect("Failed to deserialize");

        assert_eq!(response.status.state, StatementState::Running);
    }

    #[test]
    fn test_statement_response_deserialization_failed() {
        let json = r#"{
            "statement_id": "stmt-fail",
            "status": {
                "state": "FAILED",
                "error": {
                    "error_code": "SYNTAX_ERROR",
                    "message": "Invalid SQL syntax"
                }
            }
        }"#;

        let response: StatementResponse =
            serde_json::from_str(json).expect("Failed to deserialize");

        assert_eq!(response.status.state, StatementState::Failed);
        assert!(response.status.error.is_some());

        let error = response.status.error.unwrap();
        assert_eq!(error.error_code, Some("SYNTAX_ERROR".to_string()));
        assert_eq!(error.message, Some("Invalid SQL syntax".to_string()));
    }

    #[test]
    fn test_statement_response_deserialization_canceled() {
        let json = r#"{
            "statement_id": "stmt-canceled",
            "status": {
                "state": "CANCELED"
            }
        }"#;

        let response: StatementResponse =
            serde_json::from_str(json).expect("Failed to deserialize");

        assert_eq!(response.status.state, StatementState::Canceled);
    }

    #[test]
    fn test_statement_response_deserialization_closed() {
        let json = r#"{
            "statement_id": "stmt-closed",
            "status": {
                "state": "CLOSED"
            }
        }"#;

        let response: StatementResponse =
            serde_json::from_str(json).expect("Failed to deserialize");

        assert_eq!(response.status.state, StatementState::Closed);
    }

    #[test]
    fn test_statement_response_with_manifest() {
        let json = r#"{
            "statement_id": "stmt-with-manifest",
            "status": {
                "state": "SUCCEEDED"
            },
            "manifest": {
                "format": "ARROW_STREAM",
                "schema": {
                    "column_count": 2,
                    "columns": [
                        {"name": "id", "type_name": "INT", "type_text": "INT", "position": 0},
                        {"name": "name", "type_name": "STRING", "type_text": "STRING", "position": 1}
                    ]
                },
                "total_chunk_count": 1,
                "total_row_count": 100,
                "total_byte_count": 5000,
                "truncated": false
            }
        }"#;

        let response: StatementResponse =
            serde_json::from_str(json).expect("Failed to deserialize");

        assert!(response.manifest.is_some());

        let manifest = response.manifest.unwrap();
        assert_eq!(manifest.format, Some("ARROW_STREAM".to_string()));
        assert_eq!(manifest.total_chunk_count, Some(1));
        assert_eq!(manifest.total_row_count, Some(100));
        assert_eq!(manifest.total_byte_count, Some(5000));
        assert_eq!(manifest.truncated, Some(false));

        let schema = manifest.schema.unwrap();
        assert_eq!(schema.column_count, Some(2));
        assert!(schema.columns.is_some());

        let columns = schema.columns.unwrap();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[0].type_name, Some("INT".to_string()));
        assert_eq!(columns[1].name, "name");
        assert_eq!(columns[1].type_name, Some("STRING".to_string()));
    }

    #[test]
    fn test_statement_response_with_inline_result() {
        let json = r#"{
            "statement_id": "stmt-inline",
            "status": {
                "state": "SUCCEEDED"
            },
            "result": {
                "data_array": [
                    [1, "Alice"],
                    [2, "Bob"]
                ],
                "row_count": 2,
                "byte_count": 100
            }
        }"#;

        let response: StatementResponse =
            serde_json::from_str(json).expect("Failed to deserialize");

        assert!(response.result.is_some());

        let result = response.result.unwrap();
        assert!(result.data_array.is_some());
        assert_eq!(result.row_count, Some(2));
        assert_eq!(result.byte_count, Some(100));

        let data_array = result.data_array.unwrap();
        assert_eq!(data_array.len(), 2);
        assert_eq!(data_array[0][0].as_i64(), Some(1));
        assert_eq!(data_array[0][1].as_str(), Some("Alice"));
        assert_eq!(data_array[1][0].as_i64(), Some(2));
        assert_eq!(data_array[1][1].as_str(), Some("Bob"));
    }

    #[test]
    fn test_statement_response_with_external_links() {
        let json = r#"{
            "statement_id": "stmt-external",
            "status": {
                "state": "SUCCEEDED"
            },
            "result": {
                "external_links": [
                    {
                        "chunk_index": 0,
                        "external_link": "https://storage.example.com/chunk0?token=abc",
                        "expiration": "2024-12-31T23:59:59Z",
                        "row_offset": 0,
                        "row_count": 50000,
                        "byte_count": 1000000
                    },
                    {
                        "chunk_index": 1,
                        "external_link": "https://storage.example.com/chunk1?token=def",
                        "expiration": "2024-12-31T23:59:59Z",
                        "row_offset": 50000,
                        "row_count": 50000,
                        "byte_count": 1000000
                    }
                ]
            }
        }"#;

        let response: StatementResponse =
            serde_json::from_str(json).expect("Failed to deserialize");

        assert!(response.result.is_some());

        let result = response.result.unwrap();
        assert!(result.external_links.is_some());
        assert!(result.data_array.is_none());

        let links = result.external_links.unwrap();
        assert_eq!(links.len(), 2);

        assert_eq!(links[0].chunk_index, 0);
        assert_eq!(
            links[0].external_link,
            "https://storage.example.com/chunk0?token=abc"
        );
        assert_eq!(links[0].row_offset, Some(0));
        assert_eq!(links[0].row_count, Some(50000));
        assert_eq!(links[0].byte_count, Some(1000000));

        assert_eq!(links[1].chunk_index, 1);
        assert_eq!(links[1].row_offset, Some(50000));
    }

    // ========================================================================
    // StatementState Tests
    // ========================================================================

    #[test]
    fn test_statement_state_enum_deserialization() {
        assert_eq!(
            serde_json::from_str::<StatementState>("\"PENDING\"").unwrap(),
            StatementState::Pending
        );
        assert_eq!(
            serde_json::from_str::<StatementState>("\"RUNNING\"").unwrap(),
            StatementState::Running
        );
        assert_eq!(
            serde_json::from_str::<StatementState>("\"SUCCEEDED\"").unwrap(),
            StatementState::Succeeded
        );
        assert_eq!(
            serde_json::from_str::<StatementState>("\"FAILED\"").unwrap(),
            StatementState::Failed
        );
        assert_eq!(
            serde_json::from_str::<StatementState>("\"CANCELED\"").unwrap(),
            StatementState::Canceled
        );
        assert_eq!(
            serde_json::from_str::<StatementState>("\"CLOSED\"").unwrap(),
            StatementState::Closed
        );
    }

    #[test]
    fn test_statement_state_serialization() {
        assert_eq!(
            serde_json::to_string(&StatementState::Pending).unwrap(),
            "\"PENDING\""
        );
        assert_eq!(
            serde_json::to_string(&StatementState::Running).unwrap(),
            "\"RUNNING\""
        );
        assert_eq!(
            serde_json::to_string(&StatementState::Succeeded).unwrap(),
            "\"SUCCEEDED\""
        );
        assert_eq!(
            serde_json::to_string(&StatementState::Failed).unwrap(),
            "\"FAILED\""
        );
        assert_eq!(
            serde_json::to_string(&StatementState::Canceled).unwrap(),
            "\"CANCELED\""
        );
        assert_eq!(
            serde_json::to_string(&StatementState::Closed).unwrap(),
            "\"CLOSED\""
        );
    }

    // ========================================================================
    // ChunkResponse Tests
    // ========================================================================

    #[test]
    fn test_chunk_response_deserialization() {
        let json = r#"{
            "external_links": [
                {
                    "chunk_index": 5,
                    "external_link": "https://storage.example.com/chunk5",
                    "expiration": "2024-12-31T23:59:59Z",
                    "row_offset": 250000,
                    "row_count": 50000,
                    "byte_count": 1000000
                }
            ]
        }"#;

        let response: ChunkResponse = serde_json::from_str(json).expect("Failed to deserialize");

        assert!(response.external_links.is_some());
        let links = response.external_links.unwrap();
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].chunk_index, 5);
    }

    // ========================================================================
    // CreateSessionRequest Tests
    // ========================================================================

    #[test]
    fn test_create_session_request_serialization() {
        let request = CreateSessionRequest {
            warehouse_id: "wh123".to_string(),
            session_alias: Some("test_session".to_string()),
            catalog: Some("main".to_string()),
            schema: Some("default".to_string()),
        };

        let json = serde_json::to_string(&request).expect("Failed to serialize");

        assert!(json.contains("\"warehouse_id\":\"wh123\""));
        assert!(json.contains("\"session_alias\":\"test_session\""));
        assert!(json.contains("\"catalog\":\"main\""));
        assert!(json.contains("\"schema\":\"default\""));
    }

    #[test]
    fn test_create_session_request_serialization_minimal() {
        let request = CreateSessionRequest {
            warehouse_id: "wh123".to_string(),
            session_alias: None,
            catalog: None,
            schema: None,
        };

        let json = serde_json::to_string(&request).expect("Failed to serialize");

        assert!(json.contains("\"warehouse_id\":\"wh123\""));
        assert!(!json.contains("session_alias"));
        assert!(!json.contains("catalog"));
        assert!(!json.contains("schema"));
    }

    // ========================================================================
    // SessionResponse Tests
    // ========================================================================

    #[test]
    fn test_session_response_deserialization() {
        let json = r#"{"session_id": "sess-12345"}"#;

        let response: SessionResponse = serde_json::from_str(json).expect("Failed to deserialize");

        assert_eq!(response.session_id, "sess-12345");
    }
}
