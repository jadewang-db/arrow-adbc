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

//! Unit tests for HTTP client with mocked responses using wiremock.
//!
//! Tests cover:
//! - SeaClientConfig creation and validation
//! - Session management (create/delete)
//! - Statement execution and status polling
//! - Chunk retrieval
//! - Error handling for various HTTP status codes
//! - Retry behavior for transient errors

use adbc_driver_databricks::client::{
    Compression, Disposition, ExecuteStatementRequest, Format, RetryConfig, SeaClient,
    SeaClientConfig, StatementState,
};
use adbc_driver_databricks::Error;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// =============================================================================
// Test Helpers
// =============================================================================

fn create_test_config(server_uri: &str) -> SeaClientConfig {
    SeaClientConfig::new(server_uri, "test-token", "test-warehouse-id")
}

fn create_execute_request(sql: &str, warehouse_id: &str) -> ExecuteStatementRequest {
    ExecuteStatementRequest {
        statement: sql.to_string(),
        warehouse_id: warehouse_id.to_string(),
        session_id: None,
        catalog: None,
        schema: None,
        disposition: Some(Disposition::default()),
        format: Some(Format::default()),
        compression: Some(Compression::default()),
        wait_timeout: Some("10s".to_string()),
        row_limit: None,
        byte_limit: None,
    }
}

fn create_test_client(server_uri: &str) -> SeaClient {
    let config = create_test_config(server_uri);
    SeaClient::new(config).expect("Failed to create client")
}

fn success_session_response() -> serde_json::Value {
    serde_json::json!({
        "session_id": "test-session-123"
    })
}

fn success_execute_response(statement_id: &str, state: &str) -> serde_json::Value {
    serde_json::json!({
        "statement_id": statement_id,
        "status": {
            "state": state
        }
    })
}

fn success_statement_response_with_inline_data(statement_id: &str) -> serde_json::Value {
    serde_json::json!({
        "statement_id": statement_id,
        "status": {
            "state": "SUCCEEDED"
        },
        "manifest": {
            "format": "ARROW_STREAM",
            "total_chunk_count": 1,
            "total_row_count": 0
        },
        "result": {
            "data_array": "",
            "row_count": 0
        }
    })
}

fn error_response(code: &str, message: &str) -> serde_json::Value {
    serde_json::json!({
        "error_code": code,
        "message": message
    })
}

// =============================================================================
// SeaClientConfig Tests
// =============================================================================

mod config_tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_config_new() {
        let config = SeaClientConfig::new(
            "https://test.databricks.com",
            "test-token",
            "warehouse-123",
        );
        assert_eq!(config.host, "https://test.databricks.com");
        assert_eq!(config.token, "test-token");
        assert_eq!(config.warehouse_id, "warehouse-123");
    }

    #[test]
    fn test_config_with_connect_timeout() {
        let config = SeaClientConfig::new(
            "https://test.databricks.com",
            "test-token",
            "warehouse-123",
        )
        .with_connect_timeout(Duration::from_secs(30));
        assert_eq!(config.connect_timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_config_with_read_timeout() {
        let config = SeaClientConfig::new(
            "https://test.databricks.com",
            "test-token",
            "warehouse-123",
        )
        .with_read_timeout(Duration::from_secs(120));
        assert_eq!(config.read_timeout, Duration::from_secs(120));
    }

    #[test]
    fn test_config_with_retry_config() {
        let retry_config = RetryConfig::new(
            5,
            Duration::from_millis(500),
            Duration::from_secs(60),
            0.3,
        );
        let config = SeaClientConfig::new(
            "https://test.databricks.com",
            "test-token",
            "warehouse-123",
        )
        .with_retry_config(retry_config);
        assert_eq!(config.retry_config.max_retries, 5);
    }

    #[test]
    fn test_config_builder_chain() {
        let config = SeaClientConfig::new(
            "https://test.databricks.com",
            "test-token",
            "warehouse-123",
        )
        .with_connect_timeout(Duration::from_secs(15))
        .with_read_timeout(Duration::from_secs(60))
        .with_retry_config(RetryConfig::no_retry());

        assert_eq!(config.connect_timeout, Duration::from_secs(15));
        assert_eq!(config.read_timeout, Duration::from_secs(60));
        assert_eq!(config.retry_config.max_retries, 0);
    }
}

// =============================================================================
// Session Management Tests
// =============================================================================

mod session_tests {
    use super::*;

    #[tokio::test]
    async fn test_create_session_success() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .and(header("Authorization", "Bearer test-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(success_session_response()))
            .expect(1)
            .mount(&mock_server)
            .await;

        // create_session returns the session_id as a String
        let result = client.create_session(None, None).await;
        assert!(result.is_ok());

        let session_id = result.unwrap();
        assert_eq!(session_id, "test-session-123");
    }

    #[tokio::test]
    async fn test_create_session_with_catalog_and_schema() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .and(header("Authorization", "Bearer test-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(success_session_response()))
            .expect(1)
            .mount(&mock_server)
            .await;

        // create_session with catalog and schema
        let result = client
            .create_session(Some("main".to_string()), Some("default".to_string()))
            .await;
        assert!(result.is_ok());

        let session_id = result.unwrap();
        assert_eq!(session_id, "test-session-123");
    }

    #[tokio::test]
    async fn test_create_session_unauthorized() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(
                ResponseTemplate::new(401)
                    .set_body_json(error_response("UNAUTHENTICATED", "Invalid token")),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let result = client.create_session(None, None).await;
        assert!(result.is_err());

        match result.unwrap_err() {
            Error::SeaApi { http_status, .. } => {
                assert_eq!(http_status, 401);
            }
            other => panic!("Unexpected error type: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_delete_session_success() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/session-123"))
            .and(header("Authorization", "Bearer test-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&mock_server)
            .await;

        let result = client.delete_session("session-123").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_delete_session_not_found() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/non-existent"))
            .respond_with(
                ResponseTemplate::new(404)
                    .set_body_json(error_response("NOT_FOUND", "Session not found")),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let result = client.delete_session("non-existent").await;
        assert!(result.is_err());
    }
}

// =============================================================================
// Statement Execution Tests
// =============================================================================

mod statement_execution_tests {
    use super::*;

    #[tokio::test]
    async fn test_execute_statement_immediate_success() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .and(header("Authorization", "Bearer test-token"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(success_statement_response_with_inline_data("stmt-123")),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let request = create_execute_request("SELECT 1", "test-warehouse-id");
        let result = client.execute_statement(request).await;
        assert!(result.is_ok());

        let response = result.unwrap();
        assert_eq!(response.statement_id, "stmt-123");
        assert_eq!(response.status.state, StatementState::Succeeded);
    }

    #[tokio::test]
    async fn test_execute_statement_pending() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(success_execute_response("stmt-456", "PENDING")),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let request = create_execute_request("SELECT * FROM large_table", "test-warehouse-id");
        let result = client.execute_statement(request).await;
        assert!(result.is_ok());

        let response = result.unwrap();
        assert_eq!(response.statement_id, "stmt-456");
        assert_eq!(response.status.state, StatementState::Pending);
    }

    #[tokio::test]
    async fn test_execute_statement_bad_request() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(
                ResponseTemplate::new(400)
                    .set_body_json(error_response("BAD_REQUEST", "Invalid SQL syntax")),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let request = create_execute_request("INVALID SQL", "test-warehouse-id");
        let result = client.execute_statement(request).await;
        assert!(result.is_err());

        match result.unwrap_err() {
            Error::SeaApi {
                http_status, code, ..
            } => {
                assert_eq!(http_status, 400);
                // The code comes from the API response error_code field
                assert!(code.contains("BAD_REQUEST"));
            }
            other => panic!("Unexpected error type: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_execute_statement_rate_limited() {
        let mock_server = MockServer::start().await;
        let config = SeaClientConfig::new(&mock_server.uri(), "test-token", "test-warehouse")
            .with_retry_config(RetryConfig::no_retry()); // Disable retry for this test
        let client = SeaClient::new(config).unwrap();

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(
                ResponseTemplate::new(429)
                    .set_body_json(error_response("REQUEST_LIMIT_EXCEEDED", "Rate limited"))
                    .insert_header("Retry-After", "30"),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let request = create_execute_request("SELECT 1", "test-warehouse");
        let result = client.execute_statement(request).await;
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert!(err.is_retryable());
    }
}

// =============================================================================
// Statement Status Tests
// =============================================================================

mod statement_status_tests {
    use super::*;

    #[tokio::test]
    async fn test_get_statement_succeeded() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-123"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(success_statement_response_with_inline_data("stmt-123")),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let result = client.get_statement("stmt-123").await;
        assert!(result.is_ok());

        let response = result.unwrap();
        assert_eq!(response.status.state, StatementState::Succeeded);
        assert!(response.status.is_succeeded());
    }

    #[tokio::test]
    async fn test_get_statement_running() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-456"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(success_execute_response("stmt-456", "RUNNING")),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let result = client.get_statement("stmt-456").await;
        assert!(result.is_ok());

        let response = result.unwrap();
        assert_eq!(response.status.state, StatementState::Running);
        assert!(response.status.is_running());
        assert!(!response.status.is_terminal());
    }

    #[tokio::test]
    async fn test_get_statement_failed() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-789"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "statement_id": "stmt-789",
                "status": {
                    "state": "FAILED",
                    "error": {
                        "error_code": "EXECUTION_ERROR",
                        "message": "Division by zero"
                    }
                }
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let result = client.get_statement("stmt-789").await;
        assert!(result.is_ok());

        let response = result.unwrap();
        assert_eq!(response.status.state, StatementState::Failed);
        assert!(response.status.is_failed());
        assert!(response.status.is_terminal());
        assert_eq!(
            response.status.error_message(),
            Some("Division by zero".to_string())
        );
    }

    #[tokio::test]
    async fn test_get_statement_cancelled() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-cancelled"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(success_execute_response("stmt-cancelled", "CANCELLED")),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let result = client.get_statement("stmt-cancelled").await;
        assert!(result.is_ok());

        let response = result.unwrap();
        assert_eq!(response.status.state, StatementState::Cancelled);
        assert!(response.status.is_cancelled());
        assert!(response.status.is_terminal());
    }

    #[tokio::test]
    async fn test_get_statement_not_found() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/non-existent"))
            .respond_with(
                ResponseTemplate::new(404)
                    .set_body_json(error_response("NOT_FOUND", "Statement not found")),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let result = client.get_statement("non-existent").await;
        assert!(result.is_err());

        match result.unwrap_err() {
            Error::SeaApi { http_status, .. } => {
                assert_eq!(http_status, 404);
            }
            other => panic!("Unexpected error type: {:?}", other),
        }
    }
}

// =============================================================================
// Statement Cancellation Tests
// =============================================================================

mod cancellation_tests {
    use super::*;

    #[tokio::test]
    async fn test_cancel_statement_success() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements/stmt-123/cancel"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&mock_server)
            .await;

        let result = client.cancel_statement("stmt-123").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_cancel_statement_already_completed() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        // Some APIs return success even if already completed
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements/stmt-completed/cancel"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&mock_server)
            .await;

        let result = client.cancel_statement("stmt-completed").await;
        assert!(result.is_ok());
    }
}

// =============================================================================
// Chunk Retrieval Tests
// =============================================================================

mod chunk_tests {
    use super::*;

    #[tokio::test]
    async fn test_get_chunk_success() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-123/result/chunks/0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "external_links": [{
                    "chunk_index": 0,
                    "row_offset": 0,
                    "row_count": 1000,
                    "byte_count": 50000,
                    "external_link": "https://storage.example.com/chunk/0",
                    "expiration": "2099-12-31T23:59:59Z"
                }]
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let result = client.get_chunk("stmt-123", 0).await;
        assert!(result.is_ok());

        let response = result.unwrap();
        assert_eq!(response.external_links.len(), 1);
        assert_eq!(response.external_links[0].chunk_index, 0);
        assert_eq!(response.external_links[0].row_count, 1000);
    }

    #[tokio::test]
    async fn test_get_chunk_not_found() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-123/result/chunks/999"))
            .respond_with(
                ResponseTemplate::new(404)
                    .set_body_json(error_response("NOT_FOUND", "Chunk not found")),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let result = client.get_chunk("stmt-123", 999).await;
        assert!(result.is_err());
    }
}

// =============================================================================
// Error Response Tests
// =============================================================================

mod error_response_tests {
    use super::*;

    #[tokio::test]
    async fn test_server_error_500() {
        let mock_server = MockServer::start().await;
        let config = SeaClientConfig::new(&mock_server.uri(), "test-token", "test-warehouse")
            .with_retry_config(RetryConfig::no_retry());
        let client = SeaClient::new(config).unwrap();

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(
                ResponseTemplate::new(500)
                    .set_body_json(error_response("INTERNAL_ERROR", "Internal server error")),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let request = create_execute_request("SELECT 1", "test-warehouse");
        let result = client.execute_statement(request).await;
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert!(err.is_retryable());

        match err {
            Error::SeaApi { http_status, .. } => {
                assert_eq!(http_status, 500);
            }
            other => panic!("Unexpected error type: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_service_unavailable_503() {
        let mock_server = MockServer::start().await;
        let config = SeaClientConfig::new(&mock_server.uri(), "test-token", "test-warehouse")
            .with_retry_config(RetryConfig::no_retry());
        let client = SeaClient::new(config).unwrap();

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(ResponseTemplate::new(503).set_body_json(error_response(
                "TEMPORARILY_UNAVAILABLE",
                "Service temporarily unavailable",
            )))
            .expect(1)
            .mount(&mock_server)
            .await;

        let request = create_execute_request("SELECT 1", "test-warehouse");
        let result = client.execute_statement(request).await;
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert!(err.is_retryable());
    }

    #[tokio::test]
    async fn test_permission_denied_403() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(
                ResponseTemplate::new(403)
                    .set_body_json(error_response("PERMISSION_DENIED", "Access denied")),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let request = create_execute_request("SELECT 1", "test-warehouse-id");
        let result = client.execute_statement(request).await;
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_empty_error_body() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(ResponseTemplate::new(400))
            .expect(1)
            .mount(&mock_server)
            .await;

        let request = create_execute_request("SELECT 1", "test-warehouse-id");
        let result = client.execute_statement(request).await;
        assert!(result.is_err());
    }
}

// =============================================================================
// Model Enum Tests
// =============================================================================

mod model_enum_tests {
    use super::*;

    #[test]
    fn test_disposition_default() {
        assert_eq!(Disposition::default(), Disposition::InlineOrExternalLinks);
    }

    #[test]
    fn test_format_default() {
        assert_eq!(Format::default(), Format::ArrowStream);
    }

    #[test]
    fn test_compression_default() {
        assert_eq!(Compression::default(), Compression::Lz4Frame);
    }

    #[test]
    fn test_statement_state_values() {
        // Ensure all states can be pattern matched
        let states = [
            StatementState::Pending,
            StatementState::Running,
            StatementState::Succeeded,
            StatementState::Failed,
            StatementState::Cancelled,
            StatementState::Closed,
        ];

        for state in states {
            // Just verify they're valid variants
            let _ = format!("{:?}", state);
        }
    }
}

// =============================================================================
// Authorization Header Tests
// =============================================================================

mod auth_header_tests {
    use super::*;

    #[tokio::test]
    async fn test_bearer_token_sent_correctly() {
        let mock_server = MockServer::start().await;
        let config = SeaClientConfig::new(&mock_server.uri(), "my-secret-token", "warehouse-123");
        let client = SeaClient::new(config).unwrap();

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .and(header("Authorization", "Bearer my-secret-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(success_session_response()))
            .expect(1)
            .mount(&mock_server)
            .await;

        let result = client.create_session(None, None).await;
        assert!(result.is_ok());
    }
}
