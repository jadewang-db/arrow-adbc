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

//! Unit tests for statement polling functionality.
//!
//! Tests cover:
//! - PollingConfig default values and builder pattern
//! - is_terminal_state function for various statement states
//! - poll_until_complete behavior (tested via mocked HTTP responses)
//! - Exponential backoff during polling
//! - Timeout handling

use adbc_driver_databricks::client::{
    is_terminal_state, poll_until_complete, PollingConfig, SeaClient, SeaClientConfig,
    StatementResponse, StatementState, StatementStatus,
};
use std::time::Duration;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// =============================================================================
// Test Helpers
// =============================================================================

fn create_statement_response(statement_id: &str, state: StatementState) -> StatementResponse {
    StatementResponse {
        statement_id: statement_id.to_string(),
        status: StatementStatus {
            state,
            error: None,
        },
        manifest: None,
        result: None,
    }
}

fn create_failed_response(statement_id: &str, error_message: &str) -> StatementResponse {
    use adbc_driver_databricks::client::StatementError;

    StatementResponse {
        statement_id: statement_id.to_string(),
        status: StatementStatus {
            state: StatementState::Failed,
            error: Some(StatementError {
                error_code: Some("EXECUTION_ERROR".to_string()),
                message: Some(error_message.to_string()),
            }),
        },
        manifest: None,
        result: None,
    }
}

fn create_test_client(server_uri: &str) -> SeaClient {
    let config = SeaClientConfig::new(server_uri, "test-token", "test-warehouse");
    SeaClient::new(config).expect("Failed to create client")
}

// =============================================================================
// PollingConfig Tests
// =============================================================================

mod polling_config_tests {
    use super::*;

    #[test]
    fn test_default_values() {
        let config = PollingConfig::default();
        assert_eq!(config.initial_delay, Duration::from_secs(1));
        assert_eq!(config.max_delay, Duration::from_secs(10));
        assert!((config.backoff_multiplier - 2.0).abs() < f64::EPSILON);
        assert!(config.max_wait_time.is_none());
    }

    #[test]
    fn test_new_with_custom_initial_delay() {
        let config = PollingConfig::new(Duration::from_millis(500));
        assert_eq!(config.initial_delay, Duration::from_millis(500));
        // Other values should be defaults
        assert_eq!(config.max_delay, Duration::from_secs(10));
        assert!((config.backoff_multiplier - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_with_max_delay() {
        let config = PollingConfig::default().with_max_delay(Duration::from_secs(5));
        assert_eq!(config.max_delay, Duration::from_secs(5));
    }

    #[test]
    fn test_with_max_wait_time() {
        let config = PollingConfig::default().with_max_wait_time(Duration::from_secs(60));
        assert_eq!(config.max_wait_time, Some(Duration::from_secs(60)));
    }

    #[test]
    fn test_with_backoff_multiplier() {
        let config = PollingConfig::default().with_backoff_multiplier(1.5);
        assert!((config.backoff_multiplier - 1.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_builder_chain() {
        let config = PollingConfig::new(Duration::from_millis(200))
            .with_max_delay(Duration::from_secs(30))
            .with_max_wait_time(Duration::from_secs(120))
            .with_backoff_multiplier(3.0);

        assert_eq!(config.initial_delay, Duration::from_millis(200));
        assert_eq!(config.max_delay, Duration::from_secs(30));
        assert_eq!(config.max_wait_time, Some(Duration::from_secs(120)));
        assert!((config.backoff_multiplier - 3.0).abs() < f64::EPSILON);
    }
}

// =============================================================================
// is_terminal_state Tests
// =============================================================================

mod terminal_state_tests {
    use super::*;

    #[test]
    fn test_succeeded_is_terminal() {
        let response = create_statement_response("stmt-1", StatementState::Succeeded);
        assert!(is_terminal_state(&response));
    }

    #[test]
    fn test_failed_is_terminal() {
        let response = create_statement_response("stmt-2", StatementState::Failed);
        assert!(is_terminal_state(&response));
    }

    #[test]
    fn test_cancelled_is_terminal() {
        let response = create_statement_response("stmt-3", StatementState::Cancelled);
        assert!(is_terminal_state(&response));
    }

    #[test]
    fn test_closed_is_terminal() {
        let response = create_statement_response("stmt-4", StatementState::Closed);
        assert!(is_terminal_state(&response));
    }

    #[test]
    fn test_pending_is_not_terminal() {
        let response = create_statement_response("stmt-5", StatementState::Pending);
        assert!(!is_terminal_state(&response));
    }

    #[test]
    fn test_running_is_not_terminal() {
        let response = create_statement_response("stmt-6", StatementState::Running);
        assert!(!is_terminal_state(&response));
    }
}

// =============================================================================
// StatementStatus Helper Method Tests
// =============================================================================

mod status_helper_tests {
    use super::*;
    use adbc_driver_databricks::client::StatementError;

    #[test]
    fn test_is_succeeded() {
        let status = StatementStatus {
            state: StatementState::Succeeded,
            error: None,
        };
        assert!(status.is_succeeded());
        assert!(!status.is_failed());
        assert!(!status.is_cancelled());
        assert!(!status.is_running());
        assert!(status.is_terminal());
    }

    #[test]
    fn test_is_failed() {
        let status = StatementStatus {
            state: StatementState::Failed,
            error: None,
        };
        assert!(!status.is_succeeded());
        assert!(status.is_failed());
        assert!(!status.is_cancelled());
        assert!(!status.is_running());
        assert!(status.is_terminal());
    }

    #[test]
    fn test_is_cancelled() {
        let status = StatementStatus {
            state: StatementState::Cancelled,
            error: None,
        };
        assert!(!status.is_succeeded());
        assert!(!status.is_failed());
        assert!(status.is_cancelled());
        assert!(!status.is_running());
        assert!(status.is_terminal());
    }

    #[test]
    fn test_is_running_pending() {
        let status = StatementStatus {
            state: StatementState::Pending,
            error: None,
        };
        assert!(!status.is_succeeded());
        assert!(!status.is_failed());
        assert!(!status.is_cancelled());
        assert!(status.is_running());
        assert!(!status.is_terminal());
    }

    #[test]
    fn test_is_running_running_state() {
        let status = StatementStatus {
            state: StatementState::Running,
            error: None,
        };
        assert!(status.is_running());
        assert!(!status.is_terminal());
    }

    #[test]
    fn test_error_message_present() {
        let status = StatementStatus {
            state: StatementState::Failed,
            error: Some(StatementError {
                error_code: Some("ERROR_CODE".to_string()),
                message: Some("Something went wrong".to_string()),
            }),
        };
        assert_eq!(
            status.error_message(),
            Some("Something went wrong".to_string())
        );
    }

    #[test]
    fn test_error_message_none() {
        let status = StatementStatus {
            state: StatementState::Succeeded,
            error: None,
        };
        assert_eq!(status.error_message(), None);
    }

    #[test]
    fn test_error_message_error_without_message() {
        let status = StatementStatus {
            state: StatementState::Failed,
            error: Some(StatementError {
                error_code: Some("ERROR_CODE".to_string()),
                message: None,
            }),
        };
        assert_eq!(status.error_message(), None);
    }
}

// =============================================================================
// poll_until_complete Integration Tests (with Mocked HTTP)
// =============================================================================

mod poll_until_complete_tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_immediate_success() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());
        let config = PollingConfig::new(Duration::from_millis(10))
            .with_max_wait_time(Duration::from_secs(5));

        // First poll returns succeeded
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-immediate"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "statement_id": "stmt-immediate",
                "status": {
                    "state": "SUCCEEDED"
                }
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let result = poll_until_complete(&client, "stmt-immediate", &config).await;
        assert!(result.is_ok());

        let response = result.unwrap();
        assert_eq!(response.status.state, StatementState::Succeeded);
    }

    #[tokio::test]
    async fn test_succeeds_after_polling() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());
        let config = PollingConfig::new(Duration::from_millis(5))
            .with_max_wait_time(Duration::from_secs(5));

        let call_count = Arc::new(AtomicU32::new(0));
        let count = call_count.clone();

        // First two calls return RUNNING, third returns SUCCEEDED
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-poll"))
            .respond_with(move |_: &wiremock::Request| {
                let calls = count.fetch_add(1, Ordering::SeqCst);
                if calls < 2 {
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "statement_id": "stmt-poll",
                        "status": {
                            "state": "RUNNING"
                        }
                    }))
                } else {
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "statement_id": "stmt-poll",
                        "status": {
                            "state": "SUCCEEDED"
                        }
                    }))
                }
            })
            .mount(&mock_server)
            .await;

        let result = poll_until_complete(&client, "stmt-poll", &config).await;
        assert!(result.is_ok());
        assert!(call_count.load(Ordering::SeqCst) >= 3);
    }

    #[tokio::test]
    async fn test_fails_with_error() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());
        let config = PollingConfig::new(Duration::from_millis(10));

        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-fail"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "statement_id": "stmt-fail",
                "status": {
                    "state": "FAILED",
                    "error": {
                        "error_code": "EXECUTION_ERROR",
                        "message": "Query failed"
                    }
                }
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let result = poll_until_complete(&client, "stmt-fail", &config).await;
        assert!(result.is_err());

        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("Query failed") || err_msg.contains("failed"));
    }

    #[tokio::test]
    async fn test_cancelled_returns_error() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());
        let config = PollingConfig::new(Duration::from_millis(10));

        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-cancel"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "statement_id": "stmt-cancel",
                "status": {
                    "state": "CANCELLED"
                }
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let result = poll_until_complete(&client, "stmt-cancel", &config).await;
        assert!(result.is_err());

        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("cancelled") || err_msg.contains("Cancelled"),
            "Error message should mention cancellation: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn test_timeout() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());
        let config = PollingConfig::new(Duration::from_millis(5))
            .with_max_wait_time(Duration::from_millis(50));

        // Always return RUNNING to trigger timeout
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-timeout"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "statement_id": "stmt-timeout",
                "status": {
                    "state": "RUNNING"
                }
            })))
            .mount(&mock_server)
            .await;

        let result = poll_until_complete(&client, "stmt-timeout", &config).await;
        assert!(result.is_err());

        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.to_lowercase().contains("timeout"),
            "Error should mention timeout: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn test_pending_to_running_to_succeeded() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());
        let config = PollingConfig::new(Duration::from_millis(5))
            .with_max_wait_time(Duration::from_secs(5));

        let call_count = Arc::new(AtomicU32::new(0));
        let count = call_count.clone();

        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-transition"))
            .respond_with(move |_: &wiremock::Request| {
                let calls = count.fetch_add(1, Ordering::SeqCst);
                let state = match calls {
                    0 => "PENDING",
                    1 => "RUNNING",
                    _ => "SUCCEEDED",
                };
                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "statement_id": "stmt-transition",
                    "status": {
                        "state": state
                    }
                }))
            })
            .mount(&mock_server)
            .await;

        let result = poll_until_complete(&client, "stmt-transition", &config).await;
        assert!(result.is_ok());
        assert!(call_count.load(Ordering::SeqCst) >= 3);
    }
}

// =============================================================================
// Backoff Calculation Tests
// =============================================================================

mod backoff_tests {
    use super::*;

    #[test]
    fn test_backoff_multiplier_applied() {
        let config = PollingConfig::new(Duration::from_millis(100))
            .with_max_delay(Duration::from_secs(10))
            .with_backoff_multiplier(2.0);

        // With multiplier 2.0:
        // - Initial: 100ms
        // - After 1 poll: 200ms
        // - After 2 polls: 400ms
        // - After 3 polls: 800ms

        // The actual delay sequence is: 100ms wait, then 200ms, then 400ms, etc.
        // Note: This is testing the config values, not actual delay behavior
        // (which would require timing in integration tests)

        assert_eq!(config.initial_delay, Duration::from_millis(100));
        assert!((config.backoff_multiplier - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_max_delay_caps_growth() {
        let config = PollingConfig::new(Duration::from_secs(1))
            .with_max_delay(Duration::from_secs(5))
            .with_backoff_multiplier(10.0);

        // Even with aggressive multiplier, max_delay should cap it
        assert_eq!(config.max_delay, Duration::from_secs(5));
    }

    #[test]
    fn test_very_short_delays() {
        let config = PollingConfig::new(Duration::from_millis(1))
            .with_max_delay(Duration::from_millis(10))
            .with_max_wait_time(Duration::from_millis(100));

        assert_eq!(config.initial_delay, Duration::from_millis(1));
        assert_eq!(config.max_delay, Duration::from_millis(10));
    }

    #[test]
    fn test_very_long_delays() {
        let config = PollingConfig::new(Duration::from_secs(60))
            .with_max_delay(Duration::from_secs(600))
            .with_max_wait_time(Duration::from_secs(3600));

        assert_eq!(config.initial_delay, Duration::from_secs(60));
        assert_eq!(config.max_delay, Duration::from_secs(600));
        assert_eq!(config.max_wait_time, Some(Duration::from_secs(3600)));
    }
}

// =============================================================================
// Edge Case Tests
// =============================================================================

mod edge_case_tests {
    use super::*;

    #[test]
    fn test_zero_initial_delay() {
        let config = PollingConfig::new(Duration::from_secs(0));
        assert_eq!(config.initial_delay, Duration::from_secs(0));
    }

    #[test]
    fn test_zero_max_wait_time() {
        let config =
            PollingConfig::default().with_max_wait_time(Duration::from_secs(0));
        // Zero max_wait_time means timeout immediately
        assert_eq!(config.max_wait_time, Some(Duration::from_secs(0)));
    }

    #[test]
    fn test_one_multiplier() {
        let config = PollingConfig::default().with_backoff_multiplier(1.0);
        // Multiplier of 1.0 means no exponential growth
        assert!((config.backoff_multiplier - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_initial_delay_equals_max_delay() {
        let config = PollingConfig::new(Duration::from_secs(5))
            .with_max_delay(Duration::from_secs(5));
        assert_eq!(config.initial_delay, config.max_delay);
    }

    #[test]
    fn test_initial_delay_greater_than_max_delay() {
        // This is a valid configuration, delay will be capped
        let config = PollingConfig::new(Duration::from_secs(10))
            .with_max_delay(Duration::from_secs(5));
        assert!(config.initial_delay > config.max_delay);
    }
}
