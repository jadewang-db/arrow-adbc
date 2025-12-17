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

//! Tests for retry logic and exponential backoff

use adbc_driver_databricks::client::{RetryConfig, SeaClient, SeaClientConfig};
use adbc_driver_databricks::error::{Error, Result};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use wiremock::{Mock, MockServer, ResponseTemplate};
use wiremock::matchers::{method, path};

#[test]
fn test_retry_config_defaults() {
    let config = RetryConfig::default();
    assert_eq!(config.max_retries, 3);
    assert_eq!(config.base_delay, Duration::from_secs(1));
    assert_eq!(config.max_delay, Duration::from_secs(30));
    assert_eq!(config.jitter, 0.5);
}

#[test]
fn test_exponential_backoff() {
    let config = RetryConfig {
        base_delay: Duration::from_secs(1),
        max_delay: Duration::from_secs(30),
        jitter: 0.0, // Disable jitter for deterministic test
        ..Default::default()
    };

    assert_eq!(config.delay_for_attempt(0), Duration::from_secs(1));
    assert_eq!(config.delay_for_attempt(1), Duration::from_secs(2));
    assert_eq!(config.delay_for_attempt(2), Duration::from_secs(4));
    assert_eq!(config.delay_for_attempt(3), Duration::from_secs(8));
    assert_eq!(config.delay_for_attempt(4), Duration::from_secs(16));
}

#[test]
fn test_max_delay_cap() {
    let config = RetryConfig {
        base_delay: Duration::from_secs(1),
        max_delay: Duration::from_secs(30),
        jitter: 0.0,
        ..Default::default()
    };

    // 2^5 = 32 seconds, should be capped at 30
    assert_eq!(config.delay_for_attempt(5), Duration::from_secs(30));
    // 2^6 = 64 seconds, should also be capped at 30
    assert_eq!(config.delay_for_attempt(6), Duration::from_secs(30));
    // 2^10 = 1024 seconds, should be capped at 30
    assert_eq!(config.delay_for_attempt(10), Duration::from_secs(30));
}

#[test]
fn test_jitter_range() {
    let config = RetryConfig {
        base_delay: Duration::from_secs(1),
        max_delay: Duration::from_secs(30),
        jitter: 0.5,
        ..Default::default()
    };

    // Run multiple times to verify jitter is within bounds
    for _ in 0..100 {
        let delay = config.delay_for_attempt(0);
        // With base 1000ms and jitter 0.5:
        // delay = 1000 * (1 + random(0, 0.5))
        // minimum: 1000ms, maximum: 1500ms
        assert!(delay >= Duration::from_millis(1000));
        assert!(delay <= Duration::from_millis(1500));
    }

    // Test with attempt 1 (2s base)
    for _ in 0..100 {
        let delay = config.delay_for_attempt(1);
        // With base 2000ms and jitter 0.5:
        // minimum: 2000ms, maximum: 3000ms
        assert!(delay >= Duration::from_millis(2000));
        assert!(delay <= Duration::from_millis(3000));
    }
}

#[test]
fn test_jitter_with_max_delay() {
    let config = RetryConfig {
        base_delay: Duration::from_secs(1),
        max_delay: Duration::from_secs(30),
        jitter: 0.5,
        ..Default::default()
    };

    // Test with large attempt that would exceed max_delay
    for _ in 0..100 {
        let delay = config.delay_for_attempt(10);
        // With max 30000ms and jitter 0.5:
        // minimum: 30000ms, maximum: 45000ms
        assert!(delay >= Duration::from_millis(30000));
        assert!(delay <= Duration::from_millis(45000));
    }
}

#[tokio::test]
async fn test_retry_after_header() {
    let mock_server = MockServer::start().await;

    // First request returns 429 with Retry-After header
    Mock::given(method("GET"))
        .and(path("/api/2.0/sql/test"))
        .respond_with(
            ResponseTemplate::new(429)
                .insert_header("retry-after", "5")
                .set_body_json(serde_json::json!({
                    "error_code": "REQUEST_LIMIT_EXCEEDED",
                    "message": "Rate limit exceeded"
                }))
        )
        .mount(&mock_server)
        .await;

    let config = SeaClientConfig {
        host: mock_server.uri(),
        token: "test-token".to_string(),
        warehouse_id: "test-warehouse".to_string(),
        ..Default::default()
    };

    let client = SeaClient::new(config).unwrap();
    let result: Result<serde_json::Value> = client
        .get(&format!("{}/api/2.0/sql/test", mock_server.uri()))
        .await;

    assert!(result.is_err());
    match result.unwrap_err() {
        Error::SeaApi { code, http_status, retry_after, .. } => {
            assert_eq!(code, "REQUEST_LIMIT_EXCEEDED");
            assert_eq!(http_status, 429);
            assert_eq!(retry_after, Some(Duration::from_secs(5)));
        }
        _ => panic!("Expected SeaApi error"),
    }
}

#[tokio::test]
async fn test_non_retryable_fails_fast() {
    let mock_server = MockServer::start().await;

    // Track number of requests
    let request_count = Arc::new(Mutex::new(0));
    let request_count_clone = request_count.clone();

    Mock::given(method("GET"))
        .and(path("/api/2.0/sql/test"))
        .respond_with(move |_req: &wiremock::Request| {
            *request_count_clone.lock().unwrap() += 1;
            ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error_code": "INVALID_PARAMETER_VALUE",
                "message": "Invalid parameter"
            }))
        })
        .mount(&mock_server)
        .await;

    let config = SeaClientConfig {
        host: mock_server.uri(),
        token: "test-token".to_string(),
        warehouse_id: "test-warehouse".to_string(),
        ..Default::default()
    };

    let client = SeaClient::new(config).unwrap();
    let retry_config = RetryConfig::default();

    let result: Result<serde_json::Value> = client
        .with_retry(&retry_config, || async {
            client.get(&format!("{}/api/2.0/sql/test", mock_server.uri())).await
        })
        .await;

    assert!(result.is_err());
    match result.unwrap_err() {
        Error::SeaApi { code, http_status, .. } => {
            assert_eq!(code, "INVALID_PARAMETER_VALUE");
            assert_eq!(http_status, 400);
        }
        _ => panic!("Expected SeaApi error"),
    }

    // Should have made exactly 1 request (no retries for 400)
    assert_eq!(*request_count.lock().unwrap(), 1);
}

#[tokio::test]
async fn test_retryable_error_succeeds_after_retries() {
    let mock_server = MockServer::start().await;

    // Track number of requests
    let request_count = Arc::new(Mutex::new(0));
    let request_count_clone = request_count.clone();

    Mock::given(method("GET"))
        .and(path("/api/2.0/sql/test"))
        .respond_with(move |_req: &wiremock::Request| {
            let mut count = request_count_clone.lock().unwrap();
            *count += 1;

            // Fail first 2 requests with 503, succeed on 3rd
            if *count < 3 {
                ResponseTemplate::new(503).set_body_json(serde_json::json!({
                    "error_code": "TEMPORARILY_UNAVAILABLE",
                    "message": "Service temporarily unavailable"
                }))
            } else {
                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "status": "ok"
                }))
            }
        })
        .mount(&mock_server)
        .await;

    let config = SeaClientConfig {
        host: mock_server.uri(),
        token: "test-token".to_string(),
        warehouse_id: "test-warehouse".to_string(),
        ..Default::default()
    };

    let client = SeaClient::new(config).unwrap();
    let retry_config = RetryConfig {
        max_retries: 3,
        base_delay: Duration::from_millis(10), // Short delay for testing
        jitter: 0.0,
        ..Default::default()
    };

    let result: Result<serde_json::Value> = client
        .with_retry(&retry_config, || async {
            client.get(&format!("{}/api/2.0/sql/test", mock_server.uri())).await
        })
        .await;

    assert!(result.is_ok());
    let response = result.unwrap();
    assert_eq!(response["status"], "ok");

    // Should have made 3 requests total
    assert_eq!(*request_count.lock().unwrap(), 3);
}

#[tokio::test]
async fn test_retryable_error_exhausts_retries() {
    let mock_server = MockServer::start().await;

    // Track number of requests
    let request_count = Arc::new(Mutex::new(0));
    let request_count_clone = request_count.clone();

    Mock::given(method("GET"))
        .and(path("/api/2.0/sql/test"))
        .respond_with(move |_req: &wiremock::Request| {
            *request_count_clone.lock().unwrap() += 1;
            ResponseTemplate::new(500).set_body_json(serde_json::json!({
                "error_code": "INTERNAL_ERROR",
                "message": "Internal server error"
            }))
        })
        .mount(&mock_server)
        .await;

    let config = SeaClientConfig {
        host: mock_server.uri(),
        token: "test-token".to_string(),
        warehouse_id: "test-warehouse".to_string(),
        ..Default::default()
    };

    let client = SeaClient::new(config).unwrap();
    let retry_config = RetryConfig {
        max_retries: 3,
        base_delay: Duration::from_millis(10),
        jitter: 0.0,
        ..Default::default()
    };

    let result: Result<serde_json::Value> = client
        .with_retry(&retry_config, || async {
            client.get(&format!("{}/api/2.0/sql/test", mock_server.uri())).await
        })
        .await;

    assert!(result.is_err());
    match result.unwrap_err() {
        Error::SeaApi { code, http_status, .. } => {
            assert_eq!(code, "INTERNAL_ERROR");
            assert_eq!(http_status, 500);
        }
        _ => panic!("Expected SeaApi error"),
    }

    // Should have made 4 requests total (1 initial + 3 retries)
    assert_eq!(*request_count.lock().unwrap(), 4);
}

#[tokio::test]
async fn test_retry_with_retry_after_header() {
    let mock_server = MockServer::start().await;

    let request_count = Arc::new(Mutex::new(0));
    let request_count_clone = request_count.clone();
    let start_time = std::time::Instant::now();

    Mock::given(method("GET"))
        .and(path("/api/2.0/sql/test"))
        .respond_with(move |_req: &wiremock::Request| {
            let mut count = request_count_clone.lock().unwrap();
            *count += 1;

            // Fail first request with 429 and Retry-After, succeed on 2nd
            if *count == 1 {
                ResponseTemplate::new(429)
                    .insert_header("retry-after", "1")
                    .set_body_json(serde_json::json!({
                        "error_code": "REQUEST_LIMIT_EXCEEDED",
                        "message": "Rate limit exceeded"
                    }))
            } else {
                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "status": "ok"
                }))
            }
        })
        .mount(&mock_server)
        .await;

    let config = SeaClientConfig {
        host: mock_server.uri(),
        token: "test-token".to_string(),
        warehouse_id: "test-warehouse".to_string(),
        ..Default::default()
    };

    let client = SeaClient::new(config).unwrap();
    let retry_config = RetryConfig {
        max_retries: 3,
        base_delay: Duration::from_millis(100), // Would use 100ms without Retry-After
        jitter: 0.0,
        ..Default::default()
    };

    let result: Result<serde_json::Value> = client
        .with_retry(&retry_config, || async {
            client.get(&format!("{}/api/2.0/sql/test", mock_server.uri())).await
        })
        .await;

    let elapsed = start_time.elapsed();

    assert!(result.is_ok());
    let response = result.unwrap();
    assert_eq!(response["status"], "ok");

    // Should have made 2 requests
    assert_eq!(*request_count.lock().unwrap(), 2);

    // Should have waited at least 1 second (from Retry-After header)
    assert!(elapsed >= Duration::from_secs(1));
}

#[test]
fn test_error_is_retryable() {
    // Test 429 is retryable
    let error = Error::SeaApi {
        code: "REQUEST_LIMIT_EXCEEDED".to_string(),
        message: "Rate limit".to_string(),
        http_status: 429,
        retry_after: None,
    };
    assert!(error.is_retryable());

    // Test 500 is retryable
    let error = Error::SeaApi {
        code: "INTERNAL_ERROR".to_string(),
        message: "Internal error".to_string(),
        http_status: 500,
        retry_after: None,
    };
    assert!(error.is_retryable());

    // Test 503 is retryable
    let error = Error::SeaApi {
        code: "TEMPORARILY_UNAVAILABLE".to_string(),
        message: "Unavailable".to_string(),
        http_status: 503,
        retry_after: None,
    };
    assert!(error.is_retryable());

    // Test 400 is not retryable
    let error = Error::SeaApi {
        code: "INVALID_PARAMETER_VALUE".to_string(),
        message: "Invalid parameter".to_string(),
        http_status: 400,
        retry_after: None,
    };
    assert!(!error.is_retryable());

    // Test 404 is not retryable
    let error = Error::SeaApi {
        code: "NOT_FOUND".to_string(),
        message: "Not found".to_string(),
        http_status: 404,
        retry_after: None,
    };
    assert!(!error.is_retryable());
}
