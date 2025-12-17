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

//! SEA (Statement Execution API) client implementation

pub mod error;
pub mod models;

use crate::error::Result;
use std::time::Duration;

/// HTTP client for communicating with the Databricks Statement Execution API
#[derive(Debug)]
pub struct SeaClient {
    http_client: reqwest::Client,
    host: String,
    token: String,
    warehouse_id: String,
}

/// Configuration for the SEA client
pub struct SeaClientConfig {
    pub host: String,
    pub token: String,
    pub warehouse_id: String,
    pub connect_timeout: std::time::Duration,
    pub read_timeout: std::time::Duration,
}

impl Default for SeaClientConfig {
    fn default() -> Self {
        Self {
            host: String::new(),
            token: String::new(),
            warehouse_id: String::new(),
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(300),
        }
    }
}

/// Configuration for retry behavior with exponential backoff
#[derive(Clone, Debug)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_retries: u32,
    /// Base delay for exponential backoff
    pub base_delay: Duration,
    /// Maximum delay between retries
    pub max_delay: Duration,
    /// Jitter factor (0.0 - 1.0) to prevent thundering herd
    pub jitter: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            jitter: 0.5,
        }
    }
}

impl RetryConfig {
    /// Calculate the delay for a given retry attempt (0-indexed)
    ///
    /// Uses exponential backoff: base_delay * 2^attempt
    /// Capped at max_delay
    /// Jitter applied: delay * (1 + random(0, jitter))
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let base_ms = self.base_delay.as_millis() as f64;
        let exponential = base_ms * 2_f64.powi(attempt as i32);
        let capped = exponential.min(self.max_delay.as_millis() as f64);

        // Add jitter: delay * (1 + random(0, jitter))
        let mut rng = rand::thread_rng();
        let jitter_factor = 1.0 + rand::Rng::gen::<f64>(&mut rng) * self.jitter;
        let final_ms = capped * jitter_factor;

        Duration::from_millis(final_ms as u64)
    }
}

impl SeaClient {
    /// Create a new SEA client with the given configuration
    pub fn new(config: SeaClientConfig) -> Result<Self> {
        let http_client = reqwest::Client::builder()
            .connect_timeout(config.connect_timeout)
            .timeout(config.read_timeout)
            .default_headers(Self::default_headers(&config.token))
            .build()?;

        Ok(Self {
            http_client,
            host: config.host,
            token: config.token,
            warehouse_id: config.warehouse_id,
        })
    }

    /// Create default HTTP headers for API requests
    fn default_headers(token: &str) -> reqwest::header::HeaderMap {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", token).parse().unwrap(),
        );
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            "application/json".parse().unwrap(),
        );
        headers.insert(
            reqwest::header::USER_AGENT,
            "adbc-driver-databricks/0.1.0".parse().unwrap(),
        );
        headers
    }

    /// Get the base URL for SEA API endpoints
    fn base_url(&self) -> String {
        format!("{}/api/2.0/sql", self.host.trim_end_matches('/'))
    }

    /// Get the URL for the statements endpoint
    fn statements_url(&self) -> String {
        format!("{}/statements", self.base_url())
    }

    /// Get the URL for a specific statement
    fn statement_url(&self, statement_id: &str) -> String {
        format!("{}/statements/{}", self.base_url(), statement_id)
    }

    /// Get the URL for the sessions endpoint
    fn sessions_url(&self) -> String {
        format!("{}/sessions", self.base_url())
    }

    /// Get the URL for a specific session
    fn session_url(&self, session_id: &str) -> String {
        format!("{}/sessions/{}", self.base_url(), session_id)
    }

    /// Get the warehouse ID for this client
    pub fn warehouse_id(&self) -> &str {
        &self.warehouse_id
    }

    /// Send a POST request with JSON body
    async fn post<Req, Resp>(&self, url: &str, body: &Req) -> Result<Resp>
    where
        Req: serde::Serialize,
        Resp: serde::de::DeserializeOwned,
    {
        let response = self.http_client
            .post(url)
            .json(body)
            .send()
            .await?;

        self.handle_response(response).await
    }

    /// Send a GET request
    #[cfg_attr(test, allow(dead_code))]
    pub async fn get<Resp>(&self, url: &str) -> Result<Resp>
    where
        Resp: serde::de::DeserializeOwned,
    {
        let response = self.http_client
            .get(url)
            .send()
            .await?;

        self.handle_response(response).await
    }

    /// Send a DELETE request
    async fn delete(&self, url: &str) -> Result<()> {
        let response = self.http_client
            .delete(url)
            .send()
            .await?;

        if response.status().is_success() {
            Ok(())
        } else {
            self.handle_error_response(response).await
        }
    }

    /// Handle a successful response by deserializing the JSON body
    async fn handle_response<Resp>(&self, response: reqwest::Response) -> Result<Resp>
    where
        Resp: serde::de::DeserializeOwned,
    {
        let status = response.status();
        if status.is_success() {
            Ok(response.json().await?)
        } else {
            self.handle_error_response(response).await
        }
    }

    /// Handle an error response by parsing the error details
    async fn handle_error_response<T>(&self, response: reqwest::Response) -> Result<T> {
        let http_status = response.status().as_u16();

        // Extract Retry-After header if present (in seconds)
        let retry_after = response
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_secs);

        let body: serde_json::Value = response.json().await.unwrap_or_default();

        Err(crate::error::Error::SeaApi {
            code: body["error_code"].as_str().unwrap_or("UNKNOWN").to_string(),
            message: body["message"].as_str().unwrap_or("Unknown error").to_string(),
            http_status,
            retry_after,
        })
    }

    /// Create a new session with the SQL Warehouse
    pub async fn create_session(
        &self,
        catalog: Option<String>,
        schema: Option<String>,
    ) -> Result<String> {
        let request = models::CreateSessionRequest {
            warehouse_id: self.warehouse_id.clone(),
            catalog,
            schema,
        };

        let response: models::CreateSessionResponse = self
            .post(&self.sessions_url(), &request)
            .await?;

        Ok(response.session_id)
    }

    /// Delete/terminate a session
    pub async fn delete_session(&self, session_id: &str) -> Result<()> {
        self.delete(&self.session_url(session_id)).await
    }

    /// Execute an operation with retry logic
    ///
    /// Retries operations that fail with retryable errors using exponential backoff.
    /// For 429 errors, respects the Retry-After header if present.
    pub async fn with_retry<F, Fut, T>(&self, retry_config: &RetryConfig, mut f: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let mut last_error = None;

        for attempt in 0..=retry_config.max_retries {
            match f().await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    // Check if we should retry this error
                    if !e.is_retryable() || attempt == retry_config.max_retries {
                        return Err(e);
                    }

                    // For 429 errors, use Retry-After header if present
                    let delay = if let crate::error::Error::SeaApi { http_status, retry_after, .. } = &e {
                        if *http_status == 429 {
                            retry_after.unwrap_or_else(|| retry_config.delay_for_attempt(attempt))
                        } else {
                            retry_config.delay_for_attempt(attempt)
                        }
                    } else {
                        retry_config.delay_for_attempt(attempt)
                    };

                    tokio::time::sleep(delay).await;
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap())
    }

    /// Execute a SQL statement
    ///
    /// Sends a request to the SEA API to execute a SQL statement.
    /// Returns the statement ID, status, and initial results if available.
    ///
    /// # Arguments
    /// * `request` - The statement execution request containing SQL and configuration
    ///
    /// # Returns
    /// * `ExecuteStatementResponse` - Response containing statement ID, status, and results
    pub async fn execute_statement(
        &self,
        request: models::ExecuteStatementRequest,
    ) -> Result<models::ExecuteStatementResponse> {
        self.post(&self.statements_url(), &request).await
    }

    /// Get statement status and results
    ///
    /// Retrieves the current status and results for a previously executed statement.
    /// This is used to poll for completion when execute_statement returns PENDING or RUNNING.
    ///
    /// # Arguments
    /// * `statement_id` - The statement ID returned from execute_statement
    ///
    /// # Returns
    /// * `ExecuteStatementResponse` - Current status and results (if complete)
    pub async fn get_statement(&self, statement_id: &str) -> Result<models::ExecuteStatementResponse> {
        self.get(&self.statement_url(statement_id)).await
    }
}

/// Configuration for statement polling
#[derive(Clone, Debug)]
pub struct PollConfig {
    /// Initial delay before first poll (default: 1 second)
    pub initial_delay: Duration,
    /// Maximum delay between polls (default: 10 seconds)
    pub max_delay: Duration,
    /// Total timeout for polling (default: 300 seconds / 5 minutes)
    pub timeout: Duration,
}

impl Default for PollConfig {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(10),
            timeout: Duration::from_secs(300),
        }
    }
}

impl SeaClient {
    /// Poll until statement completes or fails
    ///
    /// Continuously polls the statement status until it reaches a terminal state
    /// (SUCCEEDED, FAILED, CANCELED, CLOSED) or the timeout is reached.
    /// Uses exponential backoff with a maximum delay cap.
    ///
    /// # Arguments
    /// * `statement_id` - The statement ID to poll
    /// * `config` - Polling configuration (delays and timeout)
    ///
    /// # Returns
    /// * `ExecuteStatementResponse` - Final response when SUCCEEDED
    /// * `Error::StatementFailed` - When statement fails, is canceled, or closed
    /// * `Error::Timeout` - When polling exceeds the configured timeout
    ///
    /// # Backoff Strategy
    /// - Initial delay: config.initial_delay (default 1s)
    /// - Each iteration: delay = min(delay * 2, max_delay)
    /// - Max delay: config.max_delay (default 10s)
    /// - Sequence: 1s, 2s, 4s, 8s, 10s, 10s, ...
    pub async fn poll_until_complete(
        &self,
        statement_id: &str,
        config: &PollConfig,
    ) -> Result<models::ExecuteStatementResponse> {
        let start = std::time::Instant::now();
        let mut delay = config.initial_delay;

        loop {
            // Check timeout before making request
            if start.elapsed() > config.timeout {
                return Err(crate::error::Error::Timeout);
            }

            let response = self.get_statement(statement_id).await?;

            match response.status.state {
                models::StatementState::Succeeded => {
                    return Ok(response);
                }
                models::StatementState::Failed => {
                    let error_msg = response.status.error
                        .and_then(|e| e.message)
                        .unwrap_or_else(|| "Unknown error".to_string());
                    return Err(crate::error::Error::StatementFailed(error_msg));
                }
                models::StatementState::Canceled => {
                    return Err(crate::error::Error::StatementFailed("Statement was canceled".into()));
                }
                models::StatementState::Closed => {
                    return Err(crate::error::Error::StatementFailed("Statement was closed".into()));
                }
                models::StatementState::Pending | models::StatementState::Running => {
                    // Sleep before next poll
                    tokio::time::sleep(delay).await;
                    // Exponential backoff with cap
                    delay = std::cmp::min(delay * 2, config.max_delay);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sea_client_config_defaults() {
        let config = SeaClientConfig::default();
        assert_eq!(config.host, "");
        assert_eq!(config.token, "");
        assert_eq!(config.warehouse_id, "");
        assert_eq!(config.connect_timeout, std::time::Duration::from_secs(10));
        assert_eq!(config.read_timeout, std::time::Duration::from_secs(300));
    }

    #[test]
    fn test_sea_client_new() {
        let config = SeaClientConfig {
            host: "https://workspace.cloud.databricks.com".to_string(),
            token: "test-token".to_string(),
            warehouse_id: "abc123".to_string(),
            ..Default::default()
        };

        let client = SeaClient::new(config);
        assert!(client.is_ok());

        let client = client.unwrap();
        assert_eq!(client.host, "https://workspace.cloud.databricks.com");
        assert_eq!(client.token, "test-token");
        assert_eq!(client.warehouse_id, "abc123");
        assert_eq!(client.warehouse_id(), "abc123");
    }

    #[test]
    fn test_base_url_construction() {
        let config = SeaClientConfig {
            host: "https://workspace.cloud.databricks.com".to_string(),
            token: "token".to_string(),
            warehouse_id: "abc123".to_string(),
            ..Default::default()
        };

        let client = SeaClient::new(config).unwrap();
        assert_eq!(client.base_url(), "https://workspace.cloud.databricks.com/api/2.0/sql");
    }

    #[test]
    fn test_base_url_trims_trailing_slash() {
        let config = SeaClientConfig {
            host: "https://workspace.cloud.databricks.com/".to_string(),
            token: "token".to_string(),
            warehouse_id: "abc123".to_string(),
            ..Default::default()
        };

        let client = SeaClient::new(config).unwrap();
        assert_eq!(client.base_url(), "https://workspace.cloud.databricks.com/api/2.0/sql");
    }

    #[test]
    fn test_statements_url() {
        let config = SeaClientConfig {
            host: "https://workspace.cloud.databricks.com".to_string(),
            token: "token".to_string(),
            warehouse_id: "abc123".to_string(),
            ..Default::default()
        };

        let client = SeaClient::new(config).unwrap();
        assert_eq!(client.statements_url(), "https://workspace.cloud.databricks.com/api/2.0/sql/statements");
    }

    #[test]
    fn test_statement_url() {
        let config = SeaClientConfig {
            host: "https://workspace.cloud.databricks.com".to_string(),
            token: "token".to_string(),
            warehouse_id: "abc123".to_string(),
            ..Default::default()
        };

        let client = SeaClient::new(config).unwrap();
        assert_eq!(
            client.statement_url("01ef1234-5678-1abc-def0-123456789abc"),
            "https://workspace.cloud.databricks.com/api/2.0/sql/statements/01ef1234-5678-1abc-def0-123456789abc"
        );
    }

    #[test]
    fn test_sessions_url() {
        let config = SeaClientConfig {
            host: "https://workspace.cloud.databricks.com".to_string(),
            token: "token".to_string(),
            warehouse_id: "abc123".to_string(),
            ..Default::default()
        };

        let client = SeaClient::new(config).unwrap();
        assert_eq!(client.sessions_url(), "https://workspace.cloud.databricks.com/api/2.0/sql/sessions");
    }

    #[test]
    fn test_session_url() {
        let config = SeaClientConfig {
            host: "https://workspace.cloud.databricks.com".to_string(),
            token: "token".to_string(),
            warehouse_id: "abc123".to_string(),
            ..Default::default()
        };

        let client = SeaClient::new(config).unwrap();
        assert_eq!(
            client.session_url("01ef1234-5678-1abc-def0-123456789abc"),
            "https://workspace.cloud.databricks.com/api/2.0/sql/sessions/01ef1234-5678-1abc-def0-123456789abc"
        );
    }

    #[test]
    fn test_default_headers() {
        let headers = SeaClient::default_headers("test-token-123");

        assert_eq!(
            headers.get(reqwest::header::AUTHORIZATION).unwrap(),
            "Bearer test-token-123"
        );
        assert_eq!(
            headers.get(reqwest::header::CONTENT_TYPE).unwrap(),
            "application/json"
        );
        assert_eq!(
            headers.get(reqwest::header::USER_AGENT).unwrap(),
            "adbc-driver-databricks/0.1.0"
        );
    }

    #[tokio::test]
    async fn test_handle_error_response() {
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path};

        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/test"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error_code": "INVALID_PARAMETER_VALUE",
                "message": "Invalid warehouse ID"
            })))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: mock_server.uri(),
            token: "test-token".to_string(),
            warehouse_id: "invalid".to_string(),
            ..Default::default()
        };

        let client = SeaClient::new(config).unwrap();
        let result: Result<serde_json::Value> = client.get(&format!("{}/api/2.0/sql/test", mock_server.uri())).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            crate::error::Error::SeaApi { code, message, http_status, .. } => {
                assert_eq!(code, "INVALID_PARAMETER_VALUE");
                assert_eq!(message, "Invalid warehouse ID");
                assert_eq!(http_status, 400);
            }
            _ => panic!("Expected SeaApi error"),
        }
    }

    #[tokio::test]
    async fn test_successful_get_request() {
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path, header};

        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/test"))
            .and(header("Authorization", "Bearer test-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "ok",
                "data": "test"
            })))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: mock_server.uri(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let client = SeaClient::new(config).unwrap();
        let result: Result<serde_json::Value> = client.get(&format!("{}/api/2.0/sql/test", mock_server.uri())).await;

        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response["status"], "ok");
        assert_eq!(response["data"], "test");
    }

    #[tokio::test]
    async fn test_successful_post_request() {
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path, header};

        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/test"))
            .and(header("Authorization", "Bearer test-token"))
            .and(header("Content-Type", "application/json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "created",
                "id": "123"
            })))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: mock_server.uri(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let client = SeaClient::new(config).unwrap();
        let request_body = serde_json::json!({"key": "value"});
        let result: Result<serde_json::Value> = client.post(&format!("{}/api/2.0/sql/test", mock_server.uri()), &request_body).await;

        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response["status"], "created");
        assert_eq!(response["id"], "123");
    }

    #[tokio::test]
    async fn test_successful_delete_request() {
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path, header};

        let mock_server = MockServer::start().await;

        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/test/123"))
            .and(header("Authorization", "Bearer test-token"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: mock_server.uri(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let client = SeaClient::new(config).unwrap();
        let result = client.delete(&format!("{}/api/2.0/sql/test/123", mock_server.uri())).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_delete_request_with_error() {
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path};

        let mock_server = MockServer::start().await;

        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/test/404"))
            .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                "error_code": "NOT_FOUND",
                "message": "Resource not found"
            })))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: mock_server.uri(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let client = SeaClient::new(config).unwrap();
        let result = client.delete(&format!("{}/api/2.0/sql/test/404", mock_server.uri())).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            crate::error::Error::SeaApi { code, message, http_status, .. } => {
                assert_eq!(code, "NOT_FOUND");
                assert_eq!(message, "Resource not found");
                assert_eq!(http_status, 404);
            }
            _ => panic!("Expected SeaApi error"),
        }
    }

    #[tokio::test]
    async fn test_create_session_success() {
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path, body_json};

        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .and(body_json(serde_json::json!({
                "warehouse_id": "test-warehouse",
                "catalog": "main",
                "schema": "default"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "01ef1234-5678-1abc-def0-123456789abc"
            })))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: mock_server.uri(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let client = SeaClient::new(config).unwrap();
        let session_id = client.create_session(
            Some("main".to_string()),
            Some("default".to_string())
        ).await;

        assert!(session_id.is_ok());
        assert_eq!(session_id.unwrap(), "01ef1234-5678-1abc-def0-123456789abc");
    }

    #[tokio::test]
    async fn test_create_session_without_catalog_schema() {
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path, body_json};

        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .and(body_json(serde_json::json!({
                "warehouse_id": "test-warehouse"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "01ef1234-5678-1abc-def0-123456789abc"
            })))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: mock_server.uri(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let client = SeaClient::new(config).unwrap();
        let session_id = client.create_session(None, None).await;

        assert!(session_id.is_ok());
        assert_eq!(session_id.unwrap(), "01ef1234-5678-1abc-def0-123456789abc");
    }

    #[tokio::test]
    async fn test_create_session_error() {
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path};

        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error_code": "INVALID_PARAMETER_VALUE",
                "message": "Invalid warehouse ID"
            })))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: mock_server.uri(),
            token: "test-token".to_string(),
            warehouse_id: "invalid-warehouse".to_string(),
            ..Default::default()
        };

        let client = SeaClient::new(config).unwrap();
        let result = client.create_session(None, None).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            crate::error::Error::SeaApi { code, message, http_status, .. } => {
                assert_eq!(code, "INVALID_PARAMETER_VALUE");
                assert_eq!(message, "Invalid warehouse ID");
                assert_eq!(http_status, 400);
            }
            _ => panic!("Expected SeaApi error"),
        }
    }

    #[tokio::test]
    async fn test_delete_session_success() {
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path};

        let mock_server = MockServer::start().await;

        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/01ef1234-5678-1abc-def0-123456789abc"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: mock_server.uri(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let client = SeaClient::new(config).unwrap();
        let result = client.delete_session("01ef1234-5678-1abc-def0-123456789abc").await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_delete_session_error() {
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path};

        let mock_server = MockServer::start().await;

        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/invalid-session"))
            .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                "error_code": "NOT_FOUND",
                "message": "Session not found"
            })))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: mock_server.uri(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let client = SeaClient::new(config).unwrap();
        let result = client.delete_session("invalid-session").await;

        assert!(result.is_err());
        match result.unwrap_err() {
            crate::error::Error::SeaApi { code, message, http_status, .. } => {
                assert_eq!(code, "NOT_FOUND");
                assert_eq!(message, "Session not found");
                assert_eq!(http_status, 404);
            }
            _ => panic!("Expected SeaApi error"),
        }
    }

    #[test]
    fn test_execute_request_serialization() {
        use models::ExecuteStatementRequest;

        let request = ExecuteStatementRequest {
            statement: "SELECT 1".to_string(),
            warehouse_id: "abc123".to_string(),
            session_id: Some("session456".to_string()),
            ..Default::default()
        };

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("ARROW_STREAM"));
        assert!(json.contains("INLINE_OR_EXTERNAL_LINKS"));
        assert!(json.contains("SELECT 1"));
        assert!(json.contains("abc123"));
        assert!(json.contains("session456"));
        assert!(json.contains("10s"));
    }

    #[test]
    fn test_execute_request_default_values() {
        use models::ExecuteStatementRequest;

        let request = ExecuteStatementRequest::default();
        assert_eq!(request.wait_timeout, "10s");
        assert_eq!(request.disposition, "INLINE_OR_EXTERNAL_LINKS");
        assert_eq!(request.format, "ARROW_STREAM");
        assert!(request.session_id.is_none());
        assert!(request.catalog.is_none());
        assert!(request.schema.is_none());
        assert!(request.row_limit.is_none());
        assert!(request.byte_limit.is_none());
    }

    #[test]
    fn test_execute_request_skip_serializing_none() {
        use models::ExecuteStatementRequest;

        let request = ExecuteStatementRequest {
            statement: "SELECT 1".to_string(),
            warehouse_id: "abc123".to_string(),
            session_id: None,
            catalog: None,
            schema: None,
            wait_timeout: "10s".to_string(),
            disposition: "INLINE_OR_EXTERNAL_LINKS".to_string(),
            format: "ARROW_STREAM".to_string(),
            row_limit: None,
            byte_limit: None,
        };

        let json = serde_json::to_string(&request).unwrap();
        // None fields should not be present in JSON
        assert!(!json.contains("session_id"));
        assert!(!json.contains("catalog"));
        assert!(!json.contains("schema"));
        assert!(!json.contains("row_limit"));
        assert!(!json.contains("byte_limit"));
    }

    #[test]
    fn test_execute_response_deserialization() {
        use models::ExecuteStatementResponse;

        let json = r#"{
            "statement_id": "stmt123",
            "status": { "state": "SUCCEEDED" },
            "result": { "chunk_index": 0, "row_count": 1 }
        }"#;

        let response: ExecuteStatementResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.statement_id, "stmt123");
        assert_eq!(response.status.state, models::StatementState::Succeeded);
        assert!(response.result.is_some());
        assert_eq!(response.result.unwrap().chunk_index, Some(0));
    }

    #[test]
    fn test_execute_response_with_manifest() {
        use models::ExecuteStatementResponse;

        let json = r#"{
            "statement_id": "stmt456",
            "status": { "state": "SUCCEEDED" },
            "manifest": {
                "format": "ARROW_STREAM",
                "schema": {
                    "columns": [
                        {
                            "name": "col1",
                            "type_name": "INT",
                            "type_text": "int",
                            "position": 0,
                            "nullable": false
                        }
                    ]
                },
                "total_chunk_count": 5,
                "total_row_count": 1000,
                "total_byte_count": 50000,
                "truncated": false
            }
        }"#;

        let response: ExecuteStatementResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.statement_id, "stmt456");
        assert_eq!(response.status.state, models::StatementState::Succeeded);
        assert!(response.manifest.is_some());

        let manifest = response.manifest.unwrap();
        assert_eq!(manifest.format, "ARROW_STREAM");
        assert_eq!(manifest.total_chunk_count, 5);
        assert_eq!(manifest.total_row_count, Some(1000));
        assert_eq!(manifest.schema.columns.len(), 1);
        assert_eq!(manifest.schema.columns[0].name, "col1");
    }

    #[test]
    fn test_statement_state_deserialization() {
        use models::StatementState;

        assert_eq!(
            serde_json::from_str::<StatementState>(r#""PENDING""#).unwrap(),
            StatementState::Pending
        );
        assert_eq!(
            serde_json::from_str::<StatementState>(r#""RUNNING""#).unwrap(),
            StatementState::Running
        );
        assert_eq!(
            serde_json::from_str::<StatementState>(r#""SUCCEEDED""#).unwrap(),
            StatementState::Succeeded
        );
        assert_eq!(
            serde_json::from_str::<StatementState>(r#""FAILED""#).unwrap(),
            StatementState::Failed
        );
        assert_eq!(
            serde_json::from_str::<StatementState>(r#""CANCELED""#).unwrap(),
            StatementState::Canceled
        );
        assert_eq!(
            serde_json::from_str::<StatementState>(r#""CLOSED""#).unwrap(),
            StatementState::Closed
        );
    }

    #[test]
    fn test_execute_response_with_error() {
        use models::ExecuteStatementResponse;

        let json = r#"{
            "statement_id": "stmt789",
            "status": {
                "state": "FAILED",
                "error": {
                    "error_code": "SYNTAX_ERROR",
                    "message": "Invalid SQL syntax"
                }
            }
        }"#;

        let response: ExecuteStatementResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.statement_id, "stmt789");
        assert_eq!(response.status.state, models::StatementState::Failed);
        assert!(response.status.error.is_some());

        let error = response.status.error.unwrap();
        assert_eq!(error.error_code, Some("SYNTAX_ERROR".to_string()));
        assert_eq!(error.message, Some("Invalid SQL syntax".to_string()));
    }

    #[test]
    fn test_execute_response_with_external_links() {
        use models::ExecuteStatementResponse;

        let json = r#"{
            "statement_id": "stmt999",
            "status": { "state": "SUCCEEDED" },
            "result": {
                "external_links": [
                    {
                        "external_link": "https://s3.amazonaws.com/bucket/chunk0",
                        "chunk_index": 0,
                        "row_offset": 0,
                        "row_count": 10000,
                        "byte_count": 1048576,
                        "expiration": "2024-12-31T23:59:59Z"
                    }
                ]
            }
        }"#;

        let response: ExecuteStatementResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.statement_id, "stmt999");
        assert_eq!(response.status.state, models::StatementState::Succeeded);
        assert!(response.result.is_some());

        let result = response.result.unwrap();
        assert!(result.external_links.is_some());

        let links = result.external_links.unwrap();
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].chunk_index, 0);
        assert_eq!(links[0].row_count, 10000);
        assert!(links[0].external_link.contains("s3.amazonaws.com"));
    }

    #[tokio::test]
    async fn test_execute_statement_success() {
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path, body_json};

        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .and(body_json(serde_json::json!({
                "statement": "SELECT 1",
                "warehouse_id": "test-warehouse",
                "wait_timeout": "10s",
                "disposition": "INLINE_OR_EXTERNAL_LINKS",
                "format": "ARROW_STREAM"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "statement_id": "01ef1234-5678-1abc-def0-123456789abc",
                "status": {
                    "state": "SUCCEEDED"
                },
                "result": {
                    "chunk_index": 0,
                    "row_count": 1
                }
            })))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: mock_server.uri(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let client = SeaClient::new(config).unwrap();
        let request = models::ExecuteStatementRequest {
            statement: "SELECT 1".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let response = client.execute_statement(request).await;
        assert!(response.is_ok());

        let response = response.unwrap();
        assert_eq!(response.statement_id, "01ef1234-5678-1abc-def0-123456789abc");
        assert_eq!(response.status.state, models::StatementState::Succeeded);
    }

    #[tokio::test]
    async fn test_execute_statement_with_session() {
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path, body_json};

        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .and(body_json(serde_json::json!({
                "statement": "SELECT * FROM table1",
                "warehouse_id": "test-warehouse",
                "session_id": "session-123",
                "catalog": "main",
                "schema": "default",
                "wait_timeout": "10s",
                "disposition": "INLINE_OR_EXTERNAL_LINKS",
                "format": "ARROW_STREAM"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "statement_id": "stmt-abc",
                "status": {
                    "state": "RUNNING"
                }
            })))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: mock_server.uri(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let client = SeaClient::new(config).unwrap();
        let request = models::ExecuteStatementRequest {
            statement: "SELECT * FROM table1".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            session_id: Some("session-123".to_string()),
            catalog: Some("main".to_string()),
            schema: Some("default".to_string()),
            ..Default::default()
        };

        let response = client.execute_statement(request).await;
        assert!(response.is_ok());

        let response = response.unwrap();
        assert_eq!(response.statement_id, "stmt-abc");
        assert_eq!(response.status.state, models::StatementState::Running);
    }

    #[tokio::test]
    async fn test_execute_statement_error() {
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path};

        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error_code": "INVALID_PARAMETER_VALUE",
                "message": "Invalid SQL statement"
            })))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: mock_server.uri(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let client = SeaClient::new(config).unwrap();
        let request = models::ExecuteStatementRequest {
            statement: "INVALID SQL".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let response = client.execute_statement(request).await;
        assert!(response.is_err());

        match response.unwrap_err() {
            crate::error::Error::SeaApi { code, message, http_status, .. } => {
                assert_eq!(code, "INVALID_PARAMETER_VALUE");
                assert_eq!(message, "Invalid SQL statement");
                assert_eq!(http_status, 400);
            }
            _ => panic!("Expected SeaApi error"),
        }
    }

    // === PollConfig Tests ===

    #[test]
    fn test_poll_config_default() {
        let config = PollConfig::default();
        assert_eq!(config.initial_delay, Duration::from_secs(1));
        assert_eq!(config.max_delay, Duration::from_secs(10));
        assert_eq!(config.timeout, Duration::from_secs(300));
    }

    #[test]
    fn test_poll_config_custom() {
        let config = PollConfig {
            initial_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(5),
            timeout: Duration::from_secs(60),
        };
        assert_eq!(config.initial_delay, Duration::from_millis(500));
        assert_eq!(config.max_delay, Duration::from_secs(5));
        assert_eq!(config.timeout, Duration::from_secs(60));
    }

    // === get_statement Tests ===

    #[tokio::test]
    async fn test_get_statement_success() {
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path};

        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-123"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "statement_id": "stmt-123",
                "status": {
                    "state": "SUCCEEDED"
                },
                "result": {
                    "chunk_index": 0,
                    "row_count": 100
                }
            })))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: mock_server.uri(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let client = SeaClient::new(config).unwrap();
        let response = client.get_statement("stmt-123").await;

        assert!(response.is_ok());
        let response = response.unwrap();
        assert_eq!(response.statement_id, "stmt-123");
        assert_eq!(response.status.state, models::StatementState::Succeeded);
    }

    #[tokio::test]
    async fn test_get_statement_running() {
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path};

        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-456"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "statement_id": "stmt-456",
                "status": {
                    "state": "RUNNING"
                }
            })))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: mock_server.uri(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let client = SeaClient::new(config).unwrap();
        let response = client.get_statement("stmt-456").await;

        assert!(response.is_ok());
        let response = response.unwrap();
        assert_eq!(response.statement_id, "stmt-456");
        assert_eq!(response.status.state, models::StatementState::Running);
    }

    #[tokio::test]
    async fn test_get_statement_not_found() {
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path};

        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/invalid-id"))
            .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                "error_code": "NOT_FOUND",
                "message": "Statement not found"
            })))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: mock_server.uri(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let client = SeaClient::new(config).unwrap();
        let response = client.get_statement("invalid-id").await;

        assert!(response.is_err());
        match response.unwrap_err() {
            crate::error::Error::SeaApi { code, message, http_status, .. } => {
                assert_eq!(code, "NOT_FOUND");
                assert_eq!(message, "Statement not found");
                assert_eq!(http_status, 404);
            }
            _ => panic!("Expected SeaApi error"),
        }
    }

    // === poll_until_complete Tests ===

    #[tokio::test]
    async fn test_poll_until_complete_immediate_success() {
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path};

        let mock_server = MockServer::start().await;

        // Statement is already succeeded
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-success"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "statement_id": "stmt-success",
                "status": {
                    "state": "SUCCEEDED"
                },
                "result": {
                    "chunk_index": 0,
                    "row_count": 100
                }
            })))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: mock_server.uri(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let client = SeaClient::new(config).unwrap();
        let poll_config = PollConfig::default();

        let start = std::time::Instant::now();
        let response = client.poll_until_complete("stmt-success", &poll_config).await;
        let elapsed = start.elapsed();

        assert!(response.is_ok());
        let response = response.unwrap();
        assert_eq!(response.statement_id, "stmt-success");
        assert_eq!(response.status.state, models::StatementState::Succeeded);
        // Should return immediately without polling delay
        assert!(elapsed < Duration::from_millis(500));
    }

    #[tokio::test]
    async fn test_poll_until_complete_pending_then_success() {
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path};

        let mock_server = MockServer::start().await;

        // First call returns PENDING
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-pending"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "statement_id": "stmt-pending",
                "status": {
                    "state": "PENDING"
                }
            })))
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;

        // Second call returns RUNNING
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-pending"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "statement_id": "stmt-pending",
                "status": {
                    "state": "RUNNING"
                }
            })))
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;

        // Third call returns SUCCEEDED
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-pending"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "statement_id": "stmt-pending",
                "status": {
                    "state": "SUCCEEDED"
                },
                "result": {
                    "chunk_index": 0,
                    "row_count": 100
                }
            })))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: mock_server.uri(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let client = SeaClient::new(config).unwrap();
        let poll_config = PollConfig {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            timeout: Duration::from_secs(30),
        };

        let response = client.poll_until_complete("stmt-pending", &poll_config).await;

        assert!(response.is_ok());
        let response = response.unwrap();
        assert_eq!(response.statement_id, "stmt-pending");
        assert_eq!(response.status.state, models::StatementState::Succeeded);
    }

    #[tokio::test]
    async fn test_poll_until_complete_failed() {
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path};

        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-failed"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "statement_id": "stmt-failed",
                "status": {
                    "state": "FAILED",
                    "error": {
                        "error_code": "SYNTAX_ERROR",
                        "message": "Invalid SQL syntax"
                    }
                }
            })))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: mock_server.uri(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let client = SeaClient::new(config).unwrap();
        let poll_config = PollConfig::default();

        let response = client.poll_until_complete("stmt-failed", &poll_config).await;

        assert!(response.is_err());
        match response.unwrap_err() {
            crate::error::Error::StatementFailed(msg) => {
                assert_eq!(msg, "Invalid SQL syntax");
            }
            e => panic!("Expected StatementFailed error, got {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_poll_until_complete_canceled() {
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path};

        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-canceled"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "statement_id": "stmt-canceled",
                "status": {
                    "state": "CANCELED"
                }
            })))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: mock_server.uri(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let client = SeaClient::new(config).unwrap();
        let poll_config = PollConfig::default();

        let response = client.poll_until_complete("stmt-canceled", &poll_config).await;

        assert!(response.is_err());
        match response.unwrap_err() {
            crate::error::Error::StatementFailed(msg) => {
                assert_eq!(msg, "Statement was canceled");
            }
            e => panic!("Expected StatementFailed error, got {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_poll_until_complete_closed() {
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path};

        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-closed"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "statement_id": "stmt-closed",
                "status": {
                    "state": "CLOSED"
                }
            })))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: mock_server.uri(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let client = SeaClient::new(config).unwrap();
        let poll_config = PollConfig::default();

        let response = client.poll_until_complete("stmt-closed", &poll_config).await;

        assert!(response.is_err());
        match response.unwrap_err() {
            crate::error::Error::StatementFailed(msg) => {
                assert_eq!(msg, "Statement was closed");
            }
            e => panic!("Expected StatementFailed error, got {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_poll_until_complete_timeout() {
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path};

        let mock_server = MockServer::start().await;

        // Always return RUNNING to force timeout
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

        let config = SeaClientConfig {
            host: mock_server.uri(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let client = SeaClient::new(config).unwrap();
        let poll_config = PollConfig {
            initial_delay: Duration::from_millis(50),
            max_delay: Duration::from_millis(100),
            timeout: Duration::from_millis(500), // Short timeout for test
        };

        let start = std::time::Instant::now();
        let response = client.poll_until_complete("stmt-timeout", &poll_config).await;
        let elapsed = start.elapsed();

        assert!(response.is_err());
        match response.unwrap_err() {
            crate::error::Error::Timeout => {
                // Expected
            }
            e => panic!("Expected Timeout error, got {:?}", e),
        }
        // Verify we actually waited close to the timeout duration
        assert!(elapsed >= Duration::from_millis(500));
        assert!(elapsed < Duration::from_millis(1000)); // Some buffer for test execution
    }

    #[tokio::test]
    async fn test_poll_exponential_backoff() {
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path};

        let mock_server = MockServer::start().await;

        // Return RUNNING for several iterations
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-backoff"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "statement_id": "stmt-backoff",
                "status": {
                    "state": "RUNNING"
                }
            })))
            .up_to_n_times(4)
            .mount(&mock_server)
            .await;

        // Finally return SUCCEEDED
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-backoff"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "statement_id": "stmt-backoff",
                "status": {
                    "state": "SUCCEEDED"
                }
            })))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: mock_server.uri(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let client = SeaClient::new(config).unwrap();
        let poll_config = PollConfig {
            initial_delay: Duration::from_millis(100), // 100ms
            max_delay: Duration::from_millis(500),     // Cap at 500ms
            timeout: Duration::from_secs(10),
        };

        let start = std::time::Instant::now();
        let response = client.poll_until_complete("stmt-backoff", &poll_config).await;
        let elapsed = start.elapsed();

        assert!(response.is_ok());

        // Verify exponential backoff timing
        // Expected delays: 100ms, 200ms, 400ms, 500ms (capped)
        // Total: ~1200ms minimum
        // With some buffer for request time: should be at least 1000ms
        assert!(elapsed >= Duration::from_millis(1000),
            "Expected at least 1000ms with exponential backoff, got {}ms",
            elapsed.as_millis());
    }

    #[tokio::test]
    async fn test_poll_failed_with_missing_error_message() {
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path};

        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-no-msg"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "statement_id": "stmt-no-msg",
                "status": {
                    "state": "FAILED"
                }
            })))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: mock_server.uri(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let client = SeaClient::new(config).unwrap();
        let poll_config = PollConfig::default();

        let response = client.poll_until_complete("stmt-no-msg", &poll_config).await;

        assert!(response.is_err());
        match response.unwrap_err() {
            crate::error::Error::StatementFailed(msg) => {
                assert_eq!(msg, "Unknown error");
            }
            e => panic!("Expected StatementFailed error, got {:?}", e),
        }
    }
}
