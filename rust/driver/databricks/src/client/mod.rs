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
}
