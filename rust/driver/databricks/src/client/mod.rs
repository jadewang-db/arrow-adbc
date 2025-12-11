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

//! SEA (Statement Execution API) client implementation.
//!
//! This module provides the HTTP client for interacting with the
//! Databricks Statement Execution API.
//!
//! # Example
//!
//! ```ignore
//! use adbc_driver_databricks::client::{SeaClient, SeaClientConfig};
//! use std::time::Duration;
//!
//! let config = SeaClientConfig {
//!     host: "https://my-workspace.cloud.databricks.com".to_string(),
//!     token: "dapi...".to_string(),
//!     warehouse_id: "abc123".to_string(),
//!     ..Default::default()
//! };
//!
//! let client = SeaClient::new(config)?;
//! ```

mod error;
mod models;

pub use error::{SeaError, SeaErrorCode, SeaErrorResponse};
pub use models::*;

use crate::error::{Error, Result};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE, USER_AGENT};
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::time::Duration;

/// Driver version for User-Agent header.
const DRIVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Default connect timeout in seconds.
const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 10;

/// Default read timeout in seconds.
const DEFAULT_READ_TIMEOUT_SECS: u64 = 300;

/// Configuration for the SEA client.
///
/// Contains all necessary settings to establish a connection to a
/// Databricks SQL Warehouse.
#[derive(Debug, Clone)]
pub struct SeaClientConfig {
    /// Databricks workspace host URL (e.g., "https://my-workspace.cloud.databricks.com").
    pub host: String,
    /// Personal Access Token for authentication.
    pub token: String,
    /// SQL Warehouse ID.
    pub warehouse_id: String,
    /// HTTP connect timeout.
    pub connect_timeout: Duration,
    /// HTTP read timeout (also used as overall request timeout).
    pub read_timeout: Duration,
}

impl Default for SeaClientConfig {
    fn default() -> Self {
        Self {
            host: String::new(),
            token: String::new(),
            warehouse_id: String::new(),
            connect_timeout: Duration::from_secs(DEFAULT_CONNECT_TIMEOUT_SECS),
            read_timeout: Duration::from_secs(DEFAULT_READ_TIMEOUT_SECS),
        }
    }
}

impl SeaClientConfig {
    /// Create a new configuration with required parameters.
    ///
    /// Uses default timeout values.
    pub fn new(host: impl Into<String>, token: impl Into<String>, warehouse_id: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            token: token.into(),
            warehouse_id: warehouse_id.into(),
            ..Default::default()
        }
    }

    /// Set the connect timeout.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set the read timeout.
    pub fn with_read_timeout(mut self, timeout: Duration) -> Self {
        self.read_timeout = timeout;
        self
    }

    /// Validate the configuration.
    ///
    /// Returns an error if any required field is missing or invalid.
    pub fn validate(&self) -> Result<()> {
        if self.host.is_empty() {
            return Err(Error::config("host is required"));
        }
        if self.token.is_empty() {
            return Err(Error::config("token is required"));
        }
        if self.warehouse_id.is_empty() {
            return Err(Error::config("warehouse_id is required"));
        }
        // Validate host URL format
        if !self.host.starts_with("http://") && !self.host.starts_with("https://") {
            return Err(Error::config("host must start with http:// or https://"));
        }
        Ok(())
    }
}

/// SEA (Statement Execution API) client for Databricks.
///
/// Handles all HTTP communication with the Databricks SQL Warehouse API.
/// This client is thread-safe and can be shared across multiple connections.
///
/// # API Endpoints
///
/// The client provides access to the following SEA API endpoints:
/// - `/api/2.0/sql/statements` - Execute and manage statements
/// - `/api/2.0/sql/sessions` - Manage sessions
///
/// # Error Handling
///
/// All API errors are converted to [`Error`] with appropriate status codes.
/// Transient errors (429, 500, 503) are marked as retryable.
#[derive(Debug)]
pub struct SeaClient {
    /// HTTP client with configured timeouts and headers.
    http_client: Client,
    /// Databricks workspace host URL.
    host: String,
    /// SQL Warehouse ID.
    warehouse_id: String,
}

impl SeaClient {
    /// Create a new SEA client with the given configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The configuration is invalid (missing required fields)
    /// - The HTTP client fails to build
    pub fn new(config: SeaClientConfig) -> Result<Self> {
        config.validate()?;

        let http_client = Client::builder()
            .connect_timeout(config.connect_timeout)
            .timeout(config.read_timeout)
            .default_headers(Self::build_default_headers(&config.token)?)
            .build()
            .map_err(Error::Http)?;

        Ok(Self {
            http_client,
            host: config.host,
            warehouse_id: config.warehouse_id,
        })
    }

    /// Build default headers for all requests.
    fn build_default_headers(token: &str) -> Result<HeaderMap> {
        let mut headers = HeaderMap::new();

        // Authorization header with Bearer token
        let auth_value = format!("Bearer {}", token);
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&auth_value).map_err(|e| {
                Error::config(format!("invalid token format: {}", e))
            })?,
        );

        // Content-Type header
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        // User-Agent header
        let user_agent = format!("adbc-driver-databricks/{}", DRIVER_VERSION);
        headers.insert(
            USER_AGENT,
            HeaderValue::from_str(&user_agent).map_err(|e| {
                Error::config(format!("invalid user agent: {}", e))
            })?,
        );

        Ok(headers)
    }

    // =========================================================================
    // URL Construction Helpers
    // =========================================================================

    /// Get the base URL for the SQL API.
    ///
    /// Returns the host URL with the `/api/2.0/sql` path appended.
    pub fn base_url(&self) -> String {
        format!("{}/api/2.0/sql", self.host.trim_end_matches('/'))
    }

    /// Get the URL for the statements endpoint.
    pub fn statements_url(&self) -> String {
        format!("{}/statements", self.base_url())
    }

    /// Get the URL for a specific statement.
    pub fn statement_url(&self, statement_id: &str) -> String {
        format!("{}/statements/{}", self.base_url(), statement_id)
    }

    /// Get the URL for a statement's result chunks.
    pub fn statement_chunk_url(&self, statement_id: &str, chunk_index: i32) -> String {
        format!(
            "{}/statements/{}/result/chunks/{}",
            self.base_url(),
            statement_id,
            chunk_index
        )
    }

    /// Get the URL to cancel a statement.
    pub fn statement_cancel_url(&self, statement_id: &str) -> String {
        format!("{}/statements/{}/cancel", self.base_url(), statement_id)
    }

    /// Get the URL for the sessions endpoint.
    pub fn sessions_url(&self) -> String {
        format!("{}/sessions", self.base_url())
    }

    /// Get the URL for a specific session.
    pub fn session_url(&self, session_id: &str) -> String {
        format!("{}/sessions/{}", self.base_url(), session_id)
    }

    // =========================================================================
    // Accessor Methods
    // =========================================================================

    /// Get the warehouse ID.
    pub fn warehouse_id(&self) -> &str {
        &self.warehouse_id
    }

    /// Get the host URL.
    pub fn host(&self) -> &str {
        &self.host
    }

    // =========================================================================
    // HTTP Request Methods
    // =========================================================================

    /// Send a POST request with a JSON body and parse the response.
    ///
    /// # Type Parameters
    ///
    /// * `Req` - The request body type (must implement `Serialize`)
    /// * `Resp` - The response body type (must implement `DeserializeOwned`)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request fails to send
    /// - The response indicates an error (4xx or 5xx)
    /// - The response body cannot be parsed
    pub async fn post<Req, Resp>(&self, url: &str, body: &Req) -> Result<Resp>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
    {
        let response = self
            .http_client
            .post(url)
            .json(body)
            .send()
            .await
            .map_err(Error::Http)?;

        self.handle_response(response).await
    }

    /// Send a GET request and parse the response.
    ///
    /// # Type Parameters
    ///
    /// * `Resp` - The response body type (must implement `DeserializeOwned`)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request fails to send
    /// - The response indicates an error (4xx or 5xx)
    /// - The response body cannot be parsed
    pub async fn get<Resp>(&self, url: &str) -> Result<Resp>
    where
        Resp: DeserializeOwned,
    {
        let response = self
            .http_client
            .get(url)
            .send()
            .await
            .map_err(Error::Http)?;

        self.handle_response(response).await
    }

    /// Send a DELETE request.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request fails to send
    /// - The response indicates an error (4xx or 5xx)
    pub async fn delete(&self, url: &str) -> Result<()> {
        let response = self
            .http_client
            .delete(url)
            .send()
            .await
            .map_err(Error::Http)?;

        if response.status().is_success() {
            Ok(())
        } else {
            self.handle_error_response(response).await
        }
    }

    // =========================================================================
    // Response Handling
    // =========================================================================

    /// Handle a successful or error response.
    async fn handle_response<Resp>(&self, response: reqwest::Response) -> Result<Resp>
    where
        Resp: DeserializeOwned,
    {
        let status = response.status();
        if status.is_success() {
            response.json().await.map_err(Error::Http)
        } else {
            self.handle_error_response(response).await
        }
    }

    /// Handle an error response by parsing the error body.
    async fn handle_error_response<T>(&self, response: reqwest::Response) -> Result<T> {
        let http_status = response.status().as_u16();

        // Try to extract retry-after header for rate limiting
        let retry_after = response
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok());

        // Try to parse the error response body
        let error_response: Option<SeaErrorResponse> = response.json().await.ok();

        let (code, message) = match &error_response {
            Some(resp) => {
                let code = resp.error_code.clone().unwrap_or_else(|| "UNKNOWN".to_string());
                let message = resp.message.clone().unwrap_or_else(|| format!("HTTP {}", http_status));
                (code, message)
            }
            None => ("UNKNOWN".to_string(), format!("HTTP {}", http_status)),
        };

        let mut err = Error::sea_api(code, message, http_status);

        // Log retry-after if present (could be used by retry logic in the future)
        if let Some(seconds) = retry_after {
            // For now, we just note it in a more detailed error message
            err = Error::sea_api(
                error_response.as_ref().and_then(|r| r.error_code.clone()).unwrap_or_else(|| "UNKNOWN".to_string()),
                format!(
                    "{} (retry after {} seconds)",
                    error_response.as_ref().and_then(|r| r.message.clone()).unwrap_or_else(|| format!("HTTP {}", http_status)),
                    seconds
                ),
                http_status,
            );
        }

        Err(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sea_client_config_default() {
        let config = SeaClientConfig::default();
        assert_eq!(config.host, "");
        assert_eq!(config.token, "");
        assert_eq!(config.warehouse_id, "");
        assert_eq!(config.connect_timeout, Duration::from_secs(10));
        assert_eq!(config.read_timeout, Duration::from_secs(300));
    }

    #[test]
    fn test_sea_client_config_new() {
        let config = SeaClientConfig::new(
            "https://workspace.cloud.databricks.com",
            "token123",
            "warehouse456",
        );
        assert_eq!(config.host, "https://workspace.cloud.databricks.com");
        assert_eq!(config.token, "token123");
        assert_eq!(config.warehouse_id, "warehouse456");
    }

    #[test]
    fn test_sea_client_config_with_timeouts() {
        let config = SeaClientConfig::new("https://host", "token", "warehouse")
            .with_connect_timeout(Duration::from_secs(5))
            .with_read_timeout(Duration::from_secs(60));
        assert_eq!(config.connect_timeout, Duration::from_secs(5));
        assert_eq!(config.read_timeout, Duration::from_secs(60));
    }

    #[test]
    fn test_sea_client_config_validate_missing_host() {
        let config = SeaClientConfig {
            host: "".to_string(),
            token: "token".to_string(),
            warehouse_id: "warehouse".to_string(),
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("host is required"));
    }

    #[test]
    fn test_sea_client_config_validate_missing_token() {
        let config = SeaClientConfig {
            host: "https://host".to_string(),
            token: "".to_string(),
            warehouse_id: "warehouse".to_string(),
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("token is required"));
    }

    #[test]
    fn test_sea_client_config_validate_missing_warehouse() {
        let config = SeaClientConfig {
            host: "https://host".to_string(),
            token: "token".to_string(),
            warehouse_id: "".to_string(),
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("warehouse_id is required"));
    }

    #[test]
    fn test_sea_client_config_validate_invalid_host_scheme() {
        let config = SeaClientConfig {
            host: "workspace.cloud.databricks.com".to_string(),
            token: "token".to_string(),
            warehouse_id: "warehouse".to_string(),
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("must start with http://"));
    }

    #[test]
    fn test_sea_client_config_validate_success() {
        let config = SeaClientConfig::new(
            "https://workspace.cloud.databricks.com",
            "token123",
            "warehouse456",
        );
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_sea_client_new_success() {
        let config = SeaClientConfig::new(
            "https://workspace.cloud.databricks.com",
            "dapi_token",
            "abc123",
        );
        let client = SeaClient::new(config);
        assert!(client.is_ok());
    }

    #[test]
    fn test_sea_client_new_invalid_config() {
        let config = SeaClientConfig::default();
        let result = SeaClient::new(config);
        assert!(result.is_err());
    }

    #[test]
    fn test_sea_client_base_url() {
        let config = SeaClientConfig::new(
            "https://workspace.cloud.databricks.com",
            "token",
            "warehouse",
        );
        let client = SeaClient::new(config).unwrap();
        assert_eq!(
            client.base_url(),
            "https://workspace.cloud.databricks.com/api/2.0/sql"
        );
    }

    #[test]
    fn test_sea_client_base_url_trailing_slash() {
        let config = SeaClientConfig::new(
            "https://workspace.cloud.databricks.com/",
            "token",
            "warehouse",
        );
        let client = SeaClient::new(config).unwrap();
        assert_eq!(
            client.base_url(),
            "https://workspace.cloud.databricks.com/api/2.0/sql"
        );
    }

    #[test]
    fn test_sea_client_statements_url() {
        let config = SeaClientConfig::new(
            "https://workspace.cloud.databricks.com",
            "token",
            "warehouse",
        );
        let client = SeaClient::new(config).unwrap();
        assert_eq!(
            client.statements_url(),
            "https://workspace.cloud.databricks.com/api/2.0/sql/statements"
        );
    }

    #[test]
    fn test_sea_client_statement_url() {
        let config = SeaClientConfig::new(
            "https://workspace.cloud.databricks.com",
            "token",
            "warehouse",
        );
        let client = SeaClient::new(config).unwrap();
        assert_eq!(
            client.statement_url("stmt-123"),
            "https://workspace.cloud.databricks.com/api/2.0/sql/statements/stmt-123"
        );
    }

    #[test]
    fn test_sea_client_statement_chunk_url() {
        let config = SeaClientConfig::new(
            "https://workspace.cloud.databricks.com",
            "token",
            "warehouse",
        );
        let client = SeaClient::new(config).unwrap();
        assert_eq!(
            client.statement_chunk_url("stmt-123", 5),
            "https://workspace.cloud.databricks.com/api/2.0/sql/statements/stmt-123/result/chunks/5"
        );
    }

    #[test]
    fn test_sea_client_statement_cancel_url() {
        let config = SeaClientConfig::new(
            "https://workspace.cloud.databricks.com",
            "token",
            "warehouse",
        );
        let client = SeaClient::new(config).unwrap();
        assert_eq!(
            client.statement_cancel_url("stmt-123"),
            "https://workspace.cloud.databricks.com/api/2.0/sql/statements/stmt-123/cancel"
        );
    }

    #[test]
    fn test_sea_client_sessions_url() {
        let config = SeaClientConfig::new(
            "https://workspace.cloud.databricks.com",
            "token",
            "warehouse",
        );
        let client = SeaClient::new(config).unwrap();
        assert_eq!(
            client.sessions_url(),
            "https://workspace.cloud.databricks.com/api/2.0/sql/sessions"
        );
    }

    #[test]
    fn test_sea_client_session_url() {
        let config = SeaClientConfig::new(
            "https://workspace.cloud.databricks.com",
            "token",
            "warehouse",
        );
        let client = SeaClient::new(config).unwrap();
        assert_eq!(
            client.session_url("session-456"),
            "https://workspace.cloud.databricks.com/api/2.0/sql/sessions/session-456"
        );
    }

    #[test]
    fn test_sea_client_warehouse_id() {
        let config = SeaClientConfig::new(
            "https://workspace.cloud.databricks.com",
            "token",
            "my-warehouse-id",
        );
        let client = SeaClient::new(config).unwrap();
        assert_eq!(client.warehouse_id(), "my-warehouse-id");
    }

    #[test]
    fn test_sea_client_host() {
        let config = SeaClientConfig::new(
            "https://workspace.cloud.databricks.com",
            "token",
            "warehouse",
        );
        let client = SeaClient::new(config).unwrap();
        assert_eq!(client.host(), "https://workspace.cloud.databricks.com");
    }

    #[test]
    fn test_sea_client_config_http_scheme() {
        // HTTP should also be valid (for testing/local dev)
        let config = SeaClientConfig::new(
            "http://localhost:8080",
            "token",
            "warehouse",
        );
        assert!(config.validate().is_ok());
    }
}

/// Async tests using wiremock for HTTP request/response handling.
#[cfg(test)]
mod async_tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use wiremock::matchers::{body_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Helper to create a SeaClient pointing to a mock server.
    fn create_test_client(server_uri: &str) -> SeaClient {
        let config = SeaClientConfig::new(server_uri, "test-token", "test-warehouse");
        SeaClient::new(config).unwrap()
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestRequest {
        value: String,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestResponse {
        result: String,
    }

    #[tokio::test]
    async fn test_post_success() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .and(header("authorization", "Bearer test-token"))
            .and(header("content-type", "application/json"))
            .and(body_json(&TestRequest {
                value: "test".to_string(),
            }))
            .respond_with(ResponseTemplate::new(200).set_body_json(TestResponse {
                result: "success".to_string(),
            }))
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri());
        let request = TestRequest {
            value: "test".to_string(),
        };
        let response: TestResponse = client.post(&client.statements_url(), &request).await.unwrap();
        assert_eq!(response.result, "success");
    }

    #[tokio::test]
    async fn test_get_success() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-123"))
            .and(header("authorization", "Bearer test-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(TestResponse {
                result: "fetched".to_string(),
            }))
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri());
        let response: TestResponse = client.get(&client.statement_url("stmt-123")).await.unwrap();
        assert_eq!(response.result, "fetched");
    }

    #[tokio::test]
    async fn test_delete_success() {
        let mock_server = MockServer::start().await;

        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/session-456"))
            .and(header("authorization", "Bearer test-token"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri());
        let result = client.delete(&client.session_url("session-456")).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_error_400_bad_request() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error_code": "BAD_REQUEST",
                "message": "Invalid SQL syntax"
            })))
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri());
        let request = TestRequest {
            value: "test".to_string(),
        };
        let result: Result<TestResponse> = client.post(&client.statements_url(), &request).await;

        let err = result.unwrap_err();
        assert!(matches!(err, Error::SeaApi { http_status: 400, .. }));
        assert!(err.to_string().contains("BAD_REQUEST"));
        assert!(err.to_string().contains("Invalid SQL syntax"));
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_error_401_unauthenticated() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-123"))
            .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
                "error_code": "UNAUTHENTICATED",
                "message": "Invalid or expired token"
            })))
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri());
        let result: Result<TestResponse> = client.get(&client.statement_url("stmt-123")).await;

        let err = result.unwrap_err();
        assert!(matches!(err, Error::SeaApi { http_status: 401, .. }));
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_error_403_permission_denied() {
        let mock_server = MockServer::start().await;

        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/session-456"))
            .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
                "error_code": "PERMISSION_DENIED",
                "message": "Access denied"
            })))
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri());
        let result = client.delete(&client.session_url("session-456")).await;

        let err = result.unwrap_err();
        assert!(matches!(err, Error::SeaApi { http_status: 403, .. }));
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_error_404_not_found() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/nonexistent"))
            .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                "error_code": "NOT_FOUND",
                "message": "Statement not found"
            })))
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri());
        let result: Result<TestResponse> = client.get(&client.statement_url("nonexistent")).await;

        let err = result.unwrap_err();
        assert!(matches!(err, Error::SeaApi { http_status: 404, .. }));
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_error_429_rate_limited() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(
                ResponseTemplate::new(429)
                    .insert_header("retry-after", "30")
                    .set_body_json(serde_json::json!({
                        "error_code": "REQUEST_LIMIT_EXCEEDED",
                        "message": "Rate limit exceeded"
                    })),
            )
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri());
        let request = TestRequest {
            value: "test".to_string(),
        };
        let result: Result<TestResponse> = client.post(&client.statements_url(), &request).await;

        let err = result.unwrap_err();
        assert!(matches!(err, Error::SeaApi { http_status: 429, .. }));
        assert!(err.is_retryable());
        // Check that retry-after is included in the message
        assert!(err.to_string().contains("retry after 30 seconds"));
    }

    #[tokio::test]
    async fn test_error_500_internal_error() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-123"))
            .respond_with(ResponseTemplate::new(500).set_body_json(serde_json::json!({
                "error_code": "INTERNAL_ERROR",
                "message": "Internal server error"
            })))
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri());
        let result: Result<TestResponse> = client.get(&client.statement_url("stmt-123")).await;

        let err = result.unwrap_err();
        assert!(matches!(err, Error::SeaApi { http_status: 500, .. }));
        assert!(err.is_retryable());
    }

    #[tokio::test]
    async fn test_error_503_unavailable() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(ResponseTemplate::new(503).set_body_json(serde_json::json!({
                "error_code": "TEMPORARILY_UNAVAILABLE",
                "message": "Service temporarily unavailable"
            })))
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri());
        let request = TestRequest {
            value: "test".to_string(),
        };
        let result: Result<TestResponse> = client.post(&client.statements_url(), &request).await;

        let err = result.unwrap_err();
        assert!(matches!(err, Error::SeaApi { http_status: 503, .. }));
        assert!(err.is_retryable());
    }

    #[tokio::test]
    async fn test_error_without_body() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-123"))
            .respond_with(ResponseTemplate::new(502))
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri());
        let result: Result<TestResponse> = client.get(&client.statement_url("stmt-123")).await;

        let err = result.unwrap_err();
        assert!(matches!(err, Error::SeaApi { http_status: 502, .. }));
        // Should still have a message even without response body
        assert!(err.to_string().contains("HTTP 502"));
    }

    #[tokio::test]
    async fn test_error_with_partial_body() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-123"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "message": "Something went wrong"
            })))
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri());
        let result: Result<TestResponse> = client.get(&client.statement_url("stmt-123")).await;

        let err = result.unwrap_err();
        assert!(err.to_string().contains("Something went wrong"));
    }

    #[tokio::test]
    async fn test_user_agent_header() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-123"))
            .and(header("user-agent", format!("adbc-driver-databricks/{}", DRIVER_VERSION).as_str()))
            .respond_with(ResponseTemplate::new(200).set_body_json(TestResponse {
                result: "ok".to_string(),
            }))
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri());
        let result: Result<TestResponse> = client.get(&client.statement_url("stmt-123")).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_delete_no_content() {
        let mock_server = MockServer::start().await;

        // DELETE can return 204 No Content
        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/session-456"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri());
        let result = client.delete(&client.session_url("session-456")).await;
        assert!(result.is_ok());
    }
}
