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
mod polling;
mod retry;

pub use error::{SeaError, SeaErrorCode, SeaErrorResponse};
pub use models::*;
pub use polling::{poll_until_complete, is_terminal_state, PollingConfig};
pub use retry::{retry_with_backoff, retry_with_backoff_and_retry_after, RetryConfig};

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
    /// Retry configuration for transient errors.
    pub retry_config: RetryConfig,
}

impl Default for SeaClientConfig {
    fn default() -> Self {
        Self {
            host: String::new(),
            token: String::new(),
            warehouse_id: String::new(),
            connect_timeout: Duration::from_secs(DEFAULT_CONNECT_TIMEOUT_SECS),
            read_timeout: Duration::from_secs(DEFAULT_READ_TIMEOUT_SECS),
            retry_config: RetryConfig::default(),
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

    /// Set the retry configuration.
    pub fn with_retry_config(mut self, retry_config: RetryConfig) -> Self {
        self.retry_config = retry_config;
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
///
/// # Retry Behavior
///
/// The client supports automatic retry with exponential backoff for transient
/// errors. Use `*_with_retry` methods to enable retry behavior.
#[derive(Debug)]
pub struct SeaClient {
    /// HTTP client with configured timeouts and headers.
    http_client: Client,
    /// Databricks workspace host URL.
    host: String,
    /// SQL Warehouse ID.
    warehouse_id: String,
    /// Retry configuration for transient errors.
    retry_config: RetryConfig,
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
            retry_config: config.retry_config,
        })
    }

    /// Get the retry configuration.
    pub fn retry_config(&self) -> &RetryConfig {
        &self.retry_config
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
        // Use DatabricksJDBCDriverOSS prefix for server-side feature compatibility
        // (e.g., INLINE_OR_EXTERNAL_LINKS disposition support).
        // This matches the C# driver's User-Agent format.
        let user_agent = format!("DatabricksJDBCDriverOSS/{} (ADBC)", DRIVER_VERSION);
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
    // HTTP Request Methods with Retry
    // =========================================================================

    /// Send a POST request with automatic retry for transient errors.
    ///
    /// This method wraps `post` with exponential backoff retry logic.
    /// It respects the `Retry-After` header for 429 responses.
    ///
    /// # Type Parameters
    ///
    /// * `Req` - The request body type (must implement `Serialize + Clone`)
    /// * `Resp` - The response body type (must implement `DeserializeOwned`)
    pub async fn post_with_retry<Req, Resp>(&self, url: &str, body: &Req) -> Result<Resp>
    where
        Req: Serialize + Clone,
        Resp: DeserializeOwned,
    {
        let url = url.to_string();
        let body = body.clone();

        retry_with_backoff_and_retry_after(&self.retry_config, || {
            let url = url.clone();
            let body = body.clone();
            async move {
                let result = self.post::<Req, Resp>(&url, &body).await;
                let retry_after = result.as_ref().err().and_then(|e| e.retry_after());
                (result, retry_after)
            }
        })
        .await
    }

    /// Send a GET request with automatic retry for transient errors.
    ///
    /// This method wraps `get` with exponential backoff retry logic.
    /// It respects the `Retry-After` header for 429 responses.
    ///
    /// # Type Parameters
    ///
    /// * `Resp` - The response body type (must implement `DeserializeOwned`)
    pub async fn get_with_retry<Resp>(&self, url: &str) -> Result<Resp>
    where
        Resp: DeserializeOwned,
    {
        let url = url.to_string();

        retry_with_backoff_and_retry_after(&self.retry_config, || {
            let url = url.clone();
            async move {
                let result = self.get::<Resp>(&url).await;
                let retry_after = result.as_ref().err().and_then(|e| e.retry_after());
                (result, retry_after)
            }
        })
        .await
    }

    /// Send a DELETE request with automatic retry for transient errors.
    ///
    /// This method wraps `delete` with exponential backoff retry logic.
    /// It respects the `Retry-After` header for 429 responses.
    pub async fn delete_with_retry(&self, url: &str) -> Result<()> {
        let url = url.to_string();

        retry_with_backoff_and_retry_after(&self.retry_config, || {
            let url = url.clone();
            async move {
                let result = self.delete(&url).await;
                let retry_after = result.as_ref().err().and_then(|e| e.retry_after());
                (result, retry_after)
            }
        })
        .await
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

    // =========================================================================
    // Session Management
    // =========================================================================

    /// Create a new session with the SQL Warehouse.
    ///
    /// Sessions maintain connection state with the SQL Warehouse and enable
    /// features like temporary tables and session-scoped configurations.
    ///
    /// # Arguments
    ///
    /// * `catalog` - Optional default catalog for the session
    /// * `schema` - Optional default schema for the session
    ///
    /// # Returns
    ///
    /// The session ID on success.
    ///
    /// # Errors
    ///
    /// Returns an error if the session creation fails.
    pub async fn create_session(
        &self,
        catalog: Option<String>,
        schema: Option<String>,
    ) -> Result<String> {
        let request = CreateSessionRequest {
            warehouse_id: self.warehouse_id.clone(),
            catalog,
            schema,
        };

        let response: SessionResponse = self.post(&self.sessions_url(), &request).await?;
        Ok(response.session_id)
    }

    /// Delete/terminate a session.
    ///
    /// This should be called when the connection is closed to clean up
    /// server-side resources.
    ///
    /// # Arguments
    ///
    /// * `session_id` - The session ID to delete
    ///
    /// # Errors
    ///
    /// Returns an error if the session deletion fails.
    pub async fn delete_session(&self, session_id: &str) -> Result<()> {
        self.delete(&self.session_url(session_id)).await
    }

    // =========================================================================
    // Session Management with Retry
    // =========================================================================

    /// Create a new session with automatic retry for transient errors.
    ///
    /// This is the recommended method for session creation as it handles
    /// transient failures gracefully.
    pub async fn create_session_with_retry(
        &self,
        catalog: Option<String>,
        schema: Option<String>,
    ) -> Result<String> {
        let request = CreateSessionRequest {
            warehouse_id: self.warehouse_id.clone(),
            catalog,
            schema,
        };

        let response: SessionResponse = self
            .post_with_retry(&self.sessions_url(), &request)
            .await?;
        Ok(response.session_id)
    }

    /// Delete/terminate a session with automatic retry for transient errors.
    ///
    /// This is the recommended method for session deletion as it handles
    /// transient failures gracefully.
    pub async fn delete_session_with_retry(&self, session_id: &str) -> Result<()> {
        self.delete_with_retry(&self.session_url(session_id)).await
    }

    // =========================================================================
    // Statement Execution
    // =========================================================================

    /// Execute a SQL statement.
    ///
    /// Sends the statement to the SQL Warehouse for execution. The response
    /// may contain inline results (for small result sets) or external links
    /// (for large result sets).
    ///
    /// # Arguments
    ///
    /// * `request` - The execute statement request with SQL and options
    ///
    /// # Returns
    ///
    /// The statement response containing the statement ID, status, and results.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request fails to send
    /// - The SQL is invalid
    /// - Authentication fails
    pub async fn execute_statement(
        &self,
        request: ExecuteStatementRequest,
    ) -> Result<StatementResponse> {
        self.post(&self.statements_url(), &request).await
    }

    /// Execute a SQL statement with automatic retry for transient errors.
    ///
    /// This is the recommended method for statement execution as it handles
    /// transient failures gracefully.
    pub async fn execute_statement_with_retry(
        &self,
        request: ExecuteStatementRequest,
    ) -> Result<StatementResponse> {
        self.post_with_retry(&self.statements_url(), &request).await
    }

    /// Get the status and results of a statement.
    ///
    /// Used to poll for statement completion or retrieve results after
    /// execution.
    ///
    /// # Arguments
    ///
    /// * `statement_id` - The statement ID returned from execute_statement
    ///
    /// # Returns
    ///
    /// The statement response with current status and any available results.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The statement ID is not found
    /// - Authentication fails
    pub async fn get_statement(&self, statement_id: &str) -> Result<StatementResponse> {
        self.get(&self.statement_url(statement_id)).await
    }

    /// Get statement status with automatic retry for transient errors.
    pub async fn get_statement_with_retry(&self, statement_id: &str) -> Result<StatementResponse> {
        self.get_with_retry(&self.statement_url(statement_id)).await
    }

    /// Cancel a running statement.
    ///
    /// # Arguments
    ///
    /// * `statement_id` - The statement ID to cancel
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The statement ID is not found
    /// - The statement is already completed
    pub async fn cancel_statement(&self, statement_id: &str) -> Result<()> {
        // Cancel uses POST to /statements/{id}/cancel
        let url = self.statement_cancel_url(statement_id);
        let _: serde_json::Value = self.post(&url, &serde_json::json!({})).await?;
        Ok(())
    }

    /// Cancel a running statement with automatic retry.
    pub async fn cancel_statement_with_retry(&self, statement_id: &str) -> Result<()> {
        let url = self.statement_cancel_url(statement_id);
        let _: serde_json::Value = self.post_with_retry(&url, &serde_json::json!({})).await?;
        Ok(())
    }

    /// Close a statement and release resources.
    ///
    /// # Arguments
    ///
    /// * `statement_id` - The statement ID to close
    pub async fn close_statement(&self, statement_id: &str) -> Result<()> {
        self.delete(&self.statement_url(statement_id)).await
    }

    /// Get a result chunk by index.
    ///
    /// Used to refresh external links when they expire.
    ///
    /// # Arguments
    ///
    /// * `statement_id` - The statement ID
    /// * `chunk_index` - The chunk index to retrieve
    ///
    /// # Returns
    ///
    /// The chunk response with refreshed external links.
    pub async fn get_chunk(&self, statement_id: &str, chunk_index: i32) -> Result<ChunkResponse> {
        self.get(&self.statement_chunk_url(statement_id, chunk_index)).await
    }

    // =========================================================================
    // Response Handling
    // =========================================================================

    /// Handle an error response by parsing the error body.
    async fn handle_error_response<T>(&self, response: reqwest::Response) -> Result<T> {
        let http_status = response.status().as_u16();

        // Try to extract retry-after header for rate limiting
        let retry_after_secs = response
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok());

        let retry_after = retry_after_secs.map(Duration::from_secs);

        // Try to parse the error response body
        let error_response: Option<SeaErrorResponse> = response.json().await.ok();

        let (code, message) = match &error_response {
            Some(resp) => {
                let code = resp.error_code.clone().unwrap_or_else(|| "UNKNOWN".to_string());
                let base_message = resp.message.clone().unwrap_or_else(|| format!("HTTP {}", http_status));
                // Include retry-after in message for visibility
                let message = if let Some(secs) = retry_after_secs {
                    format!("{} (retry after {} seconds)", base_message, secs)
                } else {
                    base_message
                };
                (code, message)
            }
            None => {
                let base_message = format!("HTTP {}", http_status);
                let message = if let Some(secs) = retry_after_secs {
                    format!("{} (retry after {} seconds)", base_message, secs)
                } else {
                    base_message
                };
                ("UNKNOWN".to_string(), message)
            }
        };

        Err(Error::sea_api_with_retry_after(code, message, http_status, retry_after))
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

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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

    // =========================================================================
    // Session Management Tests
    // =========================================================================

    #[tokio::test]
    async fn test_create_session_success() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .and(header("authorization", "Bearer test-token"))
            .and(header("content-type", "application/json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "session-abc-123"
            })))
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri());
        let session_id = client.create_session(None, None).await.unwrap();
        assert_eq!(session_id, "session-abc-123");
    }

    #[tokio::test]
    async fn test_create_session_with_catalog_and_schema() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .and(body_json(&serde_json::json!({
                "warehouse_id": "test-warehouse",
                "catalog": "main",
                "schema": "default"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "session-with-catalog"
            })))
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri());
        let session_id = client
            .create_session(Some("main".to_string()), Some("default".to_string()))
            .await
            .unwrap();
        assert_eq!(session_id, "session-with-catalog");
    }

    #[tokio::test]
    async fn test_create_session_without_optional_fields() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .and(body_json(&serde_json::json!({
                "warehouse_id": "test-warehouse"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "session-minimal"
            })))
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri());
        let session_id = client.create_session(None, None).await.unwrap();
        assert_eq!(session_id, "session-minimal");
    }

    #[tokio::test]
    async fn test_create_session_error_unauthenticated() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
                "error_code": "UNAUTHENTICATED",
                "message": "Invalid or expired token"
            })))
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri());
        let result = client.create_session(None, None).await;

        let err = result.unwrap_err();
        assert!(matches!(err, Error::SeaApi { http_status: 401, .. }));
    }

    #[tokio::test]
    async fn test_create_session_error_permission_denied() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
                "error_code": "PERMISSION_DENIED",
                "message": "User does not have access to warehouse"
            })))
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri());
        let result = client.create_session(None, None).await;

        let err = result.unwrap_err();
        assert!(matches!(err, Error::SeaApi { http_status: 403, .. }));
    }

    #[tokio::test]
    async fn test_delete_session_success() {
        let mock_server = MockServer::start().await;

        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/session-to-delete"))
            .and(header("authorization", "Bearer test-token"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri());
        let result = client.delete_session("session-to-delete").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_delete_session_204_no_content() {
        let mock_server = MockServer::start().await;

        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/session-204"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri());
        let result = client.delete_session("session-204").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_delete_session_not_found() {
        let mock_server = MockServer::start().await;

        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/nonexistent-session"))
            .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                "error_code": "NOT_FOUND",
                "message": "Session not found"
            })))
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri());
        let result = client.delete_session("nonexistent-session").await;

        let err = result.unwrap_err();
        assert!(matches!(err, Error::SeaApi { http_status: 404, .. }));
    }

    #[tokio::test]
    async fn test_session_lifecycle() {
        let mock_server = MockServer::start().await;

        // Create session
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "lifecycle-session"
            })))
            .mount(&mock_server)
            .await;

        // Delete session
        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/lifecycle-session"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri());

        // Create
        let session_id = client
            .create_session(Some("main".to_string()), Some("default".to_string()))
            .await
            .unwrap();
        assert_eq!(session_id, "lifecycle-session");

        // Delete
        client.delete_session(&session_id).await.unwrap();
    }

    // =========================================================================
    // Retry Integration Tests
    // =========================================================================

    /// Helper to create a SeaClient with custom retry config.
    fn create_test_client_with_retry(server_uri: &str, retry_config: RetryConfig) -> SeaClient {
        let config = SeaClientConfig::new(server_uri, "test-token", "test-warehouse")
            .with_retry_config(retry_config);
        SeaClient::new(config).unwrap()
    }

    #[tokio::test]
    async fn test_get_with_retry_succeeds_on_transient_error() {
        let mock_server = MockServer::start().await;

        // First request fails with 503, second succeeds
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-retry"))
            .respond_with(ResponseTemplate::new(503).set_body_json(serde_json::json!({
                "error_code": "TEMPORARILY_UNAVAILABLE",
                "message": "Service temporarily unavailable"
            })))
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-retry"))
            .respond_with(ResponseTemplate::new(200).set_body_json(TestResponse {
                result: "success after retry".to_string(),
            }))
            .mount(&mock_server)
            .await;

        let retry_config = RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_millis(10), // Short delay for tests
            max_delay: Duration::from_millis(100),
            jitter: 0.0,
        };
        let client = create_test_client_with_retry(&mock_server.uri(), retry_config);

        let response: TestResponse = client
            .get_with_retry(&client.statement_url("stmt-retry"))
            .await
            .unwrap();
        assert_eq!(response.result, "success after retry");
    }

    #[tokio::test]
    async fn test_post_with_retry_succeeds_on_rate_limit() {
        let mock_server = MockServer::start().await;

        // First request fails with 429, second succeeds
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(
                ResponseTemplate::new(429)
                    .insert_header("retry-after", "1")
                    .set_body_json(serde_json::json!({
                        "error_code": "REQUEST_LIMIT_EXCEEDED",
                        "message": "Rate limit exceeded"
                    })),
            )
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(ResponseTemplate::new(200).set_body_json(TestResponse {
                result: "success after rate limit".to_string(),
            }))
            .mount(&mock_server)
            .await;

        let retry_config = RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(100),
            jitter: 0.0,
        };
        let client = create_test_client_with_retry(&mock_server.uri(), retry_config);

        let request = TestRequest {
            value: "test".to_string(),
        };
        let response: TestResponse = client
            .post_with_retry(&client.statements_url(), &request)
            .await
            .unwrap();
        assert_eq!(response.result, "success after rate limit");
    }

    #[tokio::test]
    async fn test_get_with_retry_fails_on_non_retryable_error() {
        let mock_server = MockServer::start().await;

        // 400 errors are not retryable
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-bad"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error_code": "BAD_REQUEST",
                "message": "Invalid request"
            })))
            .expect(1) // Should only be called once
            .mount(&mock_server)
            .await;

        let retry_config = RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(100),
            jitter: 0.0,
        };
        let client = create_test_client_with_retry(&mock_server.uri(), retry_config);

        let result: Result<TestResponse> = client
            .get_with_retry(&client.statement_url("stmt-bad"))
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, Error::SeaApi { http_status: 400, .. }));
    }

    #[tokio::test]
    async fn test_delete_with_retry_succeeds_on_server_error() {
        let mock_server = MockServer::start().await;

        // First two requests fail with 500, third succeeds
        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/session-flaky"))
            .respond_with(ResponseTemplate::new(500).set_body_json(serde_json::json!({
                "error_code": "INTERNAL_ERROR",
                "message": "Internal server error"
            })))
            .up_to_n_times(2)
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/session-flaky"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let retry_config = RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(100),
            jitter: 0.0,
        };
        let client = create_test_client_with_retry(&mock_server.uri(), retry_config);

        let result = client
            .delete_with_retry(&client.session_url("session-flaky"))
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_with_retry_exhausts_retries() {
        let mock_server = MockServer::start().await;

        // Always fail with 503
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-always-fail"))
            .respond_with(ResponseTemplate::new(503).set_body_json(serde_json::json!({
                "error_code": "TEMPORARILY_UNAVAILABLE",
                "message": "Service temporarily unavailable"
            })))
            .expect(4) // Initial + 3 retries
            .mount(&mock_server)
            .await;

        let retry_config = RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_millis(5),
            max_delay: Duration::from_millis(50),
            jitter: 0.0,
        };
        let client = create_test_client_with_retry(&mock_server.uri(), retry_config);

        let result: Result<TestResponse> = client
            .get_with_retry(&client.statement_url("stmt-always-fail"))
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, Error::SeaApi { http_status: 503, .. }));
    }

    #[tokio::test]
    async fn test_create_session_with_retry_handles_transient_failure() {
        let mock_server = MockServer::start().await;

        // First request fails, second succeeds
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(503).set_body_json(serde_json::json!({
                "error_code": "TEMPORARILY_UNAVAILABLE",
                "message": "Warehouse starting"
            })))
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "session-after-retry"
            })))
            .mount(&mock_server)
            .await;

        let retry_config = RetryConfig {
            max_retries: 2,
            base_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(100),
            jitter: 0.0,
        };
        let client = create_test_client_with_retry(&mock_server.uri(), retry_config);

        let session_id = client.create_session_with_retry(None, None).await.unwrap();
        assert_eq!(session_id, "session-after-retry");
    }

    #[tokio::test]
    async fn test_retry_extracts_retry_after_header() {
        let mock_server = MockServer::start().await;

        // First request fails with 429 and Retry-After header
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-rate"))
            .respond_with(
                ResponseTemplate::new(429)
                    .insert_header("retry-after", "30")
                    .set_body_json(serde_json::json!({
                        "error_code": "REQUEST_LIMIT_EXCEEDED",
                        "message": "Rate limit exceeded"
                    })),
            )
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-rate"))
            .respond_with(ResponseTemplate::new(200).set_body_json(TestResponse {
                result: "ok".to_string(),
            }))
            .mount(&mock_server)
            .await;

        let retry_config = RetryConfig {
            max_retries: 2,
            base_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(100),
            jitter: 0.0,
        };
        let client = create_test_client_with_retry(&mock_server.uri(), retry_config);

        // Note: The Retry-After value (30s) is much larger than our test delays,
        // but since our test mock allows success on second try, it should work.
        // In production, the delay_with_retry_after would use the server's value.
        let result: Result<TestResponse> = client
            .get_with_retry(&client.statement_url("stmt-rate"))
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_no_retry_config_fails_immediately() {
        let mock_server = MockServer::start().await;

        // Fail with 503 (normally retryable)
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-no-retry"))
            .respond_with(ResponseTemplate::new(503).set_body_json(serde_json::json!({
                "error_code": "TEMPORARILY_UNAVAILABLE",
                "message": "Service temporarily unavailable"
            })))
            .expect(1) // Should only be called once with no retries
            .mount(&mock_server)
            .await;

        let retry_config = RetryConfig::no_retry();
        let client = create_test_client_with_retry(&mock_server.uri(), retry_config);

        let result: Result<TestResponse> = client
            .get_with_retry(&client.statement_url("stmt-no-retry"))
            .await;

        assert!(result.is_err());
    }
}
