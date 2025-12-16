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

//! SEA (Statement Execution API) client for Databricks.
//!
//! This module provides the HTTP client for communicating with the
//! Databricks SQL Statement Execution API.
//!
//! # Features
//!
//! - Bearer token authentication
//! - Configurable timeouts
//! - Automatic error parsing from SEA API responses
//! - URL helpers for all SEA API endpoints

pub mod error;
pub mod models;
pub mod retry;

use std::time::Duration;

use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE, USER_AGENT};
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::error::{Error, Result};

pub use error::ApiError;
pub use models::*;
pub use retry::{parse_retry_after, RetryConfig};

/// Driver version for User-Agent header.
const DRIVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Configuration for the SEA client.
///
/// This struct holds all configuration options for creating a [`SeaClient`].
/// Use [`Default::default()`] to get sensible defaults for timeouts.
#[derive(Debug, Clone)]
pub struct SeaClientConfig {
    /// Workspace host URL (e.g., "https://workspace.cloud.databricks.com").
    pub host: String,
    /// Personal Access Token for authentication.
    pub token: String,
    /// SQL Warehouse ID.
    pub warehouse_id: String,
    /// Connection timeout (default: 10 seconds).
    pub connect_timeout: Duration,
    /// Read timeout (default: 300 seconds / 5 minutes).
    pub read_timeout: Duration,
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

impl SeaClientConfig {
    /// Create a new configuration with the required fields.
    ///
    /// # Arguments
    ///
    /// * `host` - Workspace host URL
    /// * `token` - Personal Access Token
    /// * `warehouse_id` - SQL Warehouse ID
    pub fn new(
        host: impl Into<String>,
        token: impl Into<String>,
        warehouse_id: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            token: token.into(),
            warehouse_id: warehouse_id.into(),
            ..Default::default()
        }
    }

    /// Set the connection timeout.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set the read timeout.
    pub fn with_read_timeout(mut self, timeout: Duration) -> Self {
        self.read_timeout = timeout;
        self
    }
}

/// SEA REST API client for Databricks.
///
/// Handles all HTTP communication with the Databricks SQL Statement
/// Execution API. The client is configured with authentication credentials
/// and timeout settings, and provides methods for all SEA API endpoints.
///
/// # Thread Safety
///
/// `SeaClient` implements `Clone` and uses `reqwest::Client` internally,
/// which manages a connection pool. Cloning a `SeaClient` is cheap and
/// shares the underlying connection pool.
///
/// # Example
///
/// ```ignore
/// use adbc_databricks::client::{SeaClient, SeaClientConfig};
///
/// let config = SeaClientConfig::new(
///     "https://workspace.cloud.databricks.com",
///     "dapi...",
///     "abc123",
/// );
/// let client = SeaClient::new(config)?;
/// ```
#[derive(Debug, Clone)]
pub struct SeaClient {
    /// HTTP client with configured timeouts and headers.
    http_client: Client,
    /// Workspace host URL.
    host: String,
    /// Personal Access Token.
    token: String,
    /// SQL Warehouse ID.
    warehouse_id: String,
}

impl SeaClient {
    /// Create a new SEA client with the given configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Client configuration including host, token, and warehouse ID
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be built (e.g., invalid TLS configuration).
    pub fn new(config: SeaClientConfig) -> Result<Self> {
        let default_headers = Self::default_headers(&config.token)?;

        let http_client = Client::builder()
            .connect_timeout(config.connect_timeout)
            .timeout(config.read_timeout)
            .default_headers(default_headers)
            .build()
            .map_err(Error::Http)?;

        Ok(Self {
            http_client,
            host: config.host,
            token: config.token,
            warehouse_id: config.warehouse_id,
        })
    }

    /// Create default headers for all requests.
    ///
    /// Includes:
    /// - Authorization: Bearer <token>
    /// - Content-Type: application/json
    /// - User-Agent: adbc-driver-databricks/<version>
    fn default_headers(token: &str) -> Result<HeaderMap> {
        let mut headers = HeaderMap::new();

        // Authorization header with Bearer token
        let auth_value = format!("Bearer {}", token);
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&auth_value).map_err(|e| {
                Error::config(format!("Invalid authorization header value: {}", e))
            })?,
        );

        // Content-Type header
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );

        // User-Agent header
        let user_agent = format!("adbc-driver-databricks/{}", DRIVER_VERSION);
        headers.insert(
            USER_AGENT,
            HeaderValue::from_str(&user_agent).map_err(|e| {
                Error::config(format!("Invalid user-agent header value: {}", e))
            })?,
        );

        Ok(headers)
    }

    // ========================================================================
    // URL Helper Methods
    // ========================================================================

    /// Get the base URL for the SEA API.
    ///
    /// Returns the URL in the format: `{host}/api/2.0/sql`
    /// The host is normalized to remove any trailing slashes.
    pub fn base_url(&self) -> String {
        format!("{}/api/2.0/sql", self.host.trim_end_matches('/'))
    }

    /// Get the URL for statement operations (create new statement).
    ///
    /// Returns: `{base_url}/statements`
    pub fn statements_url(&self) -> String {
        format!("{}/statements", self.base_url())
    }

    /// Get the URL for a specific statement.
    ///
    /// Returns: `{base_url}/statements/{statement_id}`
    pub fn statement_url(&self, statement_id: &str) -> String {
        format!("{}/statements/{}", self.base_url(), statement_id)
    }

    /// Get the URL for cancelling a statement.
    ///
    /// Returns: `{base_url}/statements/{statement_id}/cancel`
    pub fn statement_cancel_url(&self, statement_id: &str) -> String {
        format!("{}/statements/{}/cancel", self.base_url(), statement_id)
    }

    /// Get the URL for fetching a result chunk.
    ///
    /// Returns: `{base_url}/statements/{statement_id}/result/chunks/{chunk_index}`
    pub fn chunk_url(&self, statement_id: &str, chunk_index: usize) -> String {
        format!(
            "{}/statements/{}/result/chunks/{}",
            self.base_url(),
            statement_id,
            chunk_index
        )
    }

    /// Get the URL for session operations (create new session).
    ///
    /// Returns: `{base_url}/sessions`
    pub fn sessions_url(&self) -> String {
        format!("{}/sessions", self.base_url())
    }

    /// Get the URL for a specific session.
    ///
    /// Returns: `{base_url}/sessions/{session_id}`
    pub fn session_url(&self, session_id: &str) -> String {
        format!("{}/sessions/{}", self.base_url(), session_id)
    }

    // ========================================================================
    // Accessor Methods
    // ========================================================================

    /// Get the warehouse ID.
    pub fn warehouse_id(&self) -> &str {
        &self.warehouse_id
    }

    /// Get the host URL.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Get the token (for debugging purposes only - be careful with logging).
    #[cfg(test)]
    pub fn token(&self) -> &str {
        &self.token
    }

    // ========================================================================
    // Generic HTTP Methods
    // ========================================================================

    /// Send a POST request with a JSON body and parse the JSON response.
    ///
    /// # Arguments
    ///
    /// * `url` - The full URL to send the request to
    /// * `body` - The request body to serialize as JSON
    ///
    /// # Type Parameters
    ///
    /// * `Req` - The request body type (must implement `Serialize`)
    /// * `Resp` - The response body type (must implement `DeserializeOwned`)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request fails (network error, timeout)
    /// - The response status is not successful (4xx, 5xx)
    /// - The response body cannot be parsed as JSON
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

    /// Send a GET request and parse the JSON response.
    ///
    /// # Arguments
    ///
    /// * `url` - The full URL to send the request to
    ///
    /// # Type Parameters
    ///
    /// * `Resp` - The response body type (must implement `DeserializeOwned`)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request fails (network error, timeout)
    /// - The response status is not successful (4xx, 5xx)
    /// - The response body cannot be parsed as JSON
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
    /// # Arguments
    ///
    /// * `url` - The full URL to send the request to
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request fails (network error, timeout)
    /// - The response status is not successful (4xx, 5xx)
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

    /// Send a DELETE request with query parameters.
    ///
    /// # Arguments
    ///
    /// * `url` - The full URL to send the request to
    /// * `params` - Query parameters to append to the URL
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request fails (network error, timeout)
    /// - The response status is not successful (4xx, 5xx)
    pub async fn delete_with_params<P>(&self, url: &str, params: &P) -> Result<()>
    where
        P: Serialize + ?Sized,
    {
        let response = self
            .http_client
            .delete(url)
            .query(params)
            .send()
            .await
            .map_err(Error::Http)?;

        if response.status().is_success() {
            Ok(())
        } else {
            self.handle_error_response(response).await
        }
    }

    /// Handle a successful or error response.
    ///
    /// If the response status is successful (2xx), parse the body as JSON.
    /// Otherwise, parse the error response and return an appropriate error.
    async fn handle_response<Resp>(&self, response: reqwest::Response) -> Result<Resp>
    where
        Resp: DeserializeOwned,
    {
        if response.status().is_success() {
            response.json().await.map_err(Error::Http)
        } else {
            self.handle_error_response(response).await
        }
    }

    /// Handle an error response from the SEA API.
    ///
    /// Parses the error body to extract the error code and message,
    /// then returns an appropriate [`Error::SeaApi`] error.
    /// For 429 responses, also extracts the Retry-After header if present.
    async fn handle_error_response<T>(&self, response: reqwest::Response) -> Result<T> {
        let http_status = response.status().as_u16();

        // Extract Retry-After header for 429 responses
        let retry_after = if http_status == 429 {
            response
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(parse_retry_after)
        } else {
            None
        };

        // Try to parse the error body as JSON
        let body: serde_json::Value = response
            .json()
            .await
            .unwrap_or_else(|_| serde_json::json!({}));

        // Extract error code and message from the response
        // SEA API error format: { "error_code": "...", "message": "..." }
        let code = body["error_code"]
            .as_str()
            .unwrap_or("UNKNOWN")
            .to_string();
        let message = body["message"]
            .as_str()
            .unwrap_or("Unknown error")
            .to_string();

        Err(Error::SeaApi {
            code,
            message,
            http_status,
            retry_after,
        })
    }

    // ========================================================================
    // SEA API Methods (Stubs for future implementation)
    // ========================================================================

    /// Execute a SQL statement.
    ///
    /// Sends a POST request to `/api/2.0/sql/statements/` with the statement
    /// execution request.
    ///
    /// # Arguments
    ///
    /// * `request` - The statement execution request
    ///
    /// # Returns
    ///
    /// The statement response containing the statement ID and initial status.
    pub async fn execute_statement(
        &self,
        request: &ExecuteStatementRequest,
    ) -> Result<StatementResponse> {
        let url = self.statements_url();
        self.post(&url, request).await
    }

    /// Get the status and result of a statement.
    ///
    /// Sends a GET request to `/api/2.0/sql/statements/{statement_id}`.
    ///
    /// # Arguments
    ///
    /// * `statement_id` - The ID of the statement to query
    ///
    /// # Returns
    ///
    /// The statement response containing the current status and any available results.
    pub async fn get_statement(&self, statement_id: &str) -> Result<StatementResponse> {
        let url = self.statement_url(statement_id);
        self.get(&url).await
    }

    /// Get a result chunk for a statement.
    ///
    /// Sends a GET request to `/api/2.0/sql/statements/{statement_id}/result/chunks/{chunk_index}`.
    ///
    /// # Arguments
    ///
    /// * `statement_id` - The ID of the statement
    /// * `chunk_index` - The index of the chunk to retrieve
    ///
    /// # Returns
    ///
    /// The chunk response containing external links to the chunk data.
    pub async fn get_chunk(&self, statement_id: &str, chunk_index: usize) -> Result<ChunkResponse> {
        let url = self.chunk_url(statement_id, chunk_index);
        self.get(&url).await
    }

    /// Cancel a running statement.
    ///
    /// Sends a POST request to `/api/2.0/sql/statements/{statement_id}/cancel`.
    ///
    /// # Arguments
    ///
    /// * `statement_id` - The ID of the statement to cancel
    pub async fn cancel_statement(&self, statement_id: &str) -> Result<()> {
        let url = self.statement_cancel_url(statement_id);
        // Cancel returns an empty response on success
        let _: serde_json::Value = self.post(&url, &serde_json::json!({})).await?;
        Ok(())
    }

    /// Close a statement and release resources.
    ///
    /// Sends a DELETE request to `/api/2.0/sql/statements/{statement_id}`.
    ///
    /// # Arguments
    ///
    /// * `statement_id` - The ID of the statement to close
    pub async fn close_statement(&self, statement_id: &str) -> Result<()> {
        let url = self.statement_url(statement_id);
        self.delete(&url).await
    }

    /// Create a new session.
    ///
    /// Sends a POST request to `/api/2.0/sql/sessions/` with the session
    /// creation request.
    ///
    /// # Arguments
    ///
    /// * `request` - The session creation request
    ///
    /// # Returns
    ///
    /// The session response containing the new session ID.
    pub async fn create_session(&self, request: &CreateSessionRequest) -> Result<SessionResponse> {
        let url = self.sessions_url();
        self.post(&url, request).await
    }

    /// Delete a session.
    ///
    /// Sends a DELETE request to `/api/2.0/sql/sessions/{session_id}` with
    /// the warehouse_id as a query parameter.
    ///
    /// # Arguments
    ///
    /// * `session_id` - The ID of the session to delete
    pub async fn delete_session(&self, session_id: &str) -> Result<()> {
        let url = self.session_url(session_id);
        let params = [("warehouse_id", &self.warehouse_id)];
        self.delete_with_params(&url, &params).await
    }

    // ========================================================================
    // Statement Polling Methods
    // ========================================================================

    /// Poll a statement until it reaches a terminal state.
    ///
    /// This method polls the statement status with exponential backoff until
    /// the statement reaches a terminal state (SUCCEEDED, FAILED, CANCELED, or CLOSED).
    ///
    /// The polling intervals follow exponential backoff:
    /// - Initial interval: 1 second
    /// - Maximum interval: 10 seconds
    /// - Backoff multiplier: 2x
    ///
    /// # Arguments
    ///
    /// * `statement_id` - The ID of the statement to poll
    /// * `max_wait` - Maximum time to wait for completion. If `None`, defaults to 5 minutes.
    ///
    /// # Returns
    ///
    /// The final statement response when the statement reaches a terminal state.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The statement fails (returns [`Error::StatementFailed`])
    /// - The statement is canceled (returns [`Error::StatementFailed`])
    /// - The statement is closed unexpectedly (returns [`Error::StatementFailed`])
    /// - The timeout is reached (returns [`Error::Timeout`])
    /// - A network or API error occurs
    ///
    /// # Example
    ///
    /// ```ignore
    /// use std::time::Duration;
    ///
    /// let response = client.poll_until_complete(
    ///     "stmt-123",
    ///     Some(Duration::from_secs(60)),
    /// ).await?;
    /// ```
    pub async fn poll_until_complete(
        &self,
        statement_id: &str,
        max_wait: Option<Duration>,
    ) -> Result<StatementResponse> {
        let start = std::time::Instant::now();
        let max_wait = max_wait.unwrap_or(Duration::from_secs(300)); // 5 min default

        let mut poll_interval = Duration::from_secs(1);
        let max_poll_interval = Duration::from_secs(10);

        loop {
            let response = self.get_statement(statement_id).await?;

            match response.status.state {
                StatementState::Succeeded => return Ok(response),
                StatementState::Failed => {
                    let error_msg = response
                        .status
                        .error
                        .as_ref()
                        .map(|e| {
                            format!(
                                "{}: {}",
                                e.error_code.as_deref().unwrap_or("UNKNOWN"),
                                e.message.as_deref().unwrap_or("Statement failed")
                            )
                        })
                        .unwrap_or_else(|| "Statement failed".to_string());
                    return Err(Error::statement_failed(error_msg));
                }
                StatementState::Canceled => {
                    return Err(Error::statement_failed("Statement was canceled"));
                }
                StatementState::Closed => {
                    return Err(Error::statement_failed("Statement was closed"));
                }
                StatementState::Pending | StatementState::Running => {
                    // Check timeout
                    if start.elapsed() >= max_wait {
                        return Err(Error::Timeout);
                    }

                    // Sleep with exponential backoff
                    tokio::time::sleep(poll_interval).await;
                    poll_interval = (poll_interval * 2).min(max_poll_interval);
                }
            }
        }
    }

    /// Execute a SQL statement and wait for completion.
    ///
    /// This is a convenience method that combines [`execute_statement`] with
    /// [`poll_until_complete`]. It first executes the statement with an initial
    /// wait timeout, and if the statement doesn't complete immediately, it polls
    /// until completion.
    ///
    /// Note: The SEA API does not allow setting `session_id` and `catalog`/`schema`
    /// at the same time. If you need to specify catalog/schema context, create the
    /// session with the desired catalog/schema, and then use this method with just
    /// the session_id.
    ///
    /// # Arguments
    ///
    /// * `session_id` - The session ID to use for execution
    /// * `sql` - The SQL statement to execute
    /// * `max_wait` - Maximum time to wait for completion. If `None`, defaults to 5 minutes.
    /// * `row_limit` - Optional limit on the number of rows returned
    /// * `byte_limit` - Optional limit on the response size in bytes
    ///
    /// # Returns
    ///
    /// The statement response with the completed results.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The statement fails to execute
    /// - The statement fails during execution
    /// - The timeout is reached
    ///
    /// # Example
    ///
    /// ```ignore
    /// use std::time::Duration;
    ///
    /// let response = client.execute_and_wait(
    ///     "session-123",
    ///     "SELECT * FROM my_table",
    ///     Some(Duration::from_secs(60)),
    ///     Some(1000),
    ///     None,
    /// ).await?;
    /// ```
    pub async fn execute_and_wait(
        &self,
        session_id: &str,
        sql: &str,
        max_wait: Option<Duration>,
        row_limit: Option<i64>,
        byte_limit: Option<i64>,
    ) -> Result<StatementResponse> {
        // Build the execute request
        // Note: catalog/schema cannot be set with session_id per SEA API constraints
        let mut request = ExecuteStatementRequest::new(&self.warehouse_id, sql)
            .with_session_id(session_id)
            .with_wait_timeout("10s"); // Initial wait for fast queries

        if let Some(limit) = row_limit {
            request = request.with_row_limit(limit);
        }
        if let Some(limit) = byte_limit {
            request = request.with_byte_limit(limit);
        }

        // Execute the statement
        let response = self.execute_statement(&request).await?;

        // Check if already completed
        match response.status.state {
            StatementState::Succeeded => Ok(response),
            StatementState::Failed => {
                let error_msg = response
                    .status
                    .error
                    .as_ref()
                    .map(|e| {
                        format!(
                            "{}: {}",
                            e.error_code.as_deref().unwrap_or("UNKNOWN"),
                            e.message.as_deref().unwrap_or("Statement failed")
                        )
                    })
                    .unwrap_or_else(|| "Statement failed".to_string());
                Err(Error::statement_failed(error_msg))
            }
            StatementState::Canceled => {
                Err(Error::statement_failed("Statement was canceled"))
            }
            StatementState::Closed => {
                Err(Error::statement_failed("Statement was closed"))
            }
            StatementState::Pending | StatementState::Running => {
                // Poll until complete
                self.poll_until_complete(&response.statement_id, max_wait).await
            }
        }
    }

    // ========================================================================
    // Retry Logic
    // ========================================================================

    /// Execute an async operation with retry logic.
    ///
    /// This method wraps an async operation and automatically retries it
    /// on transient errors (as determined by [`Error::is_retryable`]).
    /// It uses exponential backoff with jitter to calculate delays between
    /// retries.
    ///
    /// # Arguments
    ///
    /// * `retry_config` - Configuration for retry behavior
    /// * `operation` - A closure that returns a future producing a `Result<T>`
    ///
    /// # Returns
    ///
    /// The successful result, or the last error if all retries are exhausted.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use adbc_databricks::client::{SeaClient, RetryConfig};
    ///
    /// let client = SeaClient::new(config)?;
    /// let retry_config = RetryConfig::default();
    ///
    /// let result = client.with_retry(&retry_config, || async {
    ///     client.get_statement("stmt-123").await
    /// }).await?;
    /// ```
    ///
    /// # Retry Behavior
    ///
    /// - Retries only on errors where [`Error::is_retryable`] returns `true`
    /// - Delays between retries follow exponential backoff with jitter
    /// - For 429 errors with Retry-After header, uses the server-specified delay
    /// - Stops retrying after `max_retries` attempts
    pub async fn with_retry<F, Fut, T>(&self, retry_config: &RetryConfig, mut operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let mut last_error = None;

        for attempt in 0..=retry_config.max_retries {
            match operation().await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    // Check if we should retry
                    if !e.is_retryable() || attempt == retry_config.max_retries {
                        return Err(e);
                    }

                    // Calculate delay - use Retry-After if available, otherwise exponential backoff
                    let delay = if let Some(retry_after) = e.retry_after() {
                        retry_config.delay_with_retry_after(retry_after)
                    } else {
                        retry_config.delay_for_attempt(attempt)
                    };

                    tokio::time::sleep(delay).await;
                    last_error = Some(e);
                }
            }
        }

        // This should never be reached because we return Err in the loop
        // when attempt == retry_config.max_retries, but we need this for
        // the type checker when max_retries is 0
        Err(last_error.expect("No error recorded but retry loop exited"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_client() -> SeaClient {
        SeaClient::new(SeaClientConfig {
            host: "https://workspace.cloud.databricks.com".into(),
            token: "test_token".into(),
            warehouse_id: "abc123".into(),
            ..Default::default()
        })
        .unwrap()
    }

    // ========================================================================
    // Configuration Tests
    // ========================================================================

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
        let config = SeaClientConfig::new("https://host.com", "token123", "wh456");
        assert_eq!(config.host, "https://host.com");
        assert_eq!(config.token, "token123");
        assert_eq!(config.warehouse_id, "wh456");
        assert_eq!(config.connect_timeout, Duration::from_secs(10));
        assert_eq!(config.read_timeout, Duration::from_secs(300));
    }

    #[test]
    fn test_sea_client_config_with_timeouts() {
        let config = SeaClientConfig::new("https://host.com", "token", "wh")
            .with_connect_timeout(Duration::from_secs(30))
            .with_read_timeout(Duration::from_secs(600));

        assert_eq!(config.connect_timeout, Duration::from_secs(30));
        assert_eq!(config.read_timeout, Duration::from_secs(600));
    }

    #[test]
    fn test_sea_client_instantiation() {
        let client = create_test_client();
        assert_eq!(client.host(), "https://workspace.cloud.databricks.com");
        assert_eq!(client.token(), "test_token");
        assert_eq!(client.warehouse_id(), "abc123");
    }

    // ========================================================================
    // URL Construction Tests
    // ========================================================================

    #[test]
    fn test_base_url_construction() {
        let client = create_test_client();
        assert_eq!(
            client.base_url(),
            "https://workspace.cloud.databricks.com/api/2.0/sql"
        );
    }

    #[test]
    fn test_base_url_with_trailing_slash() {
        let client = SeaClient::new(SeaClientConfig {
            host: "https://workspace.cloud.databricks.com/".into(),
            token: "token".into(),
            warehouse_id: "abc123".into(),
            ..Default::default()
        })
        .unwrap();

        assert_eq!(
            client.base_url(),
            "https://workspace.cloud.databricks.com/api/2.0/sql"
        );
    }

    #[test]
    fn test_base_url_with_multiple_trailing_slashes() {
        let client = SeaClient::new(SeaClientConfig {
            host: "https://workspace.cloud.databricks.com///".into(),
            token: "token".into(),
            warehouse_id: "abc123".into(),
            ..Default::default()
        })
        .unwrap();

        assert_eq!(
            client.base_url(),
            "https://workspace.cloud.databricks.com/api/2.0/sql"
        );
    }

    #[test]
    fn test_statements_url() {
        let client = create_test_client();
        assert_eq!(
            client.statements_url(),
            "https://workspace.cloud.databricks.com/api/2.0/sql/statements"
        );
    }

    #[test]
    fn test_statement_url() {
        let client = create_test_client();
        assert_eq!(
            client.statement_url("stmt-123"),
            "https://workspace.cloud.databricks.com/api/2.0/sql/statements/stmt-123"
        );
    }

    #[test]
    fn test_statement_cancel_url() {
        let client = create_test_client();
        assert_eq!(
            client.statement_cancel_url("stmt-123"),
            "https://workspace.cloud.databricks.com/api/2.0/sql/statements/stmt-123/cancel"
        );
    }

    #[test]
    fn test_chunk_url() {
        let client = create_test_client();
        assert_eq!(
            client.chunk_url("stmt-123", 5),
            "https://workspace.cloud.databricks.com/api/2.0/sql/statements/stmt-123/result/chunks/5"
        );
    }

    #[test]
    fn test_sessions_url() {
        let client = create_test_client();
        assert_eq!(
            client.sessions_url(),
            "https://workspace.cloud.databricks.com/api/2.0/sql/sessions"
        );
    }

    #[test]
    fn test_session_url() {
        let client = create_test_client();
        assert_eq!(
            client.session_url("sess-456"),
            "https://workspace.cloud.databricks.com/api/2.0/sql/sessions/sess-456"
        );
    }

    // ========================================================================
    // Header Tests
    // ========================================================================

    #[test]
    fn test_default_headers() {
        let headers = SeaClient::default_headers("test_token").unwrap();

        assert!(headers.contains_key(AUTHORIZATION));
        assert!(headers.contains_key(CONTENT_TYPE));
        assert!(headers.contains_key(USER_AGENT));

        assert_eq!(
            headers.get(AUTHORIZATION).unwrap().to_str().unwrap(),
            "Bearer test_token"
        );
        assert_eq!(
            headers.get(CONTENT_TYPE).unwrap().to_str().unwrap(),
            "application/json"
        );
        assert!(headers
            .get(USER_AGENT)
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("adbc-driver-databricks/"));
    }

    // ========================================================================
    // Clone Tests
    // ========================================================================

    #[test]
    fn test_sea_client_clone() {
        let client1 = create_test_client();
        let client2 = client1.clone();

        assert_eq!(client1.host(), client2.host());
        assert_eq!(client1.warehouse_id(), client2.warehouse_id());
    }

    // ========================================================================
    // Retry Logic Tests
    // ========================================================================

    #[tokio::test]
    async fn test_retry_success_on_first_attempt() {
        let client = create_test_client();
        let retry_config = RetryConfig::default();

        let call_count = std::sync::atomic::AtomicU32::new(0);

        let result = client
            .with_retry(&retry_config, || {
                call_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async { Ok::<_, Error>(42) }
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_retry_success_after_transient_failures() {
        let client = create_test_client();
        let retry_config = RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_millis(10), // Short delay for tests
            max_delay: Duration::from_millis(100),
            jitter: 0.0,
        };

        let call_count = std::sync::atomic::AtomicU32::new(0);

        let result = client
            .with_retry(&retry_config, || {
                let count = call_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async move {
                    if count < 2 {
                        // First two attempts fail with retryable error
                        Err(Error::SeaApi {
                            code: "TEMPORARILY_UNAVAILABLE".into(),
                            message: "Service unavailable".into(),
                            http_status: 503,
                            retry_after: None,
                        })
                    } else {
                        // Third attempt succeeds
                        Ok(42)
                    }
                }
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_retry_exhausted() {
        let client = create_test_client();
        let retry_config = RetryConfig {
            max_retries: 2,
            base_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(100),
            jitter: 0.0,
        };

        let call_count = std::sync::atomic::AtomicU32::new(0);

        let result = client
            .with_retry(&retry_config, || {
                call_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async {
                    Err::<i32, _>(Error::SeaApi {
                        code: "INTERNAL_ERROR".into(),
                        message: "Server error".into(),
                        http_status: 500,
                        retry_after: None,
                    })
                }
            })
            .await;

        assert!(result.is_err());
        // 1 initial + 2 retries = 3 total calls
        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_non_retryable_fails_immediately() {
        let client = create_test_client();
        let retry_config = RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(100),
            jitter: 0.0,
        };

        let call_count = std::sync::atomic::AtomicU32::new(0);

        let result = client
            .with_retry(&retry_config, || {
                call_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async {
                    // 400 is not retryable
                    Err::<i32, _>(Error::SeaApi {
                        code: "BAD_REQUEST".into(),
                        message: "Invalid SQL".into(),
                        http_status: 400,
                        retry_after: None,
                    })
                }
            })
            .await;

        assert!(result.is_err());
        // Should fail immediately without retries
        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_retry_with_retry_after_header() {
        let client = create_test_client();
        let retry_config = RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_millis(100), // This should be overridden
            max_delay: Duration::from_millis(500),
            jitter: 0.0,
        };

        let call_count = std::sync::atomic::AtomicU32::new(0);
        let start = std::time::Instant::now();

        let result = client
            .with_retry(&retry_config, || {
                let count = call_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async move {
                    if count < 1 {
                        // First attempt fails with 429 and Retry-After
                        Err(Error::SeaApi {
                            code: "REQUEST_LIMIT_EXCEEDED".into(),
                            message: "Rate limited".into(),
                            http_status: 429,
                            retry_after: Some(Duration::from_millis(50)), // Short delay for test
                        })
                    } else {
                        Ok(42)
                    }
                }
            })
            .await;

        let elapsed = start.elapsed();

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 2);

        // Should have waited approximately 50ms (the Retry-After value)
        // not 100ms (the base_delay)
        assert!(elapsed >= Duration::from_millis(40), "Elapsed: {:?}", elapsed);
        assert!(elapsed < Duration::from_millis(200), "Elapsed: {:?}", elapsed);
    }

    #[tokio::test]
    async fn test_retry_no_retry_config() {
        let client = create_test_client();
        let retry_config = RetryConfig::no_retry();

        let call_count = std::sync::atomic::AtomicU32::new(0);

        let result = client
            .with_retry(&retry_config, || {
                call_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async {
                    Err::<i32, _>(Error::SeaApi {
                        code: "INTERNAL_ERROR".into(),
                        message: "Server error".into(),
                        http_status: 500,
                        retry_after: None,
                    })
                }
            })
            .await;

        assert!(result.is_err());
        // With max_retries = 0, should only call once
        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    // ========================================================================
    // Wiremock Integration Tests for execute_statement
    // ========================================================================

    mod wiremock_tests {
        use super::*;
        use wiremock::matchers::{body_partial_json, header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        /// Helper function to create a test client pointing to a mock server.
        fn create_mock_client(mock_server_uri: &str) -> SeaClient {
            SeaClient::new(SeaClientConfig {
                host: mock_server_uri.into(),
                token: "test_token".into(),
                warehouse_id: "test_warehouse".into(),
                ..Default::default()
            })
            .expect("Failed to create mock client")
        }

        #[tokio::test]
        async fn test_execute_statement_success_immediate() {
            let mock_server = MockServer::start().await;

            Mock::given(method("POST"))
                .and(path("/api/2.0/sql/statements"))
                .and(header("Authorization", "Bearer test_token"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "statement_id": "stmt-12345",
                    "status": {
                        "state": "SUCCEEDED"
                    },
                    "manifest": {
                        "format": "ARROW_STREAM",
                        "schema": {
                            "column_count": 1,
                            "columns": [
                                {"name": "1", "type_name": "INT", "type_text": "INT", "position": 0}
                            ]
                        },
                        "total_chunk_count": 1,
                        "total_row_count": 1,
                        "total_byte_count": 100,
                        "truncated": false
                    },
                    "result": {
                        "data_array": [[1]],
                        "row_count": 1,
                        "byte_count": 100
                    }
                })))
                .mount(&mock_server)
                .await;

            let client = create_mock_client(&mock_server.uri());
            let request = ExecuteStatementRequest::new("test_warehouse", "SELECT 1")
                .with_session_id("session-123");

            let response = client.execute_statement(&request).await;

            assert!(response.is_ok(), "Expected success, got {:?}", response);
            let response = response.unwrap();

            assert_eq!(response.statement_id, "stmt-12345");
            assert_eq!(response.status.state, StatementState::Succeeded);
            assert!(response.manifest.is_some());
            assert!(response.result.is_some());

            let result = response.result.unwrap();
            assert!(result.data_array.is_some());
            let data_array = result.data_array.unwrap();
            assert_eq!(data_array.len(), 1);
            assert_eq!(data_array[0][0].as_i64(), Some(1));
        }

        #[tokio::test]
        async fn test_execute_statement_returns_pending() {
            let mock_server = MockServer::start().await;

            Mock::given(method("POST"))
                .and(path("/api/2.0/sql/statements"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "statement_id": "stmt-async-456",
                    "status": {
                        "state": "PENDING"
                    }
                })))
                .mount(&mock_server)
                .await;

            let client = create_mock_client(&mock_server.uri());
            let request = ExecuteStatementRequest::new("test_warehouse", "SELECT * FROM large_table");

            let response = client.execute_statement(&request).await.unwrap();

            assert_eq!(response.statement_id, "stmt-async-456");
            assert_eq!(response.status.state, StatementState::Pending);
            assert!(response.manifest.is_none());
            assert!(response.result.is_none());
        }

        #[tokio::test]
        async fn test_execute_statement_returns_running() {
            let mock_server = MockServer::start().await;

            Mock::given(method("POST"))
                .and(path("/api/2.0/sql/statements"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "statement_id": "stmt-running-789",
                    "status": {
                        "state": "RUNNING"
                    }
                })))
                .mount(&mock_server)
                .await;

            let client = create_mock_client(&mock_server.uri());
            let request = ExecuteStatementRequest::new("test_warehouse", "SELECT * FROM medium_table")
                .with_wait_timeout("1s");

            let response = client.execute_statement(&request).await.unwrap();

            assert_eq!(response.statement_id, "stmt-running-789");
            assert_eq!(response.status.state, StatementState::Running);
        }

        #[tokio::test]
        async fn test_execute_statement_with_inline_result() {
            let mock_server = MockServer::start().await;

            Mock::given(method("POST"))
                .and(path("/api/2.0/sql/statements"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "statement_id": "stmt-inline",
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
                        "total_row_count": 3,
                        "total_byte_count": 500
                    },
                    "result": {
                        "data_array": [
                            [1, "Alice"],
                            [2, "Bob"],
                            [3, "Charlie"]
                        ],
                        "row_count": 3,
                        "byte_count": 500
                    }
                })))
                .mount(&mock_server)
                .await;

            let client = create_mock_client(&mock_server.uri());
            let request =
                ExecuteStatementRequest::new("test_warehouse", "SELECT id, name FROM users");

            let response = client.execute_statement(&request).await.unwrap();

            assert_eq!(response.status.state, StatementState::Succeeded);

            let result = response.result.unwrap();
            assert!(result.data_array.is_some());
            assert!(result.external_links.is_none());

            let data = result.data_array.unwrap();
            assert_eq!(data.len(), 3);
            assert_eq!(data[0][0].as_i64(), Some(1));
            assert_eq!(data[0][1].as_str(), Some("Alice"));
            assert_eq!(data[2][0].as_i64(), Some(3));
            assert_eq!(data[2][1].as_str(), Some("Charlie"));
        }

        #[tokio::test]
        async fn test_execute_statement_with_external_links() {
            let mock_server = MockServer::start().await;

            Mock::given(method("POST"))
                .and(path("/api/2.0/sql/statements"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "statement_id": "stmt-external",
                    "status": {
                        "state": "SUCCEEDED"
                    },
                    "manifest": {
                        "format": "ARROW_STREAM",
                        "schema": {
                            "column_count": 1,
                            "columns": [
                                {"name": "data", "type_name": "STRING", "type_text": "STRING", "position": 0}
                            ]
                        },
                        "total_chunk_count": 2,
                        "total_row_count": 100000,
                        "total_byte_count": 5000000,
                        "truncated": false
                    },
                    "result": {
                        "external_links": [
                            {
                                "chunk_index": 0,
                                "external_link": "https://storage.example.com/chunk0?token=abc123",
                                "expiration": "2025-12-31T23:59:59Z",
                                "row_offset": 0,
                                "row_count": 50000,
                                "byte_count": 2500000
                            },
                            {
                                "chunk_index": 1,
                                "external_link": "https://storage.example.com/chunk1?token=def456",
                                "expiration": "2025-12-31T23:59:59Z",
                                "row_offset": 50000,
                                "row_count": 50000,
                                "byte_count": 2500000
                            }
                        ]
                    }
                })))
                .mount(&mock_server)
                .await;

            let client = create_mock_client(&mock_server.uri());
            let request = ExecuteStatementRequest::new("test_warehouse", "SELECT * FROM huge_table");

            let response = client.execute_statement(&request).await.unwrap();

            assert_eq!(response.status.state, StatementState::Succeeded);

            let manifest = response.manifest.unwrap();
            assert_eq!(manifest.total_chunk_count, Some(2));
            assert_eq!(manifest.total_row_count, Some(100000));

            let result = response.result.unwrap();
            assert!(result.data_array.is_none());
            assert!(result.external_links.is_some());

            let links = result.external_links.unwrap();
            assert_eq!(links.len(), 2);

            assert_eq!(links[0].chunk_index, 0);
            assert!(links[0].external_link.contains("chunk0"));
            assert_eq!(links[0].row_offset, Some(0));
            assert_eq!(links[0].row_count, Some(50000));

            assert_eq!(links[1].chunk_index, 1);
            assert!(links[1].external_link.contains("chunk1"));
            assert_eq!(links[1].row_offset, Some(50000));
        }

        #[tokio::test]
        async fn test_execute_statement_failed() {
            let mock_server = MockServer::start().await;

            Mock::given(method("POST"))
                .and(path("/api/2.0/sql/statements"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "statement_id": "stmt-failed",
                    "status": {
                        "state": "FAILED",
                        "error": {
                            "error_code": "SYNTAX_ERROR",
                            "message": "Syntax error at position 7: expected expression"
                        }
                    }
                })))
                .mount(&mock_server)
                .await;

            let client = create_mock_client(&mock_server.uri());
            let request =
                ExecuteStatementRequest::new("test_warehouse", "SELEC * FROM table"); // Intentional typo

            let response = client.execute_statement(&request).await.unwrap();

            assert_eq!(response.statement_id, "stmt-failed");
            assert_eq!(response.status.state, StatementState::Failed);
            assert!(response.status.error.is_some());

            let error = response.status.error.unwrap();
            assert_eq!(error.error_code, Some("SYNTAX_ERROR".to_string()));
            assert!(error.message.unwrap().contains("Syntax error"));
        }

        #[tokio::test]
        async fn test_execute_statement_http_error_401() {
            let mock_server = MockServer::start().await;

            Mock::given(method("POST"))
                .and(path("/api/2.0/sql/statements"))
                .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
                    "error_code": "UNAUTHENTICATED",
                    "message": "Invalid or missing authentication token"
                })))
                .mount(&mock_server)
                .await;

            let client = create_mock_client(&mock_server.uri());
            let request = ExecuteStatementRequest::new("test_warehouse", "SELECT 1");

            let result = client.execute_statement(&request).await;

            assert!(result.is_err());
            let err = result.unwrap_err();

            match err {
                Error::SeaApi {
                    code,
                    http_status,
                    message,
                    ..
                } => {
                    assert_eq!(http_status, 401);
                    assert_eq!(code, "UNAUTHENTICATED");
                    assert!(message.contains("authentication"));
                }
                _ => panic!("Expected SeaApi error, got {:?}", err),
            }
        }

        #[tokio::test]
        async fn test_execute_statement_http_error_404() {
            let mock_server = MockServer::start().await;

            Mock::given(method("POST"))
                .and(path("/api/2.0/sql/statements"))
                .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                    "error_code": "NOT_FOUND",
                    "message": "Warehouse not found: invalid_warehouse"
                })))
                .mount(&mock_server)
                .await;

            let client = create_mock_client(&mock_server.uri());
            let request = ExecuteStatementRequest::new("invalid_warehouse", "SELECT 1");

            let result = client.execute_statement(&request).await;

            assert!(result.is_err());
            let err = result.unwrap_err();

            match err {
                Error::SeaApi { http_status, .. } => {
                    assert_eq!(http_status, 404);
                }
                _ => panic!("Expected SeaApi error, got {:?}", err),
            }
        }

        #[tokio::test]
        async fn test_execute_statement_http_error_429_rate_limited() {
            let mock_server = MockServer::start().await;

            Mock::given(method("POST"))
                .and(path("/api/2.0/sql/statements"))
                .respond_with(
                    ResponseTemplate::new(429)
                        .insert_header("Retry-After", "30")
                        .set_body_json(serde_json::json!({
                            "error_code": "REQUEST_LIMIT_EXCEEDED",
                            "message": "Rate limit exceeded. Please retry after 30 seconds."
                        })),
                )
                .mount(&mock_server)
                .await;

            let client = create_mock_client(&mock_server.uri());
            let request = ExecuteStatementRequest::new("test_warehouse", "SELECT 1");

            let result = client.execute_statement(&request).await;

            assert!(result.is_err());
            let err = result.unwrap_err();

            match err {
                Error::SeaApi {
                    code,
                    http_status,
                    retry_after,
                    ..
                } => {
                    assert_eq!(http_status, 429);
                    assert_eq!(code, "REQUEST_LIMIT_EXCEEDED");
                    assert!(retry_after.is_some());
                    assert_eq!(retry_after.unwrap(), Duration::from_secs(30));
                }
                _ => panic!("Expected SeaApi error, got {:?}", err),
            }
        }

        #[tokio::test]
        async fn test_execute_statement_http_error_500() {
            let mock_server = MockServer::start().await;

            Mock::given(method("POST"))
                .and(path("/api/2.0/sql/statements"))
                .respond_with(ResponseTemplate::new(500).set_body_json(serde_json::json!({
                    "error_code": "INTERNAL_ERROR",
                    "message": "Internal server error"
                })))
                .mount(&mock_server)
                .await;

            let client = create_mock_client(&mock_server.uri());
            let request = ExecuteStatementRequest::new("test_warehouse", "SELECT 1");

            let result = client.execute_statement(&request).await;

            assert!(result.is_err());
            let err = result.unwrap_err();

            match err {
                Error::SeaApi { http_status, .. } => {
                    assert_eq!(http_status, 500);
                }
                _ => panic!("Expected SeaApi error, got {:?}", err),
            }
        }

        #[tokio::test]
        async fn test_execute_statement_request_body_contains_required_fields() {
            let mock_server = MockServer::start().await;

            // Use body_partial_json matcher to verify request structure contains expected fields
            Mock::given(method("POST"))
                .and(path("/api/2.0/sql/statements"))
                .and(body_partial_json(serde_json::json!({
                    "statement": "SELECT 1",
                    "warehouse_id": "test_warehouse",
                    "session_id": "session-abc",
                    "wait_timeout": "10s",
                    "on_wait_timeout": "CONTINUE",
                    "disposition": "EXTERNAL_LINKS",
                    "format": "ARROW_STREAM"
                })))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "statement_id": "stmt-verified",
                    "status": { "state": "SUCCEEDED" }
                })))
                .mount(&mock_server)
                .await;

            let client = create_mock_client(&mock_server.uri());
            let request = ExecuteStatementRequest::new("test_warehouse", "SELECT 1")
                .with_session_id("session-abc");

            let result = client.execute_statement(&request).await;
            assert!(result.is_ok(), "Request should match expected schema: {:?}", result);
        }

        #[tokio::test]
        async fn test_execute_statement_with_all_optional_fields() {
            let mock_server = MockServer::start().await;

            Mock::given(method("POST"))
                .and(path("/api/2.0/sql/statements"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "statement_id": "stmt-full",
                    "status": { "state": "SUCCEEDED" }
                })))
                .mount(&mock_server)
                .await;

            let client = create_mock_client(&mock_server.uri());
            let request = ExecuteStatementRequest::new("test_warehouse", "SELECT 1")
                .with_session_id("session-xyz")
                .with_catalog("main")
                .with_schema("default")
                .with_wait_timeout("30s")
                .with_on_wait_timeout("CANCEL")
                .with_row_limit(1000)
                .with_byte_limit(1_000_000);

            let result = client.execute_statement(&request).await;
            assert!(result.is_ok());

            let response = result.unwrap();
            assert_eq!(response.statement_id, "stmt-full");
        }

        // ====================================================================
        // get_statement Tests
        // ====================================================================

        #[tokio::test]
        async fn test_get_statement_success() {
            let mock_server = MockServer::start().await;

            Mock::given(method("GET"))
                .and(path("/api/2.0/sql/statements/stmt-123"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "statement_id": "stmt-123",
                    "status": {
                        "state": "SUCCEEDED"
                    },
                    "manifest": {
                        "format": "ARROW_STREAM",
                        "total_row_count": 100
                    }
                })))
                .mount(&mock_server)
                .await;

            let client = create_mock_client(&mock_server.uri());
            let response = client.get_statement("stmt-123").await;

            assert!(response.is_ok());
            let response = response.unwrap();
            assert_eq!(response.statement_id, "stmt-123");
            assert_eq!(response.status.state, StatementState::Succeeded);
        }

        #[tokio::test]
        async fn test_get_statement_pending() {
            let mock_server = MockServer::start().await;

            Mock::given(method("GET"))
                .and(path("/api/2.0/sql/statements/stmt-pending"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "statement_id": "stmt-pending",
                    "status": {
                        "state": "PENDING"
                    }
                })))
                .mount(&mock_server)
                .await;

            let client = create_mock_client(&mock_server.uri());
            let response = client.get_statement("stmt-pending").await.unwrap();

            assert_eq!(response.status.state, StatementState::Pending);
        }

        #[tokio::test]
        async fn test_get_statement_running() {
            let mock_server = MockServer::start().await;

            Mock::given(method("GET"))
                .and(path("/api/2.0/sql/statements/stmt-running"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "statement_id": "stmt-running",
                    "status": {
                        "state": "RUNNING"
                    }
                })))
                .mount(&mock_server)
                .await;

            let client = create_mock_client(&mock_server.uri());
            let response = client.get_statement("stmt-running").await.unwrap();

            assert_eq!(response.status.state, StatementState::Running);
        }

        #[tokio::test]
        async fn test_get_statement_failed() {
            let mock_server = MockServer::start().await;

            Mock::given(method("GET"))
                .and(path("/api/2.0/sql/statements/stmt-failed"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "statement_id": "stmt-failed",
                    "status": {
                        "state": "FAILED",
                        "error": {
                            "error_code": "RESOURCE_EXHAUSTED",
                            "message": "Query exceeded memory limits"
                        }
                    }
                })))
                .mount(&mock_server)
                .await;

            let client = create_mock_client(&mock_server.uri());
            let response = client.get_statement("stmt-failed").await.unwrap();

            assert_eq!(response.status.state, StatementState::Failed);
            assert!(response.status.error.is_some());
            let error = response.status.error.unwrap();
            assert_eq!(error.error_code, Some("RESOURCE_EXHAUSTED".to_string()));
        }

        // ====================================================================
        // poll_until_complete Tests
        // ====================================================================

        #[tokio::test]
        async fn test_poll_until_complete_immediate_success() {
            let mock_server = MockServer::start().await;

            // First poll returns SUCCEEDED immediately
            Mock::given(method("GET"))
                .and(path("/api/2.0/sql/statements/stmt-fast"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "statement_id": "stmt-fast",
                    "status": {
                        "state": "SUCCEEDED"
                    },
                    "manifest": {
                        "total_row_count": 1
                    }
                })))
                .expect(1) // Should only call once
                .mount(&mock_server)
                .await;

            let client = create_mock_client(&mock_server.uri());
            let result = client.poll_until_complete(
                "stmt-fast",
                Some(Duration::from_secs(10)),
            ).await;

            assert!(result.is_ok());
            let response = result.unwrap();
            assert_eq!(response.status.state, StatementState::Succeeded);
        }

        #[tokio::test]
        async fn test_poll_until_complete_after_pending() {
            let mock_server = MockServer::start().await;

            // Use a sequence: PENDING -> RUNNING -> SUCCEEDED
            let call_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
            let call_count_clone = call_count.clone();

            Mock::given(method("GET"))
                .and(path("/api/2.0/sql/statements/stmt-slow"))
                .respond_with(move |_req: &wiremock::Request| {
                    let count = call_count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    match count {
                        0 => ResponseTemplate::new(200).set_body_json(serde_json::json!({
                            "statement_id": "stmt-slow",
                            "status": { "state": "PENDING" }
                        })),
                        1 => ResponseTemplate::new(200).set_body_json(serde_json::json!({
                            "statement_id": "stmt-slow",
                            "status": { "state": "RUNNING" }
                        })),
                        _ => ResponseTemplate::new(200).set_body_json(serde_json::json!({
                            "statement_id": "stmt-slow",
                            "status": { "state": "SUCCEEDED" },
                            "manifest": { "total_row_count": 100 }
                        })),
                    }
                })
                .mount(&mock_server)
                .await;

            let client = create_mock_client(&mock_server.uri());

            // Use a custom backoff with very short delays for testing
            // We can't easily change the backoff in the method, so we just test it works
            let start = std::time::Instant::now();
            let result = client.poll_until_complete(
                "stmt-slow",
                Some(Duration::from_secs(30)),
            ).await;

            assert!(result.is_ok(), "Expected success, got {:?}", result);
            let response = result.unwrap();
            assert_eq!(response.status.state, StatementState::Succeeded);

            // Verify multiple calls were made
            let total_calls = call_count.load(std::sync::atomic::Ordering::SeqCst);
            assert!(total_calls >= 3, "Expected at least 3 calls, got {}", total_calls);

            // Verify exponential backoff timing (1s + 2s = 3s minimum)
            let elapsed = start.elapsed();
            assert!(elapsed >= Duration::from_secs(2), "Expected at least 2s delay, got {:?}", elapsed);
        }

        #[tokio::test]
        async fn test_poll_until_complete_fails() {
            let mock_server = MockServer::start().await;

            // Return FAILED state
            Mock::given(method("GET"))
                .and(path("/api/2.0/sql/statements/stmt-will-fail"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "statement_id": "stmt-will-fail",
                    "status": {
                        "state": "FAILED",
                        "error": {
                            "error_code": "QUERY_TIMEOUT",
                            "message": "Query execution timed out"
                        }
                    }
                })))
                .mount(&mock_server)
                .await;

            let client = create_mock_client(&mock_server.uri());
            let result = client.poll_until_complete(
                "stmt-will-fail",
                Some(Duration::from_secs(10)),
            ).await;

            assert!(result.is_err());
            let err = result.unwrap_err();
            match err {
                Error::StatementFailed(msg) => {
                    assert!(msg.contains("QUERY_TIMEOUT"), "Error should contain error code: {}", msg);
                    assert!(msg.contains("timed out"), "Error should contain message: {}", msg);
                }
                _ => panic!("Expected StatementFailed error, got {:?}", err),
            }
        }

        #[tokio::test]
        async fn test_poll_until_complete_canceled() {
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

            let client = create_mock_client(&mock_server.uri());
            let result = client.poll_until_complete(
                "stmt-canceled",
                Some(Duration::from_secs(10)),
            ).await;

            assert!(result.is_err());
            let err = result.unwrap_err();
            match err {
                Error::StatementFailed(msg) => {
                    assert!(msg.contains("canceled"), "Error should mention cancellation: {}", msg);
                }
                _ => panic!("Expected StatementFailed error, got {:?}", err),
            }
        }

        #[tokio::test]
        async fn test_poll_until_complete_closed() {
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

            let client = create_mock_client(&mock_server.uri());
            let result = client.poll_until_complete(
                "stmt-closed",
                Some(Duration::from_secs(10)),
            ).await;

            assert!(result.is_err());
            let err = result.unwrap_err();
            match err {
                Error::StatementFailed(msg) => {
                    assert!(msg.contains("closed"), "Error should mention closed: {}", msg);
                }
                _ => panic!("Expected StatementFailed error, got {:?}", err),
            }
        }

        #[tokio::test]
        async fn test_poll_until_complete_timeout() {
            let mock_server = MockServer::start().await;

            // Always return RUNNING - will timeout
            Mock::given(method("GET"))
                .and(path("/api/2.0/sql/statements/stmt-forever"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "statement_id": "stmt-forever",
                    "status": {
                        "state": "RUNNING"
                    }
                })))
                .mount(&mock_server)
                .await;

            let client = create_mock_client(&mock_server.uri());

            // Use very short timeout for testing
            let start = std::time::Instant::now();
            let result = client.poll_until_complete(
                "stmt-forever",
                Some(Duration::from_millis(100)), // Very short timeout
            ).await;

            let elapsed = start.elapsed();
            assert!(result.is_err());
            let err = result.unwrap_err();
            match err {
                Error::Timeout => {
                    // Timeout should occur after approximately the specified duration
                    assert!(elapsed >= Duration::from_millis(100), "Should wait at least 100ms");
                }
                _ => panic!("Expected Timeout error, got {:?}", err),
            }
        }

        // ====================================================================
        // execute_and_wait Tests
        // ====================================================================

        #[tokio::test]
        async fn test_execute_and_wait_immediate_success() {
            let mock_server = MockServer::start().await;

            // Execute returns SUCCEEDED immediately
            Mock::given(method("POST"))
                .and(path("/api/2.0/sql/statements"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "statement_id": "stmt-instant",
                    "status": {
                        "state": "SUCCEEDED"
                    },
                    "manifest": {
                        "total_row_count": 1
                    },
                    "result": {
                        "data_array": [[42]],
                        "row_count": 1
                    }
                })))
                .expect(1) // Only one call needed
                .mount(&mock_server)
                .await;

            let client = create_mock_client(&mock_server.uri());
            let result = client.execute_and_wait(
                "session-123",
                "SELECT 42",
                Some(Duration::from_secs(10)),
                None,
                None,
            ).await;

            assert!(result.is_ok());
            let response = result.unwrap();
            assert_eq!(response.status.state, StatementState::Succeeded);
            assert_eq!(response.statement_id, "stmt-instant");
        }

        #[tokio::test]
        async fn test_execute_and_wait_with_polling() {
            let mock_server = MockServer::start().await;

            // Execute returns PENDING
            Mock::given(method("POST"))
                .and(path("/api/2.0/sql/statements"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "statement_id": "stmt-async",
                    "status": {
                        "state": "PENDING"
                    }
                })))
                .expect(1)
                .mount(&mock_server)
                .await;

            // Polling returns SUCCEEDED after one poll
            Mock::given(method("GET"))
                .and(path("/api/2.0/sql/statements/stmt-async"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "statement_id": "stmt-async",
                    "status": {
                        "state": "SUCCEEDED"
                    },
                    "manifest": {
                        "total_row_count": 100
                    }
                })))
                .expect(1)
                .mount(&mock_server)
                .await;

            let client = create_mock_client(&mock_server.uri());
            let result = client.execute_and_wait(
                "session-456",
                "SELECT * FROM large_table",
                Some(Duration::from_secs(60)),
                Some(1000),
                None,
            ).await;

            assert!(result.is_ok());
            let response = result.unwrap();
            assert_eq!(response.status.state, StatementState::Succeeded);
        }

        #[tokio::test]
        async fn test_execute_and_wait_fails_immediately() {
            let mock_server = MockServer::start().await;

            // Execute returns FAILED immediately
            Mock::given(method("POST"))
                .and(path("/api/2.0/sql/statements"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "statement_id": "stmt-bad-sql",
                    "status": {
                        "state": "FAILED",
                        "error": {
                            "error_code": "PARSE_SYNTAX_ERROR",
                            "message": "Syntax error at line 1"
                        }
                    }
                })))
                .expect(1)
                .mount(&mock_server)
                .await;

            let client = create_mock_client(&mock_server.uri());
            let result = client.execute_and_wait(
                "session-789",
                "SELEC 1", // Typo
                None,
                None,
                None,
            ).await;

            assert!(result.is_err());
            let err = result.unwrap_err();
            match err {
                Error::StatementFailed(msg) => {
                    assert!(msg.contains("PARSE_SYNTAX_ERROR"), "Error should contain code: {}", msg);
                }
                _ => panic!("Expected StatementFailed error, got {:?}", err),
            }
        }

        #[tokio::test]
        async fn test_execute_and_wait_fails_during_polling() {
            let mock_server = MockServer::start().await;

            // Execute returns RUNNING
            Mock::given(method("POST"))
                .and(path("/api/2.0/sql/statements"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "statement_id": "stmt-will-fail-later",
                    "status": {
                        "state": "RUNNING"
                    }
                })))
                .expect(1)
                .mount(&mock_server)
                .await;

            // Polling returns FAILED
            Mock::given(method("GET"))
                .and(path("/api/2.0/sql/statements/stmt-will-fail-later"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "statement_id": "stmt-will-fail-later",
                    "status": {
                        "state": "FAILED",
                        "error": {
                            "error_code": "RESOURCE_EXHAUSTED",
                            "message": "Out of memory"
                        }
                    }
                })))
                .mount(&mock_server)
                .await;

            let client = create_mock_client(&mock_server.uri());
            let result = client.execute_and_wait(
                "session-abc",
                "SELECT * FROM huge_table",
                Some(Duration::from_secs(30)),
                None,
                None,
            ).await;

            assert!(result.is_err());
            let err = result.unwrap_err();
            match err {
                Error::StatementFailed(msg) => {
                    assert!(msg.contains("RESOURCE_EXHAUSTED"), "Error should contain code: {}", msg);
                    assert!(msg.contains("memory"), "Error should contain message: {}", msg);
                }
                _ => panic!("Expected StatementFailed error, got {:?}", err),
            }
        }

        #[tokio::test]
        async fn test_execute_and_wait_canceled_immediately() {
            let mock_server = MockServer::start().await;

            // Execute returns CANCELED
            Mock::given(method("POST"))
                .and(path("/api/2.0/sql/statements"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "statement_id": "stmt-canceled",
                    "status": {
                        "state": "CANCELED"
                    }
                })))
                .mount(&mock_server)
                .await;

            let client = create_mock_client(&mock_server.uri());
            let result = client.execute_and_wait(
                "session-xyz",
                "SELECT 1",
                None,
                None,
                None,
            ).await;

            assert!(result.is_err());
            let err = result.unwrap_err();
            match err {
                Error::StatementFailed(msg) => {
                    assert!(msg.contains("canceled"), "Error should mention cancellation: {}", msg);
                }
                _ => panic!("Expected StatementFailed error, got {:?}", err),
            }
        }

        #[tokio::test]
        async fn test_execute_and_wait_with_options() {
            let mock_server = MockServer::start().await;

            // Verify that options are passed correctly
            // Note: catalog/schema cannot be combined with session_id per SEA API
            Mock::given(method("POST"))
                .and(path("/api/2.0/sql/statements"))
                .and(body_partial_json(serde_json::json!({
                    "session_id": "session-opts",
                    "row_limit": 500,
                    "byte_limit": 1000000
                })))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "statement_id": "stmt-with-opts",
                    "status": {
                        "state": "SUCCEEDED"
                    }
                })))
                .mount(&mock_server)
                .await;

            let client = create_mock_client(&mock_server.uri());
            let result = client.execute_and_wait(
                "session-opts",
                "SELECT * FROM table",
                Some(Duration::from_secs(30)),
                Some(500),
                Some(1_000_000),
            ).await;

            assert!(result.is_ok(), "Request should succeed with options: {:?}", result);
        }
    }
}
