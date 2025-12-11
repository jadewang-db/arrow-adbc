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

//! Error types and mapping for the Databricks ADBC driver.
//!
//! This module provides comprehensive error handling infrastructure with
//! SEA API error to ADBC status mapping.
//!
//! # Error Mapping
//!
//! SEA API errors are mapped to ADBC statuses as follows:
//!
//! | HTTP Status | SEA Error Code | ADBC Status | Retryable |
//! |-------------|----------------|-------------|-----------|
//! | 400 | BAD_REQUEST | InvalidArguments | No |
//! | 400 | INVALID_PARAMETER_VALUE | InvalidArguments | No |
//! | 401 | UNAUTHENTICATED | Unauthenticated | Once (refresh) |
//! | 403 | PERMISSION_DENIED | Unauthorized | No |
//! | 404 | NOT_FOUND | NotFound | No |
//! | 429 | REQUEST_LIMIT_EXCEEDED | IO | Yes (backoff) |
//! | 500 | INTERNAL_ERROR | Internal | Yes (3x) |
//! | 503 | TEMPORARILY_UNAVAILABLE | IO | Yes (5x) |

use adbc_core::error::Status;

use crate::client::SeaError;

/// Error type for the Databricks ADBC driver.
///
/// This enum represents all possible errors that can occur when using the
/// Databricks ADBC driver. Each variant maps to an appropriate ADBC status
/// code and provides context about whether the error is retryable.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// HTTP error from reqwest.
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    /// SEA API error with code, message, and HTTP status.
    #[error("SEA API error: {code} - {message}")]
    SeaApi {
        /// SEA error code string (e.g., "BAD_REQUEST", "UNAUTHENTICATED")
        code: String,
        /// Error message from the API
        message: String,
        /// HTTP status code
        http_status: u16,
        /// Optional Retry-After duration from the server (for 429 responses)
        retry_after: Option<std::time::Duration>,
    },

    /// Arrow error.
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    /// JSON parsing error.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Invalid configuration.
    #[error("Invalid configuration: {0}")]
    Config(String),

    /// I/O error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Statement execution failed.
    #[error("Statement failed: {0}")]
    StatementFailed(String),

    /// Session error.
    #[error("Session error: {0}")]
    Session(String),

    /// Timeout waiting for statement to complete.
    #[error("Timeout waiting for statement")]
    Timeout,

    /// URL parsing error.
    #[error("URL error: {0}")]
    Url(#[from] url::ParseError),

    /// Feature not implemented.
    #[error("Not implemented: {0}")]
    NotImplemented(String),

    /// Operation was cancelled.
    #[error("Operation cancelled: {0}")]
    Cancelled(String),
}

impl Error {
    /// Get the ADBC status code for this error.
    ///
    /// Maps the error to the appropriate ADBC status based on the error type
    /// and, for SEA API errors, the HTTP status code.
    pub fn status(&self) -> Status {
        match self {
            Error::SeaApi { http_status, .. } => match *http_status {
                400 => Status::InvalidArguments,
                401 => Status::Unauthenticated,
                403 => Status::Unauthorized,
                404 => Status::NotFound,
                429 => Status::IO, // Rate limited, retryable
                500 => Status::Internal,
                503 => Status::IO, // Unavailable, retryable
                _ => Status::Unknown,
            },
            Error::Http(_) => Status::IO,
            Error::Arrow(_) => Status::InvalidData,
            Error::Json(_) => Status::InvalidData,
            Error::Config(_) => Status::InvalidArguments,
            Error::Io(_) => Status::IO,
            Error::StatementFailed(_) => Status::Internal,
            Error::Session(_) => Status::InvalidState,
            Error::Timeout => Status::Timeout,
            Error::Url(_) => Status::InvalidArguments,
            Error::NotImplemented(_) => Status::NotImplemented,
            Error::Cancelled(_) => Status::Cancelled,
        }
    }

    /// Check if this error is retryable.
    ///
    /// Returns `true` for transient errors that may succeed if retried:
    /// - HTTP 429 (rate limited)
    /// - HTTP 500 (internal server error)
    /// - HTTP 503 (service unavailable)
    /// - Network timeouts and connection errors
    /// - I/O errors
    pub fn is_retryable(&self) -> bool {
        match self {
            Error::SeaApi { http_status, .. } => {
                matches!(*http_status, 429 | 500 | 503)
            }
            Error::Http(e) => e.is_timeout() || e.is_connect(),
            Error::Io(_) => true,
            Error::Timeout => false, // Timeout is a final state, not retryable
            _ => false,
        }
    }

    /// Create a SEA API error from components.
    pub fn sea_api(code: impl Into<String>, message: impl Into<String>, http_status: u16) -> Self {
        Error::SeaApi {
            code: code.into(),
            message: message.into(),
            http_status,
            retry_after: None,
        }
    }

    /// Create a SEA API error with a Retry-After hint.
    pub fn sea_api_with_retry_after(
        code: impl Into<String>,
        message: impl Into<String>,
        http_status: u16,
        retry_after: Option<std::time::Duration>,
    ) -> Self {
        Error::SeaApi {
            code: code.into(),
            message: message.into(),
            http_status,
            retry_after,
        }
    }

    /// Get the retry-after duration if this is a rate-limited error.
    pub fn retry_after(&self) -> Option<std::time::Duration> {
        match self {
            Error::SeaApi { retry_after, .. } => *retry_after,
            _ => None,
        }
    }

    /// Create a configuration error.
    pub fn config(message: impl Into<String>) -> Self {
        Error::Config(message.into())
    }

    /// Create a session error.
    pub fn session(message: impl Into<String>) -> Self {
        Error::Session(message.into())
    }

    /// Create a statement failed error.
    pub fn statement_failed(message: impl Into<String>) -> Self {
        Error::StatementFailed(message.into())
    }

    /// Create a not implemented error.
    pub fn not_implemented(message: impl Into<String>) -> Self {
        Error::NotImplemented(message.into())
    }

    /// Create a cancelled error.
    pub fn cancelled(message: impl Into<String>) -> Self {
        Error::Cancelled(message.into())
    }

    /// Create an internal error.
    ///
    /// This is used for internal driver errors that don't map to other categories.
    pub fn internal(message: impl Into<String>) -> Self {
        Error::StatementFailed(message.into())
    }
}

impl From<Error> for adbc_core::error::Error {
    fn from(err: Error) -> Self {
        adbc_core::error::Error::with_message_and_status(err.to_string(), err.status())
    }
}

impl From<SeaError> for Error {
    fn from(err: SeaError) -> Self {
        Error::SeaApi {
            code: format!("{:?}", err.code),
            message: err.message,
            http_status: err.code.to_http_status(),
            retry_after: None,
        }
    }
}

/// Result type for the Databricks ADBC driver.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sea_error_400_maps_to_invalid_arguments() {
        let err = Error::sea_api("BAD_REQUEST", "Invalid SQL syntax", 400);
        assert_eq!(err.status(), Status::InvalidArguments);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_sea_error_401_maps_to_unauthenticated() {
        let err = Error::sea_api("UNAUTHENTICATED", "Invalid token", 401);
        assert_eq!(err.status(), Status::Unauthenticated);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_sea_error_403_maps_to_unauthorized() {
        let err = Error::sea_api("PERMISSION_DENIED", "Access denied", 403);
        assert_eq!(err.status(), Status::Unauthorized);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_sea_error_404_maps_to_not_found() {
        let err = Error::sea_api("NOT_FOUND", "Resource not found", 404);
        assert_eq!(err.status(), Status::NotFound);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_sea_error_429_maps_to_io_and_is_retryable() {
        let err = Error::sea_api("REQUEST_LIMIT_EXCEEDED", "Rate limited", 429);
        assert_eq!(err.status(), Status::IO);
        assert!(err.is_retryable());
    }

    #[test]
    fn test_sea_error_500_maps_to_internal_and_is_retryable() {
        let err = Error::sea_api("INTERNAL_ERROR", "Internal server error", 500);
        assert_eq!(err.status(), Status::Internal);
        assert!(err.is_retryable());
    }

    #[test]
    fn test_sea_error_503_maps_to_io_and_is_retryable() {
        let err = Error::sea_api("TEMPORARILY_UNAVAILABLE", "Service unavailable", 503);
        assert_eq!(err.status(), Status::IO);
        assert!(err.is_retryable());
    }

    #[test]
    fn test_config_error_maps_to_invalid_arguments() {
        let err = Error::Config("Missing warehouse_id".into());
        assert_eq!(err.status(), Status::InvalidArguments);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_session_error_maps_to_invalid_state() {
        let err = Error::Session("Session expired".into());
        assert_eq!(err.status(), Status::InvalidState);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_timeout_error_maps_to_timeout() {
        let err = Error::Timeout;
        assert_eq!(err.status(), Status::Timeout);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_not_implemented_error_maps_to_not_implemented() {
        let err = Error::NotImplemented("Prepared statements".into());
        assert_eq!(err.status(), Status::NotImplemented);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_cancelled_error_maps_to_cancelled() {
        let err = Error::Cancelled("User cancelled".into());
        assert_eq!(err.status(), Status::Cancelled);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_json_error_maps_to_invalid_data() {
        let json_err: serde_json::Error = serde_json::from_str::<i32>("invalid").unwrap_err();
        let err = Error::Json(json_err);
        assert_eq!(err.status(), Status::InvalidData);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_url_error_maps_to_invalid_arguments() {
        let url_err: url::ParseError = "not a url".parse::<url::Url>().unwrap_err();
        let err = Error::Url(url_err);
        assert_eq!(err.status(), Status::InvalidArguments);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_io_error_is_retryable() {
        let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        let err = Error::Io(io_err);
        assert_eq!(err.status(), Status::IO);
        assert!(err.is_retryable());
    }

    #[test]
    fn test_statement_failed_error_maps_to_internal() {
        let err = Error::StatementFailed("Execution error".into());
        assert_eq!(err.status(), Status::Internal);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_error_display() {
        let err = Error::sea_api("BAD_REQUEST", "Invalid SQL", 400);
        assert_eq!(err.to_string(), "SEA API error: BAD_REQUEST - Invalid SQL");
    }

    #[test]
    fn test_error_to_adbc_error_conversion() {
        let err = Error::sea_api("NOT_FOUND", "Table not found", 404);
        let adbc_err: adbc_core::error::Error = err.into();
        assert_eq!(adbc_err.status, Status::NotFound);
        assert!(adbc_err.message.contains("NOT_FOUND"));
        assert!(adbc_err.message.contains("Table not found"));
    }

    #[test]
    fn test_unknown_http_status_maps_to_unknown() {
        let err = Error::sea_api("UNKNOWN", "Unknown error", 418); // I'm a teapot
        assert_eq!(err.status(), Status::Unknown);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_retry_after_accessor() {
        use std::time::Duration;

        // Error without retry_after
        let err = Error::sea_api("REQUEST_LIMIT_EXCEEDED", "Rate limited", 429);
        assert_eq!(err.retry_after(), None);

        // Error with retry_after
        let err = Error::sea_api_with_retry_after(
            "REQUEST_LIMIT_EXCEEDED",
            "Rate limited",
            429,
            Some(Duration::from_secs(30)),
        );
        assert_eq!(err.retry_after(), Some(Duration::from_secs(30)));

        // Non-SeaApi error
        let err = Error::Timeout;
        assert_eq!(err.retry_after(), None);
    }

    #[test]
    fn test_helper_constructors() {
        let err = Error::config("test config");
        assert!(matches!(err, Error::Config(_)));

        let err = Error::session("test session");
        assert!(matches!(err, Error::Session(_)));

        let err = Error::statement_failed("test statement");
        assert!(matches!(err, Error::StatementFailed(_)));

        let err = Error::not_implemented("test feature");
        assert!(matches!(err, Error::NotImplemented(_)));

        let err = Error::cancelled("test cancel");
        assert!(matches!(err, Error::Cancelled(_)));

        let err = Error::sea_api("CODE", "message", 400);
        assert!(matches!(err, Error::SeaApi { .. }));
    }
}
