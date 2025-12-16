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
//! This module defines the error types used throughout the driver and provides
//! mapping from SEA API errors to ADBC status codes.
//!
//! # SEA API Error Mapping
//!
//! The driver maps SEA API errors to ADBC status codes according to the following table:
//!
//! | SEA Error Code | HTTP Status | ADBC Status | Retryable |
//! |----------------|-------------|-------------|-----------|
//! | BAD_REQUEST | 400 | InvalidArguments | No |
//! | INVALID_PARAMETER_VALUE | 400 | InvalidArguments | No |
//! | UNAUTHENTICATED | 401 | Unauthenticated | Once (refresh) |
//! | PERMISSION_DENIED | 403 | Unauthorized | No |
//! | NOT_FOUND | 404 | NotFound | No |
//! | REQUEST_LIMIT_EXCEEDED | 429 | IO | Yes (backoff) |
//! | INTERNAL_ERROR | 500 | Internal | Yes (3x) |
//! | TEMPORARILY_UNAVAILABLE | 503 | IO | Yes (5x) |

use adbc_core::error::Status;
use thiserror::Error;

/// Error type for the Databricks ADBC driver.
///
/// This enum represents all possible errors that can occur during driver operations,
/// including HTTP errors, SEA API errors, Arrow conversion errors, and configuration errors.
#[derive(Debug, Error)]
pub enum Error {
    /// HTTP request error from reqwest.
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    /// SEA API error with code, message, and HTTP status.
    ///
    /// This variant captures errors returned by the Databricks Statement Execution API,
    /// preserving the error code and message for debugging and proper status mapping.
    #[error("SEA API error: {code} - {message}")]
    SeaApi {
        /// The error code from the SEA API (e.g., "BAD_REQUEST", "UNAUTHENTICATED")
        code: String,
        /// The human-readable error message
        message: String,
        /// The HTTP status code (e.g., 400, 401, 500)
        http_status: u16,
        /// Optional Retry-After duration from the response header (for 429 responses)
        retry_after: Option<std::time::Duration>,
    },

    /// Arrow error during data processing.
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    /// JSON serialization/deserialization error.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Invalid configuration provided to the driver.
    #[error("Invalid configuration: {0}")]
    Config(String),

    /// IO error (file operations, etc.).
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Statement execution failed.
    #[error("Statement failed: {0}")]
    StatementFailed(String),

    /// Session-related error.
    #[error("Session error: {0}")]
    Session(String),

    /// Timeout waiting for statement completion.
    #[error("Timeout waiting for statement")]
    Timeout,

    /// URL parsing error.
    #[error("URL error: {0}")]
    Url(#[from] url::ParseError),

    /// Operation not implemented.
    #[error("Not implemented: {0}")]
    NotImplemented(String),
}

impl Error {
    /// Get the ADBC status code for this error.
    ///
    /// Maps internal error types to the appropriate ADBC status codes according to
    /// the SEA API error mapping specification in the design document.
    ///
    /// # Returns
    ///
    /// The ADBC [`Status`] code that best represents this error.
    pub fn to_adbc_status(&self) -> Status {
        match self {
            Error::SeaApi { http_status, .. } => {
                match *http_status {
                    400 => Status::InvalidArguments,
                    401 => Status::Unauthenticated,
                    403 => Status::Unauthorized,
                    404 => Status::NotFound,
                    429 => Status::IO, // Rate limited, retryable
                    500 => Status::Internal,
                    503 => Status::IO, // Unavailable, retryable
                    _ => Status::Unknown,
                }
            }
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
        }
    }

    /// Check if this error is retryable.
    ///
    /// An error is considered retryable if it represents a transient condition
    /// that might succeed on retry. This includes:
    /// - HTTP 401 (Unauthenticated) - retryable once for token refresh
    /// - HTTP 429 (Rate Limited) - retryable with exponential backoff
    /// - HTTP 500 (Internal Error) - retryable up to 3 times
    /// - HTTP 503 (Temporarily Unavailable) - retryable up to 5 times
    /// - Network timeout or connection errors
    /// - General IO errors
    ///
    /// # Returns
    ///
    /// `true` if the operation should be retried, `false` otherwise.
    ///
    /// # Note
    ///
    /// For 401 errors, the retry logic should attempt token refresh before retrying.
    /// The actual retry limits should be enforced by the caller (e.g., 401 should only
    /// retry once with a refreshed token).
    pub fn is_retryable(&self) -> bool {
        match self {
            Error::SeaApi { http_status, .. } => {
                // 401 is retryable once for token refresh
                // 429 is rate limited, retry with backoff
                // 500 is internal error, retry up to 3 times
                // 503 is temporarily unavailable, retry up to 5 times
                matches!(*http_status, 401 | 429 | 500 | 503)
            }
            Error::Http(e) => e.is_timeout() || e.is_connect(),
            Error::Io(_) => true,
            Error::Timeout => true,
            _ => false,
        }
    }

    /// Create a new SEA API error.
    ///
    /// # Arguments
    ///
    /// * `code` - The error code from the SEA API
    /// * `message` - The human-readable error message
    /// * `http_status` - The HTTP status code
    ///
    /// # Returns
    ///
    /// A new [`Error::SeaApi`] variant.
    pub fn sea_api(code: impl Into<String>, message: impl Into<String>, http_status: u16) -> Self {
        Error::SeaApi {
            code: code.into(),
            message: message.into(),
            http_status,
            retry_after: None,
        }
    }

    /// Create a new SEA API error with a Retry-After duration.
    ///
    /// # Arguments
    ///
    /// * `code` - The error code from the SEA API
    /// * `message` - The human-readable error message
    /// * `http_status` - The HTTP status code
    /// * `retry_after` - Optional Retry-After duration from response header
    ///
    /// # Returns
    ///
    /// A new [`Error::SeaApi`] variant.
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

    /// Get the Retry-After duration if this is a rate-limited error.
    ///
    /// # Returns
    ///
    /// The Retry-After duration if this is a SEA API error with a retry_after value,
    /// `None` otherwise.
    pub fn retry_after(&self) -> Option<std::time::Duration> {
        match self {
            Error::SeaApi { retry_after, .. } => *retry_after,
            _ => None,
        }
    }

    /// Create a configuration error.
    ///
    /// # Arguments
    ///
    /// * `message` - Description of the configuration problem
    ///
    /// # Returns
    ///
    /// A new [`Error::Config`] variant.
    pub fn config(message: impl Into<String>) -> Self {
        Error::Config(message.into())
    }

    /// Create a statement failed error.
    ///
    /// # Arguments
    ///
    /// * `message` - Description of why the statement failed
    ///
    /// # Returns
    ///
    /// A new [`Error::StatementFailed`] variant.
    pub fn statement_failed(message: impl Into<String>) -> Self {
        Error::StatementFailed(message.into())
    }

    /// Create a session error.
    ///
    /// # Arguments
    ///
    /// * `message` - Description of the session problem
    ///
    /// # Returns
    ///
    /// A new [`Error::Session`] variant.
    pub fn session(message: impl Into<String>) -> Self {
        Error::Session(message.into())
    }

    /// Create a not implemented error.
    ///
    /// # Arguments
    ///
    /// * `feature` - Description of the unimplemented feature
    ///
    /// # Returns
    ///
    /// A new [`Error::NotImplemented`] variant.
    pub fn not_implemented(feature: impl Into<String>) -> Self {
        Error::NotImplemented(feature.into())
    }
}

impl From<Error> for adbc_core::error::Error {
    fn from(err: Error) -> Self {
        let status = err.to_adbc_status();
        adbc_core::error::Error::with_message_and_status(err.to_string(), status)
    }
}

/// Result type for the Databricks ADBC driver.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sea_error_to_adbc_status() {
        let err = Error::SeaApi {
            code: "BAD_REQUEST".into(),
            message: "Invalid SQL".into(),
            http_status: 400,
            retry_after: None,
        };
        assert_eq!(err.to_adbc_status(), Status::InvalidArguments);
    }

    #[test]
    fn test_retryable_errors() {
        let err_429 = Error::SeaApi {
            code: "REQUEST_LIMIT_EXCEEDED".into(),
            message: "Rate limited".into(),
            http_status: 429,
            retry_after: None,
        };
        let err_400 = Error::SeaApi {
            code: "BAD_REQUEST".into(),
            message: "Invalid request".into(),
            http_status: 400,
            retry_after: None,
        };
        assert!(err_429.is_retryable());
        assert!(!err_400.is_retryable());
    }

    #[test]
    fn test_all_sea_error_codes() {
        // Test all error codes from design doc Section 5.2
        let test_cases = vec![
            ("BAD_REQUEST", 400, Status::InvalidArguments, false),
            ("UNAUTHENTICATED", 401, Status::Unauthenticated, true),
            ("PERMISSION_DENIED", 403, Status::Unauthorized, false),
            ("NOT_FOUND", 404, Status::NotFound, false),
            ("REQUEST_LIMIT_EXCEEDED", 429, Status::IO, true),
            ("INTERNAL_ERROR", 500, Status::Internal, true),
            ("TEMPORARILY_UNAVAILABLE", 503, Status::IO, true),
        ];

        for (code, http_status, expected_adbc, expected_retry) in test_cases {
            let err = Error::SeaApi {
                code: code.into(),
                message: "test".into(),
                http_status,
                retry_after: None,
            };
            assert_eq!(
                err.to_adbc_status(),
                expected_adbc,
                "ADBC status failed for {}",
                code
            );
            assert_eq!(
                err.is_retryable(),
                expected_retry,
                "Retry check failed for {}",
                code
            );
        }
    }

    #[test]
    fn test_other_error_types_status() {
        // Config error
        let config_err = Error::config("missing token");
        assert_eq!(config_err.to_adbc_status(), Status::InvalidArguments);
        assert!(!config_err.is_retryable());

        // Statement failed error
        let stmt_err = Error::statement_failed("execution error");
        assert_eq!(stmt_err.to_adbc_status(), Status::Internal);
        assert!(!stmt_err.is_retryable());

        // Session error
        let session_err = Error::session("session expired");
        assert_eq!(session_err.to_adbc_status(), Status::InvalidState);
        assert!(!session_err.is_retryable());

        // Timeout error
        let timeout_err = Error::Timeout;
        assert_eq!(timeout_err.to_adbc_status(), Status::Timeout);
        assert!(timeout_err.is_retryable());

        // Not implemented error
        let not_impl_err = Error::not_implemented("bind parameters");
        assert_eq!(not_impl_err.to_adbc_status(), Status::NotImplemented);
        assert!(!not_impl_err.is_retryable());
    }

    #[test]
    fn test_error_display_preserves_message() {
        let err = Error::SeaApi {
            code: "BAD_REQUEST".into(),
            message: "Invalid SQL syntax at position 42".into(),
            http_status: 400,
            retry_after: None,
        };
        let display = err.to_string();
        assert!(
            display.contains("BAD_REQUEST"),
            "Display should contain error code"
        );
        assert!(
            display.contains("Invalid SQL syntax at position 42"),
            "Display should contain original message"
        );

        let config_err = Error::config("warehouse_id is required");
        assert!(
            config_err.to_string().contains("warehouse_id is required"),
            "Display should contain config message"
        );
    }

    #[test]
    fn test_conversion_to_adbc_error() {
        let err = Error::SeaApi {
            code: "NOT_FOUND".into(),
            message: "Table not found".into(),
            http_status: 404,
            retry_after: None,
        };
        let adbc_err: adbc_core::error::Error = err.into();

        // The ADBC error should preserve the message and status
        assert!(adbc_err.message.contains("NOT_FOUND"));
        assert!(adbc_err.message.contains("Table not found"));
        assert_eq!(adbc_err.status, Status::NotFound);
    }

    #[test]
    fn test_sea_api_constructor() {
        let err = Error::sea_api("INTERNAL_ERROR", "Something went wrong", 500);
        match err {
            Error::SeaApi {
                code,
                message,
                http_status,
                retry_after,
            } => {
                assert_eq!(code, "INTERNAL_ERROR");
                assert_eq!(message, "Something went wrong");
                assert_eq!(http_status, 500);
                assert_eq!(retry_after, None);
            }
            _ => panic!("Expected SeaApi variant"),
        }
    }

    #[test]
    fn test_sea_api_constructor_with_retry_after() {
        let retry_duration = std::time::Duration::from_secs(60);
        let err = Error::sea_api_with_retry_after(
            "REQUEST_LIMIT_EXCEEDED",
            "Rate limited",
            429,
            Some(retry_duration),
        );
        match &err {
            Error::SeaApi {
                code,
                message,
                http_status,
                retry_after,
            } => {
                assert_eq!(code, "REQUEST_LIMIT_EXCEEDED");
                assert_eq!(message, "Rate limited");
                assert_eq!(*http_status, 429);
                assert_eq!(*retry_after, Some(retry_duration));
            }
            _ => panic!("Expected SeaApi variant"),
        }

        // Test retry_after accessor
        assert_eq!(err.retry_after(), Some(retry_duration));

        // Test that non-SeaApi errors return None
        let config_err = Error::config("test");
        assert_eq!(config_err.retry_after(), None);
    }

    #[test]
    fn test_io_error_retryable() {
        let io_err = Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "connection reset",
        ));
        assert!(io_err.is_retryable());
        assert_eq!(io_err.to_adbc_status(), Status::IO);
    }

    #[test]
    fn test_json_error_status() {
        // Create a JSON error by trying to parse invalid JSON
        let json_err: Error = serde_json::from_str::<serde_json::Value>("invalid json")
            .unwrap_err()
            .into();
        assert_eq!(json_err.to_adbc_status(), Status::InvalidData);
        assert!(!json_err.is_retryable());
    }

    #[test]
    fn test_url_error_status() {
        let url_err: Error = url::Url::parse("not a url").unwrap_err().into();
        assert_eq!(url_err.to_adbc_status(), Status::InvalidArguments);
        assert!(!url_err.is_retryable());
    }

    #[test]
    fn test_unknown_http_status() {
        // Test that unknown HTTP status codes map to Unknown status
        let err = Error::SeaApi {
            code: "UNKNOWN".into(),
            message: "Unknown error".into(),
            http_status: 418, // I'm a teapot
            retry_after: None,
        };
        assert_eq!(err.to_adbc_status(), Status::Unknown);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_401_retryable_for_token_refresh() {
        // 401 Unauthenticated should be retryable once for token refresh
        let err = Error::SeaApi {
            code: "UNAUTHENTICATED".into(),
            message: "Token expired".into(),
            http_status: 401,
            retry_after: None,
        };
        // Note: Currently marked as retryable in the design for "once (refresh)"
        // but in implementation, we track this separately for token refresh logic
        assert!(err.is_retryable());
        assert_eq!(err.to_adbc_status(), Status::Unauthenticated);
    }
}
