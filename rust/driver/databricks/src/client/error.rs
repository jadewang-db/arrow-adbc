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

//! SEA API error handling.
//!
//! This module handles error responses from the Databricks Statement Execution API
//! and maps them to appropriate ADBC error types.
//!
//! # Error Mapping
//!
//! The SEA API returns errors in the following format:
//! ```json
//! {
//!   "error_code": "BAD_REQUEST",
//!   "message": "Invalid SQL syntax"
//! }
//! ```
//!
//! These are mapped to ADBC statuses based on HTTP status code:
//!
//! | HTTP Status | SEA Error Code | ADBC Status | Retryable |
//! |-------------|----------------|-------------|-----------|
//! | 400 | BAD_REQUEST | InvalidArguments | No |
//! | 400 | INVALID_PARAMETER_VALUE | InvalidArguments | No |
//! | 401 | UNAUTHENTICATED | Unauthenticated | Once |
//! | 403 | PERMISSION_DENIED | Unauthorized | No |
//! | 404 | NOT_FOUND | NotFound | No |
//! | 429 | REQUEST_LIMIT_EXCEEDED | IO | Yes |
//! | 500 | INTERNAL_ERROR | Internal | Yes |
//! | 503 | TEMPORARILY_UNAVAILABLE | IO | Yes |

use adbc_core::error::Status;
use serde::Deserialize;

/// Error codes from the SEA API.
///
/// These codes represent the different types of errors that can be returned
/// by the Databricks Statement Execution API.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeaErrorCode {
    /// Bad request (400) - invalid request format or parameters.
    BadRequest,
    /// Invalid parameter value (400) - specific parameter validation failure.
    InvalidParameterValue,
    /// Unauthenticated (401) - missing or invalid authentication.
    Unauthenticated,
    /// Permission denied (403) - authenticated but not authorized.
    PermissionDenied,
    /// Resource not found (404) - statement, session, or warehouse not found.
    NotFound,
    /// Request limit exceeded (429) - rate limiting applied.
    RequestLimitExceeded,
    /// Internal server error (500) - server-side error.
    InternalError,
    /// Service temporarily unavailable (503) - service is down or overloaded.
    TemporarilyUnavailable,
    /// Unknown error - unrecognized error code.
    Unknown,
}

impl SeaErrorCode {
    /// Create from HTTP status code.
    ///
    /// Maps HTTP status codes to SEA error codes. This is used when parsing
    /// error responses from the API.
    pub fn from_http_status(status: u16) -> Self {
        match status {
            400 => SeaErrorCode::BadRequest,
            401 => SeaErrorCode::Unauthenticated,
            403 => SeaErrorCode::PermissionDenied,
            404 => SeaErrorCode::NotFound,
            429 => SeaErrorCode::RequestLimitExceeded,
            500 => SeaErrorCode::InternalError,
            503 => SeaErrorCode::TemporarilyUnavailable,
            _ => SeaErrorCode::Unknown,
        }
    }

    /// Create from SEA API error code string.
    ///
    /// Parses the error_code field from SEA API error responses.
    pub fn from_error_code(code: &str) -> Self {
        match code {
            "BAD_REQUEST" => SeaErrorCode::BadRequest,
            "INVALID_PARAMETER_VALUE" => SeaErrorCode::InvalidParameterValue,
            "UNAUTHENTICATED" => SeaErrorCode::Unauthenticated,
            "PERMISSION_DENIED" => SeaErrorCode::PermissionDenied,
            "NOT_FOUND" | "RESOURCE_NOT_FOUND" => SeaErrorCode::NotFound,
            "REQUEST_LIMIT_EXCEEDED" | "RATE_LIMITED" => SeaErrorCode::RequestLimitExceeded,
            "INTERNAL_ERROR" => SeaErrorCode::InternalError,
            "TEMPORARILY_UNAVAILABLE" | "SERVICE_UNAVAILABLE" => {
                SeaErrorCode::TemporarilyUnavailable
            }
            _ => SeaErrorCode::Unknown,
        }
    }

    /// Map to ADBC status.
    ///
    /// Converts the SEA error code to the appropriate ADBC status code.
    pub fn to_adbc_status(&self) -> Status {
        match self {
            SeaErrorCode::BadRequest => Status::InvalidArguments,
            SeaErrorCode::InvalidParameterValue => Status::InvalidArguments,
            SeaErrorCode::Unauthenticated => Status::Unauthenticated,
            SeaErrorCode::PermissionDenied => Status::Unauthorized,
            SeaErrorCode::NotFound => Status::NotFound,
            SeaErrorCode::RequestLimitExceeded => Status::IO,
            SeaErrorCode::InternalError => Status::Internal,
            SeaErrorCode::TemporarilyUnavailable => Status::IO,
            SeaErrorCode::Unknown => Status::Internal,
        }
    }

    /// Convert to the corresponding HTTP status code.
    ///
    /// This is useful when converting from SeaError to the main Error type
    /// which stores the HTTP status code.
    pub fn to_http_status(&self) -> u16 {
        match self {
            SeaErrorCode::BadRequest => 400,
            SeaErrorCode::InvalidParameterValue => 400,
            SeaErrorCode::Unauthenticated => 401,
            SeaErrorCode::PermissionDenied => 403,
            SeaErrorCode::NotFound => 404,
            SeaErrorCode::RequestLimitExceeded => 429,
            SeaErrorCode::InternalError => 500,
            SeaErrorCode::TemporarilyUnavailable => 503,
            SeaErrorCode::Unknown => 500, // Default to 500 for unknown errors
        }
    }

    /// Whether this error should be retried.
    ///
    /// Returns `true` for transient errors that may succeed if retried:
    /// - 429 (rate limited) - should retry with backoff
    /// - 500 (internal error) - may be a temporary issue
    /// - 503 (service unavailable) - service may recover
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            SeaErrorCode::RequestLimitExceeded
                | SeaErrorCode::InternalError
                | SeaErrorCode::TemporarilyUnavailable
        )
    }
}

/// Error response from the SEA API.
///
/// This structure matches the JSON error response format from the
/// Databricks Statement Execution API.
#[derive(Debug, Clone, Deserialize)]
pub struct SeaErrorResponse {
    /// Error code string (e.g., "BAD_REQUEST", "UNAUTHENTICATED")
    #[serde(default)]
    pub error_code: Option<String>,
    /// Human-readable error message
    #[serde(default)]
    pub message: Option<String>,
}

/// Error from the SEA API.
///
/// Represents a parsed error from the Databricks Statement Execution API,
/// including the error code, message, and optional retry information.
#[derive(Debug, thiserror::Error)]
#[error("SEA API error: {message} (code: {code:?})")]
pub struct SeaError {
    /// Parsed error code.
    pub code: SeaErrorCode,
    /// Error message from the API.
    pub message: String,
    /// Optional retry-after duration in seconds (from Retry-After header).
    pub retry_after: Option<u64>,
}

impl SeaError {
    /// Create a new SEA error.
    pub fn new(code: SeaErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            retry_after: None,
        }
    }

    /// Create a SEA error from HTTP status and optional error response body.
    ///
    /// This is the primary constructor used when parsing API responses.
    pub fn from_response(
        http_status: u16,
        error_response: Option<SeaErrorResponse>,
    ) -> Self {
        let (code, message) = match error_response {
            Some(resp) => {
                let code = resp
                    .error_code
                    .as_deref()
                    .map(SeaErrorCode::from_error_code)
                    .unwrap_or_else(|| SeaErrorCode::from_http_status(http_status));
                let message = resp
                    .message
                    .unwrap_or_else(|| format!("HTTP {http_status}"));
                (code, message)
            }
            None => (
                SeaErrorCode::from_http_status(http_status),
                format!("HTTP {http_status}"),
            ),
        };

        Self {
            code,
            message,
            retry_after: None,
        }
    }

    /// Set the retry-after duration.
    ///
    /// This should be set from the Retry-After header if present in the response.
    pub fn with_retry_after(mut self, seconds: u64) -> Self {
        self.retry_after = Some(seconds);
        self
    }

    /// Check if this error is retryable.
    pub fn is_retryable(&self) -> bool {
        self.code.is_retryable()
    }

    /// Map to ADBC error.
    pub fn to_adbc_error(&self) -> adbc_core::error::Error {
        adbc_core::error::Error::with_message_and_status(
            self.message.clone(),
            self.code.to_adbc_status(),
        )
    }
}

impl From<SeaError> for adbc_core::error::Error {
    fn from(err: SeaError) -> Self {
        err.to_adbc_error()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sea_error_code_from_http_status() {
        assert_eq!(SeaErrorCode::from_http_status(400), SeaErrorCode::BadRequest);
        assert_eq!(
            SeaErrorCode::from_http_status(401),
            SeaErrorCode::Unauthenticated
        );
        assert_eq!(
            SeaErrorCode::from_http_status(403),
            SeaErrorCode::PermissionDenied
        );
        assert_eq!(SeaErrorCode::from_http_status(404), SeaErrorCode::NotFound);
        assert_eq!(
            SeaErrorCode::from_http_status(429),
            SeaErrorCode::RequestLimitExceeded
        );
        assert_eq!(
            SeaErrorCode::from_http_status(500),
            SeaErrorCode::InternalError
        );
        assert_eq!(
            SeaErrorCode::from_http_status(503),
            SeaErrorCode::TemporarilyUnavailable
        );
        assert_eq!(SeaErrorCode::from_http_status(418), SeaErrorCode::Unknown);
    }

    #[test]
    fn test_sea_error_code_from_error_code_string() {
        assert_eq!(
            SeaErrorCode::from_error_code("BAD_REQUEST"),
            SeaErrorCode::BadRequest
        );
        assert_eq!(
            SeaErrorCode::from_error_code("INVALID_PARAMETER_VALUE"),
            SeaErrorCode::InvalidParameterValue
        );
        assert_eq!(
            SeaErrorCode::from_error_code("UNAUTHENTICATED"),
            SeaErrorCode::Unauthenticated
        );
        assert_eq!(
            SeaErrorCode::from_error_code("PERMISSION_DENIED"),
            SeaErrorCode::PermissionDenied
        );
        assert_eq!(
            SeaErrorCode::from_error_code("NOT_FOUND"),
            SeaErrorCode::NotFound
        );
        assert_eq!(
            SeaErrorCode::from_error_code("RESOURCE_NOT_FOUND"),
            SeaErrorCode::NotFound
        );
        assert_eq!(
            SeaErrorCode::from_error_code("REQUEST_LIMIT_EXCEEDED"),
            SeaErrorCode::RequestLimitExceeded
        );
        assert_eq!(
            SeaErrorCode::from_error_code("RATE_LIMITED"),
            SeaErrorCode::RequestLimitExceeded
        );
        assert_eq!(
            SeaErrorCode::from_error_code("INTERNAL_ERROR"),
            SeaErrorCode::InternalError
        );
        assert_eq!(
            SeaErrorCode::from_error_code("TEMPORARILY_UNAVAILABLE"),
            SeaErrorCode::TemporarilyUnavailable
        );
        assert_eq!(
            SeaErrorCode::from_error_code("SERVICE_UNAVAILABLE"),
            SeaErrorCode::TemporarilyUnavailable
        );
        assert_eq!(
            SeaErrorCode::from_error_code("UNKNOWN_CODE"),
            SeaErrorCode::Unknown
        );
    }

    #[test]
    fn test_sea_error_code_to_adbc_status() {
        assert_eq!(
            SeaErrorCode::BadRequest.to_adbc_status(),
            Status::InvalidArguments
        );
        assert_eq!(
            SeaErrorCode::InvalidParameterValue.to_adbc_status(),
            Status::InvalidArguments
        );
        assert_eq!(
            SeaErrorCode::Unauthenticated.to_adbc_status(),
            Status::Unauthenticated
        );
        assert_eq!(
            SeaErrorCode::PermissionDenied.to_adbc_status(),
            Status::Unauthorized
        );
        assert_eq!(SeaErrorCode::NotFound.to_adbc_status(), Status::NotFound);
        assert_eq!(
            SeaErrorCode::RequestLimitExceeded.to_adbc_status(),
            Status::IO
        );
        assert_eq!(
            SeaErrorCode::InternalError.to_adbc_status(),
            Status::Internal
        );
        assert_eq!(
            SeaErrorCode::TemporarilyUnavailable.to_adbc_status(),
            Status::IO
        );
        assert_eq!(SeaErrorCode::Unknown.to_adbc_status(), Status::Internal);
    }

    #[test]
    fn test_sea_error_code_to_http_status() {
        assert_eq!(SeaErrorCode::BadRequest.to_http_status(), 400);
        assert_eq!(SeaErrorCode::InvalidParameterValue.to_http_status(), 400);
        assert_eq!(SeaErrorCode::Unauthenticated.to_http_status(), 401);
        assert_eq!(SeaErrorCode::PermissionDenied.to_http_status(), 403);
        assert_eq!(SeaErrorCode::NotFound.to_http_status(), 404);
        assert_eq!(SeaErrorCode::RequestLimitExceeded.to_http_status(), 429);
        assert_eq!(SeaErrorCode::InternalError.to_http_status(), 500);
        assert_eq!(SeaErrorCode::TemporarilyUnavailable.to_http_status(), 503);
        assert_eq!(SeaErrorCode::Unknown.to_http_status(), 500);
    }

    #[test]
    fn test_sea_error_code_is_retryable() {
        assert!(!SeaErrorCode::BadRequest.is_retryable());
        assert!(!SeaErrorCode::InvalidParameterValue.is_retryable());
        assert!(!SeaErrorCode::Unauthenticated.is_retryable());
        assert!(!SeaErrorCode::PermissionDenied.is_retryable());
        assert!(!SeaErrorCode::NotFound.is_retryable());
        assert!(SeaErrorCode::RequestLimitExceeded.is_retryable());
        assert!(SeaErrorCode::InternalError.is_retryable());
        assert!(SeaErrorCode::TemporarilyUnavailable.is_retryable());
        assert!(!SeaErrorCode::Unknown.is_retryable());
    }

    #[test]
    fn test_sea_error_new() {
        let err = SeaError::new(SeaErrorCode::BadRequest, "Invalid SQL");
        assert_eq!(err.code, SeaErrorCode::BadRequest);
        assert_eq!(err.message, "Invalid SQL");
        assert_eq!(err.retry_after, None);
    }

    #[test]
    fn test_sea_error_with_retry_after() {
        let err = SeaError::new(SeaErrorCode::RequestLimitExceeded, "Rate limited")
            .with_retry_after(30);
        assert_eq!(err.retry_after, Some(30));
    }

    #[test]
    fn test_sea_error_from_response_with_error_code() {
        let response = SeaErrorResponse {
            error_code: Some("BAD_REQUEST".to_string()),
            message: Some("Invalid parameter".to_string()),
        };
        let err = SeaError::from_response(400, Some(response));
        assert_eq!(err.code, SeaErrorCode::BadRequest);
        assert_eq!(err.message, "Invalid parameter");
    }

    #[test]
    fn test_sea_error_from_response_without_error_code() {
        let response = SeaErrorResponse {
            error_code: None,
            message: Some("Something went wrong".to_string()),
        };
        let err = SeaError::from_response(500, Some(response));
        assert_eq!(err.code, SeaErrorCode::InternalError);
        assert_eq!(err.message, "Something went wrong");
    }

    #[test]
    fn test_sea_error_from_response_without_body() {
        let err = SeaError::from_response(503, None);
        assert_eq!(err.code, SeaErrorCode::TemporarilyUnavailable);
        assert_eq!(err.message, "HTTP 503");
    }

    #[test]
    fn test_sea_error_is_retryable() {
        let retryable = SeaError::new(SeaErrorCode::RequestLimitExceeded, "Rate limited");
        assert!(retryable.is_retryable());

        let not_retryable = SeaError::new(SeaErrorCode::BadRequest, "Bad request");
        assert!(!not_retryable.is_retryable());
    }

    #[test]
    fn test_sea_error_to_adbc_error() {
        let err = SeaError::new(SeaErrorCode::NotFound, "Table not found");
        let adbc_err = err.to_adbc_error();
        assert_eq!(adbc_err.status, Status::NotFound);
        assert_eq!(adbc_err.message, "Table not found");
    }

    #[test]
    fn test_sea_error_display() {
        let err = SeaError::new(SeaErrorCode::BadRequest, "Invalid SQL");
        let display = format!("{err}");
        assert!(display.contains("Invalid SQL"));
        assert!(display.contains("BadRequest"));
    }

    #[test]
    fn test_sea_error_response_deserialization() {
        let json = r#"{"error_code": "BAD_REQUEST", "message": "Invalid SQL"}"#;
        let response: SeaErrorResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.error_code, Some("BAD_REQUEST".to_string()));
        assert_eq!(response.message, Some("Invalid SQL".to_string()));
    }

    #[test]
    fn test_sea_error_response_deserialization_partial() {
        let json = r#"{"message": "Something went wrong"}"#;
        let response: SeaErrorResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.error_code, None);
        assert_eq!(response.message, Some("Something went wrong".to_string()));
    }

    #[test]
    fn test_sea_error_response_deserialization_empty() {
        let json = r#"{}"#;
        let response: SeaErrorResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.error_code, None);
        assert_eq!(response.message, None);
    }
}
