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

//! API error handling for the SEA client.
//!
//! This module defines error types for SEA API responses and provides
//! mapping to ADBC error types.

use serde::Deserialize;

use crate::error::Error;

/// SEA API error response.
#[derive(Debug, Clone, Deserialize)]
pub struct ApiError {
    /// Error code.
    pub error_code: Option<String>,
    /// Error message.
    pub message: Option<String>,
}

impl ApiError {
    /// Convert the API error to a driver error.
    ///
    /// Creates a `Error::SeaApi` variant that properly captures the error code,
    /// message, and HTTP status for accurate ADBC status mapping and retry logic.
    pub fn into_error(self, status_code: reqwest::StatusCode) -> Error {
        let code = self
            .error_code
            .unwrap_or_else(|| status_code_to_error_code(status_code.as_u16()).to_string());
        let message = self
            .message
            .unwrap_or_else(|| "Unknown API error".to_string());

        Error::sea_api(code, message, status_code.as_u16())
    }
}

/// Convert HTTP status code to a default error code string.
fn status_code_to_error_code(status_code: u16) -> &'static str {
    match status_code {
        400 => error_codes::BAD_REQUEST,
        401 => error_codes::UNAUTHENTICATED,
        403 => error_codes::PERMISSION_DENIED,
        404 => error_codes::NOT_FOUND,
        429 => error_codes::REQUEST_LIMIT_EXCEEDED,
        500 => error_codes::INTERNAL_ERROR,
        503 => error_codes::TEMPORARILY_UNAVAILABLE,
        _ => "UNKNOWN_ERROR",
    }
}

/// Known SEA error codes.
pub mod error_codes {
    /// Bad request error.
    pub const BAD_REQUEST: &str = "BAD_REQUEST";
    /// Invalid parameter value.
    pub const INVALID_PARAMETER_VALUE: &str = "INVALID_PARAMETER_VALUE";
    /// Unauthenticated.
    pub const UNAUTHENTICATED: &str = "UNAUTHENTICATED";
    /// Permission denied.
    pub const PERMISSION_DENIED: &str = "PERMISSION_DENIED";
    /// Not found.
    pub const NOT_FOUND: &str = "NOT_FOUND";
    /// Request limit exceeded (rate limiting).
    pub const REQUEST_LIMIT_EXCEEDED: &str = "REQUEST_LIMIT_EXCEEDED";
    /// Internal error.
    pub const INTERNAL_ERROR: &str = "INTERNAL_ERROR";
    /// Temporarily unavailable.
    pub const TEMPORARILY_UNAVAILABLE: &str = "TEMPORARILY_UNAVAILABLE";
}

/// Determine if an HTTP status code should be retried.
pub fn is_retryable_status(status_code: u16) -> bool {
    matches!(status_code, 429 | 500 | 502 | 503 | 504)
}

#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::error::Status;
    use reqwest::StatusCode;

    #[test]
    fn test_api_error_into_error_with_code() {
        let api_error = ApiError {
            error_code: Some("BAD_REQUEST".to_string()),
            message: Some("Invalid SQL syntax".to_string()),
        };

        let err = api_error.into_error(StatusCode::BAD_REQUEST);

        match &err {
            Error::SeaApi {
                code,
                message,
                http_status,
            } => {
                assert_eq!(code, "BAD_REQUEST");
                assert_eq!(message, "Invalid SQL syntax");
                assert_eq!(*http_status, 400);
            }
            _ => panic!("Expected SeaApi error"),
        }

        // Verify ADBC status mapping
        assert_eq!(err.to_adbc_status(), Status::InvalidArguments);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_api_error_into_error_without_code() {
        let api_error = ApiError {
            error_code: None,
            message: Some("Rate limit exceeded".to_string()),
        };

        let err = api_error.into_error(StatusCode::TOO_MANY_REQUESTS);

        match &err {
            Error::SeaApi {
                code,
                message,
                http_status,
            } => {
                assert_eq!(code, "REQUEST_LIMIT_EXCEEDED");
                assert_eq!(message, "Rate limit exceeded");
                assert_eq!(*http_status, 429);
            }
            _ => panic!("Expected SeaApi error"),
        }

        // Verify ADBC status mapping
        assert_eq!(err.to_adbc_status(), Status::IO);
        assert!(err.is_retryable());
    }

    #[test]
    fn test_api_error_into_error_all_status_codes() {
        let test_cases = vec![
            (
                StatusCode::BAD_REQUEST,
                error_codes::BAD_REQUEST,
                Status::InvalidArguments,
                false,
            ),
            (
                StatusCode::UNAUTHORIZED,
                error_codes::UNAUTHENTICATED,
                Status::Unauthenticated,
                true,
            ),
            (
                StatusCode::FORBIDDEN,
                error_codes::PERMISSION_DENIED,
                Status::Unauthorized,
                false,
            ),
            (
                StatusCode::NOT_FOUND,
                error_codes::NOT_FOUND,
                Status::NotFound,
                false,
            ),
            (
                StatusCode::TOO_MANY_REQUESTS,
                error_codes::REQUEST_LIMIT_EXCEEDED,
                Status::IO,
                true,
            ),
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                error_codes::INTERNAL_ERROR,
                Status::Internal,
                true,
            ),
            (
                StatusCode::SERVICE_UNAVAILABLE,
                error_codes::TEMPORARILY_UNAVAILABLE,
                Status::IO,
                true,
            ),
        ];

        for (status_code, expected_code, expected_adbc, expected_retry) in test_cases {
            let api_error = ApiError {
                error_code: None,
                message: Some("test".to_string()),
            };

            let err = api_error.into_error(status_code);

            match &err {
                Error::SeaApi {
                    code, http_status, ..
                } => {
                    assert_eq!(code, expected_code, "Code mismatch for {}", status_code);
                    assert_eq!(*http_status, status_code.as_u16());
                }
                _ => panic!("Expected SeaApi error for {}", status_code),
            }

            assert_eq!(
                err.to_adbc_status(),
                expected_adbc,
                "ADBC status mismatch for {}",
                status_code
            );
            assert_eq!(
                err.is_retryable(),
                expected_retry,
                "Retryable mismatch for {}",
                status_code
            );
        }
    }

    #[test]
    fn test_is_retryable_status() {
        assert!(is_retryable_status(429));
        assert!(is_retryable_status(500));
        assert!(is_retryable_status(502));
        assert!(is_retryable_status(503));
        assert!(is_retryable_status(504));

        assert!(!is_retryable_status(400));
        assert!(!is_retryable_status(401));
        assert!(!is_retryable_status(403));
        assert!(!is_retryable_status(404));
    }

    #[test]
    fn test_status_code_to_error_code() {
        assert_eq!(status_code_to_error_code(400), error_codes::BAD_REQUEST);
        assert_eq!(status_code_to_error_code(401), error_codes::UNAUTHENTICATED);
        assert_eq!(
            status_code_to_error_code(403),
            error_codes::PERMISSION_DENIED
        );
        assert_eq!(status_code_to_error_code(404), error_codes::NOT_FOUND);
        assert_eq!(
            status_code_to_error_code(429),
            error_codes::REQUEST_LIMIT_EXCEEDED
        );
        assert_eq!(status_code_to_error_code(500), error_codes::INTERNAL_ERROR);
        assert_eq!(
            status_code_to_error_code(503),
            error_codes::TEMPORARILY_UNAVAILABLE
        );
        assert_eq!(status_code_to_error_code(418), "UNKNOWN_ERROR");
    }
}
