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

//! Unit tests for error mapping and ADBC status conversion.
//!
//! Tests cover:
//! - SEA error code parsing from HTTP status codes
//! - SEA error code parsing from error code strings
//! - ADBC status mapping for all error types
//! - Error retryability classification
//! - Error message formatting
//! - Error conversion between types

use adbc_core::error::Status;
use adbc_driver_databricks::client::{SeaError, SeaErrorCode, SeaErrorResponse};
use adbc_driver_databricks::Error;
use std::time::Duration;

// =============================================================================
// SeaErrorCode Tests
// =============================================================================

mod sea_error_code_tests {
    use super::*;

    #[test]
    fn test_from_http_status_400() {
        assert_eq!(SeaErrorCode::from_http_status(400), SeaErrorCode::BadRequest);
    }

    #[test]
    fn test_from_http_status_401() {
        assert_eq!(
            SeaErrorCode::from_http_status(401),
            SeaErrorCode::Unauthenticated
        );
    }

    #[test]
    fn test_from_http_status_403() {
        assert_eq!(
            SeaErrorCode::from_http_status(403),
            SeaErrorCode::PermissionDenied
        );
    }

    #[test]
    fn test_from_http_status_404() {
        assert_eq!(SeaErrorCode::from_http_status(404), SeaErrorCode::NotFound);
    }

    #[test]
    fn test_from_http_status_429() {
        assert_eq!(
            SeaErrorCode::from_http_status(429),
            SeaErrorCode::RequestLimitExceeded
        );
    }

    #[test]
    fn test_from_http_status_500() {
        assert_eq!(
            SeaErrorCode::from_http_status(500),
            SeaErrorCode::InternalError
        );
    }

    #[test]
    fn test_from_http_status_503() {
        assert_eq!(
            SeaErrorCode::from_http_status(503),
            SeaErrorCode::TemporarilyUnavailable
        );
    }

    #[test]
    fn test_from_http_status_unknown() {
        // Test various unknown status codes
        assert_eq!(SeaErrorCode::from_http_status(418), SeaErrorCode::Unknown);
        assert_eq!(SeaErrorCode::from_http_status(502), SeaErrorCode::Unknown);
        assert_eq!(SeaErrorCode::from_http_status(504), SeaErrorCode::Unknown);
        assert_eq!(SeaErrorCode::from_http_status(200), SeaErrorCode::Unknown);
    }

    #[test]
    fn test_from_error_code_string_bad_request() {
        assert_eq!(
            SeaErrorCode::from_error_code("BAD_REQUEST"),
            SeaErrorCode::BadRequest
        );
    }

    #[test]
    fn test_from_error_code_string_invalid_parameter() {
        assert_eq!(
            SeaErrorCode::from_error_code("INVALID_PARAMETER_VALUE"),
            SeaErrorCode::InvalidParameterValue
        );
    }

    #[test]
    fn test_from_error_code_string_unauthenticated() {
        assert_eq!(
            SeaErrorCode::from_error_code("UNAUTHENTICATED"),
            SeaErrorCode::Unauthenticated
        );
    }

    #[test]
    fn test_from_error_code_string_permission_denied() {
        assert_eq!(
            SeaErrorCode::from_error_code("PERMISSION_DENIED"),
            SeaErrorCode::PermissionDenied
        );
    }

    #[test]
    fn test_from_error_code_string_not_found_variants() {
        assert_eq!(
            SeaErrorCode::from_error_code("NOT_FOUND"),
            SeaErrorCode::NotFound
        );
        assert_eq!(
            SeaErrorCode::from_error_code("RESOURCE_NOT_FOUND"),
            SeaErrorCode::NotFound
        );
    }

    #[test]
    fn test_from_error_code_string_rate_limited_variants() {
        assert_eq!(
            SeaErrorCode::from_error_code("REQUEST_LIMIT_EXCEEDED"),
            SeaErrorCode::RequestLimitExceeded
        );
        assert_eq!(
            SeaErrorCode::from_error_code("RATE_LIMITED"),
            SeaErrorCode::RequestLimitExceeded
        );
    }

    #[test]
    fn test_from_error_code_string_internal_error() {
        assert_eq!(
            SeaErrorCode::from_error_code("INTERNAL_ERROR"),
            SeaErrorCode::InternalError
        );
    }

    #[test]
    fn test_from_error_code_string_unavailable_variants() {
        assert_eq!(
            SeaErrorCode::from_error_code("TEMPORARILY_UNAVAILABLE"),
            SeaErrorCode::TemporarilyUnavailable
        );
        assert_eq!(
            SeaErrorCode::from_error_code("SERVICE_UNAVAILABLE"),
            SeaErrorCode::TemporarilyUnavailable
        );
    }

    #[test]
    fn test_from_error_code_string_unknown() {
        assert_eq!(
            SeaErrorCode::from_error_code("SOME_UNKNOWN_CODE"),
            SeaErrorCode::Unknown
        );
        assert_eq!(SeaErrorCode::from_error_code(""), SeaErrorCode::Unknown);
    }

    #[test]
    fn test_to_adbc_status_mapping() {
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
    fn test_to_http_status_mapping() {
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
    fn test_is_retryable() {
        // Non-retryable errors
        assert!(!SeaErrorCode::BadRequest.is_retryable());
        assert!(!SeaErrorCode::InvalidParameterValue.is_retryable());
        assert!(!SeaErrorCode::Unauthenticated.is_retryable());
        assert!(!SeaErrorCode::PermissionDenied.is_retryable());
        assert!(!SeaErrorCode::NotFound.is_retryable());
        assert!(!SeaErrorCode::Unknown.is_retryable());

        // Retryable errors
        assert!(SeaErrorCode::RequestLimitExceeded.is_retryable());
        assert!(SeaErrorCode::InternalError.is_retryable());
        assert!(SeaErrorCode::TemporarilyUnavailable.is_retryable());
    }
}

// =============================================================================
// SeaError Tests
// =============================================================================

mod sea_error_tests {
    use super::*;

    #[test]
    fn test_new() {
        let err = SeaError::new(SeaErrorCode::BadRequest, "Invalid SQL syntax");
        assert_eq!(err.code, SeaErrorCode::BadRequest);
        assert_eq!(err.message, "Invalid SQL syntax");
        assert_eq!(err.retry_after, None);
    }

    #[test]
    fn test_with_retry_after() {
        let err =
            SeaError::new(SeaErrorCode::RequestLimitExceeded, "Rate limited").with_retry_after(30);
        assert_eq!(err.retry_after, Some(30));
    }

    #[test]
    fn test_from_response_with_error_code_and_message() {
        let response = SeaErrorResponse {
            error_code: Some("BAD_REQUEST".to_string()),
            message: Some("Invalid parameter".to_string()),
        };
        let err = SeaError::from_response(400, Some(response));
        assert_eq!(err.code, SeaErrorCode::BadRequest);
        assert_eq!(err.message, "Invalid parameter");
    }

    #[test]
    fn test_from_response_without_error_code() {
        let response = SeaErrorResponse {
            error_code: None,
            message: Some("Something went wrong".to_string()),
        };
        let err = SeaError::from_response(500, Some(response));
        assert_eq!(err.code, SeaErrorCode::InternalError);
        assert_eq!(err.message, "Something went wrong");
    }

    #[test]
    fn test_from_response_without_message() {
        let response = SeaErrorResponse {
            error_code: Some("NOT_FOUND".to_string()),
            message: None,
        };
        let err = SeaError::from_response(404, Some(response));
        assert_eq!(err.code, SeaErrorCode::NotFound);
        assert_eq!(err.message, "HTTP 404");
    }

    #[test]
    fn test_from_response_without_body() {
        let err = SeaError::from_response(503, None);
        assert_eq!(err.code, SeaErrorCode::TemporarilyUnavailable);
        assert_eq!(err.message, "HTTP 503");
    }

    #[test]
    fn test_is_retryable_delegates_to_code() {
        let retryable = SeaError::new(SeaErrorCode::RequestLimitExceeded, "Rate limited");
        assert!(retryable.is_retryable());

        let not_retryable = SeaError::new(SeaErrorCode::BadRequest, "Bad request");
        assert!(!not_retryable.is_retryable());
    }

    #[test]
    fn test_to_adbc_error() {
        let err = SeaError::new(SeaErrorCode::NotFound, "Table not found");
        let adbc_err = err.to_adbc_error();
        assert_eq!(adbc_err.status, Status::NotFound);
        assert_eq!(adbc_err.message, "Table not found");
    }

    #[test]
    fn test_display_format() {
        let err = SeaError::new(SeaErrorCode::BadRequest, "Invalid SQL");
        let display = format!("{}", err);
        assert!(display.contains("Invalid SQL"));
        assert!(display.contains("BadRequest"));
    }
}

// =============================================================================
// SeaErrorResponse Tests
// =============================================================================

mod sea_error_response_tests {
    use super::*;

    #[test]
    fn test_deserialization_full() {
        let json = r#"{"error_code": "BAD_REQUEST", "message": "Invalid SQL"}"#;
        let response: SeaErrorResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.error_code, Some("BAD_REQUEST".to_string()));
        assert_eq!(response.message, Some("Invalid SQL".to_string()));
    }

    #[test]
    fn test_deserialization_message_only() {
        let json = r#"{"message": "Something went wrong"}"#;
        let response: SeaErrorResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.error_code, None);
        assert_eq!(response.message, Some("Something went wrong".to_string()));
    }

    #[test]
    fn test_deserialization_error_code_only() {
        let json = r#"{"error_code": "INTERNAL_ERROR"}"#;
        let response: SeaErrorResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.error_code, Some("INTERNAL_ERROR".to_string()));
        assert_eq!(response.message, None);
    }

    #[test]
    fn test_deserialization_empty() {
        let json = r#"{}"#;
        let response: SeaErrorResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.error_code, None);
        assert_eq!(response.message, None);
    }

    #[test]
    fn test_deserialization_with_extra_fields() {
        // Should ignore extra fields
        let json = r#"{"error_code": "BAD_REQUEST", "message": "Error", "extra": "field"}"#;
        let response: SeaErrorResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.error_code, Some("BAD_REQUEST".to_string()));
        assert_eq!(response.message, Some("Error".to_string()));
    }
}

// =============================================================================
// Main Error Type Tests
// =============================================================================

mod error_tests {
    use super::*;

    #[test]
    fn test_sea_api_error_400_status() {
        let err = Error::sea_api("BAD_REQUEST", "Invalid SQL syntax", 400);
        assert_eq!(err.status(), Status::InvalidArguments);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_sea_api_error_401_status() {
        let err = Error::sea_api("UNAUTHENTICATED", "Invalid token", 401);
        assert_eq!(err.status(), Status::Unauthenticated);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_sea_api_error_403_status() {
        let err = Error::sea_api("PERMISSION_DENIED", "Access denied", 403);
        assert_eq!(err.status(), Status::Unauthorized);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_sea_api_error_404_status() {
        let err = Error::sea_api("NOT_FOUND", "Resource not found", 404);
        assert_eq!(err.status(), Status::NotFound);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_sea_api_error_429_status_and_retryable() {
        let err = Error::sea_api("REQUEST_LIMIT_EXCEEDED", "Rate limited", 429);
        assert_eq!(err.status(), Status::IO);
        assert!(err.is_retryable());
    }

    #[test]
    fn test_sea_api_error_500_status_and_retryable() {
        let err = Error::sea_api("INTERNAL_ERROR", "Internal server error", 500);
        assert_eq!(err.status(), Status::Internal);
        assert!(err.is_retryable());
    }

    #[test]
    fn test_sea_api_error_503_status_and_retryable() {
        let err = Error::sea_api("TEMPORARILY_UNAVAILABLE", "Service unavailable", 503);
        assert_eq!(err.status(), Status::IO);
        assert!(err.is_retryable());
    }

    #[test]
    fn test_sea_api_error_unknown_status() {
        let err = Error::sea_api("UNKNOWN", "Unknown error", 418);
        assert_eq!(err.status(), Status::Unknown);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_config_error() {
        let err = Error::config("Missing warehouse_id");
        assert_eq!(err.status(), Status::InvalidArguments);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_session_error() {
        let err = Error::session("Session expired");
        assert_eq!(err.status(), Status::InvalidState);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_statement_failed_error() {
        let err = Error::statement_failed("Execution error");
        assert_eq!(err.status(), Status::Internal);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_timeout_error() {
        let err = Error::Timeout;
        assert_eq!(err.status(), Status::Timeout);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_not_implemented_error() {
        let err = Error::not_implemented("Prepared statements");
        assert_eq!(err.status(), Status::NotImplemented);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_cancelled_error() {
        let err = Error::cancelled("User cancelled");
        assert_eq!(err.status(), Status::Cancelled);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_json_error_status() {
        let json_err: serde_json::Error = serde_json::from_str::<i32>("invalid").unwrap_err();
        let err = Error::Json(json_err);
        assert_eq!(err.status(), Status::InvalidData);
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_url_error_status() {
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
    fn test_retry_after_accessor() {
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
    fn test_error_display() {
        let err = Error::sea_api("BAD_REQUEST", "Invalid SQL", 400);
        let display = format!("{}", err);
        assert!(display.contains("BAD_REQUEST"));
        assert!(display.contains("Invalid SQL"));
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
    fn test_internal_error_helper() {
        let err = Error::internal("Internal failure");
        // internal() creates a StatementFailed variant
        assert_eq!(err.status(), Status::Internal);
        assert!(!err.is_retryable());
    }
}

// =============================================================================
// Error Conversion Tests
// =============================================================================

mod error_conversion_tests {
    use super::*;

    #[test]
    fn test_sea_error_to_main_error() {
        let sea_err = SeaError::new(SeaErrorCode::NotFound, "Resource not found");
        let err: Error = sea_err.into();

        match err {
            Error::SeaApi {
                code,
                message,
                http_status,
                ..
            } => {
                assert_eq!(code, "NotFound");
                assert_eq!(message, "Resource not found");
                assert_eq!(http_status, 404);
            }
            _ => panic!("Expected SeaApi variant"),
        }
    }

    #[test]
    fn test_all_sea_error_codes_convert_correctly() {
        let codes = [
            (SeaErrorCode::BadRequest, 400),
            (SeaErrorCode::InvalidParameterValue, 400),
            (SeaErrorCode::Unauthenticated, 401),
            (SeaErrorCode::PermissionDenied, 403),
            (SeaErrorCode::NotFound, 404),
            (SeaErrorCode::RequestLimitExceeded, 429),
            (SeaErrorCode::InternalError, 500),
            (SeaErrorCode::TemporarilyUnavailable, 503),
            (SeaErrorCode::Unknown, 500),
        ];

        for (code, expected_http) in codes {
            let sea_err = SeaError::new(code, "test message");
            let err: Error = sea_err.into();

            match err {
                Error::SeaApi { http_status, .. } => {
                    assert_eq!(
                        http_status, expected_http,
                        "Code {:?} should map to HTTP {}",
                        code, expected_http
                    );
                }
                _ => panic!("Expected SeaApi variant"),
            }
        }
    }
}

// =============================================================================
// Edge Case Tests
// =============================================================================

mod edge_case_tests {
    use super::*;

    #[test]
    fn test_empty_error_message() {
        let err = Error::sea_api("BAD_REQUEST", "", 400);
        assert_eq!(err.status(), Status::InvalidArguments);
    }

    #[test]
    fn test_very_long_error_message() {
        let long_message = "x".repeat(10000);
        let err = Error::sea_api("INTERNAL_ERROR", &long_message, 500);
        let display = format!("{}", err);
        assert!(display.contains(&long_message));
    }

    #[test]
    fn test_special_characters_in_message() {
        let special_message = "Error: <xml>&amp;'\"special\nchars\ttab";
        let err = Error::sea_api("BAD_REQUEST", special_message, 400);
        let display = format!("{}", err);
        assert!(display.contains(special_message));
    }

    #[test]
    fn test_unicode_in_error_message() {
        let unicode_message = "Error with unicode: \u{1F600} \u{4E2D}\u{6587}";
        let err = Error::sea_api("BAD_REQUEST", unicode_message, 400);
        let display = format!("{}", err);
        assert!(display.contains(unicode_message));
    }

    #[test]
    fn test_retry_after_zero_duration() {
        let err = Error::sea_api_with_retry_after(
            "REQUEST_LIMIT_EXCEEDED",
            "Rate limited",
            429,
            Some(Duration::from_secs(0)),
        );
        assert_eq!(err.retry_after(), Some(Duration::from_secs(0)));
    }

    #[test]
    fn test_retry_after_very_long_duration() {
        let long_duration = Duration::from_secs(86400); // 24 hours
        let err = Error::sea_api_with_retry_after(
            "REQUEST_LIMIT_EXCEEDED",
            "Rate limited",
            429,
            Some(long_duration),
        );
        assert_eq!(err.retry_after(), Some(long_duration));
    }
}
