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

//! Unit tests for error type mapping and retryability

use adbc_driver_databricks::error::Error;
use adbc_core::error::Status;

#[test]
fn test_sea_error_to_adbc_status_400() {
    let err = Error::SeaApi {
        code: "BAD_REQUEST".into(),
        message: "Invalid SQL".into(),
        http_status: 400,
    };
    assert_eq!(err.to_adbc_status(), Status::InvalidArguments);
}

#[test]
fn test_sea_error_to_adbc_status_401() {
    let err = Error::SeaApi {
        code: "UNAUTHENTICATED".into(),
        message: "Invalid token".into(),
        http_status: 401,
    };
    assert_eq!(err.to_adbc_status(), Status::Unauthenticated);
}

#[test]
fn test_sea_error_to_adbc_status_403() {
    let err = Error::SeaApi {
        code: "PERMISSION_DENIED".into(),
        message: "No access".into(),
        http_status: 403,
    };
    assert_eq!(err.to_adbc_status(), Status::Unauthorized);
}

#[test]
fn test_sea_error_to_adbc_status_404() {
    let err = Error::SeaApi {
        code: "NOT_FOUND".into(),
        message: "Resource not found".into(),
        http_status: 404,
    };
    assert_eq!(err.to_adbc_status(), Status::NotFound);
}

#[test]
fn test_sea_error_to_adbc_status_429() {
    let err = Error::SeaApi {
        code: "REQUEST_LIMIT_EXCEEDED".into(),
        message: "Rate limited".into(),
        http_status: 429,
    };
    assert_eq!(err.to_adbc_status(), Status::IO);
}

#[test]
fn test_sea_error_to_adbc_status_500() {
    let err = Error::SeaApi {
        code: "INTERNAL_ERROR".into(),
        message: "Server error".into(),
        http_status: 500,
    };
    assert_eq!(err.to_adbc_status(), Status::Internal);
}

#[test]
fn test_sea_error_to_adbc_status_503() {
    let err = Error::SeaApi {
        code: "TEMPORARILY_UNAVAILABLE".into(),
        message: "Service unavailable".into(),
        http_status: 503,
    };
    assert_eq!(err.to_adbc_status(), Status::IO);
}

#[test]
fn test_retryable_errors_429() {
    let err_429 = Error::SeaApi {
        code: "REQUEST_LIMIT_EXCEEDED".into(),
        message: "Rate limited".into(),
        http_status: 429,
    };
    assert!(err_429.is_retryable());
}

#[test]
fn test_retryable_errors_500() {
    let err_500 = Error::SeaApi {
        code: "INTERNAL_ERROR".into(),
        message: "Server error".into(),
        http_status: 500,
    };
    assert!(err_500.is_retryable());
}

#[test]
fn test_retryable_errors_503() {
    let err_503 = Error::SeaApi {
        code: "TEMPORARILY_UNAVAILABLE".into(),
        message: "Service unavailable".into(),
        http_status: 503,
    };
    assert!(err_503.is_retryable());
}

#[test]
fn test_non_retryable_errors_400() {
    let err_400 = Error::SeaApi {
        code: "BAD_REQUEST".into(),
        message: "Invalid".into(),
        http_status: 400,
    };
    assert!(!err_400.is_retryable());
}

#[test]
fn test_non_retryable_errors_401() {
    let err_401 = Error::SeaApi {
        code: "UNAUTHENTICATED".into(),
        message: "Invalid token".into(),
        http_status: 401,
    };
    assert!(!err_401.is_retryable());
}

#[test]
fn test_non_retryable_errors_404() {
    let err_404 = Error::SeaApi {
        code: "NOT_FOUND".into(),
        message: "Not found".into(),
        http_status: 404,
    };
    assert!(!err_404.is_retryable());
}

#[test]
fn test_all_sea_error_codes() {
    // Test all error codes from design doc Section 5.2
    let test_cases = vec![
        ("BAD_REQUEST", 400, Status::InvalidArguments, false),
        ("INVALID_PARAMETER_VALUE", 400, Status::InvalidArguments, false),
        ("UNAUTHENTICATED", 401, Status::Unauthenticated, false),
        ("PERMISSION_DENIED", 403, Status::Unauthorized, false),
        ("NOT_FOUND", 404, Status::NotFound, false),
        ("REQUEST_LIMIT_EXCEEDED", 429, Status::IO, true),
        ("INTERNAL_ERROR", 500, Status::Internal, true),
        ("TEMPORARILY_UNAVAILABLE", 503, Status::IO, true),
    ];

    for (code, status, expected_adbc, expected_retry) in test_cases {
        let err = Error::SeaApi {
            code: code.into(),
            message: "test".into(),
            http_status: status,
        };
        assert_eq!(
            err.to_adbc_status(),
            expected_adbc,
            "Failed for code: {}",
            code
        );
        assert_eq!(
            err.is_retryable(),
            expected_retry,
            "Retry check failed for code: {}",
            code
        );
    }
}

#[test]
fn test_config_error_mapping() {
    let err = Error::Config("Missing required option".into());
    assert_eq!(err.to_adbc_status(), Status::InvalidArguments);
    assert!(!err.is_retryable());
}

#[test]
fn test_timeout_error_mapping() {
    let err = Error::Timeout;
    assert_eq!(err.to_adbc_status(), Status::Timeout);
    assert!(!err.is_retryable());
}

#[test]
fn test_statement_failed_error_mapping() {
    let err = Error::StatementFailed("Query failed".into());
    assert_eq!(err.to_adbc_status(), Status::Internal);
    assert!(!err.is_retryable());
}

#[test]
fn test_session_error_mapping() {
    let err = Error::Session("Session expired".into());
    assert_eq!(err.to_adbc_status(), Status::InvalidState);
    assert!(!err.is_retryable());
}

#[test]
fn test_io_error_retryable() {
    let err = Error::Io(std::io::Error::new(
        std::io::ErrorKind::ConnectionReset,
        "Connection reset",
    ));
    assert_eq!(err.to_adbc_status(), Status::IO);
    assert!(err.is_retryable());
}

#[test]
fn test_error_conversion_to_adbc_core() {
    let err = Error::SeaApi {
        code: "BAD_REQUEST".into(),
        message: "Invalid SQL".into(),
        http_status: 400,
    };

    let adbc_err: adbc_core::error::Error = err.into();
    assert_eq!(adbc_err.status, Status::InvalidArguments);
    assert!(adbc_err.message.contains("Invalid SQL"));
}

#[test]
fn test_error_message_preserved() {
    let err = Error::SeaApi {
        code: "INTERNAL_ERROR".into(),
        message: "Database connection failed".into(),
        http_status: 500,
    };

    let err_str = err.to_string();
    assert!(err_str.contains("INTERNAL_ERROR"));
    assert!(err_str.contains("Database connection failed"));
}
