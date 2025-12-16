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
    pub fn into_error(self, status_code: reqwest::StatusCode) -> Error {
        let message = self
            .message
            .unwrap_or_else(|| "Unknown API error".to_string());

        match status_code.as_u16() {
            400 => Error::InvalidArgument(message),
            401 => Error::Unauthenticated(message),
            403 => Error::Unauthorized(message),
            404 => Error::NotFound(message),
            429 => Error::Io(format!("Rate limited: {}", message)),
            500 => Error::Internal(message),
            503 => Error::Io(format!("Service unavailable: {}", message)),
            _ => Error::Internal(format!("HTTP {}: {}", status_code, message)),
        }
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

/// Determine if an error should be retried.
pub fn is_retryable(error: &Error) -> bool {
    match error {
        Error::Io(_) => true,
        Error::Internal(_) => true,
        _ => false,
    }
}

/// Determine if an HTTP status code should be retried.
pub fn is_retryable_status(status_code: u16) -> bool {
    matches!(status_code, 429 | 500 | 502 | 503 | 504)
}
