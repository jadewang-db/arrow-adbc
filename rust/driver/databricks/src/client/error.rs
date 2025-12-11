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

use adbc_core::error::Status;

/// Error codes from the SEA API.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeaErrorCode {
    /// Bad request (400).
    BadRequest,
    /// Invalid parameter value (400).
    InvalidParameterValue,
    /// Unauthenticated (401).
    Unauthenticated,
    /// Permission denied (403).
    PermissionDenied,
    /// Resource not found (404).
    NotFound,
    /// Request limit exceeded (429).
    RequestLimitExceeded,
    /// Internal server error (500).
    InternalError,
    /// Service temporarily unavailable (503).
    TemporarilyUnavailable,
    /// Unknown error.
    Unknown,
}

impl SeaErrorCode {
    /// Create from HTTP status code.
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

    /// Map to ADBC status.
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

    /// Whether this error should be retried.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            SeaErrorCode::RequestLimitExceeded
                | SeaErrorCode::InternalError
                | SeaErrorCode::TemporarilyUnavailable
        )
    }
}

/// Error from the SEA API.
#[derive(Debug, thiserror::Error)]
#[error("SEA API error: {message} (code: {code:?})")]
pub struct SeaError {
    /// Error code.
    pub code: SeaErrorCode,
    /// Error message.
    pub message: String,
    /// Optional retry-after duration in seconds.
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

    /// Create a new SEA error with retry-after.
    pub fn with_retry_after(mut self, seconds: u64) -> Self {
        self.retry_after = Some(seconds);
        self
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
