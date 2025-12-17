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

//! Error types for the Databricks driver

use thiserror::Error;

/// Error types for Databricks driver operations
#[derive(Error, Debug)]
pub enum Error {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("SEA API error: {code} - {message}")]
    SeaApi {
        code: String,
        message: String,
        http_status: u16,
        retry_after: Option<std::time::Duration>,
    },

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Invalid configuration: {0}")]
    Config(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Statement failed: {0}")]
    StatementFailed(String),

    #[error("Session error: {0}")]
    Session(String),

    #[error("Timeout waiting for statement")]
    Timeout,
}

impl Error {
    /// Convert this error to an ADBC status code
    pub fn to_adbc_status(&self) -> adbc_core::error::Status {
        match self {
            Error::SeaApi { code: _, http_status, .. } => {
                match *http_status {
                    400 => adbc_core::error::Status::InvalidArguments,
                    401 => adbc_core::error::Status::Unauthenticated,
                    403 => adbc_core::error::Status::Unauthorized,
                    404 => adbc_core::error::Status::NotFound,
                    429 => adbc_core::error::Status::IO, // Rate limited, retryable
                    500 => adbc_core::error::Status::Internal,
                    503 => adbc_core::error::Status::IO, // Unavailable, retryable
                    _ => adbc_core::error::Status::Unknown,
                }
            }
            Error::Http(_) => adbc_core::error::Status::IO,
            Error::Arrow(_) => adbc_core::error::Status::InvalidData,
            Error::Json(_) => adbc_core::error::Status::InvalidData,
            Error::Config(_) => adbc_core::error::Status::InvalidArguments,
            Error::Io(_) => adbc_core::error::Status::IO,
            Error::StatementFailed(_) => adbc_core::error::Status::Internal,
            Error::Session(_) => adbc_core::error::Status::InvalidState,
            Error::Timeout => adbc_core::error::Status::Timeout,
        }
    }

    /// Check if this error is retryable
    pub fn is_retryable(&self) -> bool {
        match self {
            Error::SeaApi { http_status, .. } => {
                matches!(*http_status, 429 | 500 | 503)
            }
            Error::Http(e) => e.is_timeout() || e.is_connect(),
            Error::Io(_) => true,
            _ => false,
        }
    }
}

impl From<Error> for adbc_core::error::Error {
    fn from(err: Error) -> Self {
        adbc_core::error::Error::with_message_and_status(
            err.to_string(),
            err.to_adbc_status(),
        )
    }
}

/// Result type alias for Databricks driver operations
pub type Result<T> = std::result::Result<T, Error>;
