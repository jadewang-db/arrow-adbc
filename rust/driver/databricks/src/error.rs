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

use adbc_core::error::Status;

/// Error type for the Databricks ADBC driver.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid argument provided.
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    /// Authentication failed.
    #[error("Unauthenticated: {0}")]
    Unauthenticated(String),

    /// Authorization/permission denied.
    #[error("Unauthorized: {0}")]
    Unauthorized(String),

    /// Resource not found.
    #[error("Not found: {0}")]
    NotFound(String),

    /// I/O error.
    #[error("I/O error: {0}")]
    Io(String),

    /// Internal error.
    #[error("Internal error: {0}")]
    Internal(String),

    /// Feature not implemented.
    #[error("Not implemented: {0}")]
    NotImplemented(String),

    /// HTTP error from reqwest.
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    /// JSON parsing error.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Arrow error.
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    /// URL parsing error.
    #[error("URL error: {0}")]
    Url(#[from] url::ParseError),
}

impl Error {
    /// Get the ADBC status code for this error.
    pub fn status(&self) -> Status {
        match self {
            Error::InvalidArgument(_) => Status::InvalidArguments,
            Error::Unauthenticated(_) => Status::Unauthenticated,
            Error::Unauthorized(_) => Status::Unauthorized,
            Error::NotFound(_) => Status::NotFound,
            Error::Io(_) => Status::IO,
            Error::Internal(_) => Status::Internal,
            Error::NotImplemented(_) => Status::NotImplemented,
            Error::Http(_) => Status::IO,
            Error::Json(_) => Status::Internal,
            Error::Arrow(_) => Status::Internal,
            Error::Url(_) => Status::InvalidArguments,
        }
    }
}

impl From<Error> for adbc_core::error::Error {
    fn from(err: Error) -> Self {
        adbc_core::error::Error::with_message_and_status(err.to_string(), err.status())
    }
}

/// Result type for the Databricks ADBC driver.
pub type Result<T> = std::result::Result<T, Error>;
