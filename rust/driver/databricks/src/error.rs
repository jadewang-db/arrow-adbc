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

/// Result type alias for Databricks driver operations
pub type Result<T> = std::result::Result<T, Error>;
