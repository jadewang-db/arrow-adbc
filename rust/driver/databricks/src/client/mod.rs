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

//! SEA (Statement Execution API) client implementation.
//!
//! This module provides the HTTP client for interacting with the
//! Databricks Statement Execution API.

mod error;
mod models;

pub use error::SeaError;
pub use models::*;

/// SEA (Statement Execution API) client for Databricks.
///
/// Handles all HTTP communication with the Databricks SQL Warehouse API.
#[derive(Debug)]
pub struct SeaClient {
    // Will be implemented in a later work item
}

impl SeaClient {
    /// Create a new SEA client.
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for SeaClient {
    fn default() -> Self {
        Self::new()
    }
}
