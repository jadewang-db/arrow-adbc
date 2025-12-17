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

//! SEA (Statement Execution API) client implementation

pub mod error;
pub mod models;

use crate::error::Result;

/// HTTP client for communicating with the Databricks Statement Execution API
pub struct SeaClient {
    http_client: reqwest::Client,
    host: String,
    token: String,
    warehouse_id: String,
}

/// Configuration for the SEA client
pub struct SeaClientConfig {
    pub host: String,
    pub token: String,
    pub warehouse_id: String,
    pub connect_timeout: std::time::Duration,
    pub read_timeout: std::time::Duration,
}

impl Default for SeaClientConfig {
    fn default() -> Self {
        Self {
            host: String::new(),
            token: String::new(),
            warehouse_id: String::new(),
            connect_timeout: std::time::Duration::from_secs(10),
            read_timeout: std::time::Duration::from_secs(300),
        }
    }
}

impl SeaClient {
    pub fn new(_config: SeaClientConfig) -> Result<Self> {
        todo!("SeaClient::new implementation in work item 1.3")
    }
}
