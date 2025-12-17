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

//! End-to-end tests for the Databricks ADBC driver
//!
//! These tests require a real Databricks workspace and are disabled by default.
//! To run E2E tests:
//!
//! 1. Create a configuration file (e.g., `databricks.local.json`) with your test environment details
//! 2. Set the `DATABRICKS_TEST_CONFIG_FILE` environment variable to point to your config file
//! 3. Run tests with the `--ignored` flag: `cargo test --ignored`
//!
//! Example:
//! ```bash
//! export DATABRICKS_TEST_CONFIG_FILE=/path/to/databricks.local.json
//! cargo test --ignored
//! ```

pub mod config;
pub mod helpers;
