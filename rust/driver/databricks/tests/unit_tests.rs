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

//! Unit test suite for the Databricks ADBC driver.
//!
//! This module provides comprehensive unit tests organized by component:
//!
//! - **error_tests**: Tests for error mapping and ADBC status conversion
//! - **retry_tests**: Tests for retry logic with exponential backoff
//! - **type_mapping_tests**: Tests for Spark SQL to Arrow type conversion
//! - **client_tests**: Tests for HTTP client with mocked responses
//! - **polling_tests**: Tests for statement polling functionality
//! - **decompress_tests**: Tests for LZ4 decompression
//!
//! # Running Tests
//!
//! Run all unit tests:
//! ```bash
//! cargo test --package adbc_driver_databricks unit_tests
//! ```
//!
//! Run specific test module:
//! ```bash
//! cargo test --package adbc_driver_databricks unit_tests::error_tests
//! ```

mod unit;

// Re-export for test discovery
pub use unit::*;
