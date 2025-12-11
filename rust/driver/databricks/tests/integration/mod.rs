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

//! Integration test modules for the Databricks ADBC driver.
//!
//! This module organizes integration tests that verify the full driver stack:
//! - `connection_tests`: Session lifecycle, connection options, multiple connections
//! - `query_tests`: Query execution with inline/external results, empty results, cancellation
//! - `metadata_tests`: get_info, get_objects, get_table_schema, get_table_types

pub mod connection_tests;
pub mod metadata_tests;
pub mod query_tests;
pub mod test_utils;
