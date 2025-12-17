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

//! E2E configuration and infrastructure tests

#[macro_use]
mod e2e;

use e2e::helpers;

#[test]
#[ignore]
fn test_e2e_config_and_connect() {
    skip_if_no_config!();
    let config = helpers::get_test_config();

    // Verify config loaded from JSON file
    assert!(
        !config.environment.is_empty(),
        "Environment should not be empty"
    );
    assert!(!config.uri.is_empty(), "URI should not be empty");
    assert!(!config.token.is_empty(), "Token should not be empty");

    // Verify URI parsing works
    let (host, warehouse_id) = config.parse_uri().unwrap();
    assert!(
        host.starts_with("https://"),
        "Host should start with https://"
    );
    assert!(!warehouse_id.is_empty(), "Warehouse ID should not be empty");

    println!("✅ Config loaded successfully from DATABRICKS_TEST_CONFIG_FILE");
    println!("  Environment: {}", config.environment);
    println!("  Host: {}", host);
    println!("  Warehouse ID: {}", warehouse_id);
}
