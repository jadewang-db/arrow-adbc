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

//! End-to-end tests for session management
//!
//! These tests validate session creation and deletion against a real Databricks SQL Warehouse.
//! They require the DATABRICKS_TEST_CONFIG_FILE environment variable to be set.

mod e2e;

use adbc_driver_databricks::client::{SeaClient, SeaClientConfig};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
#[ignore]
async fn test_e2e_session_create_and_terminate() {
    skip_if_no_config!();

    let config = e2e::helpers::get_test_config();
    let (host, warehouse_id) = config.parse_uri().expect("Failed to parse URI");

    let client_config = SeaClientConfig {
        host,
        token: config.token,
        warehouse_id,
        connect_timeout: Duration::from_secs(10),
        read_timeout: Duration::from_secs(300),
    };

    let client = Arc::new(SeaClient::new(client_config).expect("Failed to create SeaClient"));
    let runtime = tokio::runtime::Runtime::new().expect("Failed to create runtime");

    // Create session
    let session_id = runtime
        .block_on(client.create_session(
            if config.metadata.catalog.is_empty() {
                None
            } else {
                Some(config.metadata.catalog.clone())
            },
            if config.metadata.schema.is_empty() {
                None
            } else {
                Some(config.metadata.schema.clone())
            },
        ))
        .expect("Failed to create session");

    assert!(!session_id.is_empty());
    println!("Created session: {}", session_id);

    // Terminate session
    runtime
        .block_on(client.delete_session(&session_id))
        .expect("Failed to delete session");
    println!("Terminated session: {}", session_id);
}

#[tokio::test]
#[ignore]
async fn test_e2e_session_manager_lifecycle() {
    skip_if_no_config!();

    let config = e2e::helpers::get_test_config();
    let (host, warehouse_id) = config.parse_uri().expect("Failed to parse URI");

    let client_config = SeaClientConfig {
        host,
        token: config.token,
        warehouse_id,
        connect_timeout: Duration::from_secs(10),
        read_timeout: Duration::from_secs(300),
    };

    let client = Arc::new(SeaClient::new(client_config).expect("Failed to create SeaClient"));

    let catalog = if config.metadata.catalog.is_empty() {
        None
    } else {
        Some(config.metadata.catalog.clone())
    };

    let schema = if config.metadata.schema.is_empty() {
        None
    } else {
        Some(config.metadata.schema.clone())
    };

    let manager = adbc_driver_databricks::session::SessionManager::new(
        client,
        catalog,
        schema,
    );

    // Get session ID (lazy creation)
    let session_id1 = manager
        .get_session_id()
        .await
        .expect("Failed to get session ID");
    assert!(!session_id1.is_empty());
    println!("Got session ID: {}", session_id1);

    // Get session ID again (should be cached)
    let session_id2 = manager
        .get_session_id()
        .await
        .expect("Failed to get session ID (cached)");
    assert_eq!(session_id1, session_id2);
    println!("Verified session ID is cached: {}", session_id2);

    // Terminate session
    manager
        .terminate()
        .await
        .expect("Failed to terminate session");
    println!("Terminated session: {}", session_id1);
}
