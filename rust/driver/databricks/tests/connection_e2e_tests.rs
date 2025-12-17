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

//! End-to-end tests for DatabricksConnection

mod e2e;

use adbc_core::Connection;
use e2e::helpers::{can_execute_e2e_tests, create_test_connection, create_test_database};

/// Macro for conditional test execution
macro_rules! skip_if_no_config {
    () => {
        if !can_execute_e2e_tests() {
            println!("Skipping test: DATABRICKS_TEST_CONFIG_FILE not set or file not found");
            return;
        }
    };
}

#[test]
#[ignore]
fn test_e2e_connection_lifecycle() {
    skip_if_no_config!();

    let session_id = {
        let database = create_test_database();
        let mut conn = create_test_connection(&database);

        // Verify connection is established by getting session ID
        let sid = conn
            .session_id()
            .expect("Should be able to get session ID");
        assert!(!sid.is_empty(), "Session ID should not be empty");

        // Verify connection is functional by creating a statement
        let stmt = conn.new_statement();
        assert!(
            stmt.is_ok(),
            "Should be able to create statement from connection"
        );

        sid
    }; // Connection dropped here, session should be terminated

    assert!(!session_id.is_empty());
    println!("Connection lifecycle verified with session: {}", session_id);
}

#[test]
#[ignore]
fn test_e2e_connection_cancel() {
    skip_if_no_config!();

    let database = create_test_database();
    let mut conn = create_test_connection(&database);

    // Cancel should succeed (even though there's nothing to cancel)
    let result = conn.cancel();
    assert!(
        result.is_ok(),
        "Cancel should succeed on connection: {:?}",
        result.err()
    );
}

#[test]
#[ignore]
fn test_e2e_connection_commit_not_supported() {
    skip_if_no_config!();

    let database = create_test_database();
    let mut conn = create_test_connection(&database);

    // Commit should return NotImplemented
    let result = conn.commit();
    assert!(
        result.is_err(),
        "Commit should fail (transactions not supported)"
    );

    let err = result.unwrap_err();
    assert_eq!(
        err.status,
        adbc_core::error::Status::NotImplemented,
        "Commit should return NotImplemented status"
    );
    assert!(
        err.message.contains("not support"),
        "Error message should mention transactions not supported"
    );
}

#[test]
#[ignore]
fn test_e2e_connection_rollback_not_supported() {
    skip_if_no_config!();

    let database = create_test_database();
    let mut conn = create_test_connection(&database);

    // Rollback should return NotImplemented
    let result = conn.rollback();
    assert!(
        result.is_err(),
        "Rollback should fail (transactions not supported)"
    );

    let err = result.unwrap_err();
    assert_eq!(
        err.status,
        adbc_core::error::Status::NotImplemented,
        "Rollback should return NotImplemented status"
    );
    assert!(
        err.message.contains("not support"),
        "Error message should mention transactions not supported"
    );
}

#[test]
#[ignore]
fn test_e2e_connection_multiple_statements() {
    skip_if_no_config!();

    let database = create_test_database();
    let mut conn = create_test_connection(&database);

    // Create multiple statements from same connection
    let stmt1 = conn.new_statement();
    assert!(stmt1.is_ok(), "Should be able to create first statement");

    let stmt2 = conn.new_statement();
    assert!(
        stmt2.is_ok(),
        "Should be able to create second statement from same connection"
    );

    // Both statements should be independent and functional
    // (actual functionality will be tested in statement E2E tests)
}
