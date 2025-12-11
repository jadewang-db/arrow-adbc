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

//! Integration tests for connection lifecycle.
//!
//! Tests cover:
//! - Session creation and termination
//! - Connection options (autocommit, catalog, schema)
//! - Multiple connections from the same database
//! - Connection error handling

use adbc_core::error::Status;
use adbc_core::options::{OptionConnection, OptionValue};
use adbc_core::{Connection, Database, Optionable};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::test_utils::*;

// ==============================================================================
// Session Lifecycle Tests
// ==============================================================================

/// Test that creating a connection opens a session.
#[tokio::test(flavor = "multi_thread")]
async fn test_connection_creates_session() {
    let mock_server = setup_mock_server_with_session(TEST_SESSION_ID).await;
    let db = create_test_database(&mock_server.uri(), TEST_WAREHOUSE_ID, TEST_TOKEN);

    let conn = db.new_connection();
    assert!(conn.is_ok(), "Connection should be created successfully");
}

/// Test that dropping a connection terminates the session.
#[tokio::test(flavor = "multi_thread")]
async fn test_connection_drop_terminates_session() {
    let mock_server = MockServer::start().await;

    // Session create
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/sessions"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "session-to-terminate"
            })),
        )
        .mount(&mock_server)
        .await;

    // Session delete - expect exactly one call
    Mock::given(method("DELETE"))
        .and(path("/api/2.0/sql/sessions/session-to-terminate"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&mock_server)
        .await;

    let db = create_test_database(&mock_server.uri(), TEST_WAREHOUSE_ID, TEST_TOKEN);

    {
        let _conn = db.new_connection().unwrap();
        // Connection dropped here
    }

    // Mock verification happens on MockServer drop
}

/// Test that session ID is consistent across multiple calls.
#[tokio::test(flavor = "multi_thread")]
async fn test_session_id_consistent() {
    let mock_server = MockServer::start().await;

    // Session create - should only be called once
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/sessions"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "consistent-session-id"
            })),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("DELETE"))
        .and(path("/api/2.0/sql/sessions/consistent-session-id"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&mock_server)
        .await;

    let db = create_test_database(&mock_server.uri(), TEST_WAREHOUSE_ID, TEST_TOKEN);
    let conn = db.new_connection().unwrap();

    // Multiple session ID retrievals should return the same ID
    let session_id_1 = conn.session_id().unwrap();
    let session_id_2 = conn.session_id().unwrap();

    assert_eq!(session_id_1, "consistent-session-id");
    assert_eq!(session_id_2, "consistent-session-id");
}

/// Test handling of session creation failure.
#[tokio::test(flavor = "multi_thread")]
async fn test_connection_session_creation_failure() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/sessions"))
        .respond_with(
            ResponseTemplate::new(401).set_body_json(serde_json::json!({
                "error_code": "UNAUTHENTICATED",
                "message": "Invalid authentication token"
            })),
        )
        .mount(&mock_server)
        .await;

    let db = create_test_database(&mock_server.uri(), TEST_WAREHOUSE_ID, TEST_TOKEN);
    let result = db.new_connection();

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.status, Status::IO);
}

// ==============================================================================
// Connection Options Tests
// ==============================================================================

/// Test that autocommit is always true for Databricks.
#[tokio::test(flavor = "multi_thread")]
async fn test_autocommit_always_true() {
    let mock_server = setup_mock_server_with_session(TEST_SESSION_ID).await;
    let db = create_test_database(&mock_server.uri(), TEST_WAREHOUSE_ID, TEST_TOKEN);
    let conn = db.new_connection().unwrap();

    let autocommit = conn.get_option_string(OptionConnection::AutoCommit).unwrap();
    assert_eq!(autocommit, "true");
}

/// Test that autocommit cannot be disabled.
#[tokio::test(flavor = "multi_thread")]
async fn test_autocommit_cannot_disable() {
    let mock_server = setup_mock_server_with_session(TEST_SESSION_ID).await;
    let db = create_test_database(&mock_server.uri(), TEST_WAREHOUSE_ID, TEST_TOKEN);
    let mut conn = db.new_connection().unwrap();

    // Setting to true should succeed
    let result = conn.set_option(
        OptionConnection::AutoCommit,
        OptionValue::String("true".to_string()),
    );
    assert!(result.is_ok());

    // Setting to false should fail
    let result = conn.set_option(
        OptionConnection::AutoCommit,
        OptionValue::String("false".to_string()),
    );
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().status, Status::NotImplemented);
}

/// Test setting and getting current catalog.
#[tokio::test(flavor = "multi_thread")]
async fn test_current_catalog_option() {
    let mock_server = setup_mock_server_with_session(TEST_SESSION_ID).await;
    let db = create_test_database_with_catalog(
        &mock_server.uri(),
        TEST_WAREHOUSE_ID,
        TEST_TOKEN,
        "initial_catalog",
        "initial_schema",
    );
    let mut conn = db.new_connection().unwrap();

    // Get initial catalog
    let catalog = conn
        .get_option_string(OptionConnection::CurrentCatalog)
        .unwrap();
    assert_eq!(catalog, "initial_catalog");

    // Set new catalog
    conn.set_option(
        OptionConnection::CurrentCatalog,
        OptionValue::String("new_catalog".to_string()),
    )
    .unwrap();

    // Verify new catalog
    let catalog = conn
        .get_option_string(OptionConnection::CurrentCatalog)
        .unwrap();
    assert_eq!(catalog, "new_catalog");
}

/// Test setting and getting current schema.
#[tokio::test(flavor = "multi_thread")]
async fn test_current_schema_option() {
    let mock_server = setup_mock_server_with_session(TEST_SESSION_ID).await;
    let db = create_test_database_with_catalog(
        &mock_server.uri(),
        TEST_WAREHOUSE_ID,
        TEST_TOKEN,
        "main",
        "initial_schema",
    );
    let mut conn = db.new_connection().unwrap();

    // Get initial schema
    let schema = conn
        .get_option_string(OptionConnection::CurrentSchema)
        .unwrap();
    assert_eq!(schema, "initial_schema");

    // Set new schema
    conn.set_option(
        OptionConnection::CurrentSchema,
        OptionValue::String("new_schema".to_string()),
    )
    .unwrap();

    // Verify new schema
    let schema = conn
        .get_option_string(OptionConnection::CurrentSchema)
        .unwrap();
    assert_eq!(schema, "new_schema");
}

/// Test getting catalog when not set.
#[tokio::test(flavor = "multi_thread")]
async fn test_current_catalog_not_set() {
    let mock_server = setup_mock_server_with_session(TEST_SESSION_ID).await;
    let db = create_test_database(&mock_server.uri(), TEST_WAREHOUSE_ID, TEST_TOKEN);
    let conn = db.new_connection().unwrap();

    let result = conn.get_option_string(OptionConnection::CurrentCatalog);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().status, Status::NotFound);
}

/// Test connection with options passed during creation.
#[tokio::test(flavor = "multi_thread")]
async fn test_connection_with_opts() {
    let mock_server = setup_mock_server_with_session(TEST_SESSION_ID).await;
    let db = create_test_database(&mock_server.uri(), TEST_WAREHOUSE_ID, TEST_TOKEN);

    let conn = db
        .new_connection_with_opts([
            (
                OptionConnection::CurrentCatalog,
                OptionValue::String("opts_catalog".to_string()),
            ),
            (
                OptionConnection::CurrentSchema,
                OptionValue::String("opts_schema".to_string()),
            ),
        ])
        .unwrap();

    assert_eq!(
        conn.get_option_string(OptionConnection::CurrentCatalog)
            .unwrap(),
        "opts_catalog"
    );
    assert_eq!(
        conn.get_option_string(OptionConnection::CurrentSchema)
            .unwrap(),
        "opts_schema"
    );
}

/// Test unknown option returns error.
#[tokio::test(flavor = "multi_thread")]
async fn test_unknown_option() {
    let mock_server = setup_mock_server_with_session(TEST_SESSION_ID).await;
    let db = create_test_database(&mock_server.uri(), TEST_WAREHOUSE_ID, TEST_TOKEN);
    let mut conn = db.new_connection().unwrap();

    // Set unknown option
    let result = conn.set_option(
        OptionConnection::Other("unknown.option".to_string()),
        OptionValue::String("value".to_string()),
    );
    assert!(result.is_err());

    // Get unknown option
    let result = conn.get_option_string(OptionConnection::Other("unknown.option".to_string()));
    assert!(result.is_err());
}

// ==============================================================================
// Multiple Connections Tests
// ==============================================================================

/// Test creating multiple connections from the same database.
#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_connections_same_database() {
    let mock_server = MockServer::start().await;

    // Session create - allow multiple calls for multiple connections
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/sessions"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "multi-session"
            })),
        )
        .mount(&mock_server)
        .await;

    Mock::given(method("DELETE"))
        .and(path("/api/2.0/sql/sessions/multi-session"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&mock_server)
        .await;

    let db = create_test_database(&mock_server.uri(), TEST_WAREHOUSE_ID, TEST_TOKEN);

    // Create multiple connections
    let conn1 = db.new_connection();
    let conn2 = db.new_connection();
    let conn3 = db.new_connection();

    assert!(conn1.is_ok());
    assert!(conn2.is_ok());
    assert!(conn3.is_ok());
}

/// Test that connections have independent catalog/schema settings.
#[tokio::test(flavor = "multi_thread")]
async fn test_connections_independent_options() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/sessions"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "independent-session"
            })),
        )
        .mount(&mock_server)
        .await;

    Mock::given(method("DELETE"))
        .and(path("/api/2.0/sql/sessions/independent-session"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&mock_server)
        .await;

    let db = create_test_database_with_catalog(
        &mock_server.uri(),
        TEST_WAREHOUSE_ID,
        TEST_TOKEN,
        "shared_catalog",
        "shared_schema",
    );

    let mut conn1 = db.new_connection().unwrap();
    let mut conn2 = db.new_connection().unwrap();

    // Modify conn1's catalog
    conn1
        .set_option(
            OptionConnection::CurrentCatalog,
            OptionValue::String("catalog_1".to_string()),
        )
        .unwrap();

    // Modify conn2's catalog
    conn2
        .set_option(
            OptionConnection::CurrentCatalog,
            OptionValue::String("catalog_2".to_string()),
        )
        .unwrap();

    // Verify they are independent
    assert_eq!(
        conn1
            .get_option_string(OptionConnection::CurrentCatalog)
            .unwrap(),
        "catalog_1"
    );
    assert_eq!(
        conn2
            .get_option_string(OptionConnection::CurrentCatalog)
            .unwrap(),
        "catalog_2"
    );
}

// ==============================================================================
// Connection Trait Methods Tests
// ==============================================================================

/// Test connection cancel (no-op for Databricks).
#[tokio::test(flavor = "multi_thread")]
async fn test_connection_cancel() {
    let mock_server = setup_mock_server_with_session(TEST_SESSION_ID).await;
    let db = create_test_database(&mock_server.uri(), TEST_WAREHOUSE_ID, TEST_TOKEN);
    let mut conn = db.new_connection().unwrap();

    // Cancel should succeed (no-op)
    let result = conn.cancel();
    assert!(result.is_ok());
}

/// Test commit returns NotImplemented.
#[tokio::test(flavor = "multi_thread")]
async fn test_connection_commit_not_supported() {
    let mock_server = setup_mock_server_with_session(TEST_SESSION_ID).await;
    let db = create_test_database(&mock_server.uri(), TEST_WAREHOUSE_ID, TEST_TOKEN);
    let mut conn = db.new_connection().unwrap();

    let result = conn.commit();
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().status, Status::NotImplemented);
}

/// Test rollback returns NotImplemented.
#[tokio::test(flavor = "multi_thread")]
async fn test_connection_rollback_not_supported() {
    let mock_server = setup_mock_server_with_session(TEST_SESSION_ID).await;
    let db = create_test_database(&mock_server.uri(), TEST_WAREHOUSE_ID, TEST_TOKEN);
    let mut conn = db.new_connection().unwrap();

    let result = conn.rollback();
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().status, Status::NotImplemented);
}

/// Test new_statement creates a statement.
#[tokio::test(flavor = "multi_thread")]
async fn test_connection_new_statement() {
    let mock_server = setup_mock_server_with_session(TEST_SESSION_ID).await;
    let db = create_test_database(&mock_server.uri(), TEST_WAREHOUSE_ID, TEST_TOKEN);
    let mut conn = db.new_connection().unwrap();

    let stmt = conn.new_statement();
    assert!(stmt.is_ok());
}

// ==============================================================================
// Error Handling Tests
// ==============================================================================

/// Test handling of network error during session creation.
#[tokio::test(flavor = "multi_thread")]
async fn test_connection_network_error() {
    let mock_server = MockServer::start().await;

    // Simulate 500 error
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/sessions"))
        .respond_with(
            ResponseTemplate::new(500).set_body_json(serde_json::json!({
                "error_code": "INTERNAL_ERROR",
                "message": "Internal server error"
            })),
        )
        .mount(&mock_server)
        .await;

    let db = create_test_database(&mock_server.uri(), TEST_WAREHOUSE_ID, TEST_TOKEN);
    let result = db.new_connection();

    assert!(result.is_err());
}

/// Test handling of timeout during session creation.
#[tokio::test(flavor = "multi_thread")]
async fn test_connection_timeout_error() {
    let mock_server = MockServer::start().await;

    // Simulate 503 Service Unavailable
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/sessions"))
        .respond_with(
            ResponseTemplate::new(503).set_body_json(serde_json::json!({
                "error_code": "TEMPORARILY_UNAVAILABLE",
                "message": "Service temporarily unavailable"
            })),
        )
        .mount(&mock_server)
        .await;

    let db = create_test_database(&mock_server.uri(), TEST_WAREHOUSE_ID, TEST_TOKEN);
    let result = db.new_connection();

    assert!(result.is_err());
}
