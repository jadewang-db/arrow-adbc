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

//! End-to-end tests for the Databricks ADBC driver.
//!
//! These tests validate the driver against a real Databricks SQL Warehouse.
//! They are disabled by default (marked with `#[ignore]`) and require
//! configuration via the `DATABRICKS_TEST_CONFIG_FILE` environment variable.
//!
//! # Running E2E Tests
//!
//! ```bash
//! # Set configuration file path
//! export DATABRICKS_TEST_CONFIG_FILE=/path/to/config.json
//!
//! # Run all E2E tests
//! cargo test -p adbc_databricks --test e2e_tests -- --ignored
//!
//! # Run with verbose output
//! cargo test -p adbc_databricks --test e2e_tests -- --ignored --nocapture
//! ```
//!
//! See `tests/e2e/README.md` for detailed setup instructions.

// Include the e2e module
mod e2e;

// Re-export for use in tests
use e2e::config::E2EConfig;
use e2e::helpers::{can_execute_e2e_tests, get_test_config};

/// Macro for conditional test execution.
///
/// Skips the test with a message if the E2E configuration is not available.
macro_rules! skip_if_no_config {
    () => {
        if !can_execute_e2e_tests() {
            eprintln!(
                "Skipping test: {} environment variable not set or file not found.",
                e2e::config::CONFIG_ENV_VAR
            );
            return;
        }
    };
}

/// Basic test to verify E2E configuration can be loaded and parsed.
///
/// This test validates:
/// - Configuration loads successfully from the JSON file
/// - Required fields are present and non-empty
/// - URI can be parsed to extract host and warehouse_id
#[test]
#[ignore]
fn test_e2e_config_loads_successfully() {
    skip_if_no_config!();

    let config = get_test_config();

    // Verify required fields are present
    assert!(
        !config.environment.is_empty(),
        "environment should not be empty"
    );
    assert!(!config.uri.is_empty(), "uri should not be empty");
    assert!(!config.token.is_empty(), "token should not be empty");

    println!("Configuration loaded successfully!");
    println!("  Environment: {}", config.environment);
    println!("  Driver Type: {}", config.driver_type);
    println!("  Trace Enabled: {}", config.is_trace_enabled());
}

/// Test that URI parsing extracts host and warehouse_id correctly.
#[test]
#[ignore]
fn test_e2e_config_uri_parsing() {
    skip_if_no_config!();

    let config = get_test_config();

    // Verify URI parsing works
    let (host, warehouse_id) = config.parse_uri().expect("Failed to parse URI");

    assert!(
        host.starts_with("https://"),
        "Host should start with https://, got: {}",
        host
    );
    assert!(
        !warehouse_id.is_empty(),
        "Warehouse ID should not be empty"
    );

    println!("URI parsed successfully:");
    println!("  Host: {}", host);
    println!("  Warehouse ID: {}", warehouse_id);
}

/// Test that configuration validates correctly.
#[test]
#[ignore]
fn test_e2e_config_validates() {
    skip_if_no_config!();

    let config = get_test_config();

    // Validation should pass
    config.validate().expect("Configuration validation failed");

    println!("Configuration validation passed!");
}

/// Test metadata fields are accessible.
#[test]
#[ignore]
fn test_e2e_config_metadata() {
    skip_if_no_config!();

    let config = get_test_config();

    // Print metadata (may be empty if not configured)
    println!("Test Metadata:");
    println!("  Catalog: {}", config.metadata.catalog);
    println!("  Schema: {}", config.metadata.schema);
    println!("  Table: {}", config.metadata.table);
    println!(
        "  Expected Column Count: {}",
        config.metadata.expected_column_count
    );

    if !config.metadata.catalog.is_empty() {
        println!("  Full Table Name: {}", config.get_full_table_name());
    }
}

/// Combined test that verifies all configuration aspects.
///
/// This is the main E2E configuration test that validates
/// the entire configuration loading and parsing pipeline.
#[test]
#[ignore]
fn test_e2e_config_and_connect() {
    skip_if_no_config!();

    let config = get_test_config();

    // Verify config loaded from JSON file
    assert!(!config.environment.is_empty(), "environment is required");
    assert!(!config.uri.is_empty(), "uri is required");
    assert!(!config.token.is_empty(), "token is required");

    // Verify URI parsing works
    let (host, warehouse_id) = config.parse_uri().expect("Failed to parse URI");
    assert!(host.starts_with("https://"), "Host should use HTTPS");
    assert!(!warehouse_id.is_empty(), "Warehouse ID is required");

    // Verify validation passes
    config.validate().expect("Configuration validation failed");

    println!("=== E2E Configuration Test Results ===");
    println!("Config loaded successfully from DATABRICKS_TEST_CONFIG_FILE");
    println!();
    println!("Environment: {}", config.environment);
    println!("Host: {}", host);
    println!("Warehouse ID: {}", warehouse_id);
    println!("Trace Enabled: {}", config.is_trace_enabled());
    println!();

    if !config.metadata.catalog.is_empty() {
        println!("Test Table: {}", config.get_full_table_name());
    }

    if !config.query.is_empty() {
        println!("Test Query: {}", config.query);
        println!("Expected Results: {}", config.expected_results);
    }

    println!();
    println!("Configuration verification complete!");
}

/// Test configuration from JSON string (unit test, always runs).
#[test]
fn test_config_from_json_string() {
    let json = r#"{
        "environment": "Test",
        "uri": "https://test-workspace.cloud.databricks.com/sql/1.0/warehouses/test123",
        "token": "dapi_test_token_12345",
        "query": "SELECT 1",
        "type": "databricks",
        "trace": "true",
        "expectedResults": 1,
        "metadata": {
            "catalog": "main",
            "schema": "test_schema",
            "table": "test_table",
            "expectedColumnCount": 5
        }
    }"#;

    let config = E2EConfig::from_json(json).expect("Failed to parse config");

    // Verify all fields parsed correctly
    assert_eq!(config.environment, "Test");
    assert_eq!(
        config.uri,
        "https://test-workspace.cloud.databricks.com/sql/1.0/warehouses/test123"
    );
    assert_eq!(config.token, "dapi_test_token_12345");
    assert_eq!(config.query, "SELECT 1");
    assert_eq!(config.driver_type, "databricks");
    assert!(config.is_trace_enabled());
    assert_eq!(config.expected_results, 1);

    // Verify metadata
    assert_eq!(config.metadata.catalog, "main");
    assert_eq!(config.metadata.schema, "test_schema");
    assert_eq!(config.metadata.table, "test_table");
    assert_eq!(config.metadata.expected_column_count, 5);

    // Verify URI parsing
    let (host, warehouse_id) = config.parse_uri().unwrap();
    assert_eq!(host, "https://test-workspace.cloud.databricks.com");
    assert_eq!(warehouse_id, "test123");

    // Verify validation
    config.validate().expect("Validation should pass");
}

/// Test configuration with minimal fields (unit test, always runs).
#[test]
fn test_config_minimal_fields() {
    let json = r#"{
        "environment": "Minimal",
        "uri": "https://example.com/sql/1.0/warehouses/abc",
        "token": "test_token",
        "type": "databricks"
    }"#;

    let config = E2EConfig::from_json(json).expect("Failed to parse config");

    // Verify defaults
    assert_eq!(config.query, "");
    assert_eq!(config.trace, "");
    assert_eq!(config.expected_results, 0);
    assert_eq!(config.metadata.catalog, "");
    assert!(!config.is_trace_enabled());

    // Validation should still pass
    config
        .validate()
        .expect("Validation should pass for minimal config");
}

/// Test configuration error handling (unit test, always runs).
#[test]
fn test_config_invalid_json() {
    let invalid_json = "{ invalid json }";
    let result = E2EConfig::from_json(invalid_json);
    assert!(result.is_err(), "Should fail on invalid JSON");
}

/// Test that can_execute_e2e_tests works correctly.
#[test]
fn test_can_execute_check() {
    // This test just verifies the function doesn't panic
    let can_execute = can_execute_e2e_tests();
    println!("Can execute E2E tests: {}", can_execute);

    // If env var is set and file exists, should return true
    // Otherwise should return false - both are valid
}

// ============================================================================
// Work Item 1.3: SEA Client Core HTTP Infrastructure E2E Tests
// ============================================================================

/// Test that SeaClient can be instantiated with valid configuration.
///
/// This validates:
/// - SeaClientConfig can be created from E2E config
/// - SeaClient::new() succeeds with valid configuration
/// - Client has correct host and warehouse_id
#[test]
#[ignore]
fn test_e2e_sea_client_instantiation() {
    use adbc_databricks::client::{SeaClient, SeaClientConfig};

    skip_if_no_config!();

    let config = get_test_config();
    let (host, warehouse_id) = config.parse_uri().expect("Failed to parse URI");

    // Create SeaClient config
    let sea_config = SeaClientConfig::new(&host, &config.token, &warehouse_id);

    // Create SeaClient
    let client = SeaClient::new(sea_config).expect("Failed to create SeaClient");

    // Verify configuration
    assert_eq!(client.host(), host, "Host should match");
    assert_eq!(client.warehouse_id(), warehouse_id, "Warehouse ID should match");

    println!("SeaClient instantiated successfully!");
    println!("  Host: {}", client.host());
    println!("  Warehouse ID: {}", client.warehouse_id());
}

/// Test that SeaClient can create and delete a session on a real Databricks instance.
///
/// This validates the core HTTP infrastructure:
/// - POST requests with JSON body work correctly
/// - DELETE requests work correctly
/// - Authorization headers are sent correctly
/// - Response parsing works correctly
/// - Error handling for 4xx responses works (tested implicitly)
#[test]
#[ignore]
fn test_e2e_sea_client_session_lifecycle() {
    use adbc_databricks::client::{CreateSessionRequest, SeaClient, SeaClientConfig};

    skip_if_no_config!();

    let config = get_test_config();
    let (host, warehouse_id) = config.parse_uri().expect("Failed to parse URI");

    // Create SeaClient
    let sea_config = SeaClientConfig::new(&host, &config.token, &warehouse_id);
    let client = SeaClient::new(sea_config).expect("Failed to create SeaClient");

    // Create Tokio runtime for async operations
    let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");

    // Test session creation
    let session_id = rt.block_on(async {
        let request = CreateSessionRequest {
            warehouse_id: warehouse_id.clone(),
            session_alias: Some("adbc_rust_e2e_test".to_string()),
            catalog: config.metadata.catalog.clone().into(),
            schema: config.metadata.schema.clone().into(),
        };

        println!("Creating session...");
        let response = client.create_session(&request).await.expect("Failed to create session");

        println!("Session created successfully!");
        println!("  Session ID: {}", response.session_id);

        response.session_id
    });

    // Test session deletion
    rt.block_on(async {
        println!("Deleting session...");
        client
            .delete_session(&session_id)
            .await
            .expect("Failed to delete session");

        println!("Session deleted successfully!");
    });

    println!();
    println!("=== SEA Client Session Lifecycle Test PASSED ===");
    println!("Successfully created and deleted session: {}", session_id);
}

/// Test that SeaClient handles authentication errors correctly.
///
/// This validates:
/// - Invalid token returns 401 error
/// - Error is correctly mapped to SeaApi error with proper http_status
#[test]
#[ignore]
fn test_e2e_sea_client_auth_error() {
    use adbc_databricks::client::{CreateSessionRequest, SeaClient, SeaClientConfig};
    use adbc_databricks::Error;

    skip_if_no_config!();

    let config = get_test_config();
    let (host, warehouse_id) = config.parse_uri().expect("Failed to parse URI");

    // Create SeaClient with invalid token
    let sea_config = SeaClientConfig::new(&host, "invalid_token_12345", &warehouse_id);
    let client = SeaClient::new(sea_config).expect("Failed to create SeaClient");

    // Create Tokio runtime for async operations
    let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");

    // Attempt to create session - should fail with 401 or 403
    let result = rt.block_on(async {
        let request = CreateSessionRequest {
            warehouse_id: warehouse_id.clone(),
            session_alias: None,
            catalog: None,
            schema: None,
        };

        client.create_session(&request).await
    });

    // Verify we got an authentication error
    match result {
        Ok(_) => panic!("Expected authentication error, but request succeeded"),
        Err(Error::SeaApi { http_status, code, message, .. }) => {
            println!("Got expected authentication error:");
            println!("  HTTP Status: {}", http_status);
            println!("  Error Code: {}", code);
            println!("  Message: {}", message);

            // Should be 401 (Unauthenticated) or 403 (Permission Denied)
            assert!(
                http_status == 401 || http_status == 403,
                "Expected 401 or 403, got {}",
                http_status
            );
        }
        Err(e) => panic!("Expected SeaApi error, got: {:?}", e),
    }

    println!();
    println!("=== SEA Client Auth Error Test PASSED ===");
}

// ============================================================================
// Work Item 1.4: Session Management E2E Tests
// ============================================================================

/// Test SessionManager creates and caches session on a real Databricks instance.
///
/// This validates:
/// - SessionManager can create sessions via SEA API
/// - Session IDs are cached (second call returns same ID without API call)
/// - Catalog and schema are respected during session creation
#[test]
#[ignore]
fn test_e2e_session_manager_create_and_cache() {
    use adbc_databricks::client::{SeaClient, SeaClientConfig};
    use adbc_databricks::SessionManager;
    use std::sync::Arc;

    skip_if_no_config!();

    let config = get_test_config();
    let (host, warehouse_id) = config.parse_uri().expect("Failed to parse URI");

    // Create SeaClient wrapped in Arc
    let sea_config = SeaClientConfig::new(&host, &config.token, &warehouse_id);
    let client = Arc::new(SeaClient::new(sea_config).expect("Failed to create SeaClient"));

    // Create Tokio runtime for async operations
    let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");

    // Create SessionManager with optional catalog/schema from config
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

    let session_manager = SessionManager::new(client.clone(), catalog.clone(), schema.clone());

    // Initially no session should be active
    let is_active_before = rt.block_on(session_manager.is_active());
    assert!(
        !is_active_before,
        "Session should not be active before first get_session_id call"
    );

    // Get session ID (should create a new session)
    let session_id_1 = rt
        .block_on(session_manager.get_session_id())
        .expect("Failed to create session");

    println!("Created session: {}", session_id_1);
    assert!(!session_id_1.is_empty(), "Session ID should not be empty");

    // Session should now be active
    let is_active_after = rt.block_on(session_manager.is_active());
    assert!(is_active_after, "Session should be active after creation");

    // Get session ID again (should return cached value)
    let session_id_2 = rt
        .block_on(session_manager.get_session_id())
        .expect("Failed to get cached session");

    println!("Cached session returned: {}", session_id_2);
    assert_eq!(
        session_id_1, session_id_2,
        "Second call should return the same session ID"
    );

    // Verify catalog and schema are stored correctly
    assert_eq!(
        session_manager.catalog(),
        catalog.as_deref(),
        "Catalog should match"
    );
    assert_eq!(
        session_manager.schema(),
        schema.as_deref(),
        "Schema should match"
    );

    // Clean up: terminate the session
    rt.block_on(session_manager.terminate())
        .expect("Failed to terminate session");

    println!("Session terminated");

    println!();
    println!("=== SessionManager Create and Cache Test PASSED ===");
}

/// Test SessionManager terminate functionality on a real Databricks instance.
///
/// This validates:
/// - SessionManager can terminate active sessions
/// - is_active() returns false after termination
/// - New session can be created after termination
#[test]
#[ignore]
fn test_e2e_session_manager_terminate() {
    use adbc_databricks::client::{SeaClient, SeaClientConfig};
    use adbc_databricks::SessionManager;
    use std::sync::Arc;

    skip_if_no_config!();

    let config = get_test_config();
    let (host, warehouse_id) = config.parse_uri().expect("Failed to parse URI");

    // Create SeaClient wrapped in Arc
    let sea_config = SeaClientConfig::new(&host, &config.token, &warehouse_id);
    let client = Arc::new(SeaClient::new(sea_config).expect("Failed to create SeaClient"));

    // Create Tokio runtime for async operations
    let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");

    let session_manager = SessionManager::new(client.clone(), None, None);

    // Create a session
    let session_id_1 = rt
        .block_on(session_manager.get_session_id())
        .expect("Failed to create session");

    println!("Created first session: {}", session_id_1);
    assert!(
        rt.block_on(session_manager.is_active()),
        "Session should be active"
    );

    // Terminate the session
    rt.block_on(session_manager.terminate())
        .expect("Failed to terminate session");

    println!("Terminated session");

    // Session should no longer be active
    assert!(
        !rt.block_on(session_manager.is_active()),
        "Session should not be active after termination"
    );

    // Create a new session (should get a different session ID)
    let session_id_2 = rt
        .block_on(session_manager.get_session_id())
        .expect("Failed to create second session");

    println!("Created second session: {}", session_id_2);
    assert!(
        !session_id_2.is_empty(),
        "New session ID should not be empty"
    );

    // Clean up
    rt.block_on(session_manager.terminate())
        .expect("Failed to terminate second session");

    println!();
    println!("=== SessionManager Terminate Test PASSED ===");
}

/// Complete E2E test for session lifecycle using SessionManager.
///
/// This validates the complete session lifecycle:
/// 1. Session creation via get_session_id()
/// 2. Session ID caching (repeated calls return same ID)
/// 3. Session termination via terminate()
/// 4. Session recreation after termination
/// 5. is_active() correctly reflects session state
#[test]
#[ignore]
fn test_e2e_session_manager_full_lifecycle() {
    use adbc_databricks::client::{SeaClient, SeaClientConfig};
    use adbc_databricks::SessionManager;
    use std::sync::Arc;

    skip_if_no_config!();

    let config = get_test_config();
    let (host, warehouse_id) = config.parse_uri().expect("Failed to parse URI");

    println!("=== SessionManager Full Lifecycle E2E Test ===");
    println!("Host: {}", host);
    println!("Warehouse ID: {}", warehouse_id);
    println!();

    // Create SeaClient wrapped in Arc
    let sea_config = SeaClientConfig::new(&host, &config.token, &warehouse_id);
    let client = Arc::new(SeaClient::new(sea_config).expect("Failed to create SeaClient"));

    // Create Tokio runtime for async operations
    let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");

    // Create SessionManager with catalog/schema from config if available
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

    let session_manager = SessionManager::new(client.clone(), catalog, schema);

    // Step 1: Initial state - no session
    println!("Step 1: Verify initial state");
    assert!(
        !rt.block_on(session_manager.is_active()),
        "No session should exist initially"
    );
    println!("  - No active session (expected)");

    // Step 2: Create session (lazy initialization)
    println!("Step 2: Create session");
    let session_id_1 = rt
        .block_on(session_manager.get_session_id())
        .expect("Failed to create session");
    assert!(!session_id_1.is_empty(), "Session ID should not be empty");
    assert!(
        rt.block_on(session_manager.is_active()),
        "Session should be active"
    );
    println!("  - Created session: {}", session_id_1);

    // Step 3: Verify caching
    println!("Step 3: Verify caching");
    let session_id_2 = rt
        .block_on(session_manager.get_session_id())
        .expect("Failed to get cached session");
    assert_eq!(
        session_id_1, session_id_2,
        "Should return cached session ID"
    );
    println!("  - Cached session ID returned: {}", session_id_2);

    // Step 4: Terminate session
    println!("Step 4: Terminate session");
    rt.block_on(session_manager.terminate())
        .expect("Failed to terminate session");
    assert!(
        !rt.block_on(session_manager.is_active()),
        "Session should not be active after termination"
    );
    println!("  - Session terminated successfully");

    // Step 5: Recreate session
    println!("Step 5: Recreate session");
    let session_id_3 = rt
        .block_on(session_manager.get_session_id())
        .expect("Failed to recreate session");
    assert!(!session_id_3.is_empty(), "New session ID should not be empty");
    assert!(
        rt.block_on(session_manager.is_active()),
        "Session should be active after recreation"
    );
    println!("  - Recreated session: {}", session_id_3);

    // Clean up
    rt.block_on(session_manager.terminate())
        .expect("Failed to clean up session");

    println!();
    println!("=== SessionManager Full Lifecycle E2E Test PASSED ===");
    println!("All session management operations completed successfully!");
}

// ============================================================================
// Work Item 1.6: DatabricksDriver E2E Tests
// ============================================================================

/// Test that DatabricksDriver can create a database instance.
///
/// This validates:
/// - DatabricksDriver::new() creates a valid driver
/// - Driver::new_database() returns a DatabricksDatabase
#[test]
fn test_driver_creates_database() {
    use adbc_core::Driver;
    use adbc_databricks::DatabricksDriver;

    let mut driver = DatabricksDriver::new();
    let db = driver.new_database();
    assert!(db.is_ok(), "new_database should succeed");
}

/// Test that DatabricksDriver can create a database with options.
///
/// This validates:
/// - Driver::new_database_with_opts() accepts options
/// - Options are correctly set on the database
#[test]
fn test_driver_creates_database_with_opts() {
    use adbc_core::options::{OptionDatabase, OptionValue};
    use adbc_core::{Driver, Optionable};
    use adbc_databricks::DatabricksDriver;

    let mut driver = DatabricksDriver::new();
    let db = driver.new_database_with_opts([
        (
            OptionDatabase::Uri,
            OptionValue::String("https://example.cloud.databricks.com".into()),
        ),
        (
            OptionDatabase::Password,
            OptionValue::String("dapi_token".into()),
        ),
    ]);
    assert!(db.is_ok(), "new_database_with_opts should succeed");

    let db = db.unwrap();
    let uri = db.get_option_string(OptionDatabase::Uri);
    assert!(uri.is_ok(), "Should be able to get uri option");
    assert_eq!(
        uri.unwrap(),
        "https://example.cloud.databricks.com",
        "URI should match"
    );
}

/// Test DatabricksDriver using E2E config to create a fully configured database.
///
/// This validates:
/// - Driver can create database with real Databricks configuration
/// - All required options (uri, warehouse_id, token) can be set
/// - Optional options (catalog, schema) can be set
#[test]
#[ignore]
fn test_e2e_driver_creates_database_with_real_config() {
    use adbc_core::options::{OptionDatabase, OptionValue};
    use adbc_core::{Driver, Optionable};
    use adbc_databricks::DatabricksDriver;

    skip_if_no_config!();

    let config = get_test_config();
    let (host, warehouse_id) = config.parse_uri().expect("Failed to parse URI");

    println!("=== DatabricksDriver Database Creation E2E Test ===");
    println!("Host: {}", host);
    println!("Warehouse ID: {}", warehouse_id);
    println!();

    // Create driver
    let mut driver = DatabricksDriver::new();

    // Build options from config
    let mut opts: Vec<(OptionDatabase, OptionValue)> = vec![
        (OptionDatabase::Uri, OptionValue::String(host.clone())),
        (OptionDatabase::Password, OptionValue::String(config.token.clone())),
        (
            OptionDatabase::Other("databricks.warehouse_id".into()),
            OptionValue::String(warehouse_id.clone()),
        ),
    ];

    // Add optional catalog and schema if available
    if !config.metadata.catalog.is_empty() {
        opts.push((
            OptionDatabase::Other("databricks.catalog".into()),
            OptionValue::String(config.metadata.catalog.clone()),
        ));
    }
    if !config.metadata.schema.is_empty() {
        opts.push((
            OptionDatabase::Other("databricks.schema".into()),
            OptionValue::String(config.metadata.schema.clone()),
        ));
    }

    // Create database with options
    let db = driver.new_database_with_opts(opts);
    assert!(db.is_ok(), "new_database_with_opts should succeed with real config");

    let db = db.unwrap();

    // Verify uri was set correctly
    let uri = db.get_option_string(OptionDatabase::Uri).unwrap();
    assert_eq!(uri, host, "URI should match configured host");

    // Verify warehouse_id was set correctly
    let wh_id = db
        .get_option_string(OptionDatabase::Other("databricks.warehouse_id".into()))
        .unwrap();
    assert_eq!(wh_id, warehouse_id, "Warehouse ID should match");

    // Verify catalog if set
    if !config.metadata.catalog.is_empty() {
        let catalog = db
            .get_option_string(OptionDatabase::Other("databricks.catalog".into()))
            .unwrap();
        assert_eq!(
            catalog, config.metadata.catalog,
            "Catalog should match config"
        );
        println!("  Catalog: {}", catalog);
    }

    // Verify schema if set
    if !config.metadata.schema.is_empty() {
        let schema = db
            .get_option_string(OptionDatabase::Other("databricks.schema".into()))
            .unwrap();
        assert_eq!(schema, config.metadata.schema, "Schema should match config");
        println!("  Schema: {}", schema);
    }

    println!();
    println!("=== DatabricksDriver Database Creation E2E Test PASSED ===");
}

/// Test DatabricksDriver end-to-end: create driver -> database -> connection.
///
/// This validates the complete ADBC workflow:
/// 1. Create DatabricksDriver
/// 2. Create DatabricksDatabase with options
/// 3. Create DatabricksConnection from database
/// 4. Verify connection has a valid session
#[test]
#[ignore]
fn test_e2e_driver_database_connection_workflow() {
    use adbc_core::options::{OptionDatabase, OptionValue};
    use adbc_core::{Database, Driver};
    use adbc_databricks::DatabricksDriver;

    skip_if_no_config!();

    let config = get_test_config();
    let (host, warehouse_id) = config.parse_uri().expect("Failed to parse URI");

    println!("=== DatabricksDriver -> Database -> Connection E2E Test ===");
    println!("Host: {}", host);
    println!("Warehouse ID: {}", warehouse_id);
    println!();

    // Step 1: Create driver
    println!("Step 1: Create DatabricksDriver");
    let mut driver = DatabricksDriver::new();
    println!("  - Driver created successfully");

    // Step 2: Create database with options
    println!("Step 2: Create DatabricksDatabase with options");
    let mut opts: Vec<(OptionDatabase, OptionValue)> = vec![
        (OptionDatabase::Uri, OptionValue::String(host.clone())),
        (OptionDatabase::Password, OptionValue::String(config.token.clone())),
        (
            OptionDatabase::Other("databricks.warehouse_id".into()),
            OptionValue::String(warehouse_id.clone()),
        ),
    ];

    // Add optional catalog and schema if available
    if !config.metadata.catalog.is_empty() {
        opts.push((
            OptionDatabase::Other("databricks.catalog".into()),
            OptionValue::String(config.metadata.catalog.clone()),
        ));
    }
    if !config.metadata.schema.is_empty() {
        opts.push((
            OptionDatabase::Other("databricks.schema".into()),
            OptionValue::String(config.metadata.schema.clone()),
        ));
    }

    let db = driver
        .new_database_with_opts(opts)
        .expect("Failed to create database");
    println!("  - Database created successfully");

    // Step 3: Create connection from database
    println!("Step 3: Create DatabricksConnection from database");
    let connection = db.new_connection();
    assert!(
        connection.is_ok(),
        "new_connection should succeed: {:?}",
        connection.err()
    );

    let connection = connection.unwrap();
    println!("  - Connection created successfully");

    // Step 4: Verify connection has session
    println!("Step 4: Verify connection state");
    let session_id = connection.session_id();
    assert!(session_id.is_some(), "Connection should have a session ID");
    println!("  - Session ID: {}", session_id.unwrap());

    println!();
    println!("=== DatabricksDriver -> Database -> Connection E2E Test PASSED ===");
    println!("Complete ADBC workflow validated successfully!");
}

/// Test DatabricksDriver creates connection with options.
///
/// This validates:
/// - Database::new_connection_with_opts() works correctly
/// - Connection options (current_catalog, current_schema) can be set
#[test]
#[ignore]
fn test_e2e_driver_connection_with_opts() {
    use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
    use adbc_core::{Database, Driver, Optionable};
    use adbc_databricks::DatabricksDriver;

    skip_if_no_config!();

    let config = get_test_config();
    let (host, warehouse_id) = config.parse_uri().expect("Failed to parse URI");

    println!("=== DatabricksDriver Connection with Options E2E Test ===");
    println!("Host: {}", host);
    println!("Warehouse ID: {}", warehouse_id);
    println!();

    // Create driver and database
    let mut driver = DatabricksDriver::new();
    let db = driver
        .new_database_with_opts([
            (OptionDatabase::Uri, OptionValue::String(host.clone())),
            (OptionDatabase::Password, OptionValue::String(config.token.clone())),
            (
                OptionDatabase::Other("databricks.warehouse_id".into()),
                OptionValue::String(warehouse_id.clone()),
            ),
        ])
        .expect("Failed to create database");

    // Create connection with options
    let connection = db.new_connection_with_opts([
        (
            OptionConnection::CurrentCatalog,
            OptionValue::String("main".into()),
        ),
        (
            OptionConnection::CurrentSchema,
            OptionValue::String("default".into()),
        ),
    ]);
    assert!(
        connection.is_ok(),
        "new_connection_with_opts should succeed: {:?}",
        connection.err()
    );

    let connection = connection.unwrap();

    // Verify connection options
    let catalog = connection
        .get_option_string(OptionConnection::CurrentCatalog)
        .unwrap();
    assert_eq!(catalog, "main", "Catalog should be 'main'");

    let schema = connection
        .get_option_string(OptionConnection::CurrentSchema)
        .unwrap();
    assert_eq!(schema, "default", "Schema should be 'default'");

    // Verify autocommit (always true for Databricks)
    let autocommit = connection
        .get_option_string(OptionConnection::AutoCommit)
        .unwrap();
    assert_eq!(autocommit, "true", "Autocommit should be 'true'");

    println!("Connection options verified:");
    println!("  Current Catalog: {}", catalog);
    println!("  Current Schema: {}", schema);
    println!("  AutoCommit: {}", autocommit);

    println!();
    println!("=== DatabricksDriver Connection with Options E2E Test PASSED ===");
}
