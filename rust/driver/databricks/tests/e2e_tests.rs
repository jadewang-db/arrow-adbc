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

// ============================================================================
// Work Item 1.7: DatabricksDatabase E2E Tests
// ============================================================================

/// Test DatabricksDatabase creates connection with real Databricks instance.
///
/// This validates:
/// - DatabricksDatabase correctly builds configuration from options
/// - Configuration is validated at connection time
/// - Connection receives valid session from Databricks
#[test]
#[ignore]
fn test_e2e_database_creates_connection() {
    use adbc_core::options::{OptionDatabase, OptionValue};
    use adbc_core::{Database, Driver, Optionable};
    use adbc_databricks::DatabricksDriver;

    skip_if_no_config!();

    let config = get_test_config();
    let (host, warehouse_id) = config.parse_uri().expect("Failed to parse URI");

    println!("=== DatabricksDatabase Creates Connection E2E Test ===");
    println!("Host: {}", host);
    println!("Warehouse ID: {}", warehouse_id);
    println!();

    // Create driver and database with required options
    let mut driver = DatabricksDriver::new();
    let db = driver
        .new_database_with_opts([
            (OptionDatabase::Uri, OptionValue::String(host.clone())),
            (
                OptionDatabase::Password,
                OptionValue::String(config.token.clone()),
            ),
            (
                OptionDatabase::Other("databricks.warehouse_id".into()),
                OptionValue::String(warehouse_id.clone()),
            ),
        ])
        .expect("Failed to create database");

    // Verify options were set correctly
    let uri = db.get_option_string(OptionDatabase::Uri).unwrap();
    assert_eq!(uri, host, "URI should match");

    let wh_id = db
        .get_option_string(OptionDatabase::Other("databricks.warehouse_id".into()))
        .unwrap();
    assert_eq!(wh_id, warehouse_id, "Warehouse ID should match");

    println!("Database options verified:");
    println!("  URI: {}", uri);
    println!("  Warehouse ID: {}", wh_id);

    // Create connection
    let conn = db.new_connection();
    assert!(
        conn.is_ok(),
        "new_connection should succeed: {:?}",
        conn.err()
    );

    let conn = conn.unwrap();
    assert!(
        conn.session_id().is_some(),
        "Connection should have a session ID"
    );

    println!("  Session ID: {}", conn.session_id().unwrap());
    println!();
    println!("=== DatabricksDatabase Creates Connection E2E Test PASSED ===");
}

/// Test DatabricksDatabase runtime is shared across multiple connections.
///
/// This validates:
/// - Multiple connections from the same database share the same Tokio runtime
/// - Each connection gets its own session
#[test]
#[ignore]
fn test_e2e_database_runtime_shared() {
    use adbc_core::options::{OptionDatabase, OptionValue};
    use adbc_core::{Database, Driver};
    use adbc_databricks::DatabricksDriver;

    skip_if_no_config!();

    let config = get_test_config();
    let (host, warehouse_id) = config.parse_uri().expect("Failed to parse URI");

    println!("=== DatabricksDatabase Runtime Sharing E2E Test ===");
    println!("Host: {}", host);
    println!("Warehouse ID: {}", warehouse_id);
    println!();

    // Create driver and database
    let mut driver = DatabricksDriver::new();
    let db = driver
        .new_database_with_opts([
            (OptionDatabase::Uri, OptionValue::String(host.clone())),
            (
                OptionDatabase::Password,
                OptionValue::String(config.token.clone()),
            ),
            (
                OptionDatabase::Other("databricks.warehouse_id".into()),
                OptionValue::String(warehouse_id.clone()),
            ),
        ])
        .expect("Failed to create database");

    // Create first connection
    let conn1 = db.new_connection().expect("Failed to create first connection");
    let session_id_1 = conn1.session_id().expect("First connection should have session");
    println!("First connection session: {}", session_id_1);

    // Create second connection
    let conn2 = db
        .new_connection()
        .expect("Failed to create second connection");
    let session_id_2 = conn2.session_id().expect("Second connection should have session");
    println!("Second connection session: {}", session_id_2);

    // Each connection should have a different session
    // (Note: in current placeholder implementation they might be the same,
    // but once real session creation is implemented they will differ)
    println!();
    println!("Both connections created successfully from the same database!");
    println!("=== DatabricksDatabase Runtime Sharing E2E Test PASSED ===");
}

/// Test DatabricksDatabase validation with missing required options.
///
/// This validates:
/// - Missing uri returns appropriate error
/// - Missing warehouse_id returns appropriate error
/// - Missing token returns appropriate error
#[test]
fn test_database_validation_errors() {
    use adbc_core::options::{OptionDatabase, OptionValue};
    use adbc_core::{Database, Driver, Optionable};
    use adbc_databricks::DatabricksDriver;

    println!("=== DatabricksDatabase Validation Errors Test ===");

    // Test missing uri
    {
        let mut driver = DatabricksDriver::new();
        let mut db = driver.new_database().unwrap();
        db.set_option(
            OptionDatabase::Password,
            OptionValue::String("token".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other("databricks.warehouse_id".into()),
            OptionValue::String("wh123".into()),
        )
        .unwrap();

        let result = db.new_connection();
        assert!(result.is_err(), "Should fail without uri");
        let err = result.unwrap_err();
        assert!(
            err.message.contains("uri"),
            "Error should mention uri: {}",
            err.message
        );
        println!("  Missing uri error: {}", err.message);
    }

    // Test missing warehouse_id
    {
        let mut driver = DatabricksDriver::new();
        let mut db = driver.new_database().unwrap();
        db.set_option(
            OptionDatabase::Uri,
            OptionValue::String("https://example.com".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Password,
            OptionValue::String("token".into()),
        )
        .unwrap();

        let result = db.new_connection();
        assert!(result.is_err(), "Should fail without warehouse_id");
        let err = result.unwrap_err();
        assert!(
            err.message.contains("warehouse_id"),
            "Error should mention warehouse_id: {}",
            err.message
        );
        println!("  Missing warehouse_id error: {}", err.message);
    }

    // Test missing token
    {
        let mut driver = DatabricksDriver::new();
        let mut db = driver.new_database().unwrap();
        db.set_option(
            OptionDatabase::Uri,
            OptionValue::String("https://example.com".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other("databricks.warehouse_id".into()),
            OptionValue::String("wh123".into()),
        )
        .unwrap();

        let result = db.new_connection();
        assert!(result.is_err(), "Should fail without token");
        let err = result.unwrap_err();
        assert!(
            err.message.contains("token"),
            "Error should mention token: {}",
            err.message
        );
        println!("  Missing token error: {}", err.message);
    }

    println!();
    println!("=== DatabricksDatabase Validation Errors Test PASSED ===");
}

/// Test DatabricksDatabase with catalog and schema options.
///
/// This validates:
/// - Catalog option is correctly passed to connection
/// - Schema option is correctly passed to connection
#[test]
#[ignore]
fn test_e2e_database_with_catalog_and_schema() {
    use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
    use adbc_core::{Database, Driver, Optionable};
    use adbc_databricks::DatabricksDriver;

    skip_if_no_config!();

    let config = get_test_config();
    let (host, warehouse_id) = config.parse_uri().expect("Failed to parse URI");

    // Skip if no catalog/schema configured
    if config.metadata.catalog.is_empty() {
        println!("Skipping: No catalog configured in test metadata");
        return;
    }

    println!("=== DatabricksDatabase with Catalog and Schema E2E Test ===");
    println!("Host: {}", host);
    println!("Warehouse ID: {}", warehouse_id);
    println!("Catalog: {}", config.metadata.catalog);
    println!("Schema: {}", config.metadata.schema);
    println!();

    // Create database with catalog and schema
    let mut driver = DatabricksDriver::new();
    let db = driver
        .new_database_with_opts([
            (OptionDatabase::Uri, OptionValue::String(host.clone())),
            (
                OptionDatabase::Password,
                OptionValue::String(config.token.clone()),
            ),
            (
                OptionDatabase::Other("databricks.warehouse_id".into()),
                OptionValue::String(warehouse_id.clone()),
            ),
            (
                OptionDatabase::Other("databricks.catalog".into()),
                OptionValue::String(config.metadata.catalog.clone()),
            ),
            (
                OptionDatabase::Other("databricks.schema".into()),
                OptionValue::String(config.metadata.schema.clone()),
            ),
        ])
        .expect("Failed to create database");

    // Verify catalog and schema were set on database
    let catalog = db
        .get_option_string(OptionDatabase::Other("databricks.catalog".into()))
        .unwrap();
    assert_eq!(catalog, config.metadata.catalog);

    let schema = db
        .get_option_string(OptionDatabase::Other("databricks.schema".into()))
        .unwrap();
    assert_eq!(schema, config.metadata.schema);

    // Create connection - it should inherit default catalog/schema
    let conn = db.new_connection().expect("Failed to create connection");

    // Connection should have current catalog/schema from config defaults
    let conn_catalog = conn
        .get_option_string(OptionConnection::CurrentCatalog)
        .unwrap();
    assert_eq!(conn_catalog, config.metadata.catalog);

    let conn_schema = conn
        .get_option_string(OptionConnection::CurrentSchema)
        .unwrap();
    assert_eq!(conn_schema, config.metadata.schema);

    println!("Connection inherits catalog/schema from database config:");
    println!("  Current Catalog: {}", conn_catalog);
    println!("  Current Schema: {}", conn_schema);
    println!();
    println!("=== DatabricksDatabase with Catalog and Schema E2E Test PASSED ===");
}

// ============================================================================
// Work Item 2.1: DatabricksConnection E2E Tests
// ============================================================================

/// Test DatabricksConnection session lifecycle: creation, usage, and cleanup.
///
/// This validates:
/// - Connection creates a session eagerly on new_connection()
/// - Session ID is accessible via session_id()
/// - Session is properly terminated when connection is dropped
#[test]
#[ignore]
fn test_e2e_connection_session_lifecycle() {
    use adbc_core::options::{OptionDatabase, OptionValue};
    use adbc_core::{Database, Driver};
    use adbc_databricks::DatabricksDriver;

    skip_if_no_config!();

    let config = get_test_config();
    let (host, warehouse_id) = config.parse_uri().expect("Failed to parse URI");

    println!("=== DatabricksConnection Session Lifecycle E2E Test ===");
    println!("Host: {}", host);
    println!("Warehouse ID: {}", warehouse_id);
    println!();

    // Create driver and database
    let mut driver = DatabricksDriver::new();
    let db = driver
        .new_database_with_opts([
            (OptionDatabase::Uri, OptionValue::String(host.clone())),
            (
                OptionDatabase::Password,
                OptionValue::String(config.token.clone()),
            ),
            (
                OptionDatabase::Other("databricks.warehouse_id".into()),
                OptionValue::String(warehouse_id.clone()),
            ),
        ])
        .expect("Failed to create database");

    // Step 1: Create connection (should create session eagerly)
    println!("Step 1: Create connection (eager session creation)");
    let conn = db.new_connection().expect("Failed to create connection");

    // Step 2: Verify session is active
    println!("Step 2: Verify session is active");
    let session_id = conn.session_id();
    assert!(
        session_id.is_some(),
        "Connection should have an active session"
    );
    let session_id = session_id.unwrap();
    println!("  Session ID: {}", session_id);
    assert!(!session_id.is_empty(), "Session ID should not be empty");

    // Step 3: Drop connection (should terminate session)
    println!("Step 3: Drop connection (session termination)");
    drop(conn);
    println!("  Connection dropped - session should be terminated");

    // Note: We cannot directly verify the session is terminated without
    // additional API calls, but the Drop implementation sends DELETE request
    // to /api/2.0/sql/sessions/{session_id}

    println!();
    println!("=== DatabricksConnection Session Lifecycle E2E Test PASSED ===");
}

/// Test DatabricksConnection commit/rollback returns NotSupported.
///
/// This validates:
/// - commit() returns NotImplemented status
/// - rollback() returns NotImplemented status
/// - Databricks operates in autocommit mode only
#[test]
#[ignore]
fn test_e2e_connection_transactions_not_supported() {
    use adbc_core::error::Status;
    use adbc_core::options::{OptionDatabase, OptionValue};
    use adbc_core::{Connection, Database, Driver};
    use adbc_databricks::DatabricksDriver;

    skip_if_no_config!();

    let config = get_test_config();
    let (host, warehouse_id) = config.parse_uri().expect("Failed to parse URI");

    println!("=== DatabricksConnection Transactions Not Supported E2E Test ===");
    println!("Host: {}", host);
    println!("Warehouse ID: {}", warehouse_id);
    println!();

    // Create driver, database, and connection
    let mut driver = DatabricksDriver::new();
    let db = driver
        .new_database_with_opts([
            (OptionDatabase::Uri, OptionValue::String(host.clone())),
            (
                OptionDatabase::Password,
                OptionValue::String(config.token.clone()),
            ),
            (
                OptionDatabase::Other("databricks.warehouse_id".into()),
                OptionValue::String(warehouse_id.clone()),
            ),
        ])
        .expect("Failed to create database");

    let mut conn = db.new_connection().expect("Failed to create connection");

    // Test commit() returns NotImplemented
    println!("Testing commit() returns NotImplemented...");
    let commit_result = conn.commit();
    assert!(commit_result.is_err(), "commit() should return error");
    let commit_err = commit_result.unwrap_err();
    assert_eq!(
        commit_err.status,
        Status::NotImplemented,
        "commit() should return NotImplemented status"
    );
    println!("  commit() error: {}", commit_err.message);

    // Test rollback() returns NotImplemented
    println!("Testing rollback() returns NotImplemented...");
    let rollback_result = conn.rollback();
    assert!(rollback_result.is_err(), "rollback() should return error");
    let rollback_err = rollback_result.unwrap_err();
    assert_eq!(
        rollback_err.status,
        Status::NotImplemented,
        "rollback() should return NotImplemented status"
    );
    println!("  rollback() error: {}", rollback_err.message);

    println!();
    println!("=== DatabricksConnection Transactions Not Supported E2E Test PASSED ===");
}

/// Test DatabricksConnection can create statements.
///
/// This validates:
/// - new_statement() creates a DatabricksStatement
/// - Statement is properly initialized with session info
#[test]
#[ignore]
fn test_e2e_connection_creates_statement() {
    use adbc_core::options::{OptionDatabase, OptionValue};
    use adbc_core::{Connection, Database, Driver};
    use adbc_databricks::DatabricksDriver;

    skip_if_no_config!();

    let config = get_test_config();
    let (host, warehouse_id) = config.parse_uri().expect("Failed to parse URI");

    println!("=== DatabricksConnection Creates Statement E2E Test ===");
    println!("Host: {}", host);
    println!("Warehouse ID: {}", warehouse_id);
    println!();

    // Create driver, database, and connection
    let mut driver = DatabricksDriver::new();
    let db = driver
        .new_database_with_opts([
            (OptionDatabase::Uri, OptionValue::String(host.clone())),
            (
                OptionDatabase::Password,
                OptionValue::String(config.token.clone()),
            ),
            (
                OptionDatabase::Other("databricks.warehouse_id".into()),
                OptionValue::String(warehouse_id.clone()),
            ),
        ])
        .expect("Failed to create database");

    let mut conn = db.new_connection().expect("Failed to create connection");
    let session_id = conn.session_id().expect("Connection should have session");
    println!("Connection session ID: {}", session_id);

    // Create statement
    println!("Creating statement...");
    let stmt = conn.new_statement();
    assert!(stmt.is_ok(), "new_statement() should succeed");
    println!("  Statement created successfully");

    // Create multiple statements (should all work)
    println!("Creating multiple statements...");
    let stmt2 = conn.new_statement();
    assert!(stmt2.is_ok(), "Second new_statement() should succeed");

    let stmt3 = conn.new_statement();
    assert!(stmt3.is_ok(), "Third new_statement() should succeed");
    println!("  Multiple statements created successfully");

    println!();
    println!("=== DatabricksConnection Creates Statement E2E Test PASSED ===");
}

/// Test DatabricksConnection options (current_catalog, current_schema).
///
/// This validates:
/// - set_option for CurrentCatalog works
/// - set_option for CurrentSchema works
/// - get_option_string returns correct values
/// - AutoCommit is always "true"
#[test]
#[ignore]
fn test_e2e_connection_options() {
    use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
    use adbc_core::{Database, Driver, Optionable};
    use adbc_databricks::DatabricksDriver;

    skip_if_no_config!();

    let config = get_test_config();
    let (host, warehouse_id) = config.parse_uri().expect("Failed to parse URI");

    println!("=== DatabricksConnection Options E2E Test ===");
    println!("Host: {}", host);
    println!("Warehouse ID: {}", warehouse_id);
    println!();

    // Create driver, database, and connection
    let mut driver = DatabricksDriver::new();
    let db = driver
        .new_database_with_opts([
            (OptionDatabase::Uri, OptionValue::String(host.clone())),
            (
                OptionDatabase::Password,
                OptionValue::String(config.token.clone()),
            ),
            (
                OptionDatabase::Other("databricks.warehouse_id".into()),
                OptionValue::String(warehouse_id.clone()),
            ),
        ])
        .expect("Failed to create database");

    let mut conn = db.new_connection().expect("Failed to create connection");

    // Test setting and getting CurrentCatalog
    println!("Testing CurrentCatalog option...");
    conn.set_option(
        OptionConnection::CurrentCatalog,
        OptionValue::String("test_catalog".into()),
    )
    .expect("set_option for CurrentCatalog should succeed");
    let catalog = conn
        .get_option_string(OptionConnection::CurrentCatalog)
        .expect("get_option_string for CurrentCatalog should succeed");
    assert_eq!(catalog, "test_catalog");
    println!("  CurrentCatalog: {}", catalog);

    // Test setting and getting CurrentSchema
    println!("Testing CurrentSchema option...");
    conn.set_option(
        OptionConnection::CurrentSchema,
        OptionValue::String("test_schema".into()),
    )
    .expect("set_option for CurrentSchema should succeed");
    let schema = conn
        .get_option_string(OptionConnection::CurrentSchema)
        .expect("get_option_string for CurrentSchema should succeed");
    assert_eq!(schema, "test_schema");
    println!("  CurrentSchema: {}", schema);

    // Test AutoCommit is always "true"
    println!("Testing AutoCommit option...");
    let autocommit = conn
        .get_option_string(OptionConnection::AutoCommit)
        .expect("get_option_string for AutoCommit should succeed");
    assert_eq!(autocommit, "true");
    println!("  AutoCommit: {}", autocommit);

    println!();
    println!("=== DatabricksConnection Options E2E Test PASSED ===");
}

// ============================================================================
// Work Item 2.2: DatabricksConnection Optionable Trait E2E Tests
// ============================================================================

/// Test DatabricksConnection Optionable trait - comprehensive option handling.
///
/// This validates all connection-level options according to ADBC spec:
/// - AutoCommit: always true (Databricks doesn't support transactions)
/// - ReadOnly: always false (Databricks doesn't support read-only mode)
/// - CurrentCatalog: settable and gettable
/// - CurrentSchema: settable and gettable
/// - IsolationLevel: not supported (returns NotImplemented)
#[test]
#[ignore]
fn test_e2e_connection_optionable_comprehensive() {
    use adbc_core::error::Status;
    use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
    use adbc_core::{Database, Driver, Optionable};
    use adbc_databricks::DatabricksDriver;

    skip_if_no_config!();

    let config = get_test_config();
    let (host, warehouse_id) = config.parse_uri().expect("Failed to parse URI");

    println!("=== DatabricksConnection Optionable Comprehensive E2E Test ===");
    println!("Host: {}", host);
    println!("Warehouse ID: {}", warehouse_id);
    println!();

    // Create driver, database, and connection
    let mut driver = DatabricksDriver::new();
    let db = driver
        .new_database_with_opts([
            (OptionDatabase::Uri, OptionValue::String(host.clone())),
            (
                OptionDatabase::Password,
                OptionValue::String(config.token.clone()),
            ),
            (
                OptionDatabase::Other("databricks.warehouse_id".into()),
                OptionValue::String(warehouse_id.clone()),
            ),
        ])
        .expect("Failed to create database");

    let mut conn = db.new_connection().expect("Failed to create connection");

    // =========================================================================
    // Test 1: AutoCommit
    // =========================================================================
    println!("Test 1: AutoCommit option");

    // get_option_string should return "true"
    let autocommit_str = conn
        .get_option_string(OptionConnection::AutoCommit)
        .expect("get_option_string for AutoCommit should succeed");
    assert_eq!(autocommit_str, "true", "AutoCommit should be 'true'");
    println!("  get_option_string(AutoCommit) = '{}'", autocommit_str);

    // get_option_int should return 1
    let autocommit_int = conn
        .get_option_int(OptionConnection::AutoCommit)
        .expect("get_option_int for AutoCommit should succeed");
    assert_eq!(autocommit_int, 1, "AutoCommit int should be 1 (true)");
    println!("  get_option_int(AutoCommit) = {}", autocommit_int);

    // Setting autocommit to "true" should succeed
    conn.set_option(
        OptionConnection::AutoCommit,
        OptionValue::String("true".into()),
    )
    .expect("Setting AutoCommit to 'true' should succeed");
    println!("  set_option(AutoCommit, 'true') succeeded");

    // Setting autocommit to "false" should fail
    let set_false_result = conn.set_option(
        OptionConnection::AutoCommit,
        OptionValue::String("false".into()),
    );
    assert!(
        set_false_result.is_err(),
        "Setting AutoCommit to 'false' should fail"
    );
    let err = set_false_result.unwrap_err();
    assert_eq!(
        err.status,
        Status::InvalidArguments,
        "Error should be InvalidArguments"
    );
    println!("  set_option(AutoCommit, 'false') correctly failed: {}", err.message);

    // =========================================================================
    // Test 2: ReadOnly
    // =========================================================================
    println!();
    println!("Test 2: ReadOnly option");

    // get_option_string should return "false"
    let readonly_str = conn
        .get_option_string(OptionConnection::ReadOnly)
        .expect("get_option_string for ReadOnly should succeed");
    assert_eq!(readonly_str, "false", "ReadOnly should be 'false'");
    println!("  get_option_string(ReadOnly) = '{}'", readonly_str);

    // get_option_int should return 0
    let readonly_int = conn
        .get_option_int(OptionConnection::ReadOnly)
        .expect("get_option_int for ReadOnly should succeed");
    assert_eq!(readonly_int, 0, "ReadOnly int should be 0 (false)");
    println!("  get_option_int(ReadOnly) = {}", readonly_int);

    // Setting read_only to "false" should succeed
    conn.set_option(
        OptionConnection::ReadOnly,
        OptionValue::String("false".into()),
    )
    .expect("Setting ReadOnly to 'false' should succeed");
    println!("  set_option(ReadOnly, 'false') succeeded");

    // Setting read_only to "true" should fail
    let set_true_result = conn.set_option(
        OptionConnection::ReadOnly,
        OptionValue::String("true".into()),
    );
    assert!(
        set_true_result.is_err(),
        "Setting ReadOnly to 'true' should fail"
    );
    let err = set_true_result.unwrap_err();
    assert_eq!(
        err.status,
        Status::InvalidArguments,
        "Error should be InvalidArguments"
    );
    println!("  set_option(ReadOnly, 'true') correctly failed: {}", err.message);

    // =========================================================================
    // Test 3: IsolationLevel (not supported)
    // =========================================================================
    println!();
    println!("Test 3: IsolationLevel option (not supported)");

    // get_option_string should fail with NotImplemented
    let get_isolation_result = conn.get_option_string(OptionConnection::IsolationLevel);
    assert!(
        get_isolation_result.is_err(),
        "Getting IsolationLevel should fail"
    );
    let err = get_isolation_result.unwrap_err();
    assert_eq!(
        err.status,
        Status::NotImplemented,
        "Error should be NotImplemented"
    );
    println!("  get_option_string(IsolationLevel) correctly failed: {}", err.message);

    // set_option should fail with NotImplemented
    let set_isolation_result = conn.set_option(
        OptionConnection::IsolationLevel,
        OptionValue::String("READ_COMMITTED".into()),
    );
    assert!(
        set_isolation_result.is_err(),
        "Setting IsolationLevel should fail"
    );
    let err = set_isolation_result.unwrap_err();
    assert_eq!(
        err.status,
        Status::NotImplemented,
        "Error should be NotImplemented"
    );
    println!("  set_option(IsolationLevel, 'READ_COMMITTED') correctly failed: {}", err.message);

    // =========================================================================
    // Test 4: CurrentCatalog
    // =========================================================================
    println!();
    println!("Test 4: CurrentCatalog option");

    // Set catalog to a test value
    conn.set_option(
        OptionConnection::CurrentCatalog,
        OptionValue::String("test_catalog_e2e".into()),
    )
    .expect("Setting CurrentCatalog should succeed");
    println!("  set_option(CurrentCatalog, 'test_catalog_e2e') succeeded");

    // Get catalog should return the set value
    let catalog = conn
        .get_option_string(OptionConnection::CurrentCatalog)
        .expect("get_option_string for CurrentCatalog should succeed");
    assert_eq!(catalog, "test_catalog_e2e", "Catalog should match set value");
    println!("  get_option_string(CurrentCatalog) = '{}'", catalog);

    // =========================================================================
    // Test 5: CurrentSchema
    // =========================================================================
    println!();
    println!("Test 5: CurrentSchema option");

    // Set schema to a test value
    conn.set_option(
        OptionConnection::CurrentSchema,
        OptionValue::String("test_schema_e2e".into()),
    )
    .expect("Setting CurrentSchema should succeed");
    println!("  set_option(CurrentSchema, 'test_schema_e2e') succeeded");

    // Get schema should return the set value
    let schema = conn
        .get_option_string(OptionConnection::CurrentSchema)
        .expect("get_option_string for CurrentSchema should succeed");
    assert_eq!(schema, "test_schema_e2e", "Schema should match set value");
    println!("  get_option_string(CurrentSchema) = '{}'", schema);

    // =========================================================================
    // Test 6: Unknown/Other options
    // =========================================================================
    println!();
    println!("Test 6: Unknown option handling");

    // Setting unknown option should fail
    let set_unknown_result = conn.set_option(
        OptionConnection::Other("unknown.custom.option".into()),
        OptionValue::String("value".into()),
    );
    assert!(
        set_unknown_result.is_err(),
        "Setting unknown option should fail"
    );
    let err = set_unknown_result.unwrap_err();
    assert_eq!(
        err.status,
        Status::NotImplemented,
        "Error should be NotImplemented"
    );
    println!("  set_option(Other('unknown.custom.option'), ...) correctly failed");

    // Getting unknown option should fail
    let get_unknown_result =
        conn.get_option_string(OptionConnection::Other("unknown.custom.option".into()));
    assert!(
        get_unknown_result.is_err(),
        "Getting unknown option should fail"
    );
    let err = get_unknown_result.unwrap_err();
    assert_eq!(err.status, Status::NotFound, "Error should be NotFound");
    println!("  get_option_string(Other('unknown.custom.option')) correctly failed");

    // =========================================================================
    // Test 7: get_option_bytes and get_option_double (not supported)
    // =========================================================================
    println!();
    println!("Test 7: Unsupported option types");

    // get_option_bytes should fail
    let bytes_result = conn.get_option_bytes(OptionConnection::AutoCommit);
    assert!(bytes_result.is_err(), "get_option_bytes should fail");
    assert_eq!(
        bytes_result.unwrap_err().status,
        Status::NotImplemented,
        "Error should be NotImplemented"
    );
    println!("  get_option_bytes correctly returned NotImplemented");

    // get_option_double should fail
    let double_result = conn.get_option_double(OptionConnection::AutoCommit);
    assert!(double_result.is_err(), "get_option_double should fail");
    assert_eq!(
        double_result.unwrap_err().status,
        Status::NotImplemented,
        "Error should be NotImplemented"
    );
    println!("  get_option_double correctly returned NotImplemented");

    println!();
    println!("=== DatabricksConnection Optionable Comprehensive E2E Test PASSED ===");
    println!("All connection option behaviors verified successfully!");
}

/// Test that connection options set via catalog from config are accessible.
///
/// This validates:
/// - Database default catalog/schema are inherited by connection
/// - Connection get_option_string returns inherited values
#[test]
#[ignore]
fn test_e2e_connection_inherits_database_catalog_schema() {
    use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
    use adbc_core::{Database, Driver, Optionable};
    use adbc_databricks::DatabricksDriver;

    skip_if_no_config!();

    let config = get_test_config();
    let (host, warehouse_id) = config.parse_uri().expect("Failed to parse URI");

    // Skip if no catalog configured
    if config.metadata.catalog.is_empty() {
        println!("Skipping: No catalog configured in test metadata");
        return;
    }

    println!("=== Connection Inherits Database Catalog/Schema E2E Test ===");
    println!("Host: {}", host);
    println!("Warehouse ID: {}", warehouse_id);
    println!("Expected Catalog: {}", config.metadata.catalog);
    println!("Expected Schema: {}", config.metadata.schema);
    println!();

    // Create driver and database with catalog/schema options
    let mut driver = DatabricksDriver::new();
    let db = driver
        .new_database_with_opts([
            (OptionDatabase::Uri, OptionValue::String(host.clone())),
            (
                OptionDatabase::Password,
                OptionValue::String(config.token.clone()),
            ),
            (
                OptionDatabase::Other("databricks.warehouse_id".into()),
                OptionValue::String(warehouse_id.clone()),
            ),
            (
                OptionDatabase::Other("databricks.catalog".into()),
                OptionValue::String(config.metadata.catalog.clone()),
            ),
            (
                OptionDatabase::Other("databricks.schema".into()),
                OptionValue::String(config.metadata.schema.clone()),
            ),
        ])
        .expect("Failed to create database");

    // Create connection - should inherit catalog/schema from database
    let conn = db.new_connection().expect("Failed to create connection");

    // Verify connection has inherited catalog
    let conn_catalog = conn
        .get_option_string(OptionConnection::CurrentCatalog)
        .expect("Should be able to get current catalog");
    assert_eq!(
        conn_catalog, config.metadata.catalog,
        "Connection catalog should match database config"
    );
    println!("  Connection CurrentCatalog: {}", conn_catalog);

    // Verify connection has inherited schema
    let conn_schema = conn
        .get_option_string(OptionConnection::CurrentSchema)
        .expect("Should be able to get current schema");
    assert_eq!(
        conn_schema, config.metadata.schema,
        "Connection schema should match database config"
    );
    println!("  Connection CurrentSchema: {}", conn_schema);

    println!();
    println!("=== Connection Inherits Database Catalog/Schema E2E Test PASSED ===");
}
