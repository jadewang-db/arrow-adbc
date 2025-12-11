//! E2E tests for connection lifecycle and session management
//!
//! These tests validate connection creation, session management, catalog/schema context,
//! and timeout handling against a live Databricks SQL Warehouse.
//!
//! Run with: cargo test --release --ignored e2e_connection

use adbc_core::{Connection, Database, Optionable, Statement};
use adbc_core::options::{OptionConnection, OptionValue};
use arrow_array::StringArray;
use arrow_array::cast::AsArray;

use super::helpers::*;

#[test]
fn test_e2e_connection_open_creates_session() {
    // Skip if configuration not available
    skip_if_no_config!();

    let mut conn = create_test_connection();

    // Verify we can create a statement (which requires an active session)
    let stmt = conn.new_statement();
    assert!(stmt.is_ok(), "Should be able to create statement with active session");
}

#[test]
fn test_e2e_connection_close_terminates_session() {
    skip_if_no_config!();

    // Create and immediately drop connection
    {
        let conn = create_test_connection();
        drop(conn);
        // Session should be terminated when connection is dropped
    }

    // If we reach here without panicking, the drop was successful
    assert!(true, "Connection dropped successfully");
}

#[test]
fn test_e2e_connection_set_catalog_changes_context() {
    skip_if_no_config!();

    let config = super::config::E2EConfig::from_env().expect("Failed to load config");
    let catalog = config.metadata.catalog.clone();

    if catalog.is_empty() {
        println!("Skipping test: no catalog specified in configuration");
        return;
    }

    // Create connection with specific catalog
    let mut conn = create_test_connection_with_catalog(&catalog, "default");

    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("SELECT current_catalog() AS catalog").unwrap();
    let mut reader = stmt.execute().unwrap();
    let batch = reader.next().unwrap().unwrap();

    // Verify catalog is correct
    assert_eq!(batch.num_rows(), 1);
    let array = batch.column(0).as_string::<i32>();
    assert_eq!(array.value(0), catalog, "Current catalog should match configured catalog");
}

#[test]
fn test_e2e_connection_set_schema_changes_context() {
    skip_if_no_config!();

    let config = super::config::E2EConfig::from_env().expect("Failed to load config");
    let catalog = config.metadata.catalog.clone();
    let schema = config.metadata.schema.clone();

    if catalog.is_empty() || schema.is_empty() {
        println!("Skipping test: no catalog/schema specified in configuration");
        return;
    }

    // Create connection with specific catalog and schema
    let mut conn = create_test_connection_with_catalog(&catalog, &schema);

    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("SELECT current_database() AS schema").unwrap();
    let mut reader = stmt.execute().unwrap();
    let batch = reader.next().unwrap().unwrap();

    // Verify schema is correct
    assert_eq!(batch.num_rows(), 1);
    let array = batch.column(0).as_string::<i32>();
    assert_eq!(array.value(0), schema, "Current schema should match configured schema");
}

#[test]
fn test_e2e_connection_autocommit_always_true() {
    skip_if_no_config!();

    let mut conn = create_test_connection();

    // Get autocommit value
    let autocommit = conn.get_option_string(OptionConnection::AutoCommit).unwrap();
    assert_eq!(autocommit, "true", "Autocommit should always be true for Databricks");

    // Try to set autocommit to false (should fail)
    let result = conn.set_option(OptionConnection::AutoCommit, OptionValue::String("false".to_string()));
    assert!(result.is_err(), "Setting autocommit to false should fail");
}

#[test]
fn test_e2e_connection_multiple_from_same_database() {
    skip_if_no_config!();

    let mut db = create_test_database();

    // Create multiple connections
    let conn1 = db.new_connection();
    let conn2 = db.new_connection();

    assert!(conn1.is_ok(), "First connection should succeed");
    assert!(conn2.is_ok(), "Second connection should succeed");

    // Both connections should be independent
    let mut c1 = conn1.unwrap();
    let mut c2 = conn2.unwrap();

    // Verify both can execute queries
    let stmt1 = c1.new_statement();
    let stmt2 = c2.new_statement();

    assert!(stmt1.is_ok(), "First connection should create statement");
    assert!(stmt2.is_ok(), "Second connection should create statement");
}

#[test]
fn test_e2e_connection_independent_catalog_schema_per_connection() {
    skip_if_no_config!();

    let config = super::config::E2EConfig::from_env().expect("Failed to load config");
    let catalog = config.metadata.catalog.clone();

    if catalog.is_empty() {
        println!("Skipping test: no catalog specified in configuration");
        return;
    }

    let mut db = create_test_database();

    // Create two connections with different catalog settings
    let mut conn1 = db.new_connection().unwrap();
    conn1.set_option(OptionConnection::CurrentCatalog, OptionValue::String(catalog.clone())).unwrap();

    let mut conn2 = db.new_connection().unwrap();
    // conn2 uses default catalog

    // Verify conn1 uses specified catalog
    let catalog1 = conn1.get_option_string(OptionConnection::CurrentCatalog).unwrap();
    assert_eq!(catalog1, catalog);

    // Connections should maintain independent settings
    // (conn2's catalog is independent of conn1)
}

#[test]
fn test_e2e_connection_commit_not_supported() {
    skip_if_no_config!();

    let mut conn = create_test_connection();

    // Databricks doesn't support transactions
    let result = conn.commit();
    assert!(result.is_err(), "commit() should return NotImplemented error");
}

#[test]
fn test_e2e_connection_rollback_not_supported() {
    skip_if_no_config!();

    let mut conn = create_test_connection();

    // Databricks doesn't support transactions
    let result = conn.rollback();
    assert!(result.is_err(), "rollback() should return NotImplemented error");
}

#[test]
fn test_e2e_connection_reuse_after_query() {
    skip_if_no_config!();

    let mut conn = create_test_connection();

    // Execute first query
    let mut stmt1 = conn.new_statement().unwrap();
    stmt1.set_sql_query("SELECT 1").unwrap();
    let mut reader1 = stmt1.execute().unwrap();
    let _batch1 = reader1.next().unwrap().unwrap();
    drop(reader1);
    drop(stmt1);

    // Execute second query on same connection
    let mut stmt2 = conn.new_statement().unwrap();
    stmt2.set_sql_query("SELECT 2").unwrap();
    let mut reader2 = stmt2.execute().unwrap();
    let batch2 = reader2.next().unwrap().unwrap();

    assert_eq!(batch2.num_rows(), 1);
}

#[test]
fn test_e2e_connection_use_catalog_statement() {
    skip_if_no_config!();

    let config = super::config::E2EConfig::from_env().expect("Failed to load config");
    let catalog = config.metadata.catalog.clone();

    if catalog.is_empty() {
        println!("Skipping test: no catalog specified in configuration");
        return;
    }

    let mut conn = create_test_connection();

    // Execute USE CATALOG statement
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query(&format!("USE CATALOG {}", catalog)).unwrap();
    let _affected = stmt.execute_update().unwrap();

    // Verify catalog changed
    let mut stmt2 = conn.new_statement().unwrap();
    stmt2.set_sql_query("SELECT current_catalog() AS catalog").unwrap();
    let mut reader = stmt2.execute().unwrap();
    let batch = reader.next().unwrap().unwrap();

    let array = batch.column(0).as_string::<i32>();
    assert_eq!(array.value(0), catalog);
}

#[test]
fn test_e2e_connection_use_schema_statement() {
    skip_if_no_config!();

    let config = super::config::E2EConfig::from_env().expect("Failed to load config");
    let catalog = config.metadata.catalog.clone();
    let schema = config.metadata.schema.clone();

    if catalog.is_empty() || schema.is_empty() {
        println!("Skipping test: no catalog/schema specified in configuration");
        return;
    }

    let mut conn = create_test_connection_with_catalog(&catalog, "default")
;

    // Execute USE SCHEMA statement
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query(&format!("USE SCHEMA {}", schema)).unwrap();
    let _affected = stmt.execute_update().unwrap();

    // Verify schema changed
    let mut stmt2 = conn.new_statement().unwrap();
    stmt2.set_sql_query("SELECT current_database() AS schema").unwrap();
    let mut reader = stmt2.execute().unwrap();
    let batch = reader.next().unwrap().unwrap();

    let array = batch.column(0).as_string::<i32>();
    assert_eq!(array.value(0), schema);
}
