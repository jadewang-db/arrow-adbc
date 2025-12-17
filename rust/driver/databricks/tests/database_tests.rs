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

//! Unit tests for DatabricksDatabase implementation

use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::{Database, Optionable};
use adbc_driver_databricks::DatabricksDatabase;

#[test]
fn test_database_new() {
    let db = DatabricksDatabase::new();
    // Database should be created without error
    // No network connection is established at this point
    drop(db);
}

#[test]
fn test_database_config_validation_missing_uri() {
    let mut db = DatabricksDatabase::new();

    // Set only warehouse_id and token, missing URI
    db.set_option(
        OptionDatabase::Other("databricks.warehouse_id".into()),
        OptionValue::String("test-warehouse".into()),
    )
    .unwrap();

    db.set_option(
        OptionDatabase::Other("databricks.token".into()),
        OptionValue::String("test-token".into()),
    )
    .unwrap();

    // Should fail with missing URI error
    let result = db.new_connection();
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("uri"), "Error should mention uri");
}

#[test]
fn test_database_config_validation_missing_warehouse_id() {
    let mut db = DatabricksDatabase::new();

    // Set only URI and token, missing warehouse_id
    db.set_option(
        OptionDatabase::Uri,
        OptionValue::String("https://test.databricks.com".into()),
    )
    .unwrap();

    db.set_option(
        OptionDatabase::Other("databricks.token".into()),
        OptionValue::String("test-token".into()),
    )
    .unwrap();

    // Should fail with missing warehouse_id error
    let result = db.new_connection();
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("warehouse_id"),
        "Error should mention warehouse_id"
    );
}

#[test]
fn test_database_config_validation_missing_token() {
    let mut db = DatabricksDatabase::new();

    // Set only URI and warehouse_id, missing token
    db.set_option(
        OptionDatabase::Uri,
        OptionValue::String("https://test.databricks.com".into()),
    )
    .unwrap();

    db.set_option(
        OptionDatabase::Other("databricks.warehouse_id".into()),
        OptionValue::String("test-warehouse".into()),
    )
    .unwrap();

    // Should fail with missing token error
    let result = db.new_connection();
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("token"),
        "Error should mention token"
    );
}

#[test]
fn test_database_options_uri() {
    let mut db = DatabricksDatabase::new();

    db.set_option(
        OptionDatabase::Uri,
        OptionValue::String("https://test.databricks.com".into()),
    )
    .unwrap();

    let uri = db.get_option_string(OptionDatabase::Uri).unwrap();
    assert_eq!(uri, "https://test.databricks.com");
}

#[test]
fn test_database_options_uri_with_protocol() {
    let mut db = DatabricksDatabase::new();

    // Test that https:// is properly handled
    db.set_option(
        OptionDatabase::Uri,
        OptionValue::String("https://test.databricks.com".into()),
    )
    .unwrap();

    let uri = db.get_option_string(OptionDatabase::Uri).unwrap();
    assert_eq!(uri, "https://test.databricks.com");

    // Test that http:// is converted to https://
    db.set_option(
        OptionDatabase::Uri,
        OptionValue::String("http://test.databricks.com".into()),
    )
    .unwrap();

    let uri = db.get_option_string(OptionDatabase::Uri).unwrap();
    assert_eq!(uri, "https://test.databricks.com");
}

#[test]
fn test_database_options_token_via_password() {
    let mut db = DatabricksDatabase::new();

    // Token can be set via Password option
    db.set_option(
        OptionDatabase::Password,
        OptionValue::String("my-token-123".into()),
    )
    .unwrap();

    let token = db.get_option_string(OptionDatabase::Password).unwrap();
    assert_eq!(token, "my-token-123");
}

#[test]
fn test_database_custom_options_warehouse_id() {
    let mut db = DatabricksDatabase::new();

    db.set_option(
        OptionDatabase::Other("databricks.warehouse_id".into()),
        OptionValue::String("abc123".into()),
    )
    .unwrap();

    let warehouse_id = db
        .get_option_string(OptionDatabase::Other("databricks.warehouse_id".into()))
        .unwrap();
    assert_eq!(warehouse_id, "abc123");
}

#[test]
fn test_database_custom_options_token() {
    let mut db = DatabricksDatabase::new();

    db.set_option(
        OptionDatabase::Other("databricks.token".into()),
        OptionValue::String("token-xyz".into()),
    )
    .unwrap();

    let token = db
        .get_option_string(OptionDatabase::Other("databricks.token".into()))
        .unwrap();
    assert_eq!(token, "token-xyz");
}

#[test]
fn test_database_custom_options_catalog() {
    let mut db = DatabricksDatabase::new();

    db.set_option(
        OptionDatabase::Other("databricks.catalog".into()),
        OptionValue::String("main".into()),
    )
    .unwrap();

    let catalog = db
        .get_option_string(OptionDatabase::Other("databricks.catalog".into()))
        .unwrap();
    assert_eq!(catalog, "main");
}

#[test]
fn test_database_custom_options_schema() {
    let mut db = DatabricksDatabase::new();

    db.set_option(
        OptionDatabase::Other("databricks.schema".into()),
        OptionValue::String("default".into()),
    )
    .unwrap();

    let schema = db
        .get_option_string(OptionDatabase::Other("databricks.schema".into()))
        .unwrap();
    assert_eq!(schema, "default");
}

#[test]
fn test_database_custom_options_http_timeouts() {
    let mut db = DatabricksDatabase::new();

    // Set connect timeout
    db.set_option(
        OptionDatabase::Other("databricks.http.connect_timeout".into()),
        OptionValue::Int(5000), // 5 seconds in milliseconds
    )
    .unwrap();

    let connect_timeout = db
        .get_option_int(OptionDatabase::Other(
            "databricks.http.connect_timeout".into(),
        ))
        .unwrap();
    assert_eq!(connect_timeout, 5000);

    // Set read timeout
    db.set_option(
        OptionDatabase::Other("databricks.http.read_timeout".into()),
        OptionValue::Int(60000), // 60 seconds in milliseconds
    )
    .unwrap();

    let read_timeout = db
        .get_option_int(OptionDatabase::Other("databricks.http.read_timeout".into()))
        .unwrap();
    assert_eq!(read_timeout, 60000);
}

#[test]
fn test_database_custom_options_fetch_config() {
    let mut db = DatabricksDatabase::new();

    // Set fetch concurrency
    db.set_option(
        OptionDatabase::Other("databricks.fetch.concurrency".into()),
        OptionValue::Int(16),
    )
    .unwrap();

    let concurrency = db
        .get_option_int(OptionDatabase::Other(
            "databricks.fetch.concurrency".into(),
        ))
        .unwrap();
    assert_eq!(concurrency, 16);

    // Set fetch compression
    db.set_option(
        OptionDatabase::Other("databricks.fetch.compression".into()),
        OptionValue::String("ZSTD".into()),
    )
    .unwrap();

    let compression = db
        .get_option_string(OptionDatabase::Other(
            "databricks.fetch.compression".into(),
        ))
        .unwrap();
    assert_eq!(compression, "ZSTD");
}

#[test]
fn test_database_invalid_option() {
    let mut db = DatabricksDatabase::new();

    // Setting an unrecognized option should fail
    let result = db.set_option(
        OptionDatabase::Other("databricks.invalid_option".into()),
        OptionValue::String("value".into()),
    );

    assert!(result.is_err());
}

#[test]
fn test_database_option_not_set() {
    let db = DatabricksDatabase::new();

    // Getting an option that was never set should fail
    let result = db.get_option_string(OptionDatabase::Uri);
    assert!(result.is_err());

    let result = db.get_option_string(OptionDatabase::Other("databricks.warehouse_id".into()));
    assert!(result.is_err());
}

#[test]
fn test_database_option_type_mismatch() {
    let mut db = DatabricksDatabase::new();

    // Set URI as string
    db.set_option(
        OptionDatabase::Uri,
        OptionValue::String("https://test.databricks.com".into()),
    )
    .unwrap();

    // Try to get it as int (should fail)
    let result = db.get_option_int(OptionDatabase::Uri);
    assert!(result.is_err());
}

#[test]
fn test_database_multiple_options() {
    let mut db = DatabricksDatabase::new();

    // Set multiple options
    db.set_option(
        OptionDatabase::Uri,
        OptionValue::String("https://test.databricks.com".into()),
    )
    .unwrap();

    db.set_option(
        OptionDatabase::Other("databricks.warehouse_id".into()),
        OptionValue::String("warehouse-1".into()),
    )
    .unwrap();

    db.set_option(
        OptionDatabase::Other("databricks.token".into()),
        OptionValue::String("secret-token".into()),
    )
    .unwrap();

    db.set_option(
        OptionDatabase::Other("databricks.catalog".into()),
        OptionValue::String("prod".into()),
    )
    .unwrap();

    // Verify all options are stored correctly
    assert_eq!(
        db.get_option_string(OptionDatabase::Uri).unwrap(),
        "https://test.databricks.com"
    );
    assert_eq!(
        db.get_option_string(OptionDatabase::Other("databricks.warehouse_id".into()))
            .unwrap(),
        "warehouse-1"
    );
    assert_eq!(
        db.get_option_string(OptionDatabase::Other("databricks.token".into()))
            .unwrap(),
        "secret-token"
    );
    assert_eq!(
        db.get_option_string(OptionDatabase::Other("databricks.catalog".into()))
            .unwrap(),
        "prod"
    );
}
