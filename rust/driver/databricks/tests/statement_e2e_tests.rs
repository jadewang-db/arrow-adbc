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

//! End-to-end tests for statement execution
//!
//! These tests verify the complete statement execution path against a real Databricks
//! SQL Warehouse.  They require the DATABRICKS_TEST_CONFIG_FILE environment variable
//! to be set to a valid configuration file.

use adbc_core::{Connection, Database, Statement};
use arrow_array::cast::AsArray;
use arrow_array::Array;

mod e2e;

/// Test simple SELECT 1 query returns correct result
#[test]
#[ignore]
fn test_e2e_query_select_basic() {
    skip_if_no_config!();

    let database = e2e::helpers::create_test_database();
    let mut conn = database.new_connection().expect("Failed to create connection");

    // Test 1: SELECT 1
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query("SELECT 1 AS one").unwrap();
        let mut reader = stmt.execute().unwrap();

        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "one");

        let array = batch.column(0).as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(array.value(0), 1);
    }

    // Test 2: SELECT with multiple columns
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query("SELECT 42 AS num, 'hello' AS msg").unwrap();
        let mut reader = stmt.execute().unwrap();

        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 2);

        let num_array = batch.column(0).as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(num_array.value(0), 42);

        let msg_array = batch.column(1).as_string::<i32>();
        assert_eq!(msg_array.value(0), "hello");
    }

    // Test 3: Empty result
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query("SELECT * FROM range(0, 0)").unwrap();
        let reader = stmt.execute().unwrap();
        let batches: Vec<_> = reader.collect::<std::result::Result<Vec<_>, _>>().unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0);
    }

    println!("Basic query execution verified");
}

/// Test data type handling for all basic types
#[test]
#[ignore]
fn test_e2e_query_data_types() {
    skip_if_no_config!();

    let database = e2e::helpers::create_test_database();
    let mut conn = database.new_connection().expect("Failed to create connection");
    let mut stmt = conn.new_statement().expect("Failed to create statement");

    stmt.set_sql_query(
        "SELECT \
         true AS bool_col, \
         CAST(127 AS TINYINT) AS tinyint_col, \
         CAST(32767 AS SMALLINT) AS smallint_col, \
         CAST(2147483647 AS INT) AS int_col, \
         CAST(9223372036854775807 AS BIGINT) AS bigint_col, \
         CAST(3.14 AS FLOAT) AS float_col, \
         CAST(2.718281828 AS DOUBLE) AS double_col, \
         'Hello, World!' AS string_col"
    ).unwrap();

    let mut reader = stmt.execute().unwrap();
    let batch = reader.next().unwrap().unwrap();

    assert_eq!(batch.num_columns(), 8);
    assert_eq!(batch.num_rows(), 1);

    // Verify boolean
    let bool_col = batch.column(0).as_boolean();
    assert_eq!(bool_col.value(0), true);

    // Verify tinyint
    let tinyint_col = batch.column(1).as_primitive::<arrow_array::types::Int8Type>();
    assert_eq!(tinyint_col.value(0), 127);

    // Verify smallint
    let smallint_col = batch.column(2).as_primitive::<arrow_array::types::Int16Type>();
    assert_eq!(smallint_col.value(0), 32767);

    // Verify int
    let int_col = batch.column(3).as_primitive::<arrow_array::types::Int32Type>();
    assert_eq!(int_col.value(0), 2147483647);

    // Verify bigint
    let bigint_col = batch.column(4).as_primitive::<arrow_array::types::Int64Type>();
    assert_eq!(bigint_col.value(0), 9223372036854775807);

    // Verify float (with tolerance for floating point comparison)
    let float_col = batch.column(5).as_primitive::<arrow_array::types::Float32Type>();
    assert!((float_col.value(0) - 3.14).abs() < 0.01);

    // Verify double (with tolerance for floating point comparison)
    let double_col = batch.column(6).as_primitive::<arrow_array::types::Float64Type>();
    assert!((double_col.value(0) - 2.718281828).abs() < 0.000001);

    // Verify string
    let string_col = batch.column(7).as_string::<i32>();
    assert_eq!(string_col.value(0), "Hello, World!");

    println!("Data type handling verified");
}

/// Test query with multiple rows
#[test]
#[ignore]
fn test_e2e_query_multiple_rows() {
    skip_if_no_config!();

    let database = e2e::helpers::create_test_database();
    let mut conn = database.new_connection().expect("Failed to create connection");
    let mut stmt = conn.new_statement().expect("Failed to create statement");

    // Query that generates multiple rows
    stmt.set_sql_query("SELECT id FROM range(0, 10)").unwrap();
    let reader = stmt.execute().unwrap();

    let batches: Vec<_> = reader.collect::<std::result::Result<Vec<_>, _>>().unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 10);

    // Verify first batch has the correct data
    let first_batch = &batches[0];
    assert_eq!(first_batch.num_columns(), 1);

    let id_col = first_batch.column(0).as_primitive::<arrow_array::types::Int64Type>();
    for i in 0..first_batch.num_rows() {
        assert_eq!(id_col.value(i), i as i64);
    }

    println!("Multiple row query verified");
}

/// Test query with NULL values
#[test]
#[ignore]
fn test_e2e_query_null_values() {
    skip_if_no_config!();

    let database = e2e::helpers::create_test_database();
    let mut conn = database.new_connection().expect("Failed to create connection");
    let mut stmt = conn.new_statement().expect("Failed to create statement");

    stmt.set_sql_query("SELECT CAST(NULL AS INT) AS null_col, 42 AS not_null_col").unwrap();
    let mut reader = stmt.execute().unwrap();

    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 2);

    // Verify null column
    let null_col = batch.column(0).as_primitive::<arrow_array::types::Int32Type>();
    assert!(null_col.is_null(0));

    // Verify non-null column
    let not_null_col = batch.column(1).as_primitive::<arrow_array::types::Int32Type>();
    assert!(!not_null_col.is_null(0));
    assert_eq!(not_null_col.value(0), 42);

    println!("NULL value handling verified");
}

/// Test query error handling
#[test]
#[ignore]
fn test_e2e_query_error_invalid_sql() {
    skip_if_no_config!();

    let database = e2e::helpers::create_test_database();
    let mut conn = database.new_connection().expect("Failed to create connection");
    let mut stmt = conn.new_statement().expect("Failed to create statement");

    // Invalid SQL syntax
    stmt.set_sql_query("INVALID SQL SYNTAX").unwrap();
    let result = stmt.execute();

    assert!(result.is_err());
    if let Err(err) = result {
        let err_msg = format!("{}", err);
        assert!(err_msg.contains("INVALID") || err_msg.contains("syntax") || err_msg.contains("parse"));
    }

    println!("Error handling verified");
}
