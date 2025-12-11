//! E2E tests for basic query execution
//!
//! These tests validate basic query execution paths including simple queries,
//! empty results, NULL values, and syntax errors against a live Databricks SQL Warehouse.
//!
//! Run with: cargo test --release --ignored e2e_query_basic

use adbc_core::{Connection, Statement};
use arrow_array::cast::AsArray;
use arrow_array::{Int32Array, StringArray};

use super::helpers::*;

#[test]
fn test_e2e_query_select_one() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    stmt.set_sql_query("SELECT 1 AS one").unwrap();
    let mut reader = stmt.execute().unwrap();

    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 1);

    let array = batch.column(0).as_primitive::<arrow_array::types::Int32Type>();
    assert_eq!(array.value(0), 1);
}

#[test]
fn test_e2e_query_multiple_rows() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    stmt.set_sql_query("SELECT * FROM range(0, 10)").unwrap();
    let mut reader = stmt.execute().unwrap();

    let mut total_rows = 0;
    for batch_result in &mut reader {
        total_rows += batch_result.unwrap().num_rows();
    }
    assert_eq!(total_rows, 10);
}

#[test]
fn test_e2e_query_multiple_columns() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    stmt.set_sql_query("SELECT 1 AS col1, 'text' AS col2, true AS col3").unwrap();
    let mut reader = stmt.execute().unwrap();

    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 3);

    // Verify column names
    let schema = batch.schema();
    assert_eq!(schema.field(0).name(), "col1");
    assert_eq!(schema.field(1).name(), "col2");
    assert_eq!(schema.field(2).name(), "col3");
}

#[test]
fn test_e2e_query_empty_result() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    stmt.set_sql_query("SELECT * FROM range(0, 0)").unwrap();
    let reader = stmt.execute().unwrap();

    // Count rows (schema will be checked in first batch even if no rows)
    let total_rows = count_reader_rows(reader);
    assert_eq!(total_rows, 0);
}

#[test]
fn test_e2e_query_null_values() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    stmt.set_sql_query("SELECT NULL AS null_col, 1 AS int_col").unwrap();
    let mut reader = stmt.execute().unwrap();

    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);

    // First column should be null
    let null_col = batch.column(0);
    assert!(null_col.is_null(0));

    // Second column should not be null
    let int_col = batch.column(1);
    assert!(!int_col.is_null(0));
}

#[test]
fn test_e2e_query_string_special_characters() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    stmt.set_sql_query("SELECT 'Hello, \"World\"!' AS str_col").unwrap();
    let mut reader = stmt.execute().unwrap();

    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);

    let array = batch.column(0).as_string::<i32>();
    assert_eq!(array.value(0), "Hello, \"World\"!");
}

#[test]
fn test_e2e_query_unicode_strings() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    stmt.set_sql_query("SELECT 'Hello, World! 🌍' AS unicode_col").unwrap();
    let mut reader = stmt.execute().unwrap();

    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);

    let array = batch.column(0).as_string::<i32>();
    assert_eq!(array.value(0), "Hello, World! 🌍");
}

#[test]
fn test_e2e_query_syntax_error() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    stmt.set_sql_query("INVALID SQL SYNTAX").unwrap();
    let result = stmt.execute();

    assert!(result.is_err(), "Invalid SQL should return an error");
}

#[test]
fn test_e2e_query_table_not_found() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    stmt.set_sql_query("SELECT * FROM nonexistent_table_12345").unwrap();
    let result = stmt.execute();

    assert!(result.is_err(), "Querying non-existent table should return an error");
}

#[test]
fn test_e2e_query_with_where_clause() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    stmt.set_sql_query("SELECT * FROM range(0, 100) WHERE id < 10").unwrap();
    let mut reader = stmt.execute().unwrap();

    let mut total_rows = 0;
    for batch_result in &mut reader {
        total_rows += batch_result.unwrap().num_rows();
    }
    assert_eq!(total_rows, 10);
}

#[test]
fn test_e2e_query_with_aggregation() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    stmt.set_sql_query("SELECT COUNT(*) AS cnt FROM range(0, 100)").unwrap();
    let mut reader = stmt.execute().unwrap();

    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);

    let array = batch.column(0).as_primitive::<arrow_array::types::Int64Type>();
    assert_eq!(array.value(0), 100);
}

#[test]
fn test_e2e_query_with_group_by() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    stmt.set_sql_query(
        "SELECT id % 10 AS group_id, COUNT(*) AS cnt FROM range(0, 100) GROUP BY id % 10"
    ).unwrap();
    let mut reader = stmt.execute().unwrap();

    let mut total_rows = 0;
    for batch_result in &mut reader {
        total_rows += batch_result.unwrap().num_rows();
    }
    assert_eq!(total_rows, 10); // 10 groups
}

#[test]
fn test_e2e_query_with_order_by() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    stmt.set_sql_query("SELECT id FROM range(0, 10) ORDER BY id DESC").unwrap();
    let mut reader = stmt.execute().unwrap();

    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 10);

    // First row should have the highest ID
    let array = batch.column(0).as_primitive::<arrow_array::types::Int64Type>();
    assert_eq!(array.value(0), 9);
}
