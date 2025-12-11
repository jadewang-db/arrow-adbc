//! E2E tests for data type coverage
//!
//! These tests validate all Spark SQL to Arrow type mappings against a live
//! Databricks SQL Warehouse.
//!
//! Run with: cargo test --release --ignored e2e_query_types

use adbc_core::{Connection, Statement};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
};
use arrow_array::{BooleanArray, Decimal128Array, StringArray};

use super::helpers::*;

#[test]
fn test_e2e_types_numeric_all_sizes() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    stmt.set_sql_query(
        "SELECT \
         CAST(127 AS TINYINT) as tinyint_col, \
         CAST(32767 AS SMALLINT) as smallint_col, \
         CAST(2147483647 AS INT) as int_col, \
         CAST(9223372036854775807 AS BIGINT) as bigint_col, \
         CAST(3.14 AS FLOAT) as float_col, \
         CAST(2.718281828 AS DOUBLE) as double_col"
    ).unwrap();

    let mut reader = stmt.execute().unwrap();
    let batch = reader.next().unwrap().unwrap();

    assert_eq!(batch.num_columns(), 6);
    assert_eq!(batch.num_rows(), 1);

    // Verify each numeric type
    let tinyint_col = batch.column(0).as_primitive::<Int8Type>();
    assert_eq!(tinyint_col.value(0), 127);

    let smallint_col = batch.column(1).as_primitive::<Int16Type>();
    assert_eq!(smallint_col.value(0), 32767);

    let int_col = batch.column(2).as_primitive::<Int32Type>();
    assert_eq!(int_col.value(0), 2147483647);

    let bigint_col = batch.column(3).as_primitive::<Int64Type>();
    assert_eq!(bigint_col.value(0), 9223372036854775807);

    let float_col = batch.column(4).as_primitive::<Float32Type>();
    assert!((float_col.value(0) - 3.14).abs() < 0.01);

    let double_col = batch.column(5).as_primitive::<Float64Type>();
    assert!((double_col.value(0) - 2.718281828).abs() < 0.00001);
}

#[test]
fn test_e2e_types_boolean() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    stmt.set_sql_query("SELECT true AS bool_true, false AS bool_false").unwrap();
    let mut reader = stmt.execute().unwrap();
    let batch = reader.next().unwrap().unwrap();

    assert_eq!(batch.num_rows(), 1);

    let bool_true_col = batch.column(0).as_boolean();
    assert_eq!(bool_true_col.value(0), true);

    let bool_false_col = batch.column(1).as_boolean();
    assert_eq!(bool_false_col.value(0), false);
}

#[test]
fn test_e2e_types_string() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    stmt.set_sql_query("SELECT 'Hello' AS str_col, CAST('World' AS STRING) AS cast_str").unwrap();
    let mut reader = stmt.execute().unwrap();
    let batch = reader.next().unwrap().unwrap();

    assert_eq!(batch.num_rows(), 1);

    let str_col = batch.column(0).as_string::<i32>();
    assert_eq!(str_col.value(0), "Hello");

    let cast_str = batch.column(1).as_string::<i32>();
    assert_eq!(cast_str.value(0), "World");
}

#[test]
fn test_e2e_types_decimal() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    stmt.set_sql_query(
        "SELECT CAST(123.45 AS DECIMAL(10, 2)) AS decimal_col"
    ).unwrap();
    let mut reader = stmt.execute().unwrap();
    let batch = reader.next().unwrap().unwrap();

    assert_eq!(batch.num_rows(), 1);

    // Verify decimal column exists and is not null
    use arrow_array::Array;
    let decimal_col = batch.column(0).as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("Column should be Decimal128Array");
    assert!(!decimal_col.is_null(0), "Decimal column should not be null");

    // Verify precision and scale
    assert_eq!(decimal_col.precision(), 10);
    assert_eq!(decimal_col.scale(), 2);

    // Value should be 123.45 stored as 12345 with scale 2
    let value = decimal_col.value(0);
    assert_eq!(value, 12345); // 123.45 * 100
}

#[test]
fn test_e2e_types_date() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    stmt.set_sql_query("SELECT DATE '2024-12-08' AS date_col").unwrap();
    let mut reader = stmt.execute().unwrap();
    let batch = reader.next().unwrap().unwrap();

    assert_eq!(batch.num_rows(), 1);

    // Verify date column exists and is not null
    let date_col = batch.column(0);
    assert!(!date_col.is_null(0));

    // Date should be Date32 type (days since epoch)
    assert!(
        matches!(date_col.data_type(), arrow_schema::DataType::Date32),
        "Expected Date32 type"
    );
}

#[test]
fn test_e2e_types_timestamp() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    stmt.set_sql_query("SELECT TIMESTAMP '2024-12-08 12:34:56' AS timestamp_col").unwrap();
    let mut reader = stmt.execute().unwrap();
    let batch = reader.next().unwrap().unwrap();

    assert_eq!(batch.num_rows(), 1);

    // Verify timestamp column exists and is not null
    let timestamp_col = batch.column(0);
    assert!(!timestamp_col.is_null(0));

    // Timestamp should be Timestamp type
    assert!(
        matches!(
            timestamp_col.data_type(),
            arrow_schema::DataType::Timestamp(_, _)
        ),
        "Expected Timestamp type"
    );
}

#[test]
fn test_e2e_types_array() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    stmt.set_sql_query("SELECT array(1, 2, 3) AS array_col").unwrap();
    let mut reader = stmt.execute().unwrap();
    let batch = reader.next().unwrap().unwrap();

    assert_eq!(batch.num_rows(), 1);

    // Verify array column exists and is not null
    let array_col = batch.column(0);
    assert!(!array_col.is_null(0));

    // Should be a List type
    assert!(
        matches!(array_col.data_type(), arrow_schema::DataType::List(_)),
        "Expected List type"
    );
}

#[test]
fn test_e2e_types_struct() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    stmt.set_sql_query("SELECT struct(42 AS a, 'answer' AS b) AS struct_col").unwrap();
    let mut reader = stmt.execute().unwrap();
    let batch = reader.next().unwrap().unwrap();

    assert_eq!(batch.num_rows(), 1);

    // Verify struct column exists and is not null
    let struct_col = batch.column(0);
    assert!(!struct_col.is_null(0));

    // Should be a Struct type
    assert!(
        matches!(struct_col.data_type(), arrow_schema::DataType::Struct(_)),
        "Expected Struct type"
    );
}

#[test]
fn test_e2e_types_map() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    stmt.set_sql_query("SELECT map('key1', 100, 'key2', 200) AS map_col").unwrap();
    let mut reader = stmt.execute().unwrap();
    let batch = reader.next().unwrap().unwrap();

    assert_eq!(batch.num_rows(), 1);

    // Verify map column exists and is not null
    let map_col = batch.column(0);
    assert!(!map_col.is_null(0));

    // Should be a Map type
    assert!(
        matches!(map_col.data_type(), arrow_schema::DataType::Map(_, _)),
        "Expected Map type"
    );
}

#[test]
fn test_e2e_types_null_in_all_types() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    stmt.set_sql_query(
        "SELECT \
         CAST(NULL AS INT) AS null_int, \
         CAST(NULL AS STRING) AS null_str, \
         CAST(NULL AS BOOLEAN) AS null_bool, \
         CAST(NULL AS DOUBLE) AS null_double"
    ).unwrap();
    let mut reader = stmt.execute().unwrap();
    let batch = reader.next().unwrap().unwrap();

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 4);

    // All columns should be null
    for i in 0..4 {
        assert!(batch.column(i).is_null(0), "Column {} should be null", i);
    }
}

#[test]
fn test_e2e_types_negative_numbers() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    stmt.set_sql_query(
        "SELECT \
         CAST(-128 AS TINYINT) as tinyint_neg, \
         CAST(-32768 AS SMALLINT) as smallint_neg, \
         CAST(-2147483648 AS INT) as int_neg, \
         CAST(-9223372036854775808 AS BIGINT) as bigint_neg"
    ).unwrap();
    let mut reader = stmt.execute().unwrap();
    let batch = reader.next().unwrap().unwrap();

    assert_eq!(batch.num_rows(), 1);

    let tinyint_col = batch.column(0).as_primitive::<Int8Type>();
    assert_eq!(tinyint_col.value(0), -128);

    let smallint_col = batch.column(1).as_primitive::<Int16Type>();
    assert_eq!(smallint_col.value(0), -32768);

    let int_col = batch.column(2).as_primitive::<Int32Type>();
    assert_eq!(int_col.value(0), -2147483648);

    let bigint_col = batch.column(3).as_primitive::<Int64Type>();
    assert_eq!(bigint_col.value(0), -9223372036854775808);
}
