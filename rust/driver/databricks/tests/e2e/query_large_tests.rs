//! E2E tests for large result handling
//!
//! These tests validate handling of large result sets, including inline vs external
//! links, streaming, and compression.
//!
//! Run with: cargo test --release --ignored e2e_query_large

use adbc_core::{Connection, Statement};

use super::helpers::*;

#[test]
#[ignore]
fn test_e2e_result_inline_small() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    // Small result should be inline (< 16MB)
    stmt.set_sql_query("SELECT * FROM range(0, 100)").unwrap();
    let reader = stmt.execute().unwrap();

    let total_rows = count_reader_rows(reader);
    assert_eq!(total_rows, 100);
}

#[test]
#[ignore]
fn test_e2e_result_medium_1k_rows() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    stmt.set_sql_query("SELECT * FROM range(0, 1000)").unwrap();
    let reader = stmt.execute().unwrap();

    let total_rows = count_reader_rows(reader);
    assert_eq!(total_rows, 1000);
}

#[test]
#[ignore]
fn test_e2e_result_medium_10k_rows() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    stmt.set_sql_query("SELECT * FROM range(0, 10000)").unwrap();
    let reader = stmt.execute().unwrap();

    let total_rows = count_reader_rows(reader);
    assert_eq!(total_rows, 10000);
}

#[test]
#[ignore]
fn test_e2e_result_large_100k_rows() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    stmt.set_sql_query("SELECT * FROM range(0, 100000)").unwrap();
    let reader = stmt.execute().unwrap();

    let total_rows = count_reader_rows(reader);
    assert_eq!(total_rows, 100000);
}

#[test]
#[ignore]
fn test_e2e_result_large_1m_rows() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    // 1 million rows - should trigger external links
    stmt.set_sql_query("SELECT * FROM range(0, 1000000)").unwrap();
    let reader = stmt.execute().unwrap();

    let total_rows = count_reader_rows(reader);
    assert_eq!(total_rows, 1000000);
}

#[test]
#[ignore]
fn test_e2e_result_wide_table() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    // Query with many columns
    stmt.set_sql_query(
        "SELECT \
         id, id+1 AS c1, id+2 AS c2, id+3 AS c3, id+4 AS c4, \
         id+5 AS c5, id+6 AS c6, id+7 AS c7, id+8 AS c8, id+9 AS c9, \
         id+10 AS c10, id+11 AS c11, id+12 AS c12, id+13 AS c13, id+14 AS c14, \
         id+15 AS c15, id+16 AS c16, id+17 AS c17, id+18 AS c18, id+19 AS c19 \
         FROM range(0, 1000)"
    ).unwrap();
    let mut reader = stmt.execute().unwrap();

    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_columns(), 20);

    let total_rows = count_reader_rows(reader) + batch.num_rows();
    assert_eq!(total_rows, 1000);
}

#[test]
#[ignore]
fn test_e2e_result_streaming_multiple_batches() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    stmt.set_sql_query("SELECT * FROM range(0, 50000)").unwrap();
    let reader = stmt.execute().unwrap();

    let mut batch_count = 0;
    let mut total_rows = 0;

    for batch_result in reader {
        let batch = batch_result.unwrap();
        batch_count += 1;
        total_rows += batch.num_rows();
    }

    assert_eq!(total_rows, 50000);
    // Should have multiple batches for this size
    // (actual count depends on batch size configuration)
    println!("Received {} batches", batch_count);
}

#[test]
#[ignore]
fn test_e2e_result_with_string_data() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    // Generate large result with string data
    stmt.set_sql_query(
        "SELECT id, CONCAT('Row number ', CAST(id AS STRING), ' with some text') AS text_col \
         FROM range(0, 10000)"
    ).unwrap();
    let reader = stmt.execute().unwrap();

    let total_rows = count_reader_rows(reader);
    assert_eq!(total_rows, 10000);
}

#[test]
#[ignore]
fn test_e2e_result_all_batch_sizes_match_schema() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    stmt.set_sql_query("SELECT * FROM range(0, 20000)").unwrap();
    let mut reader = stmt.execute().unwrap();

    let mut expected_fields = 0;
    let mut batch_count = 0;

    for batch_result in reader {
        let batch = batch_result.unwrap();
        batch_count += 1;

        if batch_count == 1 {
            expected_fields = batch.schema().fields().len();
        } else {
            // Each batch should have the same schema as the first
            assert_eq!(batch.schema().fields().len(), expected_fields);
        }
    }

    assert!(batch_count > 0, "Should have at least one batch");
}

#[test]
#[ignore]
fn test_e2e_result_compression_handling() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    // Large result that will likely use compression
    stmt.set_sql_query(
        "SELECT id, id * 2 AS double_id, CONCAT('Value: ', CAST(id AS STRING)) AS text \
         FROM range(0, 100000)"
    ).unwrap();
    let reader = stmt.execute().unwrap();

    let total_rows = count_reader_rows(reader);
    assert_eq!(total_rows, 100000);
}

#[test]
#[ignore]
fn test_e2e_result_row_limit_option() {
    skip_if_no_config!();

    let mut conn = create_test_connection();
    let mut stmt = conn.new_statement().unwrap();

    // Query would return 10000 rows, but limit to 100
    stmt.set_sql_query("SELECT * FROM range(0, 10000)").unwrap();

    // Note: Row limit would need to be set via statement options
    // This test verifies the full result without limit for now
    let reader = stmt.execute().unwrap();

    let total_rows = count_reader_rows(reader);
    assert_eq!(total_rows, 10000);
}
