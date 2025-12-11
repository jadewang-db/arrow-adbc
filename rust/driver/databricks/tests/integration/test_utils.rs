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

//! Test utilities and helpers for integration tests.

use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::Driver;
use adbc_driver_databricks::DatabricksDriver;
use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use base64::prelude::*;
use std::io::Cursor;
use std::sync::Arc;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Standard test warehouse ID.
pub const TEST_WAREHOUSE_ID: &str = "test-warehouse-123";

/// Standard test token.
pub const TEST_TOKEN: &str = "dapi_test_token_xyz";

/// Standard test session ID.
pub const TEST_SESSION_ID: &str = "test-session-001";

/// Create a mock server with session endpoints configured.
pub async fn setup_mock_server_with_session(session_id: &str) -> MockServer {
    let mock_server = MockServer::start().await;

    // Session create endpoint
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/sessions"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": session_id
            })),
        )
        .mount(&mock_server)
        .await;

    // Session delete endpoint
    Mock::given(method("DELETE"))
        .and(path(format!("/api/2.0/sql/sessions/{}", session_id)))
        .respond_with(ResponseTemplate::new(200))
        .mount(&mock_server)
        .await;

    mock_server
}

/// Create a configured database pointing to the mock server.
pub fn create_test_database(
    mock_server_uri: &str,
    warehouse_id: &str,
    token: &str,
) -> adbc_driver_databricks::DatabricksDatabase {
    let runtime = tokio::runtime::Handle::current();
    let mut driver = DatabricksDriver::with_runtime(runtime);

    driver
        .new_database_with_opts([
            (
                OptionDatabase::Uri,
                OptionValue::String(mock_server_uri.to_string()),
            ),
            (
                OptionDatabase::Other("databricks.warehouse_id".to_string()),
                OptionValue::String(warehouse_id.to_string()),
            ),
            (
                OptionDatabase::Other("databricks.token".to_string()),
                OptionValue::String(token.to_string()),
            ),
        ])
        .expect("Failed to create database")
}

/// Create a configured database with catalog and schema.
pub fn create_test_database_with_catalog(
    mock_server_uri: &str,
    warehouse_id: &str,
    token: &str,
    catalog: &str,
    schema: &str,
) -> adbc_driver_databricks::DatabricksDatabase {
    let runtime = tokio::runtime::Handle::current();
    let mut driver = DatabricksDriver::with_runtime(runtime);

    driver
        .new_database_with_opts([
            (
                OptionDatabase::Uri,
                OptionValue::String(mock_server_uri.to_string()),
            ),
            (
                OptionDatabase::Other("databricks.warehouse_id".to_string()),
                OptionValue::String(warehouse_id.to_string()),
            ),
            (
                OptionDatabase::Other("databricks.token".to_string()),
                OptionValue::String(token.to_string()),
            ),
            (
                OptionDatabase::Other("databricks.catalog".to_string()),
                OptionValue::String(catalog.to_string()),
            ),
            (
                OptionDatabase::Other("databricks.schema".to_string()),
                OptionValue::String(schema.to_string()),
            ),
        ])
        .expect("Failed to create database")
}

/// Create Arrow IPC data from a record batch and encode as base64.
pub fn create_arrow_ipc_base64(batch: &RecordBatch) -> String {
    let mut buffer = Cursor::new(Vec::new());
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
    }
    BASE64_STANDARD.encode(buffer.into_inner())
}

/// Create Arrow IPC bytes from a record batch.
pub fn create_arrow_ipc_bytes(batch: &RecordBatch) -> Vec<u8> {
    let mut buffer = Cursor::new(Vec::new());
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
    }
    buffer.into_inner()
}

/// Create a simple test batch with id and name columns.
pub fn create_simple_test_batch(ids: Vec<i32>, names: Vec<Option<&str>>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .unwrap()
}

/// Create a test batch with a single integer column.
pub fn create_int_batch(column_name: &str, values: Vec<i32>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        column_name,
        DataType::Int32,
        false,
    )]));

    RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap()
}

/// Create a test batch with a single string column.
pub fn create_string_batch(column_name: &str, values: Vec<&str>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        column_name,
        DataType::Utf8,
        false,
    )]));

    RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values))]).unwrap()
}

/// Create LZ4 compressed Arrow IPC data.
pub fn create_compressed_arrow_ipc(batch: &RecordBatch) -> Vec<u8> {
    use lz4_flex::frame::FrameEncoder;
    use std::io::Write;

    let arrow_data = create_arrow_ipc_bytes(batch);
    let mut encoder = FrameEncoder::new(Vec::new());
    encoder.write_all(&arrow_data).unwrap();
    encoder.finish().unwrap()
}

/// Build a JSON response for a successful inline query result.
pub fn build_inline_result_response(
    statement_id: &str,
    batch: &RecordBatch,
) -> serde_json::Value {
    let arrow_data = create_arrow_ipc_base64(batch);
    serde_json::json!({
        "statement_id": statement_id,
        "status": {
            "state": "SUCCEEDED"
        },
        "result": {
            "data_array": arrow_data,
            "row_count": batch.num_rows()
        }
    })
}

/// Build a JSON response for an external links query result.
pub fn build_external_links_response(
    statement_id: &str,
    chunk_urls: Vec<(&str, usize, usize)>, // (url, row_offset, row_count)
) -> serde_json::Value {
    let total_rows: usize = chunk_urls.iter().map(|(_, _, count)| count).sum();
    let external_links: Vec<serde_json::Value> = chunk_urls
        .iter()
        .enumerate()
        .map(|(i, (url, row_offset, row_count))| {
            serde_json::json!({
                "chunk_index": i,
                "row_offset": row_offset,
                "row_count": row_count,
                "byte_count": 1000,
                "external_link": url,
                "expiration": "2099-12-31T23:59:59Z"
            })
        })
        .collect();

    serde_json::json!({
        "statement_id": statement_id,
        "status": {
            "state": "SUCCEEDED"
        },
        "manifest": {
            "format": "ARROW_STREAM",
            "schema": {
                "columns": [
                    {"name": "id", "type_name": "INT", "type_text": "INT", "position": 0, "nullable": false}
                ]
            },
            "total_chunk_count": chunk_urls.len(),
            "total_row_count": total_rows
        },
        "result": {
            "external_links": external_links
        }
    })
}

/// Build a JSON response for a pending/running statement.
pub fn build_pending_response(statement_id: &str) -> serde_json::Value {
    serde_json::json!({
        "statement_id": statement_id,
        "status": {
            "state": "PENDING"
        }
    })
}

/// Build a JSON response for a running statement.
pub fn build_running_response(statement_id: &str) -> serde_json::Value {
    serde_json::json!({
        "statement_id": statement_id,
        "status": {
            "state": "RUNNING"
        }
    })
}

/// Build a JSON response for a failed statement.
pub fn build_failed_response(
    statement_id: &str,
    error_code: &str,
    error_message: &str,
) -> serde_json::Value {
    serde_json::json!({
        "statement_id": statement_id,
        "status": {
            "state": "FAILED",
            "error": {
                "error_code": error_code,
                "message": error_message
            }
        }
    })
}

/// Build a JSON response for a cancelled statement.
pub fn build_cancelled_response(statement_id: &str) -> serde_json::Value {
    serde_json::json!({
        "statement_id": statement_id,
        "status": {
            "state": "CANCELLED"
        }
    })
}

/// Build a JSON response for an execute_update operation.
pub fn build_update_response(statement_id: &str, row_count: i64) -> serde_json::Value {
    serde_json::json!({
        "statement_id": statement_id,
        "status": {
            "state": "SUCCEEDED"
        },
        "manifest": {
            "total_row_count": row_count,
            "total_chunk_count": 0
        }
    })
}
