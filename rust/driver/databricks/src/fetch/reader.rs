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

//! Arrow result reader implementation.
//!
//! This module handles reading and parsing Arrow IPC data from
//! both inline results and external links.

use std::io::Cursor;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use base64::prelude::*;

use crate::client::{ColumnInfo, ResultData, StatementResponse};

/// Arrow result reader for Databricks query results.
///
/// Handles both inline (small) results and external link (large) results.
/// For inline results, parses base64-encoded Arrow IPC stream data.
#[derive(Debug)]
pub struct ArrowResultReader {
    /// The schema of the result set.
    schema: SchemaRef,
    /// The current batch index.
    current_index: usize,
    /// All record batches (for inline results).
    batches: Vec<RecordBatch>,
}

impl ArrowResultReader {
    /// Create a reader from a statement response with inline results.
    ///
    /// # Arguments
    ///
    /// * `response` - The statement response containing inline data_array
    ///
    /// # Returns
    ///
    /// A new ArrowResultReader with the parsed batches.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The response does not contain inline results
    /// - The Arrow IPC data cannot be parsed
    pub fn from_inline_response(response: &StatementResponse) -> Result<Self, ArrowError> {
        let result = response.result.as_ref().ok_or_else(|| {
            ArrowError::InvalidArgumentError("Response does not contain result data".to_string())
        })?;

        Self::from_result_data(result, response)
    }

    /// Create a reader from result data.
    ///
    /// # Arguments
    ///
    /// * `result` - The result data containing data_array
    /// * `response` - The statement response for schema information
    pub fn from_result_data(
        result: &ResultData,
        response: &StatementResponse,
    ) -> Result<Self, ArrowError> {
        let data_array = result.data_array.as_ref().ok_or_else(|| {
            ArrowError::InvalidArgumentError("Result does not contain inline data".to_string())
        })?;

        // data_array is a single base64-encoded string
        if data_array.is_empty() {
            // Empty result - return empty schema
            let schema = Self::schema_from_manifest(response)?;
            return Ok(Self {
                schema,
                current_index: 0,
                batches: vec![],
            });
        }

        // Parse batches from the single data_array string
        let (schema, batches) = Self::parse_data_array_string(data_array, response)?;

        Ok(Self {
            schema,
            current_index: 0,
            batches,
        })
    }

    /// Parse the data_array field (a single base64 string) into record batches.
    ///
    /// The data_array is a single base64-encoded Arrow IPC stream.
    fn parse_data_array_string(
        data_array: &str,
        response: &StatementResponse,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), ArrowError> {
        // Decode base64
        let decoded = BASE64_STANDARD.decode(data_array).map_err(|e| {
            ArrowError::InvalidArgumentError(format!("Failed to decode base64 data: {}", e))
        })?;

        // Parse Arrow IPC stream
        let cursor = Cursor::new(decoded);
        let reader = StreamReader::try_new(cursor, None)?;
        let schema = reader.schema();

        let mut all_batches = Vec::new();

        // Read all batches from this chunk
        for batch_result in reader {
            let batch = batch_result?;
            all_batches.push(batch);
        }

        if all_batches.is_empty() {
            let schema = Self::schema_from_manifest(response)?;
            Ok((schema, vec![]))
        } else {
            Ok((schema, all_batches))
        }
    }

    /// Extract schema from the manifest if available.
    fn schema_from_manifest(response: &StatementResponse) -> Result<SchemaRef, ArrowError> {
        if let Some(manifest) = &response.manifest {
            if let Some(manifest_schema) = &manifest.schema {
                let fields: Vec<Field> = manifest_schema
                    .columns
                    .iter()
                    .map(|col| column_info_to_field(col))
                    .collect();
                return Ok(Arc::new(Schema::new(fields)));
            }
        }

        // Return empty schema as fallback
        Ok(Arc::new(Schema::empty()))
    }

    /// Create an empty reader with the given schema.
    pub fn empty(schema: SchemaRef) -> Self {
        Self {
            schema,
            current_index: 0,
            batches: vec![],
        }
    }

    /// Get the total number of rows across all batches.
    pub fn total_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    /// Get the number of batches.
    pub fn num_batches(&self) -> usize {
        self.batches.len()
    }
}

impl Iterator for ArrowResultReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_index < self.batches.len() {
            let batch = self.batches[self.current_index].clone();
            self.current_index += 1;
            Some(Ok(batch))
        } else {
            None
        }
    }
}

impl arrow_array::RecordBatchReader for ArrowResultReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Convert a ColumnInfo from the manifest to an Arrow Field.
fn column_info_to_field(col: &ColumnInfo) -> Field {
    let data_type = spark_type_to_arrow(&col.type_name);
    Field::new(&col.name, data_type, col.nullable)
}

/// Convert Spark SQL type name to Arrow DataType.
///
/// Maps common Spark SQL types to their Arrow equivalents.
fn spark_type_to_arrow(spark_type: &str) -> DataType {
    // Normalize the type name for matching
    let type_upper = spark_type.to_uppercase();

    match type_upper.as_str() {
        // Boolean
        "BOOLEAN" | "BOOL" => DataType::Boolean,

        // Integer types
        "BYTE" | "TINYINT" => DataType::Int8,
        "SHORT" | "SMALLINT" => DataType::Int16,
        "INT" | "INTEGER" => DataType::Int32,
        "LONG" | "BIGINT" => DataType::Int64,

        // Floating point
        "FLOAT" | "REAL" => DataType::Float32,
        "DOUBLE" => DataType::Float64,

        // Decimal - use a reasonable default precision/scale
        "DECIMAL" => DataType::Decimal128(38, 18),

        // String types
        "STRING" | "VARCHAR" | "CHAR" | "TEXT" => DataType::Utf8,

        // Binary
        "BINARY" | "VARBINARY" => DataType::Binary,

        // Date and time
        "DATE" => DataType::Date32,
        "TIMESTAMP" | "TIMESTAMP_NTZ" => DataType::Timestamp(
            arrow_schema::TimeUnit::Microsecond,
            None,
        ),
        "TIMESTAMP_LTZ" => DataType::Timestamp(
            arrow_schema::TimeUnit::Microsecond,
            Some("UTC".into()),
        ),

        // Interval (map to duration for now)
        "INTERVAL" => DataType::Duration(arrow_schema::TimeUnit::Microsecond),

        // For complex types and unknown types, default to string
        // This allows the driver to handle unexpected types gracefully
        _ => {
            // Check for parameterized types like DECIMAL(10,2), ARRAY<INT>, etc.
            if type_upper.starts_with("DECIMAL") {
                // Parse DECIMAL(precision, scale)
                if let Some(params) = extract_type_params(&type_upper) {
                    if let Ok((precision, scale)) = parse_decimal_params(&params) {
                        return DataType::Decimal128(precision, scale);
                    }
                }
                DataType::Decimal128(38, 18)
            } else if type_upper.starts_with("VARCHAR") || type_upper.starts_with("CHAR") {
                DataType::Utf8
            } else if type_upper.starts_with("ARRAY") {
                // For arrays, we'd need to parse the element type
                // For now, just use a list of strings
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
            } else if type_upper.starts_with("MAP") {
                // For maps, default to string keys and values
                let key_field = Field::new("key", DataType::Utf8, false);
                let value_field = Field::new("value", DataType::Utf8, true);
                let fields: Vec<Arc<Field>> = vec![Arc::new(key_field), Arc::new(value_field)];
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(fields.into()),
                        false,
                    )),
                    false,
                )
            } else if type_upper.starts_with("STRUCT") {
                // For structs, we'd need to parse the field definitions
                // For now, just use an empty struct
                let empty_fields: Vec<Arc<Field>> = vec![];
                DataType::Struct(empty_fields.into())
            } else {
                // Unknown type - default to string for safety
                DataType::Utf8
            }
        }
    }
}

/// Extract type parameters from a parameterized type like DECIMAL(10,2).
fn extract_type_params(type_str: &str) -> Option<String> {
    let start = type_str.find('(')?;
    let end = type_str.rfind(')')?;
    if start < end {
        Some(type_str[start + 1..end].to_string())
    } else {
        None
    }
}

/// Parse DECIMAL precision and scale from parameters string.
fn parse_decimal_params(params: &str) -> Result<(u8, i8), ()> {
    let parts: Vec<&str> = params.split(',').map(|s| s.trim()).collect();
    if parts.len() == 2 {
        let precision: u8 = parts[0].parse().map_err(|_| ())?;
        let scale: i8 = parts[1].parse().map_err(|_| ())?;
        Ok((precision, scale))
    } else if parts.len() == 1 {
        let precision: u8 = parts[0].parse().map_err(|_| ())?;
        Ok((precision, 0))
    } else {
        Err(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{StatementState, StatementStatus};
    use arrow_array::{Int32Array, RecordBatchReader, StringArray};
    use arrow_ipc::writer::StreamWriter;
    use std::io::Cursor;

    /// Create a test record batch.
    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
            ],
        )
        .unwrap()
    }

    fn create_status_succeeded() -> StatementStatus {
        StatementStatus {
            state: StatementState::Succeeded,
            error: None,
        }
    }

    /// Encode a record batch to base64 Arrow IPC stream.
    fn encode_batch_to_base64(batch: &RecordBatch) -> String {
        let mut buffer = Cursor::new(Vec::new());
        {
            let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema()).unwrap();
            writer.write(batch).unwrap();
            writer.finish().unwrap();
        }
        BASE64_STANDARD.encode(buffer.into_inner())
    }

    #[test]
    fn test_parse_single_chunk() {
        let batch = create_test_batch();
        let encoded = encode_batch_to_base64(&batch);

        let response = StatementResponse {
            statement_id: "test".to_string(),
            status: create_status_succeeded(),
            manifest: None,
            result: Some(ResultData {
                data_array: Some(encoded),
                external_links: None,
                row_count: None,
                byte_count: None,
            }),
        };

        let reader = ArrowResultReader::from_inline_response(&response).unwrap();

        assert_eq!(reader.num_batches(), 1);
        assert_eq!(reader.total_rows(), 3);
        assert_eq!(reader.schema().fields().len(), 2);
    }

    #[test]
    fn test_empty_result() {
        let response = StatementResponse {
            statement_id: "test".to_string(),
            status: create_status_succeeded(),
            manifest: None,
            result: Some(ResultData {
                data_array: Some(String::new()),
                external_links: None,
                row_count: None,
                byte_count: None,
            }),
        };

        let reader = ArrowResultReader::from_inline_response(&response).unwrap();

        assert_eq!(reader.num_batches(), 0);
        assert_eq!(reader.total_rows(), 0);
    }

    #[test]
    fn test_reader_iterator() {
        let batch = create_test_batch();
        let encoded = encode_batch_to_base64(&batch);

        let response = StatementResponse {
            statement_id: "test".to_string(),
            status: create_status_succeeded(),
            manifest: None,
            result: Some(ResultData {
                data_array: Some(encoded),
                external_links: None,
                row_count: None,
                byte_count: None,
            }),
        };

        let mut reader = ArrowResultReader::from_inline_response(&response).unwrap();

        // First call should return the batch
        let first = reader.next();
        assert!(first.is_some());
        assert_eq!(first.unwrap().unwrap().num_rows(), 3);

        // Second call should return None
        assert!(reader.next().is_none());
    }

    #[test]
    fn test_reader_schema() {
        let batch = create_test_batch();
        let encoded = encode_batch_to_base64(&batch);

        let response = StatementResponse {
            statement_id: "test".to_string(),
            status: create_status_succeeded(),
            manifest: None,
            result: Some(ResultData {
                data_array: Some(encoded),
                external_links: None,
                row_count: None,
                byte_count: None,
            }),
        };

        let reader = ArrowResultReader::from_inline_response(&response).unwrap();
        let schema = arrow_array::RecordBatchReader::schema(&reader);

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
    }

    #[test]
    fn test_spark_type_to_arrow_basic() {
        assert_eq!(spark_type_to_arrow("BOOLEAN"), DataType::Boolean);
        assert_eq!(spark_type_to_arrow("INT"), DataType::Int32);
        assert_eq!(spark_type_to_arrow("BIGINT"), DataType::Int64);
        assert_eq!(spark_type_to_arrow("DOUBLE"), DataType::Float64);
        assert_eq!(spark_type_to_arrow("STRING"), DataType::Utf8);
        assert_eq!(spark_type_to_arrow("DATE"), DataType::Date32);
    }

    #[test]
    fn test_spark_type_to_arrow_case_insensitive() {
        assert_eq!(spark_type_to_arrow("int"), DataType::Int32);
        assert_eq!(spark_type_to_arrow("Int"), DataType::Int32);
        assert_eq!(spark_type_to_arrow("STRING"), DataType::Utf8);
        assert_eq!(spark_type_to_arrow("string"), DataType::Utf8);
    }

    #[test]
    fn test_spark_type_decimal_with_params() {
        match spark_type_to_arrow("DECIMAL(10,2)") {
            DataType::Decimal128(precision, scale) => {
                assert_eq!(precision, 10);
                assert_eq!(scale, 2);
            }
            _ => panic!("Expected Decimal128 type"),
        }
    }

    #[test]
    fn test_spark_type_unknown_defaults_to_string() {
        assert_eq!(spark_type_to_arrow("UNKNOWN_TYPE"), DataType::Utf8);
    }

    #[test]
    fn test_empty_reader() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
        ]));

        let mut reader = ArrowResultReader::empty(schema.clone());

        assert_eq!(reader.num_batches(), 0);
        assert_eq!(reader.total_rows(), 0);
        assert!(reader.next().is_none());
        assert_eq!(arrow_array::RecordBatchReader::schema(&reader), schema);
    }

    #[test]
    fn test_no_result_data_error() {
        let response = StatementResponse {
            statement_id: "test".to_string(),
            status: create_status_succeeded(),
            manifest: None,
            result: None,
        };

        let result = ArrowResultReader::from_inline_response(&response);
        assert!(result.is_err());
    }

    #[test]
    fn test_no_data_array_error() {
        let response = StatementResponse {
            statement_id: "test".to_string(),
            status: create_status_succeeded(),
            manifest: None,
            result: Some(ResultData {
                data_array: None,
                external_links: None,
                row_count: None,
                byte_count: None,
            }),
        };

        let result = ArrowResultReader::from_inline_response(&response);
        assert!(result.is_err());
    }
}
