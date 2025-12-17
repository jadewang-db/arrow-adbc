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

//! Arrow result reader implementation

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use arrow_ipc::reader::StreamReader;
use crate::error::Result;

/// Reader for Arrow IPC data from Databricks
#[derive(Debug)]
pub struct ArrowResultReader {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    current_index: usize,
    affected_rows: Option<i64>,
}

impl ArrowResultReader {
    /// Create reader from inline Arrow IPC stream data
    ///
    /// Parses Arrow IPC stream format data and creates a reader with all batches.
    /// The data should be in Arrow IPC stream format (not file format).
    ///
    /// # Arguments
    /// * `schema` - Expected Arrow schema for validation
    /// * `data` - Arrow IPC stream bytes (may be LZ4-compressed)
    pub fn from_inline_data(schema: SchemaRef, data: &[u8]) -> Result<Self> {
        use std::io::Cursor;

        if data.is_empty() {
            // Empty data means no rows
            return Ok(Self::empty(schema));
        }

        // Try to decompress if the data appears to be LZ4-compressed
        let decompressed_data = if Self::is_lz4_compressed(data) {
            super::decompress::decompress_lz4(data)?
        } else {
            data.to_vec()
        };

        // Parse Arrow IPC stream
        let cursor = Cursor::new(decompressed_data);
        let mut stream_reader = StreamReader::try_new(cursor, None)
            .map_err(|e| crate::error::Error::ArrowIpc(format!("Failed to parse Arrow IPC stream: {}", e)))?;

        // Validate schema matches expected schema
        let actual_schema = stream_reader.schema();
        if !Self::schemas_compatible(&schema, &actual_schema) {
            return Err(crate::error::Error::ArrowIpc(format!(
                "Schema mismatch: expected {:?}, got {:?}",
                schema, actual_schema
            )));
        }

        // Collect all batches from the stream
        let mut batches = Vec::new();
        while let Some(batch_result) = stream_reader.next() {
            let batch = batch_result
                .map_err(|e| crate::error::Error::ArrowIpc(format!("Failed to read batch: {}", e)))?;
            batches.push(batch);
        }

        Ok(Self {
            schema,
            batches,
            current_index: 0,
            affected_rows: None,
        })
    }

    /// Check if data appears to be LZ4-compressed
    ///
    /// LZ4 frame format starts with magic number: 0x184D2204
    fn is_lz4_compressed(data: &[u8]) -> bool {
        data.len() >= 4 && data[0..4] == [0x04, 0x22, 0x4D, 0x18]
    }

    /// Check if two schemas are compatible (same field names and types)
    ///
    /// This allows for minor differences like metadata that don't affect data compatibility.
    fn schemas_compatible(expected: &SchemaRef, actual: &SchemaRef) -> bool {
        if expected.fields().len() != actual.fields().len() {
            return false;
        }

        for (exp_field, act_field) in expected.fields().iter().zip(actual.fields().iter()) {
            if exp_field.name() != act_field.name() || exp_field.data_type() != act_field.data_type() {
                return false;
            }
        }

        true
    }

    /// Create an empty reader (typically for DDL statements with no result data)
    pub fn empty(schema: SchemaRef) -> Self {
        Self {
            schema,
            batches: Vec::new(),
            current_index: 0,
            affected_rows: None,
        }
    }

    /// Set the number of affected rows for DML operations
    ///
    /// This is a builder method that consumes self and returns a new instance.
    pub fn with_affected_rows(mut self, rows: i64) -> Self {
        self.affected_rows = Some(rows);
        self
    }

    /// Get the number of affected rows for DML operations
    ///
    /// Returns None for queries that don't affect rows (SELECT, DDL).
    /// Returns Some(count) for DML operations (INSERT, UPDATE, DELETE).
    pub fn affected_rows(&self) -> Option<i64> {
        self.affected_rows
    }
}

impl Iterator for ArrowResultReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

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

impl RecordBatchReader for ArrowResultReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn create_test_batch(schema: SchemaRef, start: i32) -> RecordBatch {
        let id_array = Int32Array::from(vec![start, start + 1, start + 2]);
        let name_array = StringArray::from(vec![
            Some(format!("name{}", start)),
            Some(format!("name{}", start + 1)),
            Some(format!("name{}", start + 2)),
        ]);

        RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)]).unwrap()
    }

    #[test]
    fn test_empty_reader() {
        let schema = create_test_schema();
        let reader = ArrowResultReader::empty(schema.clone());

        assert_eq!(reader.schema(), schema);
        assert_eq!(reader.affected_rows(), None);

        let batches: Vec<_> = reader.collect();
        assert_eq!(batches.len(), 0);
    }

    #[test]
    fn test_reader_iteration_single_batch() {
        let schema = create_test_schema();
        let batch = create_test_batch(schema.clone(), 1);

        let mut reader = ArrowResultReader {
            schema: schema.clone(),
            batches: vec![batch.clone()],
            current_index: 0,
            affected_rows: None,
        };

        assert_eq!(reader.schema(), schema);

        // First call should return the batch
        let result = reader.next();
        assert!(result.is_some());
        let result_batch = result.unwrap().unwrap();
        assert_eq!(result_batch.num_rows(), 3);
        assert_eq!(result_batch.num_columns(), 2);

        // Second call should return None
        let result = reader.next();
        assert!(result.is_none());
    }

    #[test]
    fn test_reader_iteration_multiple_batches() {
        let schema = create_test_schema();
        let batch1 = create_test_batch(schema.clone(), 1);
        let batch2 = create_test_batch(schema.clone(), 4);
        let batch3 = create_test_batch(schema.clone(), 7);

        let reader = ArrowResultReader {
            schema: schema.clone(),
            batches: vec![batch1, batch2, batch3],
            current_index: 0,
            affected_rows: None,
        };

        let results: Vec<_> = reader.collect();
        assert_eq!(results.len(), 3);

        // All results should be Ok
        for result in &results {
            assert!(result.is_ok());
        }

        // Verify content of first batch
        let first_batch = results[0].as_ref().unwrap();
        assert_eq!(first_batch.num_rows(), 3);
        assert_eq!(first_batch.num_columns(), 2);
    }

    #[test]
    fn test_with_affected_rows() {
        let schema = create_test_schema();
        let reader = ArrowResultReader::empty(schema).with_affected_rows(42);

        assert_eq!(reader.affected_rows(), Some(42));
    }

    #[test]
    fn test_with_affected_rows_builder_pattern() {
        let schema = create_test_schema();
        let batch = create_test_batch(schema.clone(), 1);

        let reader = ArrowResultReader {
            schema,
            batches: vec![batch],
            current_index: 0,
            affected_rows: None,
        }
        .with_affected_rows(100);

        assert_eq!(reader.affected_rows(), Some(100));

        // Verify iterator still works
        let results: Vec<_> = reader.collect();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_from_inline_data_empty() {
        let schema = create_test_schema();
        let data = b"";

        // Empty data should return empty reader
        let reader = ArrowResultReader::from_inline_data(schema.clone(), data).unwrap();
        assert_eq!(reader.schema(), schema);
        assert_eq!(reader.affected_rows(), None);

        let results: Vec<_> = reader.collect();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_from_inline_data_with_ipc() {
        use arrow_ipc::writer::StreamWriter;
        use std::io::Cursor;

        let schema = create_test_schema();
        let batch = create_test_batch(schema.clone(), 1);

        // Create Arrow IPC stream data
        let mut buffer = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buffer, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        // Parse it back
        let reader = ArrowResultReader::from_inline_data(schema.clone(), &buffer).unwrap();
        assert_eq!(reader.schema(), schema);

        let results: Vec<_> = reader.collect();
        assert_eq!(results.len(), 1);

        let result_batch = results[0].as_ref().unwrap();
        assert_eq!(result_batch.num_rows(), 3);
        assert_eq!(result_batch.num_columns(), 2);
    }

    #[test]
    fn test_from_inline_data_invalid_ipc() {
        let schema = create_test_schema();
        let data = b"invalid ipc data";

        // Invalid IPC data should return an error
        let result = ArrowResultReader::from_inline_data(schema.clone(), data);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Failed to parse Arrow IPC stream"));
    }

    #[test]
    fn test_record_batch_reader_trait() {
        let schema = create_test_schema();
        let batch = create_test_batch(schema.clone(), 1);

        let reader: Box<dyn RecordBatchReader> = Box::new(ArrowResultReader {
            schema: schema.clone(),
            batches: vec![batch],
            current_index: 0,
            affected_rows: None,
        });

        // Test schema() method from RecordBatchReader trait
        assert_eq!(reader.schema(), schema);

        // Test iteration through trait
        let results: Vec<_> = reader.collect();
        assert_eq!(results.len(), 1);
    }

}
