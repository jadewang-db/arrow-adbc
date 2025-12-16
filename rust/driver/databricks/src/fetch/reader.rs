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

//! Arrow result reader for parsing IPC streams.
//!
//! This module provides utilities for parsing Arrow IPC stream data
//! returned from Databricks.
//!
//! # Overview
//!
//! When Databricks returns query results with INLINE disposition and ARROW_STREAM format,
//! the data is returned as a base64-encoded Arrow IPC stream in the response body.
//! This module handles:
//!
//! 1. Decoding the base64 data
//! 2. Parsing the Arrow IPC stream format
//! 3. Returning the data as RecordBatches through the standard RecordBatchReader trait
//!
//! # Example
//!
//! ```ignore
//! use adbc_databricks::fetch::ArrowResultReader;
//!
//! // Create a reader from inline base64-encoded Arrow data
//! let reader = ArrowResultReader::from_inline_data(
//!     schema,
//!     Some("base64_encoded_arrow_ipc_data"),
//! )?;
//!
//! // Iterate over the record batches
//! for batch_result in reader {
//!     let batch = batch_result?;
//!     println!("Got {} rows", batch.num_rows());
//! }
//! ```

use std::io::Cursor;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_schema::{ArrowError, Schema, SchemaRef};
use base64::{engine::general_purpose::STANDARD, Engine as _};

use crate::error::{Error, Result};

/// Arrow result reader that wraps record batches.
///
/// Implements `RecordBatchReader` to provide a standard Arrow interface
/// for consuming query results.
#[derive(Debug)]
pub struct ArrowResultReader {
    /// Schema of the result set.
    schema: Arc<Schema>,
    /// Record batches.
    batches: Vec<RecordBatch>,
    /// Current position.
    position: usize,
}

impl ArrowResultReader {
    /// Create a new result reader from record batches.
    pub fn new(schema: Schema, batches: Vec<RecordBatch>) -> Self {
        Self {
            schema: Arc::new(schema),
            batches,
            position: 0,
        }
    }

    /// Create an empty result reader with the given schema.
    pub fn empty(schema: Schema) -> Self {
        Self {
            schema: Arc::new(schema),
            batches: Vec::new(),
            position: 0,
        }
    }

    /// Create a reader from inline Arrow IPC data (base64 encoded).
    ///
    /// This method handles the INLINE disposition format from Databricks SEA API,
    /// where results are returned as base64-encoded Arrow IPC stream data.
    ///
    /// # Arguments
    ///
    /// * `fallback_schema` - Schema to use if the IPC data is empty or missing
    /// * `data` - Optional base64-encoded Arrow IPC stream data
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing an `ArrowResultReader` that can iterate over
    /// the record batches.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The base64 data cannot be decoded
    /// - The Arrow IPC stream is malformed
    ///
    /// # Example
    ///
    /// ```ignore
    /// let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
    /// let reader = ArrowResultReader::from_inline_data(schema, Some(base64_data))?;
    /// for batch in reader {
    ///     let batch = batch?;
    ///     println!("Got {} rows", batch.num_rows());
    /// }
    /// ```
    pub fn from_inline_data(fallback_schema: Schema, data: Option<&str>) -> Result<Self> {
        let batches = match data {
            Some(base64_data) if !base64_data.is_empty() => {
                // Decode base64
                let bytes = STANDARD.decode(base64_data).map_err(|e| {
                    Error::Config(format!("Failed to decode base64 Arrow data: {}", e))
                })?;

                if bytes.is_empty() {
                    return Ok(Self::empty(fallback_schema));
                }

                // Parse Arrow IPC stream
                let cursor = Cursor::new(bytes);
                let reader = StreamReader::try_new(cursor, None).map_err(Error::Arrow)?;

                // Get schema from IPC data
                let ipc_schema = reader.schema();

                // Collect all batches
                let batches: std::result::Result<Vec<RecordBatch>, ArrowError> = reader.collect();
                let batches = batches.map_err(Error::Arrow)?;

                // Return reader with schema from IPC data (more accurate than fallback)
                return Ok(Self {
                    schema: ipc_schema,
                    batches,
                    position: 0,
                });
            }
            _ => Vec::new(),
        };

        Ok(Self {
            schema: Arc::new(fallback_schema),
            batches,
            position: 0,
        })
    }

    /// Create a reader from raw Arrow IPC bytes.
    ///
    /// Unlike `from_inline_data`, this method takes raw bytes instead of base64-encoded data.
    /// This is useful when fetching data from external links where the data is not base64 encoded.
    ///
    /// # Arguments
    ///
    /// * `fallback_schema` - Schema to use if the IPC data is empty
    /// * `data` - Raw Arrow IPC stream bytes
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing an `ArrowResultReader`.
    pub fn from_ipc_bytes(fallback_schema: Schema, data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            return Ok(Self::empty(fallback_schema));
        }

        let cursor = Cursor::new(data);
        let reader = StreamReader::try_new(cursor, None).map_err(Error::Arrow)?;

        let ipc_schema = reader.schema();
        let batches: std::result::Result<Vec<RecordBatch>, ArrowError> = reader.collect();
        let batches = batches.map_err(Error::Arrow)?;

        Ok(Self {
            schema: ipc_schema,
            batches,
            position: 0,
        })
    }

    /// Parse Arrow IPC stream data into record batches.
    pub fn parse_ipc_stream(data: &[u8]) -> Result<(Schema, Vec<RecordBatch>)> {
        let cursor = Cursor::new(data);
        let reader = StreamReader::try_new(cursor, None).map_err(Error::Arrow)?;

        let schema = reader.schema().as_ref().clone();
        let batches: std::result::Result<Vec<RecordBatch>, ArrowError> = reader.collect();
        let batches = batches.map_err(Error::Arrow)?;

        Ok((schema, batches))
    }

    /// Get the schema of the result set.
    pub fn schema_ref(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Get the number of batches in the result set.
    pub fn num_batches(&self) -> usize {
        self.batches.len()
    }

    /// Check if the result set is empty.
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }
}

impl Iterator for ArrowResultReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position < self.batches.len() {
            let batch = self.batches[self.position].clone();
            self.position += 1;
            Some(Ok(batch))
        } else {
            None
        }
    }
}

impl arrow_array::RecordBatchReader for ArrowResultReader {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatchReader, StringArray};
    use arrow_ipc::writer::StreamWriter;
    use arrow_schema::{DataType, Field};

    /// Helper function to create a test schema with Int32 and Utf8 columns.
    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ])
    }

    /// Helper function to create a test RecordBatch.
    fn create_test_batch(schema: &Schema, ids: &[i32], names: &[&str]) -> RecordBatch {
        let id_array = Int32Array::from(ids.to_vec());
        let name_array = StringArray::from(names.to_vec());

        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .expect("Failed to create test batch")
    }

    /// Helper function to encode a RecordBatch to base64 Arrow IPC stream.
    fn encode_batch_to_base64(schema: &Schema, batch: &RecordBatch) -> String {
        let mut buffer = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buffer, schema).expect("Failed to create writer");
            writer.write(batch).expect("Failed to write batch");
            writer.finish().expect("Failed to finish writing");
        }
        STANDARD.encode(&buffer)
    }

    // ==========================================================================
    // Test: Empty Reader
    // ==========================================================================

    #[test]
    fn test_empty_reader() {
        let schema = create_test_schema();
        let reader = ArrowResultReader::empty(schema.clone());

        assert_eq!(reader.schema().fields().len(), 2);
        assert_eq!(reader.num_batches(), 0);
        assert!(reader.is_empty());

        // Iterator should return nothing
        let batches: Vec<_> = reader.collect();
        assert!(batches.is_empty());
    }

    // ==========================================================================
    // Test: New Reader with Batches
    // ==========================================================================

    #[test]
    fn test_new_reader_with_batches() {
        let schema = create_test_schema();
        let batch = create_test_batch(&schema, &[1, 2, 3], &["Alice", "Bob", "Charlie"]);

        let reader = ArrowResultReader::new(schema.clone(), vec![batch.clone()]);

        assert_eq!(reader.num_batches(), 1);
        assert!(!reader.is_empty());

        let batches: Vec<_> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    // ==========================================================================
    // Test: Iterator
    // ==========================================================================

    #[test]
    fn test_iterator_multiple_batches() {
        let schema = create_test_schema();
        let batch1 = create_test_batch(&schema, &[1, 2], &["Alice", "Bob"]);
        let batch2 = create_test_batch(&schema, &[3, 4, 5], &["Charlie", "David", "Eve"]);

        let reader = ArrowResultReader::new(schema, vec![batch1, batch2]);

        let batches: Vec<_> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[1].num_rows(), 3);
    }

    // ==========================================================================
    // Test: from_inline_data with None
    // ==========================================================================

    #[test]
    fn test_from_inline_data_none() {
        let schema = create_test_schema();
        let reader = ArrowResultReader::from_inline_data(schema.clone(), None).unwrap();

        assert_eq!(reader.schema().fields().len(), 2);
        assert!(reader.is_empty());
    }

    // ==========================================================================
    // Test: from_inline_data with Empty String
    // ==========================================================================

    #[test]
    fn test_from_inline_data_empty_string() {
        let schema = create_test_schema();
        let reader = ArrowResultReader::from_inline_data(schema.clone(), Some("")).unwrap();

        assert_eq!(reader.schema().fields().len(), 2);
        assert!(reader.is_empty());
    }

    // ==========================================================================
    // Test: from_inline_data with Valid Data
    // ==========================================================================

    #[test]
    fn test_from_inline_data_valid() {
        let schema = create_test_schema();
        let batch = create_test_batch(&schema, &[1, 2, 3], &["Alice", "Bob", "Charlie"]);

        // Encode to base64
        let base64_data = encode_batch_to_base64(&schema, &batch);

        // Create reader from inline data
        let reader = ArrowResultReader::from_inline_data(schema.clone(), Some(&base64_data)).unwrap();

        // Verify schema
        let reader_schema = reader.schema();
        assert_eq!(reader_schema.fields().len(), 2);
        assert_eq!(reader_schema.field(0).name(), "id");
        assert_eq!(reader_schema.field(1).name(), "name");

        // Verify data
        let batches: Vec<_> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);

        // Verify column values
        let id_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Expected Int32Array");
        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(1), 2);
        assert_eq!(id_col.value(2), 3);

        let name_col = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Expected StringArray");
        assert_eq!(name_col.value(0), "Alice");
        assert_eq!(name_col.value(1), "Bob");
        assert_eq!(name_col.value(2), "Charlie");
    }

    // ==========================================================================
    // Test: from_inline_data with Invalid Base64
    // ==========================================================================

    #[test]
    fn test_from_inline_data_invalid_base64() {
        let schema = create_test_schema();
        let result = ArrowResultReader::from_inline_data(schema, Some("not_valid_base64!!!"));

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("base64"));
    }

    // ==========================================================================
    // Test: from_ipc_bytes
    // ==========================================================================

    #[test]
    fn test_from_ipc_bytes_valid() {
        let schema = create_test_schema();
        let batch = create_test_batch(&schema, &[10, 20], &["Test", "Data"]);

        // Encode to raw bytes (not base64)
        let mut buffer = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buffer, &schema).expect("Failed to create writer");
            writer.write(&batch).expect("Failed to write batch");
            writer.finish().expect("Failed to finish writing");
        }

        // Create reader from raw bytes
        let reader = ArrowResultReader::from_ipc_bytes(schema.clone(), &buffer).unwrap();

        let batches: Vec<_> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[test]
    fn test_from_ipc_bytes_empty() {
        let schema = create_test_schema();
        let reader = ArrowResultReader::from_ipc_bytes(schema.clone(), &[]).unwrap();

        assert!(reader.is_empty());
    }

    // ==========================================================================
    // Test: parse_ipc_stream
    // ==========================================================================

    #[test]
    fn test_parse_ipc_stream() {
        let schema = create_test_schema();
        let batch = create_test_batch(&schema, &[100, 200], &["Foo", "Bar"]);

        // Encode to raw bytes
        let mut buffer = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buffer, &schema).expect("Failed to create writer");
            writer.write(&batch).expect("Failed to write batch");
            writer.finish().expect("Failed to finish writing");
        }

        // Parse IPC stream
        let (parsed_schema, batches) = ArrowResultReader::parse_ipc_stream(&buffer).unwrap();

        assert_eq!(parsed_schema.fields().len(), 2);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
    }

    // ==========================================================================
    // Test: RecordBatchReader trait
    // ==========================================================================

    #[test]
    fn test_record_batch_reader_trait() {
        let schema = create_test_schema();
        let batch = create_test_batch(&schema, &[1], &["Test"]);
        let base64_data = encode_batch_to_base64(&schema, &batch);

        let reader = ArrowResultReader::from_inline_data(schema.clone(), Some(&base64_data)).unwrap();

        // Verify RecordBatchReader trait
        let reader_schema = RecordBatchReader::schema(&reader);
        assert_eq!(reader_schema.fields().len(), 2);
    }

    // ==========================================================================
    // Test: Multiple Batches via IPC
    // ==========================================================================

    #[test]
    fn test_multiple_batches_in_ipc_stream() {
        let schema = create_test_schema();
        let batch1 = create_test_batch(&schema, &[1, 2], &["A", "B"]);
        let batch2 = create_test_batch(&schema, &[3, 4, 5], &["C", "D", "E"]);

        // Encode multiple batches to a single IPC stream
        let mut buffer = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buffer, &schema).expect("Failed to create writer");
            writer.write(&batch1).expect("Failed to write batch1");
            writer.write(&batch2).expect("Failed to write batch2");
            writer.finish().expect("Failed to finish writing");
        }

        let base64_data = STANDARD.encode(&buffer);

        // Create reader
        let reader = ArrowResultReader::from_inline_data(schema, Some(&base64_data)).unwrap();

        let batches: Vec<_> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[1].num_rows(), 3);
    }
}
