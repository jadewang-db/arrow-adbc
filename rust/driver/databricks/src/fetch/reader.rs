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
use crate::error::Result;

/// Reader for Arrow IPC data from Databricks
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
    /// # Implementation Note
    ///
    /// The full IPC parsing implementation will be added in Sprint 2.8 (Statement Execute - Inline Path).
    /// For now, this returns an empty reader as a stub. The schema conversion functionality
    /// (manifest_to_arrow_schema) is fully implemented and tested.
    ///
    /// TODO(work-item-2.8): Implement full IPC stream parsing when integrating with statement execution.
    pub fn from_inline_data(schema: SchemaRef, _data: &[u8]) -> Result<Self> {
        // Stub implementation - will be completed in work item 2.8
        // when we integrate with actual Databricks API responses
        Ok(Self::empty(schema))
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
    fn test_from_inline_data_stub() {
        let schema = create_test_schema();
        let data = b"stub data";

        // Currently returns empty reader (stub implementation)
        let reader = ArrowResultReader::from_inline_data(schema.clone(), data).unwrap();
        assert_eq!(reader.schema(), schema);
        assert_eq!(reader.affected_rows(), None);

        let results: Vec<_> = reader.collect();
        assert_eq!(results.len(), 0);
    }

    // Note: Full IPC parsing implementation and tests will be added in work item 2.8
    // when integrating with actual Databricks API responses. The stub implementation
    // above ensures the API signature is correct.

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
