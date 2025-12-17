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

/// Reader for Arrow IPC data from Databricks
pub struct ArrowResultReader {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    current_index: usize,
    affected_rows: Option<i64>,
}

impl ArrowResultReader {
    /// Create an empty reader
    ///
    /// This is a stub implementation that will be completed in work item 2.7.
    /// For now, it returns an empty reader with no batches.
    pub fn empty(schema: SchemaRef) -> Self {
        Self {
            schema,
            batches: Vec::new(),
            current_index: 0,
            affected_rows: None,
        }
    }

    /// Get the number of affected rows for DML operations
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
