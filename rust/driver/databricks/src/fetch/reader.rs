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

use std::io::Cursor;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_schema::{ArrowError, Schema};

use crate::error::{Error, Result};

/// Arrow result reader that wraps record batches.
///
/// Implements `RecordBatchReader` to provide a standard Arrow interface
/// for consuming query results.
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

    /// Parse Arrow IPC stream data into record batches.
    pub fn parse_ipc_stream(data: &[u8]) -> Result<(Schema, Vec<RecordBatch>)> {
        let cursor = Cursor::new(data);
        let reader = StreamReader::try_new(cursor, None)
            .map_err(|e| Error::Arrow(e))?;

        let schema = reader.schema().as_ref().clone();
        let batches: std::result::Result<Vec<RecordBatch>, ArrowError> = reader.collect();
        let batches = batches.map_err(|e| Error::Arrow(e))?;

        Ok((schema, batches))
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
