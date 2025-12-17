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

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

/// Reader for Arrow IPC data from Databricks
pub struct ArrowResultReader {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    current_index: usize,
    affected_rows: Option<i64>,
}

impl ArrowResultReader {
    /// Create an empty reader
    pub fn empty(_schema: SchemaRef) -> Self {
        todo!("ArrowResultReader::empty implementation in work item 2.7")
    }

    /// Get the number of affected rows for DML operations
    pub fn affected_rows(&self) -> Option<i64> {
        self.affected_rows
    }
}
