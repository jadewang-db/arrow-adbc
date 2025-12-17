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

//! Statement implementation for Databricks

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{OptionStatement, OptionValue};
use adbc_core::{Optionable, PartitionedResult, Statement};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, Schema, SchemaRef};

/// SQL statement handle for executing queries
pub struct DatabricksStatement;

impl DatabricksStatement {
    pub fn new() -> Self {
        Self
    }
}

impl Default for DatabricksStatement {
    fn default() -> Self {
        Self::new()
    }
}

/// Empty reader for stub implementations
struct EmptyBatchReader {
    schema: SchemaRef,
}

impl EmptyBatchReader {
    fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

impl Iterator for EmptyBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

impl RecordBatchReader for EmptyBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Optionable for DatabricksStatement {
    type Option = OptionStatement;

    fn set_option(&mut self, _key: Self::Option, _value: OptionValue) -> Result<()> {
        // Stub implementation - will be completed in later work items
        Ok(())
    }

    fn get_option_string(&self, _key: Self::Option) -> Result<String> {
        Err(Error::with_message_and_status(
            "Statement options not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn get_option_bytes(&self, _key: Self::Option) -> Result<Vec<u8>> {
        Err(Error::with_message_and_status(
            "Statement options not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn get_option_int(&self, _key: Self::Option) -> Result<i64> {
        Err(Error::with_message_and_status(
            "Statement options not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn get_option_double(&self, _key: Self::Option) -> Result<f64> {
        Err(Error::with_message_and_status(
            "Statement options not yet implemented",
            Status::NotImplemented,
        ))
    }
}

impl Statement for DatabricksStatement {
    fn bind(&mut self, _batch: RecordBatch) -> Result<()> {
        Err(Error::with_message_and_status(
            "Parameter binding not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn bind_stream(&mut self, _reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        Err(Error::with_message_and_status(
            "Stream binding not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn cancel(&mut self) -> Result<()> {
        Err(Error::with_message_and_status(
            "Statement cancellation not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn execute(&mut self) -> Result<impl RecordBatchReader> {
        // Stub implementation - will be completed in later work items
        Ok(EmptyBatchReader::new(std::sync::Arc::new(Schema::empty())))
    }

    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        Err(Error::with_message_and_status(
            "Partitioned execution not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        Err(Error::with_message_and_status(
            "Schema execution not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        Err(Error::with_message_and_status(
            "Update execution not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn get_parameter_schema(&self) -> Result<Schema> {
        Err(Error::with_message_and_status(
            "Parameter schema not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn prepare(&mut self) -> Result<()> {
        Err(Error::with_message_and_status(
            "Statement preparation not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn set_sql_query(&mut self, _query: impl AsRef<str>) -> Result<()> {
        // Stub implementation - will be completed in later work items
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> Result<()> {
        Err(Error::with_message_and_status(
            "Substrait plans not supported",
            Status::NotImplemented,
        ))
    }
}
