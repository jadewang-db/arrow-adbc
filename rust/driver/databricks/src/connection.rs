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

//! Connection implementation for Databricks

use std::collections::HashSet;

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{InfoCode, ObjectDepth, OptionConnection, OptionValue};
use adbc_core::{Connection, Optionable};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, Schema, SchemaRef};

use crate::statement::DatabricksStatement;

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

/// Connection to a Databricks SQL Warehouse
pub struct DatabricksConnection;

impl DatabricksConnection {
    pub fn new() -> Self {
        Self
    }
}

impl Default for DatabricksConnection {
    fn default() -> Self {
        Self::new()
    }
}

impl Optionable for DatabricksConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, _key: Self::Option, _value: OptionValue) -> Result<()> {
        // Stub implementation for now - will be completed in work item 1.7
        Ok(())
    }

    fn get_option_string(&self, _key: Self::Option) -> Result<String> {
        Err(Error::with_message_and_status(
            "Connection options not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn get_option_bytes(&self, _key: Self::Option) -> Result<Vec<u8>> {
        Err(Error::with_message_and_status(
            "Connection options not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn get_option_int(&self, _key: Self::Option) -> Result<i64> {
        Err(Error::with_message_and_status(
            "Connection options not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn get_option_double(&self, _key: Self::Option) -> Result<f64> {
        Err(Error::with_message_and_status(
            "Connection options not yet implemented",
            Status::NotImplemented,
        ))
    }
}

impl Connection for DatabricksConnection {
    type StatementType = DatabricksStatement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        // Stub implementation - will be completed in work item 1.7
        Ok(DatabricksStatement::new())
    }

    fn cancel(&mut self) -> Result<()> {
        Err(Error::with_message_and_status(
            "Connection methods not yet implemented",
            Status::NotImplemented,
        ))
    }

    fn get_info(&self, _codes: Option<HashSet<InfoCode>>) -> Result<impl RecordBatchReader> {
        // Stub implementation - will be completed in work item 1.7
        Ok(EmptyBatchReader::new(std::sync::Arc::new(Schema::empty())))
    }

    fn get_objects(
        &self,
        _depth: ObjectDepth,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _table_type: Option<Vec<&str>>,
        _column_name: Option<&str>,
    ) -> Result<impl RecordBatchReader> {
        // Stub implementation - will be completed in work item 1.7
        Ok(EmptyBatchReader::new(std::sync::Arc::new(Schema::empty())))
    }

    fn get_table_schema(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: &str,
    ) -> Result<Schema> {
        // Stub implementation - will be completed in work item 1.7
        Ok(Schema::empty())
    }

    fn get_table_types(&self) -> Result<impl RecordBatchReader> {
        // Stub implementation - will be completed in work item 1.7
        Ok(EmptyBatchReader::new(std::sync::Arc::new(Schema::empty())))
    }

    fn commit(&mut self) -> Result<()> {
        Err(Error::with_message_and_status(
            "Transactions not supported by Databricks",
            Status::NotImplemented,
        ))
    }

    fn rollback(&mut self) -> Result<()> {
        Err(Error::with_message_and_status(
            "Transactions not supported by Databricks",
            Status::NotImplemented,
        ))
    }

    fn read_partition(&self, _partition: impl AsRef<[u8]>) -> Result<impl RecordBatchReader> {
        // Stub implementation - will be completed in work item 1.7
        Ok(EmptyBatchReader::new(std::sync::Arc::new(Schema::empty())))
    }

    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> Result<impl RecordBatchReader> {
        // Stub implementation - will be completed in work item 1.7
        Ok(EmptyBatchReader::new(std::sync::Arc::new(Schema::empty())))
    }

    fn get_statistic_names(&self) -> Result<impl RecordBatchReader> {
        // Stub implementation - will be completed in work item 1.7
        Ok(EmptyBatchReader::new(std::sync::Arc::new(Schema::empty())))
    }
}
