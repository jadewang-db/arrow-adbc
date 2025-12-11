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

//! DatabricksDriver implementation.

use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::Driver;

use crate::database::DatabricksDatabase;

/// Entry point for creating Databricks database connections.
///
/// The driver is responsible for creating new database instances.
/// Each database instance holds configuration and a shared Tokio runtime.
#[derive(Debug, Default)]
pub struct DatabricksDriver {
    /// Optional Tokio runtime handle to use.
    /// If None, a new runtime will be created for each database.
    handle: Option<tokio::runtime::Handle>,
}

impl DatabricksDriver {
    /// Create a new DatabricksDriver instance.
    pub fn new() -> Self {
        Self { handle: None }
    }

    /// Create a new DatabricksDriver with a custom Tokio runtime handle.
    ///
    /// This allows sharing a runtime across multiple databases.
    pub fn with_runtime(handle: tokio::runtime::Handle) -> Self {
        Self {
            handle: Some(handle),
        }
    }
}

impl Driver for DatabricksDriver {
    type DatabaseType = DatabricksDatabase;

    fn new_database(&mut self) -> adbc_core::error::Result<Self::DatabaseType> {
        Ok(DatabricksDatabase::new(self.handle.clone()))
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> adbc_core::error::Result<Self::DatabaseType> {
        let mut database = DatabricksDatabase::new(self.handle.clone());
        for (key, value) in opts {
            database.set_option(key, value)?;
        }
        Ok(database)
    }
}

// Import Optionable for set_option method
use adbc_core::Optionable;
