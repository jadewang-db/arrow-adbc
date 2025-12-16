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

//! Databricks connection implementation.
//!
//! This module provides the connection type that represents an active session
//! with a Databricks SQL Warehouse.
//!
//! # Session Lifecycle
//!
//! - Session is created eagerly on `new_connection()`
//! - Session is kept alive automatically (statements refresh the idle timeout)
//! - Session is terminated on Connection drop
//!
//! # Example
//!
//! ```ignore
//! use adbc_core::{Driver, Database};
//! use adbc_databricks::DatabricksDriver;
//!
//! let mut driver = DatabricksDriver::new();
//! let db = driver.new_database_with_opts([
//!     (OptionDatabase::Uri, OptionValue::String("https://...".into())),
//!     (OptionDatabase::Password, OptionValue::String("dapi...".into())),
//!     (OptionDatabase::Other("databricks.warehouse_id".into()), OptionValue::String("...".into())),
//! ])?;
//!
//! // Connection is created with an active session
//! let conn = db.new_connection()?;
//! assert!(conn.session_id().is_some());
//!
//! // Create statements from the connection
//! let mut stmt = conn.new_statement()?;
//! stmt.set_sql_query("SELECT 1")?;
//! let reader = stmt.execute()?;
//! ```

use std::collections::HashSet;
use std::sync::Arc;

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{InfoCode, ObjectDepth, OptionConnection, OptionValue};
use adbc_core::{Connection, Optionable};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::Schema;
use tokio::runtime::Runtime;

use crate::client::{SeaClient, SeaClientConfig, StatementResponse};
use crate::fetch::{ArrowResultReader, ChunkFetcher};
use crate::options::DatabaseConfig;
use crate::runtime::{block_on_async, block_on_async_or_spawn, block_on_async_simple};
use crate::session::SessionManager;
use crate::statement::DatabricksStatement;

/// Databricks connection.
///
/// Represents an active session with a Databricks SQL Warehouse.
///
/// # Session Lifecycle
///
/// - Session created eagerly on `new_connection()` via [`SessionManager`]
/// - Session kept alive automatically (statements refresh the idle timeout)
/// - Session terminated on Connection drop
///
/// # Thread Safety
///
/// The connection holds an `Arc<SessionManager>` which is thread-safe. Multiple
/// statements can be created from the same connection and used concurrently.
#[derive(Debug)]
pub struct DatabricksConnection {
    /// SEA client for API calls.
    client: Arc<SeaClient>,
    /// Session manager for session lifecycle.
    session_manager: Arc<SessionManager>,
    /// Shared Tokio runtime.
    runtime: Arc<Runtime>,
    /// Current catalog.
    current_catalog: Option<String>,
    /// Current schema.
    current_schema: Option<String>,
}

impl DatabricksConnection {
    /// Create a new connection.
    ///
    /// Creates a connection and eagerly establishes a session with the SQL Warehouse.
    ///
    /// # Arguments
    ///
    /// * `config` - Database configuration including host, token, warehouse_id
    /// * `runtime` - Shared Tokio runtime for async operations
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - SeaClient creation fails
    /// - Session creation fails (network error, authentication failure, etc.)
    pub(crate) fn new(config: Arc<DatabaseConfig>, runtime: Arc<Runtime>) -> Result<Self> {
        // Create SEA client configuration
        let client_config = SeaClientConfig::new(
            &config.host,
            &config.token,
            &config.warehouse_id,
        )
        .with_connect_timeout(config.http_config.connect_timeout)
        .with_read_timeout(config.http_config.read_timeout);

        // Create SEA client
        let client = SeaClient::new(client_config).map_err(|e| {
            Error::with_message_and_status(
                format!("Failed to create SEA client: {}", e),
                Status::Internal,
            )
        })?;
        let client = Arc::new(client);

        // Create session manager with default catalog/schema from config
        let session_manager = Arc::new(SessionManager::new(
            client.clone(),
            config.default_catalog.clone(),
            config.default_schema.clone(),
        ));

        // Create session eagerly using the async/sync bridge
        // Note: This will fail if called from within an async context
        let session_id = block_on_async(&runtime, session_manager.get_session_id()).map_err(|e| {
            Error::with_message_and_status(
                format!("Failed to create session: {}", e),
                Status::IO,
            )
        })?;

        // Session created successfully
        let _ = session_id; // Suppress unused variable warning

        Ok(Self {
            client,
            session_manager,
            runtime,
            current_catalog: config.default_catalog.clone(),
            current_schema: config.default_schema.clone(),
        })
    }

    /// Get the session ID.
    ///
    /// Returns the session ID if a session is active.
    ///
    /// # Note
    ///
    /// This method uses `block_on` internally and should not be called from
    /// within an async context. If called from async code, it will return `None`.
    pub fn session_id(&self) -> Option<String> {
        // Use the async/sync bridge to get the session ID synchronously
        // Since we create the session eagerly in new(), is_active() will return cached state
        //
        // If we're in an async context, we return None rather than panicking.
        // This is a defensive approach for the session_id() accessor.
        let is_active = block_on_async_simple(&self.runtime, self.session_manager.is_active())?;

        if is_active {
            block_on_async(&self.runtime, self.session_manager.get_session_id()).ok()
        } else {
            None
        }
    }

    /// Get the SEA client.
    ///
    /// Returns a reference to the underlying SEA client for direct API access.
    pub fn client(&self) -> &Arc<SeaClient> {
        &self.client
    }

    /// Get the session manager.
    ///
    /// Returns a reference to the session manager for session lifecycle operations.
    pub fn session_manager(&self) -> &Arc<SessionManager> {
        &self.session_manager
    }

    /// Get the Tokio runtime.
    ///
    /// Returns a reference to the shared Tokio runtime.
    pub fn runtime(&self) -> &Arc<Runtime> {
        &self.runtime
    }

    /// Convert a DESCRIBE TABLE result into an Arrow Schema.
    ///
    /// The DESCRIBE TABLE output in Databricks has columns:
    /// - col_name: column name
    /// - data_type: Spark SQL type (e.g., "int", "string", "decimal(10,2)")
    /// - comment: optional column comment
    ///
    /// This method parses the result and maps Spark SQL types to Arrow types.
    fn describe_result_to_schema(&self, response: &StatementResponse) -> Result<Schema> {

        // Get the result data from the response
        // For DESCRIBE TABLE, results should be returned as inline Arrow data or external links
        let result = match &response.result {
            Some(r) => r,
            None => {
                // Empty result means table has no columns (shouldn't happen)
                return Ok(Schema::empty());
            }
        };

        // Try to get inline Arrow data first
        if let Some(ref chunk) = result.chunk {
            if !chunk.is_empty() {
                // Get schema for the reader
                let reader_schema = self.build_describe_manifest_schema(response)?;

                // Parse inline Arrow data
                let reader =
                    ArrowResultReader::from_inline_data(reader_schema.clone(), Some(chunk))
                        .map_err(|e| {
                            Error::with_message_and_status(
                                format!("Failed to parse DESCRIBE result: {}", e),
                                Status::Internal,
                            )
                        })?;

                return self.extract_schema_from_describe_batches(reader);
            }
        }

        // Try external links if no inline data
        if let Some(ref external_links) = result.external_links {
            if !external_links.is_empty() {
                let reader_schema = self.build_describe_manifest_schema(response)?;

                // Use ChunkFetcher to download the Arrow IPC data from external links
                let fetcher = ChunkFetcher::new(self.client.clone(), 1).map_err(|e| {
                    Error::with_message_and_status(
                        format!("Failed to create chunk fetcher: {}", e),
                        Status::Internal,
                    )
                })?;

                let manifest = response.manifest.as_ref();
                let statement_id = &response.statement_id;
                let links = external_links.clone();
                let runtime = self.runtime.clone();

                // Fetch chunks using the runtime
                let batches = block_on_async(&runtime, async move {
                    fetcher
                        .fetch_chunks(
                            statement_id,
                            manifest.unwrap_or(&Default::default()),
                            &links,
                        )
                        .await
                })
                .map_err(|e| {
                    Error::with_message_and_status(
                        format!("Failed to fetch DESCRIBE result chunks: {}", e),
                        Status::IO,
                    )
                })?;

                let reader = ArrowResultReader::new(reader_schema, batches);
                return self.extract_schema_from_describe_batches(reader);
            }
        }

        // No data - return empty schema
        Ok(Schema::empty())
    }

    /// Build schema for the DESCRIBE TABLE result from the manifest.
    fn build_describe_manifest_schema(&self, response: &StatementResponse) -> Result<Schema> {
        use arrow_schema::{DataType, Field};

        let manifest = match &response.manifest {
            Some(m) => m,
            None => {
                // Default DESCRIBE TABLE schema if no manifest
                return Ok(Schema::new(vec![
                    Field::new("col_name", DataType::Utf8, false),
                    Field::new("data_type", DataType::Utf8, false),
                    Field::new("comment", DataType::Utf8, true),
                ]));
            }
        };

        let result_schema = match &manifest.schema {
            Some(s) => s,
            None => {
                return Ok(Schema::new(vec![
                    Field::new("col_name", DataType::Utf8, false),
                    Field::new("data_type", DataType::Utf8, false),
                    Field::new("comment", DataType::Utf8, true),
                ]));
            }
        };

        let columns = match &result_schema.columns {
            Some(c) => c,
            None => {
                return Ok(Schema::new(vec![
                    Field::new("col_name", DataType::Utf8, false),
                    Field::new("data_type", DataType::Utf8, false),
                    Field::new("comment", DataType::Utf8, true),
                ]));
            }
        };

        // Build schema from manifest columns
        let fields: Vec<Field> = columns
            .iter()
            .map(|col| Field::new(&col.name, DataType::Utf8, true))
            .collect();

        Ok(Schema::new(fields))
    }

    /// Extract the table schema from DESCRIBE TABLE result batches.
    ///
    /// DESCRIBE TABLE returns rows like:
    /// | col_name | data_type | comment |
    /// | id       | int       | null    |
    /// | name     | string    | null    |
    fn extract_schema_from_describe_batches(
        &self,
        reader: impl RecordBatchReader,
    ) -> Result<Schema> {
        use arrow_array::cast::AsArray;
        use arrow_schema::Field;

        let mut fields = Vec::new();

        for batch_result in reader {
            let batch = batch_result.map_err(|e| {
                Error::with_message_and_status(
                    format!("Failed to read DESCRIBE result batch: {}", e),
                    Status::Internal,
                )
            })?;

            // Find col_name and data_type column indices
            let schema = batch.schema();
            let col_name_idx = schema
                .fields()
                .iter()
                .position(|f| f.name() == "col_name")
                .ok_or_else(|| {
                    Error::with_message_and_status(
                        "DESCRIBE result missing 'col_name' column",
                        Status::Internal,
                    )
                })?;

            let data_type_idx = schema
                .fields()
                .iter()
                .position(|f| f.name() == "data_type")
                .ok_or_else(|| {
                    Error::with_message_and_status(
                        "DESCRIBE result missing 'data_type' column",
                        Status::Internal,
                    )
                })?;

            // Get the columns as string arrays
            let col_names = batch.column(col_name_idx).as_string::<i32>();
            let data_types = batch.column(data_type_idx).as_string::<i32>();

            // Process each row
            for row_idx in 0..batch.num_rows() {
                let col_name = col_names.value(row_idx);
                let type_str = data_types.value(row_idx);

                // Skip partition info separator and partition columns
                // DESCRIBE TABLE returns column info followed by partition info
                // The separator looks like: "# Partition Information", "# col_name", etc.
                if col_name.starts_with('#') || col_name.is_empty() {
                    continue;
                }

                // Map Spark SQL type to Arrow type
                let arrow_type = Self::spark_type_to_arrow(type_str);
                fields.push(Field::new(col_name, arrow_type, true));
            }
        }

        Ok(Schema::new(fields))
    }

    /// Map a Spark SQL type string to an Arrow DataType.
    ///
    /// Handles types like:
    /// - Simple types: int, bigint, string, boolean, double, float, etc.
    /// - Parameterized types: decimal(10,2), varchar(100), char(10)
    /// - Complex types: array<int>, map<string,int>, struct<a:int,b:string>
    pub(crate) fn spark_type_to_arrow(spark_type: &str) -> arrow_schema::DataType {
        use arrow_schema::{DataType, TimeUnit};

        let type_lower = spark_type.to_lowercase();
        let type_str = type_lower.trim();

        // Handle simple types first
        match type_str {
            "boolean" | "bool" => DataType::Boolean,
            "tinyint" | "byte" => DataType::Int8,
            "smallint" | "short" => DataType::Int16,
            "int" | "integer" => DataType::Int32,
            "bigint" | "long" => DataType::Int64,
            "float" | "real" => DataType::Float32,
            "double" => DataType::Float64,
            "string" => DataType::Utf8,
            "binary" => DataType::Binary,
            "date" => DataType::Date32,
            "timestamp" | "timestamp_ltz" => DataType::Timestamp(TimeUnit::Microsecond, None),
            "timestamp_ntz" => DataType::Timestamp(TimeUnit::Microsecond, None),
            "void" | "null" => DataType::Null,
            _ => {
                // Handle parameterized types
                if type_str.starts_with("decimal") {
                    // Parse decimal(precision, scale)
                    Self::parse_decimal_type(type_str)
                } else if type_str.starts_with("varchar") || type_str.starts_with("char") {
                    // VARCHAR and CHAR map to Utf8
                    DataType::Utf8
                } else if type_str.starts_with("array<") {
                    // Parse array<element_type>
                    Self::parse_array_type(type_str)
                } else if type_str.starts_with("map<") {
                    // Parse map<key_type, value_type>
                    Self::parse_map_type(type_str)
                } else if type_str.starts_with("struct<") {
                    // Parse struct<field1:type1, field2:type2>
                    Self::parse_struct_type(type_str)
                } else {
                    // Default to string for unknown types
                    DataType::Utf8
                }
            }
        }
    }

    /// Parse a decimal type string like "decimal(10,2)" into Decimal128.
    fn parse_decimal_type(type_str: &str) -> arrow_schema::DataType {
        use arrow_schema::DataType;

        // Default precision and scale
        let mut precision: u8 = 38;
        let mut scale: i8 = 18;

        // Try to parse decimal(precision, scale)
        if let Some(params) = type_str.strip_prefix("decimal(") {
            if let Some(params) = params.strip_suffix(')') {
                let parts: Vec<&str> = params.split(',').collect();
                if parts.len() >= 1 {
                    if let Ok(p) = parts[0].trim().parse::<u8>() {
                        precision = p;
                    }
                }
                if parts.len() >= 2 {
                    if let Ok(s) = parts[1].trim().parse::<i8>() {
                        scale = s;
                    }
                }
            }
        }

        DataType::Decimal128(precision, scale)
    }

    /// Parse an array type string like "array<int>" into a List type.
    fn parse_array_type(type_str: &str) -> arrow_schema::DataType {
        use arrow_schema::{DataType, Field};

        if let Some(inner) = type_str.strip_prefix("array<") {
            if let Some(inner) = inner.strip_suffix('>') {
                let element_type = Self::spark_type_to_arrow(inner.trim());
                return DataType::List(Arc::new(Field::new("item", element_type, true)));
            }
        }

        // Fallback
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
    }

    /// Parse a map type string like "map<string,int>" into a Map type.
    fn parse_map_type(type_str: &str) -> arrow_schema::DataType {
        use arrow_schema::{DataType, Field, Fields};

        if let Some(inner) = type_str.strip_prefix("map<") {
            if let Some(inner) = inner.strip_suffix('>') {
                // Split on first comma (simple approach - doesn't handle nested types perfectly)
                if let Some(comma_pos) = Self::find_top_level_comma(inner) {
                    let key_type_str = &inner[..comma_pos];
                    let value_type_str = &inner[comma_pos + 1..];

                    let key_type = Self::spark_type_to_arrow(key_type_str.trim());
                    let value_type = Self::spark_type_to_arrow(value_type_str.trim());

                    let struct_field = Field::new(
                        "entries",
                        DataType::Struct(Fields::from(vec![
                            Field::new("key", key_type, false),
                            Field::new("value", value_type, true),
                        ])),
                        false,
                    );

                    return DataType::Map(Arc::new(struct_field), false);
                }
            }
        }

        // Fallback
        let struct_field = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, true),
            ])),
            false,
        );
        DataType::Map(Arc::new(struct_field), false)
    }

    /// Parse a struct type string like "struct<a:int,b:string>" into a Struct type.
    fn parse_struct_type(type_str: &str) -> arrow_schema::DataType {
        use arrow_schema::{DataType, Field, Fields};

        if let Some(inner) = type_str.strip_prefix("struct<") {
            if let Some(inner) = inner.strip_suffix('>') {
                let mut fields = Vec::new();

                // Split fields on top-level commas
                let field_strs = Self::split_on_top_level_commas(inner);

                for field_str in field_strs {
                    let field_str = field_str.trim();
                    // Each field is "name:type"
                    if let Some(colon_pos) = field_str.find(':') {
                        let name = &field_str[..colon_pos];
                        let type_str = &field_str[colon_pos + 1..];
                        let data_type = Self::spark_type_to_arrow(type_str.trim());
                        fields.push(Field::new(name.trim(), data_type, true));
                    }
                }

                if !fields.is_empty() {
                    return DataType::Struct(Fields::from(fields));
                }
            }
        }

        // Fallback - empty struct
        DataType::Struct(Fields::empty())
    }

    /// Find the position of a top-level comma (not inside angle brackets).
    fn find_top_level_comma(s: &str) -> Option<usize> {
        let mut depth = 0;
        for (i, c) in s.char_indices() {
            match c {
                '<' => depth += 1,
                '>' => depth -= 1,
                ',' if depth == 0 => return Some(i),
                _ => {}
            }
        }
        None
    }

    /// Split a string on top-level commas (not inside angle brackets).
    fn split_on_top_level_commas(s: &str) -> Vec<&str> {
        let mut result = Vec::new();
        let mut depth = 0;
        let mut start = 0;

        for (i, c) in s.char_indices() {
            match c {
                '<' => depth += 1,
                '>' => depth -= 1,
                ',' if depth == 0 => {
                    result.push(&s[start..i]);
                    start = i + 1;
                }
                _ => {}
            }
        }

        // Don't forget the last segment
        if start < s.len() {
            result.push(&s[start..]);
        }

        result
    }
}

impl Drop for DatabricksConnection {
    fn drop(&mut self) {
        // Terminate session on drop using the async/sync bridge
        // We ignore errors here since we're in drop and can't propagate them
        //
        // The block_on_async_or_spawn helper handles the runtime context:
        // - If outside a tokio runtime: blocks synchronously
        // - If inside a tokio runtime: spawns a detached task
        let session_manager = self.session_manager.clone();

        if let Some(result) = block_on_async_or_spawn(&self.runtime, async move {
            session_manager.terminate().await
        }) {
            // We blocked synchronously - log any errors
            if let Err(_e) = result {
                #[cfg(debug_assertions)]
                eprintln!("Failed to terminate session on connection drop: {}", _e);
            }
        }
        // If None, the task was spawned asynchronously - errors logged by the helper
    }
}

/// Empty record batch reader for placeholder implementations.
struct EmptyRecordBatchReader {
    schema: Arc<Schema>,
}

impl EmptyRecordBatchReader {
    fn new(schema: Schema) -> Self {
        Self {
            schema: Arc::new(schema),
        }
    }
}

impl Iterator for EmptyRecordBatchReader {
    type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

impl RecordBatchReader for EmptyRecordBatchReader {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

impl Optionable for DatabricksConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match key {
            OptionConnection::AutoCommit => {
                // AutoCommit must be true for Databricks (no transaction support)
                let autocommit = match value {
                    OptionValue::String(s) => s,
                    OptionValue::Int(i) => i.to_string(),
                    _ => {
                        return Err(Error::with_message_and_status(
                            "autocommit must be a string or int",
                            Status::InvalidArguments,
                        ));
                    }
                };
                // Accept "true", "1", or empty string (which means enable)
                if autocommit != "true" && autocommit != "1" && !autocommit.is_empty() {
                    return Err(Error::with_message_and_status(
                        "Databricks requires autocommit=true; transactions are not supported",
                        Status::InvalidArguments,
                    ));
                }
                Ok(())
            }
            OptionConnection::CurrentCatalog => {
                if let OptionValue::String(s) = value {
                    self.current_catalog = Some(s);
                    Ok(())
                } else {
                    Err(Error::with_message_and_status(
                        "current_catalog must be a string",
                        Status::InvalidArguments,
                    ))
                }
            }
            OptionConnection::CurrentSchema => {
                if let OptionValue::String(s) = value {
                    self.current_schema = Some(s);
                    Ok(())
                } else {
                    Err(Error::with_message_and_status(
                        "current_schema must be a string",
                        Status::InvalidArguments,
                    ))
                }
            }
            OptionConnection::ReadOnly => {
                // Databricks connections do not support read-only mode
                let readonly = match value {
                    OptionValue::String(s) => s,
                    OptionValue::Int(i) => i.to_string(),
                    _ => {
                        return Err(Error::with_message_and_status(
                            "read_only must be a string or int",
                            Status::InvalidArguments,
                        ));
                    }
                };
                // Only allow setting to "false" or "0"
                if readonly != "false" && readonly != "0" && !readonly.is_empty() {
                    return Err(Error::with_message_and_status(
                        "Databricks does not support read-only mode",
                        Status::InvalidArguments,
                    ));
                }
                Ok(())
            }
            OptionConnection::IsolationLevel => {
                // Databricks doesn't support transaction isolation levels
                Err(Error::with_message_and_status(
                    "Databricks does not support transaction isolation levels",
                    Status::NotImplemented,
                ))
            }
            OptionConnection::Other(ref key) => Err(Error::with_message_and_status(
                format!("Unknown connection option: {}", key),
                Status::NotImplemented,
            )),
            // Handle any future OptionConnection variants
            _ => Err(Error::with_message_and_status(
                format!("Unsupported connection option: {:?}", key),
                Status::NotImplemented,
            )),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        Err(Error::with_message_and_status(
            format!("Option {:?} is not a byte array", key),
            Status::NotImplemented,
        ))
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        Err(Error::with_message_and_status(
            format!("Option {:?} is not a double", key),
            Status::NotImplemented,
        ))
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        match key {
            OptionConnection::AutoCommit => Ok(1), // true = 1
            OptionConnection::ReadOnly => Ok(0),   // false = 0
            _ => Err(Error::with_message_and_status(
                format!("Option {:?} is not an integer", key),
                Status::NotImplemented,
            )),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match key {
            OptionConnection::AutoCommit => Ok("true".to_string()),
            OptionConnection::CurrentCatalog => self.current_catalog.clone().ok_or_else(|| {
                Error::with_message_and_status("Current catalog not set", Status::NotFound)
            }),
            OptionConnection::CurrentSchema => self.current_schema.clone().ok_or_else(|| {
                Error::with_message_and_status("Current schema not set", Status::NotFound)
            }),
            OptionConnection::ReadOnly => Ok("false".to_string()),
            OptionConnection::IsolationLevel => Err(Error::with_message_and_status(
                "Databricks does not support transaction isolation levels",
                Status::NotImplemented,
            )),
            OptionConnection::Other(ref key) => Err(Error::with_message_and_status(
                format!("Unknown connection option: {}", key),
                Status::NotFound,
            )),
            // Handle any future OptionConnection variants
            _ => Err(Error::with_message_and_status(
                format!("Unsupported connection option: {:?}", key),
                Status::NotFound,
            )),
        }
    }
}

impl Connection for DatabricksConnection {
    type StatementType = DatabricksStatement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        DatabricksStatement::new(
            self.client.clone(),
            self.session_manager.clone(),
            self.runtime.clone(),
        )
    }

    fn cancel(&mut self) -> Result<()> {
        // Connection-level cancel is a no-op for now
        // Statement-level cancel is handled in DatabricksStatement
        Ok(())
    }

    fn get_info(
        &self,
        _codes: Option<HashSet<InfoCode>>,
    ) -> Result<impl RecordBatchReader + Send> {
        // TODO: Implement driver info retrieval
        Ok(EmptyRecordBatchReader::new(Schema::empty()))
    }

    fn get_objects(
        &self,
        _depth: ObjectDepth,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _table_type: Option<Vec<&str>>,
        _column_name: Option<&str>,
    ) -> Result<impl RecordBatchReader + Send> {
        // TODO: Implement metadata retrieval
        Ok(EmptyRecordBatchReader::new(Schema::empty()))
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema> {
        // Build fully qualified table name
        // Use provided values or fall back to connection defaults, then to hardcoded defaults
        let catalog_name = catalog
            .or(self.current_catalog.as_deref())
            .unwrap_or("main");
        let schema_name = db_schema
            .or(self.current_schema.as_deref())
            .unwrap_or("default");

        // Construct the DESCRIBE TABLE query
        // Use backtick quoting to handle special characters in identifiers
        let sql = format!(
            "DESCRIBE TABLE `{}`.`{}`.`{}`",
            catalog_name, schema_name, table_name
        );

        // Execute the DESCRIBE query using a statement
        // We need to create a mutable copy of self for new_statement, but since
        // Connection::get_table_schema takes &self, we work around this by using
        // the internal components directly
        let session_manager = self.session_manager.clone();
        let client = self.client.clone();
        let runtime = self.runtime.clone();

        // Get session ID
        let session_id =
            block_on_async(&runtime, async move { session_manager.get_session_id().await })
                .map_err(|e| {
                    Error::with_message_and_status(
                        format!("Failed to get session: {}", e),
                        Status::IO,
                    )
                })?;

        // Execute the DESCRIBE statement
        let response = block_on_async(&runtime, async move {
            client
                .execute_and_wait(&session_id, &sql, None, None, None)
                .await
        })
        .map_err(|e| {
            Error::with_message_and_status(
                format!("Failed to execute DESCRIBE TABLE: {}", e),
                Status::IO,
            )
        })?;

        // Parse the DESCRIBE result into an Arrow Schema
        self.describe_result_to_schema(&response)
    }

    fn get_table_types(&self) -> Result<impl RecordBatchReader + Send> {
        // TODO: Implement table types retrieval
        Ok(EmptyRecordBatchReader::new(Schema::empty()))
    }

    fn get_statistic_names(&self) -> Result<impl RecordBatchReader + Send> {
        // TODO: Implement statistic names retrieval
        Ok(EmptyRecordBatchReader::new(Schema::empty()))
    }

    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> Result<impl RecordBatchReader + Send> {
        // TODO: Implement statistics retrieval
        Ok(EmptyRecordBatchReader::new(Schema::empty()))
    }

    fn commit(&mut self) -> Result<()> {
        // Databricks does not support transactions
        Err(Error::with_message_and_status(
            "Transactions are not supported by Databricks",
            Status::NotImplemented,
        ))
    }

    fn rollback(&mut self) -> Result<()> {
        // Databricks does not support transactions
        Err(Error::with_message_and_status(
            "Transactions are not supported by Databricks",
            Status::NotImplemented,
        ))
    }

    fn read_partition(
        &self,
        _partition: impl AsRef<[u8]>,
    ) -> Result<impl RecordBatchReader + Send> {
        // TODO: Implement partition reading
        Ok(EmptyRecordBatchReader::new(Schema::empty()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use wiremock::matchers::{method, path, path_regex};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Create a test database config.
    fn create_test_config(host: &str, warehouse_id: &str) -> Arc<DatabaseConfig> {
        Arc::new(DatabaseConfig {
            host: host.to_string(),
            warehouse_id: warehouse_id.to_string(),
            token: "test_token".to_string(),
            default_catalog: Some("main".to_string()),
            default_schema: Some("default".to_string()),
            http_config: crate::options::HttpConfig::default(),
            fetch_concurrency: 8,
        })
    }

    /// Create a multi-threaded runtime that supports nested block_on operations.
    fn create_mt_runtime() -> Arc<Runtime> {
        Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("Failed to create multi-threaded runtime"),
        )
    }

    // ============================================================================
    // Connection Creation Tests
    // ============================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_connection_creates_session_eagerly() {
        let mock_server = MockServer::start().await;

        // Set up mock for session creation
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "test-session-abc123"
            })))
            .mount(&mock_server)
            .await;

        // Set up mock for session deletion (for drop)
        Mock::given(method("DELETE"))
            .and(path_regex(r"/api/2\.0/sql/sessions/.*"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri(), "test_warehouse");
        let runtime = create_mt_runtime();

        // Use spawn_blocking for all connection operations to avoid nested runtime issues
        // All connection usage (including session_id() which uses block_on) must be inside spawn_blocking
        let (is_ok, session_id) = tokio::task::spawn_blocking(move || {
            let conn = DatabricksConnection::new(config, runtime);
            match conn {
                Ok(conn) => (true, conn.session_id()),
                Err(_) => (false, None),
            }
            // Connection is dropped here within spawn_blocking
        })
        .await
        .expect("spawn_blocking failed");

        assert!(is_ok, "Connection creation should succeed");
        assert_eq!(
            session_id,
            Some("test-session-abc123".to_string()),
            "Session ID should be set"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_connection_fails_when_session_creation_fails() {
        let mock_server = MockServer::start().await;

        // Set up mock to return an error
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
                "error_code": "UNAUTHENTICATED",
                "message": "Invalid token"
            })))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri(), "test_warehouse");
        let runtime = create_mt_runtime();

        let conn = tokio::task::spawn_blocking(move || {
            DatabricksConnection::new(config, runtime)
        })
        .await
        .expect("spawn_blocking failed");

        assert!(conn.is_err(), "Connection should fail when session creation fails");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_connection_inherits_catalog_schema_from_config() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "catalog-schema-session"
            })))
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path_regex(r"/api/2\.0/sql/sessions/.*"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = Arc::new(DatabaseConfig {
            host: mock_server.uri(),
            warehouse_id: "test_warehouse".to_string(),
            token: "test_token".to_string(),
            default_catalog: Some("my_catalog".to_string()),
            default_schema: Some("my_schema".to_string()),
            http_config: crate::options::HttpConfig::default(),
            fetch_concurrency: 8,
        });
        let runtime = create_mt_runtime();

        // All connection operations must happen inside spawn_blocking
        let (catalog, schema) = tokio::task::spawn_blocking(move || {
            let conn = DatabricksConnection::new(config, runtime).unwrap();
            let result = (conn.current_catalog.clone(), conn.current_schema.clone());
            // Connection is dropped here within spawn_blocking
            result
        })
        .await
        .expect("spawn_blocking failed");

        assert_eq!(catalog, Some("my_catalog".to_string()));
        assert_eq!(schema, Some("my_schema".to_string()));
    }

    // ============================================================================
    // Session Lifecycle Tests
    // ============================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_connection_terminates_session_on_drop() {
        let mock_server = MockServer::start().await;

        let delete_count = Arc::new(AtomicUsize::new(0));
        let delete_count_clone = delete_count.clone();

        // Set up mock for session creation
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "drop-test-session"
            })))
            .mount(&mock_server)
            .await;

        // Set up mock for session deletion
        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/drop-test-session"))
            .respond_with(move |_: &wiremock::Request| {
                delete_count_clone.fetch_add(1, Ordering::SeqCst);
                ResponseTemplate::new(200)
            })
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri(), "test_warehouse");
        let runtime = create_mt_runtime();

        tokio::task::spawn_blocking(move || {
            let conn = DatabricksConnection::new(config, runtime).unwrap();
            assert_eq!(conn.session_id(), Some("drop-test-session".to_string()));
            // Connection is dropped here
        })
        .await
        .expect("spawn_blocking failed");

        // Give the spawned cleanup task time to complete
        // Since drop spawns an async task when inside a runtime, we need to yield
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Verify session was terminated
        assert_eq!(
            delete_count.load(Ordering::SeqCst),
            1,
            "Session should be terminated on drop"
        );
    }

    // ============================================================================
    // Optionable Tests
    // ============================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_connection_set_current_catalog() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "option-test-session"
            })))
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path_regex(r"/api/2\.0/sql/sessions/.*"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri(), "test_warehouse");
        let runtime = create_mt_runtime();

        let result = tokio::task::spawn_blocking(move || {
            let mut conn = DatabricksConnection::new(config, runtime).unwrap();

            conn.set_option(
                OptionConnection::CurrentCatalog,
                OptionValue::String("new_catalog".into()),
            )
            .unwrap();

            let catalog = conn
                .get_option_string(OptionConnection::CurrentCatalog)
                .unwrap();
            catalog
        })
        .await
        .expect("spawn_blocking failed");

        assert_eq!(result, "new_catalog");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_connection_set_current_schema() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "schema-test-session"
            })))
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path_regex(r"/api/2\.0/sql/sessions/.*"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri(), "test_warehouse");
        let runtime = create_mt_runtime();

        let result = tokio::task::spawn_blocking(move || {
            let mut conn = DatabricksConnection::new(config, runtime).unwrap();

            conn.set_option(
                OptionConnection::CurrentSchema,
                OptionValue::String("new_schema".into()),
            )
            .unwrap();

            let schema = conn
                .get_option_string(OptionConnection::CurrentSchema)
                .unwrap();
            schema
        })
        .await
        .expect("spawn_blocking failed");

        assert_eq!(result, "new_schema");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_connection_autocommit_always_true() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "autocommit-test-session"
            })))
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path_regex(r"/api/2\.0/sql/sessions/.*"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri(), "test_warehouse");
        let runtime = create_mt_runtime();

        let result = tokio::task::spawn_blocking(move || {
            let conn = DatabricksConnection::new(config, runtime).unwrap();
            conn.get_option_string(OptionConnection::AutoCommit).unwrap()
        })
        .await
        .expect("spawn_blocking failed");

        assert_eq!(result, "true");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_connection_set_invalid_option_type() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "invalid-opt-session"
            })))
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path_regex(r"/api/2\.0/sql/sessions/.*"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri(), "test_warehouse");
        let runtime = create_mt_runtime();

        let result = tokio::task::spawn_blocking(move || {
            let mut conn = DatabricksConnection::new(config, runtime).unwrap();
            conn.set_option(OptionConnection::CurrentCatalog, OptionValue::Int(42))
        })
        .await
        .expect("spawn_blocking failed");

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);
        assert!(err.message.contains("must be a string"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_connection_autocommit_set_true_succeeds() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "autocommit-set-true-session"
            })))
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path_regex(r"/api/2\.0/sql/sessions/.*"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri(), "test_warehouse");
        let runtime = create_mt_runtime();

        let result = tokio::task::spawn_blocking(move || {
            let mut conn = DatabricksConnection::new(config, runtime).unwrap();
            // Setting autocommit to "true" should succeed
            conn.set_option(
                OptionConnection::AutoCommit,
                OptionValue::String("true".into()),
            )
        })
        .await
        .expect("spawn_blocking failed");

        assert!(result.is_ok(), "Setting autocommit to true should succeed");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_connection_autocommit_set_false_fails() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "autocommit-set-false-session"
            })))
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path_regex(r"/api/2\.0/sql/sessions/.*"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri(), "test_warehouse");
        let runtime = create_mt_runtime();

        let result = tokio::task::spawn_blocking(move || {
            let mut conn = DatabricksConnection::new(config, runtime).unwrap();
            // Setting autocommit to "false" should fail
            conn.set_option(
                OptionConnection::AutoCommit,
                OptionValue::String("false".into()),
            )
        })
        .await
        .expect("spawn_blocking failed");

        assert!(result.is_err(), "Setting autocommit to false should fail");
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);
        assert!(
            err.message.contains("autocommit=true"),
            "Error message should mention autocommit: {}",
            err.message
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_connection_autocommit_get_int() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "autocommit-int-session"
            })))
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path_regex(r"/api/2\.0/sql/sessions/.*"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri(), "test_warehouse");
        let runtime = create_mt_runtime();

        let result = tokio::task::spawn_blocking(move || {
            let conn = DatabricksConnection::new(config, runtime).unwrap();
            conn.get_option_int(OptionConnection::AutoCommit)
        })
        .await
        .expect("spawn_blocking failed");

        assert!(result.is_ok(), "get_option_int for AutoCommit should succeed");
        assert_eq!(result.unwrap(), 1, "AutoCommit should return 1 (true)");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_connection_readonly_set_false_succeeds() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "readonly-set-false-session"
            })))
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path_regex(r"/api/2\.0/sql/sessions/.*"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri(), "test_warehouse");
        let runtime = create_mt_runtime();

        let result = tokio::task::spawn_blocking(move || {
            let mut conn = DatabricksConnection::new(config, runtime).unwrap();
            // Setting read_only to "false" should succeed
            conn.set_option(
                OptionConnection::ReadOnly,
                OptionValue::String("false".into()),
            )
        })
        .await
        .expect("spawn_blocking failed");

        assert!(result.is_ok(), "Setting read_only to false should succeed");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_connection_readonly_set_true_fails() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "readonly-set-true-session"
            })))
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path_regex(r"/api/2\.0/sql/sessions/.*"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri(), "test_warehouse");
        let runtime = create_mt_runtime();

        let result = tokio::task::spawn_blocking(move || {
            let mut conn = DatabricksConnection::new(config, runtime).unwrap();
            // Setting read_only to "true" should fail
            conn.set_option(
                OptionConnection::ReadOnly,
                OptionValue::String("true".into()),
            )
        })
        .await
        .expect("spawn_blocking failed");

        assert!(result.is_err(), "Setting read_only to true should fail");
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);
        assert!(
            err.message.contains("read-only"),
            "Error message should mention read-only: {}",
            err.message
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_connection_readonly_get_string() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "readonly-get-string-session"
            })))
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path_regex(r"/api/2\.0/sql/sessions/.*"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri(), "test_warehouse");
        let runtime = create_mt_runtime();

        let result = tokio::task::spawn_blocking(move || {
            let conn = DatabricksConnection::new(config, runtime).unwrap();
            conn.get_option_string(OptionConnection::ReadOnly)
        })
        .await
        .expect("spawn_blocking failed");

        assert!(result.is_ok(), "get_option_string for ReadOnly should succeed");
        assert_eq!(result.unwrap(), "false", "ReadOnly should return 'false'");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_connection_readonly_get_int() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "readonly-get-int-session"
            })))
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path_regex(r"/api/2\.0/sql/sessions/.*"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri(), "test_warehouse");
        let runtime = create_mt_runtime();

        let result = tokio::task::spawn_blocking(move || {
            let conn = DatabricksConnection::new(config, runtime).unwrap();
            conn.get_option_int(OptionConnection::ReadOnly)
        })
        .await
        .expect("spawn_blocking failed");

        assert!(result.is_ok(), "get_option_int for ReadOnly should succeed");
        assert_eq!(result.unwrap(), 0, "ReadOnly should return 0 (false)");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_connection_isolation_level_not_supported() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "isolation-level-session"
            })))
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path_regex(r"/api/2\.0/sql/sessions/.*"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri(), "test_warehouse");
        let runtime = create_mt_runtime();

        let (set_result, get_result) = tokio::task::spawn_blocking(move || {
            let mut conn = DatabricksConnection::new(config, runtime).unwrap();
            let set_res = conn.set_option(
                OptionConnection::IsolationLevel,
                OptionValue::String("READ_COMMITTED".into()),
            );
            let get_res = conn.get_option_string(OptionConnection::IsolationLevel);
            (set_res, get_res)
        })
        .await
        .expect("spawn_blocking failed");

        // set_option should fail with NotImplemented
        assert!(set_result.is_err(), "Setting IsolationLevel should fail");
        let set_err = set_result.unwrap_err();
        assert_eq!(set_err.status, Status::NotImplemented);
        assert!(
            set_err.message.contains("isolation level"),
            "Error message should mention isolation level: {}",
            set_err.message
        );

        // get_option_string should fail with NotImplemented
        assert!(get_result.is_err(), "Getting IsolationLevel should fail");
        let get_err = get_result.unwrap_err();
        assert_eq!(get_err.status, Status::NotImplemented);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_connection_unknown_option_fails() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "unknown-opt-session"
            })))
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path_regex(r"/api/2\.0/sql/sessions/.*"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri(), "test_warehouse");
        let runtime = create_mt_runtime();

        let (set_result, get_result) = tokio::task::spawn_blocking(move || {
            let mut conn = DatabricksConnection::new(config, runtime).unwrap();
            let set_res = conn.set_option(
                OptionConnection::Other("unknown.option".into()),
                OptionValue::String("value".into()),
            );
            let get_res = conn.get_option_string(OptionConnection::Other("unknown.option".into()));
            (set_res, get_res)
        })
        .await
        .expect("spawn_blocking failed");

        // set_option should fail for unknown option
        assert!(set_result.is_err(), "Setting unknown option should fail");
        let set_err = set_result.unwrap_err();
        assert_eq!(set_err.status, Status::NotImplemented);
        assert!(
            set_err.message.contains("Unknown") || set_err.message.contains("unknown"),
            "Error message should mention unknown option: {}",
            set_err.message
        );

        // get_option_string should fail for unknown option
        assert!(get_result.is_err(), "Getting unknown option should fail");
        let get_err = get_result.unwrap_err();
        assert_eq!(get_err.status, Status::NotFound);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_connection_get_option_bytes_not_supported() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "bytes-opt-session"
            })))
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path_regex(r"/api/2\.0/sql/sessions/.*"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri(), "test_warehouse");
        let runtime = create_mt_runtime();

        let result = tokio::task::spawn_blocking(move || {
            let conn = DatabricksConnection::new(config, runtime).unwrap();
            conn.get_option_bytes(OptionConnection::AutoCommit)
        })
        .await
        .expect("spawn_blocking failed");

        assert!(result.is_err(), "get_option_bytes should fail");
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::NotImplemented);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_connection_get_option_double_not_supported() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "double-opt-session"
            })))
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path_regex(r"/api/2\.0/sql/sessions/.*"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri(), "test_warehouse");
        let runtime = create_mt_runtime();

        let result = tokio::task::spawn_blocking(move || {
            let conn = DatabricksConnection::new(config, runtime).unwrap();
            conn.get_option_double(OptionConnection::AutoCommit)
        })
        .await
        .expect("spawn_blocking failed");

        assert!(result.is_err(), "get_option_double should fail");
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::NotImplemented);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_connection_catalog_not_set_returns_not_found() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "catalog-not-set-session"
            })))
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path_regex(r"/api/2\.0/sql/sessions/.*"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        // Create config without default catalog
        let config = Arc::new(DatabaseConfig {
            host: mock_server.uri(),
            warehouse_id: "test_warehouse".to_string(),
            token: "test_token".to_string(),
            default_catalog: None,
            default_schema: None,
            http_config: crate::options::HttpConfig::default(),
            fetch_concurrency: 8,
        });
        let runtime = create_mt_runtime();

        let result = tokio::task::spawn_blocking(move || {
            let conn = DatabricksConnection::new(config, runtime).unwrap();
            conn.get_option_string(OptionConnection::CurrentCatalog)
        })
        .await
        .expect("spawn_blocking failed");

        assert!(result.is_err(), "get_option_string for unset catalog should fail");
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::NotFound);
        assert!(
            err.message.contains("not set") || err.message.contains("Not set"),
            "Error message should indicate not set: {}",
            err.message
        );
    }

    // ============================================================================
    // Connection Trait Tests
    // ============================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_connection_commit_not_supported() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "commit-test-session"
            })))
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path_regex(r"/api/2\.0/sql/sessions/.*"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri(), "test_warehouse");
        let runtime = create_mt_runtime();

        let result = tokio::task::spawn_blocking(move || {
            let mut conn = DatabricksConnection::new(config, runtime).unwrap();
            conn.commit()
        })
        .await
        .expect("spawn_blocking failed");

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::NotImplemented);
        assert!(err.message.contains("Transactions are not supported"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_connection_rollback_not_supported() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "rollback-test-session"
            })))
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path_regex(r"/api/2\.0/sql/sessions/.*"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri(), "test_warehouse");
        let runtime = create_mt_runtime();

        let result = tokio::task::spawn_blocking(move || {
            let mut conn = DatabricksConnection::new(config, runtime).unwrap();
            conn.rollback()
        })
        .await
        .expect("spawn_blocking failed");

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::NotImplemented);
        assert!(err.message.contains("Transactions are not supported"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_connection_cancel_succeeds() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "cancel-test-session"
            })))
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path_regex(r"/api/2\.0/sql/sessions/.*"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri(), "test_warehouse");
        let runtime = create_mt_runtime();

        let result = tokio::task::spawn_blocking(move || {
            let mut conn = DatabricksConnection::new(config, runtime).unwrap();
            conn.cancel()
        })
        .await
        .expect("spawn_blocking failed");

        assert!(result.is_ok(), "cancel() should succeed");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_connection_new_statement() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "stmt-test-session"
            })))
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path_regex(r"/api/2\.0/sql/sessions/.*"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = create_test_config(&mock_server.uri(), "test_warehouse");
        let runtime = create_mt_runtime();

        // All connection operations must happen inside spawn_blocking
        // new_statement() returns a DatabricksStatement which holds Arc references,
        // so it's safe to return it, but we must let the connection drop inside spawn_blocking
        let stmt_is_ok = tokio::task::spawn_blocking(move || {
            let mut conn = DatabricksConnection::new(config, runtime).unwrap();
            let stmt_result = conn.new_statement();
            let is_ok = stmt_result.is_ok();
            // Both connection and statement are dropped here within spawn_blocking
            is_ok
        })
        .await
        .expect("spawn_blocking failed");

        assert!(stmt_is_ok, "new_statement() should succeed");
    }

    // ============================================================================
    // Spark Type Mapping Tests
    // ============================================================================

    #[test]
    fn test_spark_type_to_arrow_simple_types() {
        use arrow_schema::{DataType, TimeUnit};

        // Boolean types
        assert_eq!(
            DatabricksConnection::spark_type_to_arrow("boolean"),
            DataType::Boolean
        );
        assert_eq!(
            DatabricksConnection::spark_type_to_arrow("BOOLEAN"),
            DataType::Boolean
        );
        assert_eq!(
            DatabricksConnection::spark_type_to_arrow("bool"),
            DataType::Boolean
        );

        // Integer types
        assert_eq!(
            DatabricksConnection::spark_type_to_arrow("tinyint"),
            DataType::Int8
        );
        assert_eq!(
            DatabricksConnection::spark_type_to_arrow("byte"),
            DataType::Int8
        );
        assert_eq!(
            DatabricksConnection::spark_type_to_arrow("smallint"),
            DataType::Int16
        );
        assert_eq!(
            DatabricksConnection::spark_type_to_arrow("short"),
            DataType::Int16
        );
        assert_eq!(
            DatabricksConnection::spark_type_to_arrow("int"),
            DataType::Int32
        );
        assert_eq!(
            DatabricksConnection::spark_type_to_arrow("integer"),
            DataType::Int32
        );
        assert_eq!(
            DatabricksConnection::spark_type_to_arrow("bigint"),
            DataType::Int64
        );
        assert_eq!(
            DatabricksConnection::spark_type_to_arrow("long"),
            DataType::Int64
        );

        // Floating point types
        assert_eq!(
            DatabricksConnection::spark_type_to_arrow("float"),
            DataType::Float32
        );
        assert_eq!(
            DatabricksConnection::spark_type_to_arrow("real"),
            DataType::Float32
        );
        assert_eq!(
            DatabricksConnection::spark_type_to_arrow("double"),
            DataType::Float64
        );

        // String and binary types
        assert_eq!(
            DatabricksConnection::spark_type_to_arrow("string"),
            DataType::Utf8
        );
        assert_eq!(
            DatabricksConnection::spark_type_to_arrow("binary"),
            DataType::Binary
        );

        // Date and timestamp types
        assert_eq!(
            DatabricksConnection::spark_type_to_arrow("date"),
            DataType::Date32
        );
        assert_eq!(
            DatabricksConnection::spark_type_to_arrow("timestamp"),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            DatabricksConnection::spark_type_to_arrow("timestamp_ltz"),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            DatabricksConnection::spark_type_to_arrow("timestamp_ntz"),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );

        // Null types
        assert_eq!(
            DatabricksConnection::spark_type_to_arrow("void"),
            DataType::Null
        );
        assert_eq!(
            DatabricksConnection::spark_type_to_arrow("null"),
            DataType::Null
        );
    }

    #[test]
    fn test_spark_type_to_arrow_decimal() {
        use arrow_schema::DataType;

        // Default decimal (no params)
        assert_eq!(
            DatabricksConnection::spark_type_to_arrow("decimal"),
            DataType::Decimal128(38, 18)
        );

        // Decimal with precision and scale
        assert_eq!(
            DatabricksConnection::spark_type_to_arrow("decimal(10,2)"),
            DataType::Decimal128(10, 2)
        );

        // Decimal with different precision/scale
        assert_eq!(
            DatabricksConnection::spark_type_to_arrow("decimal(38,10)"),
            DataType::Decimal128(38, 10)
        );

        // Decimal with spaces
        assert_eq!(
            DatabricksConnection::spark_type_to_arrow("decimal( 5 , 3 )"),
            DataType::Decimal128(5, 3)
        );
    }

    #[test]
    fn test_spark_type_to_arrow_varchar_char() {
        use arrow_schema::DataType;

        // VARCHAR maps to Utf8
        assert_eq!(
            DatabricksConnection::spark_type_to_arrow("varchar(100)"),
            DataType::Utf8
        );

        // CHAR maps to Utf8
        assert_eq!(
            DatabricksConnection::spark_type_to_arrow("char(10)"),
            DataType::Utf8
        );
    }

    #[test]
    fn test_spark_type_to_arrow_array() {
        use arrow_schema::DataType;

        // Array of int
        let arr_type = DatabricksConnection::spark_type_to_arrow("array<int>");
        match arr_type {
            DataType::List(field) => {
                assert_eq!(field.data_type(), &DataType::Int32);
            }
            _ => panic!("Expected List type, got {:?}", arr_type),
        }

        // Array of string
        let arr_type = DatabricksConnection::spark_type_to_arrow("array<string>");
        match arr_type {
            DataType::List(field) => {
                assert_eq!(field.data_type(), &DataType::Utf8);
            }
            _ => panic!("Expected List type, got {:?}", arr_type),
        }

        // Nested array
        let arr_type = DatabricksConnection::spark_type_to_arrow("array<array<int>>");
        match arr_type {
            DataType::List(field) => match field.data_type() {
                DataType::List(inner) => {
                    assert_eq!(inner.data_type(), &DataType::Int32);
                }
                _ => panic!("Expected nested List type"),
            },
            _ => panic!("Expected List type, got {:?}", arr_type),
        }
    }

    #[test]
    fn test_spark_type_to_arrow_map() {
        use arrow_schema::DataType;

        // Map<string,int>
        let map_type = DatabricksConnection::spark_type_to_arrow("map<string,int>");
        match map_type {
            DataType::Map(field, _) => match field.data_type() {
                DataType::Struct(fields) => {
                    assert_eq!(fields.len(), 2);
                    assert_eq!(fields[0].name(), "key");
                    assert_eq!(fields[0].data_type(), &DataType::Utf8);
                    assert_eq!(fields[1].name(), "value");
                    assert_eq!(fields[1].data_type(), &DataType::Int32);
                }
                _ => panic!("Expected Struct inside Map"),
            },
            _ => panic!("Expected Map type, got {:?}", map_type),
        }
    }

    #[test]
    fn test_spark_type_to_arrow_struct() {
        use arrow_schema::DataType;

        // Simple struct
        let struct_type =
            DatabricksConnection::spark_type_to_arrow("struct<a:int,b:string>");
        match struct_type {
            DataType::Struct(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].name(), "a");
                assert_eq!(fields[0].data_type(), &DataType::Int32);
                assert_eq!(fields[1].name(), "b");
                assert_eq!(fields[1].data_type(), &DataType::Utf8);
            }
            _ => panic!("Expected Struct type, got {:?}", struct_type),
        }

        // Struct with nested array
        let struct_type =
            DatabricksConnection::spark_type_to_arrow("struct<id:int,tags:array<string>>");
        match struct_type {
            DataType::Struct(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].name(), "id");
                assert_eq!(fields[0].data_type(), &DataType::Int32);
                assert_eq!(fields[1].name(), "tags");
                match fields[1].data_type() {
                    DataType::List(inner) => {
                        assert_eq!(inner.data_type(), &DataType::Utf8);
                    }
                    _ => panic!("Expected List for tags field"),
                }
            }
            _ => panic!("Expected Struct type, got {:?}", struct_type),
        }
    }

    #[test]
    fn test_spark_type_to_arrow_unknown_defaults_to_string() {
        use arrow_schema::DataType;

        // Unknown type should default to Utf8
        assert_eq!(
            DatabricksConnection::spark_type_to_arrow("unknown_type"),
            DataType::Utf8
        );
    }

    #[test]
    fn test_spark_type_to_arrow_case_insensitive() {
        use arrow_schema::DataType;

        // Uppercase
        assert_eq!(
            DatabricksConnection::spark_type_to_arrow("INT"),
            DataType::Int32
        );
        assert_eq!(
            DatabricksConnection::spark_type_to_arrow("STRING"),
            DataType::Utf8
        );
        assert_eq!(
            DatabricksConnection::spark_type_to_arrow("BIGINT"),
            DataType::Int64
        );

        // Mixed case
        assert_eq!(
            DatabricksConnection::spark_type_to_arrow("Int"),
            DataType::Int32
        );
        assert_eq!(
            DatabricksConnection::spark_type_to_arrow("String"),
            DataType::Utf8
        );
    }

    #[test]
    fn test_split_on_top_level_commas() {
        // Simple split
        let result = DatabricksConnection::split_on_top_level_commas("a,b,c");
        assert_eq!(result, vec!["a", "b", "c"]);

        // Split with nested angle brackets
        let result = DatabricksConnection::split_on_top_level_commas("a:int,b:array<string>");
        assert_eq!(result, vec!["a:int", "b:array<string>"]);

        // Deeply nested
        let result =
            DatabricksConnection::split_on_top_level_commas("a:int,b:map<string,array<int>>,c:double");
        assert_eq!(result, vec!["a:int", "b:map<string,array<int>>", "c:double"]);

        // Single element
        let result = DatabricksConnection::split_on_top_level_commas("a:int");
        assert_eq!(result, vec!["a:int"]);

        // Empty string
        let result = DatabricksConnection::split_on_top_level_commas("");
        assert!(result.is_empty() || result == vec![""]);
    }

    #[test]
    fn test_find_top_level_comma() {
        // Simple case
        assert_eq!(
            DatabricksConnection::find_top_level_comma("string,int"),
            Some(6)
        );

        // Nested - should find after the nested type
        assert_eq!(
            DatabricksConnection::find_top_level_comma("array<int>,string"),
            Some(10)
        );

        // No comma
        assert_eq!(DatabricksConnection::find_top_level_comma("string"), None);

        // Comma inside nested type should be ignored
        assert_eq!(
            DatabricksConnection::find_top_level_comma("map<string,int>"),
            None
        );
    }
}
