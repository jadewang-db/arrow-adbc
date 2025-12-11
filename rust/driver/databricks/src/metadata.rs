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

//! Metadata API implementations for DatabricksConnection.
//!
//! This module provides implementations for:
//! - `get_info()` - Returns driver and connection information
//! - `get_table_types()` - Returns supported table types
//! - `get_objects()` - Returns database metadata (catalogs, schemas, tables, columns)
//! - `get_table_schema()` - Returns the Arrow schema for a specific table
//!
//! These implementations query Databricks using SQL statements against
//! INFORMATION_SCHEMA or SHOW commands to retrieve metadata.

use std::collections::HashSet;
use std::sync::Arc;

use adbc_core::constants;
use adbc_core::options::{InfoCode, ObjectDepth};
use adbc_core::schemas;
use arrow_array::{
    ArrayRef, BooleanArray, Int16Array, Int32Array, Int64Array, ListArray, RecordBatch,
    StringArray, StructArray, UInt32Array, UnionArray,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema, UnionFields};

use crate::connection::SingleBatchReader;
use crate::error::{Error, Result};

/// Driver name for get_info.
const DRIVER_NAME: &str = "adbc_driver_databricks";

/// Driver version for get_info.
const DRIVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Arrow version used by the driver.
const ARROW_VERSION: &str = "53.0.0";

/// Vendor name for get_info.
const VENDOR_NAME: &str = "Databricks";

/// Build the get_info result for the requested info codes.
///
/// Returns a RecordBatch with the standard get_info schema:
/// - info_name: UInt32 (info code)
/// - info_value: DenseUnion (string, bool, int64, etc.)
pub(crate) fn build_get_info_result(
    codes: Option<HashSet<InfoCode>>,
) -> Result<SingleBatchReader> {
    // Determine which codes to return
    let all_codes = [
        InfoCode::VendorName,
        InfoCode::VendorVersion,
        InfoCode::VendorArrowVersion,
        InfoCode::VendorSql,
        InfoCode::VendorSubstrait,
        InfoCode::DriverName,
        InfoCode::DriverVersion,
        InfoCode::DriverArrowVersion,
        InfoCode::DriverAdbcVersion,
    ];

    let codes_to_return: Vec<InfoCode> = match codes {
        Some(requested) => all_codes
            .into_iter()
            .filter(|c| requested.contains(c))
            .collect(),
        None => all_codes.to_vec(),
    };

    if codes_to_return.is_empty() {
        return Ok(SingleBatchReader::empty(schemas::GET_INFO_SCHEMA.clone()));
    }

    // Build arrays for each info code
    let mut info_names: Vec<u32> = Vec::new();
    let mut type_ids: Vec<i8> = Vec::new();
    let mut value_offsets: Vec<i32> = Vec::new();

    // Union member arrays - we'll build these incrementally
    let mut string_values: Vec<Option<String>> = Vec::new();
    let mut bool_values: Vec<Option<bool>> = Vec::new();
    let mut int64_values: Vec<Option<i64>> = Vec::new();

    for code in codes_to_return {
        let info_name: u32 = (&code).into();
        info_names.push(info_name);

        match code {
            InfoCode::VendorName => {
                type_ids.push(0); // string_value
                value_offsets.push(string_values.len() as i32);
                string_values.push(Some(VENDOR_NAME.to_string()));
            }
            InfoCode::VendorVersion => {
                type_ids.push(0);
                value_offsets.push(string_values.len() as i32);
                // We don't have a specific vendor version, use driver version
                string_values.push(Some(DRIVER_VERSION.to_string()));
            }
            InfoCode::VendorArrowVersion => {
                type_ids.push(0);
                value_offsets.push(string_values.len() as i32);
                string_values.push(Some(ARROW_VERSION.to_string()));
            }
            InfoCode::VendorSql => {
                type_ids.push(1); // bool_value
                value_offsets.push(bool_values.len() as i32);
                bool_values.push(Some(true)); // Databricks supports SQL
            }
            InfoCode::VendorSubstrait => {
                type_ids.push(1);
                value_offsets.push(bool_values.len() as i32);
                bool_values.push(Some(false)); // Databricks doesn't support Substrait
            }
            InfoCode::VendorSubstraitMinVersion | InfoCode::VendorSubstraitMaxVersion => {
                type_ids.push(0);
                value_offsets.push(string_values.len() as i32);
                string_values.push(None); // Substrait not supported
            }
            InfoCode::DriverName => {
                type_ids.push(0);
                value_offsets.push(string_values.len() as i32);
                string_values.push(Some(DRIVER_NAME.to_string()));
            }
            InfoCode::DriverVersion => {
                type_ids.push(0);
                value_offsets.push(string_values.len() as i32);
                string_values.push(Some(DRIVER_VERSION.to_string()));
            }
            InfoCode::DriverArrowVersion => {
                type_ids.push(0);
                value_offsets.push(string_values.len() as i32);
                string_values.push(Some(ARROW_VERSION.to_string()));
            }
            InfoCode::DriverAdbcVersion => {
                type_ids.push(2); // int64_value
                value_offsets.push(int64_values.len() as i32);
                int64_values.push(Some(constants::ADBC_VERSION_1_1_0 as i64));
            }
            // Handle any other info codes - skip them
            _ => {
                // Skip unknown info codes
                info_names.pop(); // Remove the last info_name we added
            }
        }
    }

    // Build the union array
    let string_array = StringArray::from(string_values);
    let bool_array = BooleanArray::from(bool_values);
    let int64_array = Int64Array::from(int64_values);

    // Empty arrays for unused union members
    let int32_bitmask_array = Int32Array::from(Vec::<i32>::new());
    let string_list_array = ListArray::new_null(
        Arc::new(Field::new_list_field(DataType::Utf8, true)),
        0,
    );
    let map_array = create_empty_int32_to_int32_list_map();

    let type_id_buffer: ScalarBuffer<i8> = type_ids.into_iter().collect();
    let value_offsets_buffer: ScalarBuffer<i32> = value_offsets.into_iter().collect();

    let union_fields = UnionFields::new(
        [0, 1, 2, 3, 4, 5],
        [
            Field::new("string_value", DataType::Utf8, true),
            Field::new("bool_value", DataType::Boolean, true),
            Field::new("int64_value", DataType::Int64, true),
            Field::new("int32_bitmask", DataType::Int32, true),
            Field::new_list("string_list", Field::new_list_field(DataType::Utf8, true), true),
            Field::new_map(
                "int32_to_int32_list_map",
                "entries",
                Field::new("key", DataType::Int32, false),
                Field::new_list("value", Field::new_list_field(DataType::Int32, true), true),
                false,
                true,
            ),
        ],
    );

    let value_array = UnionArray::try_new(
        union_fields,
        type_id_buffer,
        Some(value_offsets_buffer),
        vec![
            Arc::new(string_array),
            Arc::new(bool_array),
            Arc::new(int64_array),
            Arc::new(int32_bitmask_array),
            Arc::new(string_list_array),
            Arc::new(map_array),
        ],
    )
    .map_err(|e| Error::arrow(e))?;

    let info_name_array = UInt32Array::from(info_names);

    let batch = RecordBatch::try_new(
        schemas::GET_INFO_SCHEMA.clone(),
        vec![Arc::new(info_name_array), Arc::new(value_array)],
    )
    .map_err(|e| Error::arrow(e))?;

    Ok(SingleBatchReader::new(batch))
}

/// Create an empty map array for the union.
fn create_empty_int32_to_int32_list_map() -> ListArray {
    let key_field = Field::new("key", DataType::Int32, false);
    let value_field = Field::new_list("value", Field::new_list_field(DataType::Int32, true), true);
    let _entries_field = Field::new_struct("entries", vec![key_field.clone(), value_field.clone()], false);

    let map_field = Field::new_map("int32_to_int32_list_map", "entries", key_field, value_field, false, true);

    // Create empty arrays
    ListArray::new_null(Arc::new(map_field), 0)
}

/// Build the get_table_types result.
///
/// Returns a RecordBatch with a single "table_type" column containing
/// the supported table types (TABLE, VIEW, etc.).
pub(crate) fn build_get_table_types_result() -> Result<SingleBatchReader> {
    // Databricks supports these table types
    let table_types = vec!["TABLE", "VIEW", "EXTERNAL", "MANAGED", "STREAMING_TABLE"];

    let array = Arc::new(StringArray::from(table_types));
    let batch = RecordBatch::try_new(schemas::GET_TABLE_TYPES_SCHEMA.clone(), vec![array])
        .map_err(|e| Error::arrow(e))?;

    Ok(SingleBatchReader::new(batch))
}

/// Build an empty get_objects result with the correct schema.
pub(crate) fn build_empty_get_objects_result() -> Result<SingleBatchReader> {
    Ok(SingleBatchReader::empty(schemas::GET_OBJECTS_SCHEMA.clone()))
}

/// Builder for constructing get_objects results.
pub(crate) struct GetObjectsBuilder {
    catalogs: Vec<String>,
    db_schemas: Vec<Vec<DbSchemaInfo>>,
}

/// Information about a database schema.
pub(crate) struct DbSchemaInfo {
    pub name: Option<String>,
    pub tables: Vec<TableInfo>,
}

/// Information about a table.
pub(crate) struct TableInfo {
    pub name: String,
    pub table_type: String,
    pub columns: Vec<ColumnInfo>,
}

/// Information about a column.
pub(crate) struct ColumnInfo {
    pub name: String,
    pub ordinal_position: i32,
    pub data_type: String,
    pub nullable: bool,
    pub remarks: Option<String>,
}

impl GetObjectsBuilder {
    pub fn new() -> Self {
        Self {
            catalogs: Vec::new(),
            db_schemas: Vec::new(),
        }
    }

    pub fn add_catalog(&mut self, catalog: String, schemas: Vec<DbSchemaInfo>) {
        self.catalogs.push(catalog);
        self.db_schemas.push(schemas);
    }

    pub fn build(self, depth: ObjectDepth) -> Result<SingleBatchReader> {
        if self.catalogs.is_empty() {
            return build_empty_get_objects_result();
        }

        // Build the catalog_name array
        let catalog_name_array = StringArray::from(self.catalogs.clone());

        // Build the catalog_db_schemas array based on depth
        let catalog_db_schemas_array = match &depth {
            ObjectDepth::Catalogs => {
                // No schemas, just null for each catalog
                let null_count = self.catalogs.len();
                ListArray::new_null(
                    Arc::new(Field::new("item", schemas::OBJECTS_DB_SCHEMA_SCHEMA.clone(), true)),
                    null_count,
                )
            }
            ObjectDepth::Schemas | ObjectDepth::Tables | ObjectDepth::All | ObjectDepth::Columns => {
                self.build_db_schemas_array(&depth)?
            }
        };

        let batch = RecordBatch::try_new(
            schemas::GET_OBJECTS_SCHEMA.clone(),
            vec![
                Arc::new(catalog_name_array),
                Arc::new(catalog_db_schemas_array),
            ],
        )
        .map_err(|e| Error::arrow(e))?;

        Ok(SingleBatchReader::new(batch))
    }

    fn build_db_schemas_array(&self, depth: &ObjectDepth) -> Result<ListArray> {
        let mut offsets: Vec<i32> = vec![0];
        let mut schema_names: Vec<Option<String>> = Vec::new();
        let mut schema_tables: Vec<ListArray> = Vec::new();

        for schemas in &self.db_schemas {
            for schema in schemas {
                schema_names.push(schema.name.clone());

                // Build tables array based on depth
                let tables_array = match depth {
                    ObjectDepth::Schemas => {
                        // No tables, just null
                        ListArray::new_null(
                            Arc::new(Field::new("item", schemas::TABLE_SCHEMA.clone(), true)),
                            0,
                        )
                    }
                    ObjectDepth::Tables | ObjectDepth::All | ObjectDepth::Columns => {
                        self.build_tables_array(&schema.tables, depth)?
                    }
                    ObjectDepth::Catalogs => {
                        // Should not reach here
                        ListArray::new_null(
                            Arc::new(Field::new("item", schemas::TABLE_SCHEMA.clone(), true)),
                            0,
                        )
                    }
                };
                schema_tables.push(tables_array);
            }
            offsets.push(schema_names.len() as i32);
        }

        if schema_names.is_empty() {
            return Ok(ListArray::new_null(
                Arc::new(Field::new("item", schemas::OBJECTS_DB_SCHEMA_SCHEMA.clone(), true)),
                self.catalogs.len(),
            ));
        }

        // Build concatenated tables array
        let all_tables = self.concatenate_list_arrays(&schema_tables)?;

        let db_schema_struct = StructArray::from(vec![
            (
                Arc::new(Field::new("db_schema_name", DataType::Utf8, true)),
                Arc::new(StringArray::from(schema_names)) as ArrayRef,
            ),
            (
                Arc::new(Field::new_list(
                    "db_schema_tables",
                    Arc::new(Field::new("item", schemas::TABLE_SCHEMA.clone(), true)),
                    true,
                )),
                Arc::new(all_tables) as ArrayRef,
            ),
        ]);

        let offsets_buffer = OffsetBuffer::new(ScalarBuffer::from(offsets));

        Ok(ListArray::new(
            Arc::new(Field::new("item", schemas::OBJECTS_DB_SCHEMA_SCHEMA.clone(), true)),
            offsets_buffer,
            Arc::new(db_schema_struct),
            None,
        ))
    }

    fn build_tables_array(&self, tables: &[TableInfo], depth: &ObjectDepth) -> Result<ListArray> {
        if tables.is_empty() {
            return Ok(ListArray::new_null(
                Arc::new(Field::new("item", schemas::TABLE_SCHEMA.clone(), true)),
                0,
            ));
        }

        let table_names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
        let table_types: Vec<&str> = tables.iter().map(|t| t.table_type.as_str()).collect();

        // Build columns arrays based on depth
        let include_columns = matches!(depth, ObjectDepth::All | ObjectDepth::Columns);

        let mut column_list_arrays: Vec<ListArray> = Vec::new();
        for table in tables {
            if include_columns {
                column_list_arrays.push(self.build_columns_array(&table.columns)?);
            } else {
                column_list_arrays.push(ListArray::new_null(
                    Arc::new(Field::new("item", schemas::COLUMN_SCHEMA.clone(), true)),
                    0,
                ));
            }
        }

        let all_columns = self.concatenate_list_arrays(&column_list_arrays)?;

        // Empty constraints array
        let constraints_array = ListArray::new_null(
            Arc::new(Field::new("item", schemas::CONSTRAINT_SCHEMA.clone(), true)),
            tables.len(),
        );

        let table_struct = StructArray::from(vec![
            (
                Arc::new(Field::new("table_name", DataType::Utf8, false)),
                Arc::new(StringArray::from(table_names)) as ArrayRef,
            ),
            (
                Arc::new(Field::new("table_type", DataType::Utf8, false)),
                Arc::new(StringArray::from(table_types)) as ArrayRef,
            ),
            (
                Arc::new(Field::new_list(
                    "table_columns",
                    Arc::new(Field::new("item", schemas::COLUMN_SCHEMA.clone(), true)),
                    true,
                )),
                Arc::new(all_columns) as ArrayRef,
            ),
            (
                Arc::new(Field::new_list(
                    "table_constraints",
                    Arc::new(Field::new("item", schemas::CONSTRAINT_SCHEMA.clone(), true)),
                    true,
                )),
                Arc::new(constraints_array) as ArrayRef,
            ),
        ]);

        // Single list containing all tables
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, tables.len() as i32]));

        Ok(ListArray::new(
            Arc::new(Field::new("item", schemas::TABLE_SCHEMA.clone(), true)),
            offsets,
            Arc::new(table_struct),
            None,
        ))
    }

    fn build_columns_array(&self, columns: &[ColumnInfo]) -> Result<ListArray> {
        if columns.is_empty() {
            return Ok(ListArray::new_null(
                Arc::new(Field::new("item", schemas::COLUMN_SCHEMA.clone(), true)),
                0,
            ));
        }

        let column_names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
        let ordinal_positions: Vec<i32> = columns.iter().map(|c| c.ordinal_position).collect();
        let remarks: Vec<Option<&str>> = columns.iter().map(|c| c.remarks.as_deref()).collect();
        let type_names: Vec<&str> = columns.iter().map(|c| c.data_type.as_str()).collect();
        let nullables: Vec<i16> = columns.iter().map(|c| if c.nullable { 1 } else { 0 }).collect();

        let num_cols = columns.len();

        let column_struct = StructArray::from(vec![
            (
                Arc::new(Field::new("column_name", DataType::Utf8, false)),
                Arc::new(StringArray::from(column_names)) as ArrayRef,
            ),
            (
                Arc::new(Field::new("ordinal_position", DataType::Int32, true)),
                Arc::new(Int32Array::from(ordinal_positions)) as ArrayRef,
            ),
            (
                Arc::new(Field::new("remarks", DataType::Utf8, true)),
                Arc::new(StringArray::from(remarks)) as ArrayRef,
            ),
            (
                Arc::new(Field::new("xdbc_data_type", DataType::Int16, true)),
                Arc::new(Int16Array::from(vec![None; num_cols])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("xdbc_type_name", DataType::Utf8, true)),
                Arc::new(StringArray::from(type_names)) as ArrayRef,
            ),
            (
                Arc::new(Field::new("xdbc_column_size", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![None; num_cols])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("xdbc_decimal_digits", DataType::Int16, true)),
                Arc::new(Int16Array::from(vec![None; num_cols])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("xdbc_num_prec_radix", DataType::Int16, true)),
                Arc::new(Int16Array::from(vec![None; num_cols])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("xdbc_nullable", DataType::Int16, true)),
                Arc::new(Int16Array::from(nullables)) as ArrayRef,
            ),
            (
                Arc::new(Field::new("xdbc_column_def", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec![None::<&str>; num_cols])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("xdbc_sql_data_type", DataType::Int16, true)),
                Arc::new(Int16Array::from(vec![None; num_cols])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("xdbc_datetime_sub", DataType::Int16, true)),
                Arc::new(Int16Array::from(vec![None; num_cols])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("xdbc_char_octet_length", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![None; num_cols])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("xdbc_is_nullable", DataType::Utf8, true)),
                Arc::new(StringArray::from(
                    columns.iter().map(|c| if c.nullable { "YES" } else { "NO" }).collect::<Vec<_>>()
                )) as ArrayRef,
            ),
            (
                Arc::new(Field::new("xdbc_scope_catalog", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec![None::<&str>; num_cols])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("xdbc_scope_schema", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec![None::<&str>; num_cols])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("xdbc_scope_table", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec![None::<&str>; num_cols])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("xdbc_is_autoincrement", DataType::Boolean, true)),
                Arc::new(BooleanArray::from(vec![None; num_cols])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("xdbc_is_generatedcolumn", DataType::Boolean, true)),
                Arc::new(BooleanArray::from(vec![None; num_cols])) as ArrayRef,
            ),
        ]);

        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, columns.len() as i32]));

        Ok(ListArray::new(
            Arc::new(Field::new("item", schemas::COLUMN_SCHEMA.clone(), true)),
            offsets,
            Arc::new(column_struct),
            None,
        ))
    }

    fn concatenate_list_arrays(&self, arrays: &[ListArray]) -> Result<ListArray> {
        if arrays.is_empty() {
            return Ok(ListArray::new_null(
                Arc::new(Field::new("item", DataType::Null, true)),
                0,
            ));
        }

        // For simplicity, if we only have one array, return it
        if arrays.len() == 1 {
            return Ok(arrays[0].clone());
        }

        // Concatenate multiple arrays - use arrow's concat for proper handling
        let refs: Vec<&dyn arrow_array::Array> = arrays.iter().map(|a| a as &dyn arrow_array::Array).collect();
        let concatenated = arrow_select::concat::concat(&refs)
            .map_err(|e| Error::arrow(e))?;

        concatenated
            .as_any()
            .downcast_ref::<ListArray>()
            .cloned()
            .ok_or_else(|| Error::internal("Failed to downcast concatenated array to ListArray"))
    }
}

/// Convert Databricks data type string to Arrow DataType.
pub(crate) fn databricks_type_to_arrow(type_name: &str) -> DataType {
    let type_upper = type_name.to_uppercase();

    match type_upper.as_str() {
        "BOOLEAN" | "BOOL" => DataType::Boolean,
        "TINYINT" | "BYTE" => DataType::Int8,
        "SMALLINT" | "SHORT" => DataType::Int16,
        "INT" | "INTEGER" => DataType::Int32,
        "BIGINT" | "LONG" => DataType::Int64,
        "FLOAT" | "REAL" => DataType::Float32,
        "DOUBLE" => DataType::Float64,
        "STRING" | "VARCHAR" | "CHAR" | "TEXT" => DataType::Utf8,
        "BINARY" | "VARBINARY" => DataType::Binary,
        "DATE" => DataType::Date32,
        "TIMESTAMP" | "TIMESTAMP_NTZ" => DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
        "TIMESTAMP_LTZ" => DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
        s if s.starts_with("DECIMAL") || s.starts_with("NUMERIC") => {
            // Parse DECIMAL(precision, scale)
            parse_decimal_type(s).unwrap_or(DataType::Decimal128(38, 10))
        }
        s if s.starts_with("ARRAY") => {
            // ARRAY<element_type>
            let inner = parse_array_element_type(s);
            DataType::List(Arc::new(Field::new("item", inner, true)))
        }
        s if s.starts_with("MAP") => {
            // MAP<key_type, value_type>
            let (key_type, value_type) = parse_map_types(s);
            DataType::Map(
                Arc::new(Field::new_struct(
                    "entries",
                    vec![
                        Field::new("key", key_type, false),
                        Field::new("value", value_type, true),
                    ],
                    false,
                )),
                false,
            )
        }
        s if s.starts_with("STRUCT") => {
            // For complex struct types, return as Utf8 (we'd need full parsing)
            DataType::Utf8
        }
        _ => DataType::Utf8, // Default to string for unknown types
    }
}

fn parse_decimal_type(s: &str) -> Option<DataType> {
    // Parse DECIMAL(p,s) or DECIMAL(p)
    let s = s.trim();
    if let Some(start) = s.find('(') {
        if let Some(end) = s.find(')') {
            let params = &s[start + 1..end];
            let parts: Vec<&str> = params.split(',').map(|p| p.trim()).collect();
            let precision: u8 = parts.first()?.parse().ok()?;
            let scale: i8 = parts.get(1).map(|s| s.parse().unwrap_or(0)).unwrap_or(0);
            return Some(DataType::Decimal128(precision, scale));
        }
    }
    None
}

fn parse_array_element_type(s: &str) -> DataType {
    // Parse ARRAY<element_type>
    if let Some(start) = s.find('<') {
        if let Some(end) = s.rfind('>') {
            let inner = s[start + 1..end].trim();
            return databricks_type_to_arrow(inner);
        }
    }
    DataType::Utf8
}

fn parse_map_types(s: &str) -> (DataType, DataType) {
    // Parse MAP<key_type, value_type>
    if let Some(start) = s.find('<') {
        if let Some(end) = s.rfind('>') {
            let inner = &s[start + 1..end];
            // Simple split - doesn't handle nested types well
            if let Some(comma) = inner.find(',') {
                let key_type = inner[..comma].trim();
                let value_type = inner[comma + 1..].trim();
                return (
                    databricks_type_to_arrow(key_type),
                    databricks_type_to_arrow(value_type),
                );
            }
        }
    }
    (DataType::Utf8, DataType::Utf8)
}

/// Build an Arrow Schema from column metadata.
pub(crate) fn build_table_schema(columns: &[ColumnInfo]) -> Schema {
    let fields: Vec<Field> = columns
        .iter()
        .map(|col| {
            let data_type = databricks_type_to_arrow(&col.data_type);
            Field::new(&col.name, data_type, col.nullable)
        })
        .collect();

    Schema::new(fields)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;

    #[test]
    fn test_build_get_info_result_all() {
        let reader = build_get_info_result(None).unwrap();
        let schema = arrow_array::RecordBatchReader::schema(&reader);
        assert_eq!(schema, schemas::GET_INFO_SCHEMA.clone());
    }

    #[test]
    fn test_build_get_info_result_filtered() {
        let mut codes = HashSet::new();
        codes.insert(InfoCode::VendorName);
        codes.insert(InfoCode::DriverName);

        let mut reader = build_get_info_result(Some(codes)).unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn test_build_get_table_types_result() {
        let mut reader = build_get_table_types_result().unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(batch.num_columns(), 1);
        assert!(batch.num_rows() > 0);

        let col = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let types: Vec<&str> = (0..col.len()).map(|i| col.value(i)).collect();
        assert!(types.contains(&"TABLE"));
        assert!(types.contains(&"VIEW"));
    }

    #[test]
    fn test_databricks_type_to_arrow() {
        assert_eq!(databricks_type_to_arrow("BOOLEAN"), DataType::Boolean);
        assert_eq!(databricks_type_to_arrow("INT"), DataType::Int32);
        assert_eq!(databricks_type_to_arrow("BIGINT"), DataType::Int64);
        assert_eq!(databricks_type_to_arrow("STRING"), DataType::Utf8);
        assert_eq!(databricks_type_to_arrow("DOUBLE"), DataType::Float64);
        assert_eq!(databricks_type_to_arrow("DATE"), DataType::Date32);

        // Test decimal
        let decimal = databricks_type_to_arrow("DECIMAL(10,2)");
        assert!(matches!(decimal, DataType::Decimal128(10, 2)));

        // Test array
        let array = databricks_type_to_arrow("ARRAY<INT>");
        assert!(matches!(array, DataType::List(_)));
    }

    #[test]
    fn test_build_table_schema() {
        let columns = vec![
            ColumnInfo {
                name: "id".to_string(),
                ordinal_position: 1,
                data_type: "BIGINT".to_string(),
                nullable: false,
                remarks: None,
            },
            ColumnInfo {
                name: "name".to_string(),
                ordinal_position: 2,
                data_type: "STRING".to_string(),
                nullable: true,
                remarks: Some("User name".to_string()),
            },
        ];

        let schema = build_table_schema(&columns);
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert!(!schema.field(0).is_nullable());
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert!(schema.field(1).is_nullable());
    }

    #[test]
    fn test_get_objects_builder_catalogs_only() {
        let mut builder = GetObjectsBuilder::new();
        builder.add_catalog("main".to_string(), vec![]);
        builder.add_catalog("hive_metastore".to_string(), vec![]);

        let reader = builder.build(ObjectDepth::Catalogs).unwrap();
        let schema = arrow_array::RecordBatchReader::schema(&reader);
        assert_eq!(schema, schemas::GET_OBJECTS_SCHEMA.clone());
    }
}
