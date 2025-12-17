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

//! Parallel chunk fetching for large result sets

pub mod decompress;
pub mod reader;

use crate::client::models::ExternalLink;
use crate::client::SeaClient;
use crate::error::{Error, Result};
use crate::client::models::ManifestSchema;
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use reqwest::Client;
use std::sync::Arc;
use std::time::Duration;

/// Fetches result chunks from external links in parallel
///
/// The ChunkFetcher is responsible for downloading Arrow data from cloud storage
/// using presigned URLs (external links). It supports:
/// - Parallel chunk fetching with configurable concurrency
/// - Automatic URL refresh when presigned URLs expire (403 Forbidden)
/// - LZ4 decompression of downloaded data
pub struct ChunkFetcher {
    /// HTTP client for downloading chunks from cloud storage
    http_client: Client,
    /// SEA client for refreshing expired URLs
    sea_client: Arc<SeaClient>,
    /// Statement ID for this result set
    statement_id: String,
    /// Maximum number of concurrent chunk downloads
    concurrency: usize,
}

impl ChunkFetcher {
    /// Create a new ChunkFetcher
    ///
    /// # Arguments
    /// * `sea_client` - SEA client for refreshing expired URLs
    /// * `statement_id` - Statement ID for this result set
    /// * `concurrency` - Maximum number of concurrent chunk downloads (default: 8)
    ///
    /// # Returns
    /// A new ChunkFetcher instance configured for parallel downloads
    ///
    /// # Example
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use adbc_driver_databricks::fetch::ChunkFetcher;
    /// # use adbc_driver_databricks::client::{SeaClient, SeaClientConfig};
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let sea_client = Arc::new(SeaClient::new(SeaClientConfig::default())?);
    /// let fetcher = ChunkFetcher::new(
    ///     sea_client,
    ///     "stmt-id-123".to_string(),
    ///     8
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(
        sea_client: Arc<SeaClient>,
        statement_id: String,
        concurrency: usize,
    ) -> Result<Self> {
        // Create HTTP client with 300 second (5 minute) timeout for chunk downloads
        let http_client = Client::builder()
            .timeout(Duration::from_secs(300))
            .build()?;

        Ok(Self {
            http_client,
            sea_client,
            statement_id,
            concurrency,
        })
    }

    /// Fetch a single chunk from its external link
    ///
    /// Downloads the Arrow IPC data from cloud storage using the presigned URL.
    /// If the URL has expired (403 Forbidden), returns UrlExpired error to trigger refresh.
    ///
    /// # Arguments
    /// * `link` - External link containing the presigned URL and metadata
    ///
    /// # Returns
    /// * `Ok(Vec<u8>)` - Raw bytes of the chunk (may be LZ4 compressed)
    /// * `Err(Error::UrlExpired)` - If the presigned URL has expired (403 response)
    /// * `Err(Error::Http)` - For other HTTP errors
    ///
    /// # Example
    /// ```no_run
    /// # use adbc_driver_databricks::fetch::ChunkFetcher;
    /// # use adbc_driver_databricks::client::models::ExternalLink;
    /// # async fn example(fetcher: ChunkFetcher, link: ExternalLink) -> Result<(), Box<dyn std::error::Error>> {
    /// let data = fetcher.fetch_chunk(&link).await?;
    /// println!("Downloaded {} bytes", data.len());
    /// # Ok(())
    /// # }
    /// ```
    async fn fetch_chunk(&self, link: &ExternalLink) -> Result<Vec<u8>> {
        let response = self.http_client
            .get(&link.external_link)
            .send()
            .await?;

        // Check for 403 Forbidden - indicates expired URL
        if response.status() == reqwest::StatusCode::FORBIDDEN {
            return Err(Error::UrlExpired(link.chunk_index));
        }

        // Check for other errors
        if !response.status().is_success() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to fetch chunk {}: HTTP {}", link.chunk_index, response.status())
            )));
        }

        // Read response body
        let bytes = response.bytes().await?;
        Ok(bytes.to_vec())
    }
}

/// Convert SEA manifest schema to Arrow Schema
///
/// This converts the schema information from the Databricks SEA API response
/// into an Arrow Schema that can be used for creating Arrow arrays and record batches.
pub fn manifest_to_arrow_schema(manifest: &ManifestSchema) -> Result<SchemaRef> {
    let fields: Vec<Field> = manifest
        .columns
        .iter()
        .map(|col| {
            let data_type = spark_type_to_arrow(&col.type_text)?;
            Ok(Field::new(&col.name, data_type, col.nullable))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Arc::new(Schema::new(fields)))
}

/// Convert Spark SQL type string to Arrow DataType
///
/// Supports basic types (INT, STRING, BOOLEAN, etc.) and complex types
/// (DECIMAL, ARRAY, MAP, STRUCT) with their nested structure.
fn spark_type_to_arrow(spark_type: &str) -> Result<DataType> {
    // Normalize the type string by trimming whitespace
    let spark_type = spark_type.trim();

    // Check for complex types first (before uppercasing, to preserve field names)
    let spark_type_upper = spark_type.to_uppercase();
    if spark_type_upper.starts_with("DECIMAL") {
        return parse_decimal_type(&spark_type_upper);
    } else if spark_type_upper.starts_with("ARRAY<") {
        return parse_array_type(spark_type);  // Use original to preserve inner type case
    } else if spark_type_upper.starts_with("MAP<") {
        return parse_map_type(spark_type);  // Use original to preserve inner type case
    } else if spark_type_upper.starts_with("STRUCT<") {
        return parse_struct_type(spark_type);  // Use original to preserve field names
    }

    // Handle basic types (case-insensitive)
    match spark_type_upper.as_str() {
        "BOOLEAN" => Ok(DataType::Boolean),
        "TINYINT" | "BYTE" => Ok(DataType::Int8),
        "SMALLINT" | "SHORT" => Ok(DataType::Int16),
        "INT" | "INTEGER" => Ok(DataType::Int32),
        "BIGINT" | "LONG" => Ok(DataType::Int64),
        "FLOAT" | "REAL" => Ok(DataType::Float32),
        "DOUBLE" => Ok(DataType::Float64),
        "STRING" | "VARCHAR" | "CHAR" => Ok(DataType::Utf8),
        "BINARY" => Ok(DataType::Binary),
        "DATE" => Ok(DataType::Date32),
        "TIMESTAMP" | "TIMESTAMP_NTZ" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        // TIMESTAMP_LTZ is timestamp with local timezone, but Arrow uses UTC, so we use None
        "TIMESTAMP_LTZ" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        _ => Err(Error::Config(format!("Unsupported Spark SQL type: {}", spark_type))),
    }
}

/// Parse DECIMAL(precision, scale) type
///
/// Examples: "DECIMAL(10,2)", "DECIMAL(38, 18)"
fn parse_decimal_type(s: &str) -> Result<DataType> {
    // Extract the part between parentheses
    let start = s.find('(').ok_or_else(|| Error::Config(format!("Invalid DECIMAL type: {}", s)))?;
    let end = s.find(')').ok_or_else(|| Error::Config(format!("Invalid DECIMAL type: {}", s)))?;

    let params = &s[start + 1..end];
    let parts: Vec<&str> = params.split(',').map(|s| s.trim()).collect();

    if parts.len() != 2 {
        return Err(Error::Config(format!("Invalid DECIMAL type parameters: {}", s)));
    }

    let precision = parts[0].parse::<u8>()
        .map_err(|_| Error::Config(format!("Invalid DECIMAL precision: {}", parts[0])))?;
    let scale = parts[1].parse::<i8>()
        .map_err(|_| Error::Config(format!("Invalid DECIMAL scale: {}", parts[1])))?;

    // Arrow Decimal128 supports up to 38 digits of precision
    if precision > 38 {
        return Err(Error::Config(format!("DECIMAL precision {} exceeds maximum of 38", precision)));
    }

    Ok(DataType::Decimal128(precision, scale))
}

/// Parse ARRAY<element_type> type
///
/// Examples: "ARRAY<INT>", "ARRAY<STRING>", "ARRAY<ARRAY<INT>>"
fn parse_array_type(s: &str) -> Result<DataType> {
    // Extract the element type between < and >
    let start = s.find('<').ok_or_else(|| Error::Config(format!("Invalid ARRAY type: {}", s)))?;
    let end = find_matching_bracket(s, start)?;

    let element_type_str = &s[start + 1..end];
    let element_type = spark_type_to_arrow(element_type_str)?;

    Ok(DataType::List(Arc::new(Field::new("item", element_type, true))))
}

/// Parse MAP<key_type, value_type> type
///
/// Examples: "MAP<STRING, INT>", "MAP<INT, ARRAY<STRING>>"
fn parse_map_type(s: &str) -> Result<DataType> {
    // Extract the content between < and >
    let start = s.find('<').ok_or_else(|| Error::Config(format!("Invalid MAP type: {}", s)))?;
    let end = find_matching_bracket(s, start)?;

    let content = &s[start + 1..end];

    // Find the comma that separates key and value types
    // We need to be careful about nested types like MAP<STRING, ARRAY<INT>>
    let comma_pos = find_top_level_comma(content)?;

    let key_type_str = content[..comma_pos].trim();
    let value_type_str = content[comma_pos + 1..].trim();

    let key_type = spark_type_to_arrow(key_type_str)?;
    let value_type = spark_type_to_arrow(value_type_str)?;

    // Arrow Map type requires the key field to be non-nullable
    Ok(DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(vec![
                Field::new("key", key_type, false),
                Field::new("value", value_type, true),
            ].into()),
            false,
        )),
        false,
    ))
}

/// Parse STRUCT<field1:type1, field2:type2, ...> type
///
/// Examples: "STRUCT<name:STRING, age:INT>", "STRUCT<id:INT, tags:ARRAY<STRING>>"
fn parse_struct_type(s: &str) -> Result<DataType> {
    // Extract the content between < and >
    let start = s.find('<').ok_or_else(|| Error::Config(format!("Invalid STRUCT type: {}", s)))?;
    let end = find_matching_bracket(s, start)?;

    let content = &s[start + 1..end];

    // Parse field definitions
    let fields = parse_struct_fields(content)?;

    Ok(DataType::Struct(fields.into()))
}

/// Parse struct field definitions
///
/// Input: "name:STRING, age:INT, tags:ARRAY<STRING>"
/// Output: Vec of Fields
fn parse_struct_fields(content: &str) -> Result<Vec<Field>> {
    let mut fields = Vec::new();
    let mut current_pos = 0;

    while current_pos < content.len() {
        // Find the next field separator (comma at top level)
        let field_end = find_next_field_separator(content, current_pos)
            .unwrap_or(content.len());

        let field_def = content[current_pos..field_end].trim();

        if !field_def.is_empty() {
            // Parse field: "name:type" or "name:type:nullable"
            let colon_pos = field_def.find(':')
                .ok_or_else(|| Error::Config(format!("Invalid struct field definition: {}", field_def)))?;

            let field_name = field_def[..colon_pos].trim();
            let remaining = &field_def[colon_pos + 1..];

            // Check if there's a second colon for nullability specification
            // Most Spark types don't include this, so fields are nullable by default
            let field_type = spark_type_to_arrow(remaining)?;

            fields.push(Field::new(field_name, field_type, true));
        }

        current_pos = field_end + 1;
    }

    if fields.is_empty() {
        return Err(Error::Config("STRUCT type must have at least one field".to_string()));
    }

    Ok(fields)
}

/// Find the matching closing bracket for an opening bracket
///
/// Given a string and the position of '<', finds the matching '>'
fn find_matching_bracket(s: &str, start: usize) -> Result<usize> {
    let chars: Vec<char> = s.chars().collect();

    if chars[start] != '<' {
        return Err(Error::Config("Expected '<' at start position".to_string()));
    }

    let mut depth = 1;
    let mut pos = start + 1;

    while pos < chars.len() && depth > 0 {
        match chars[pos] {
            '<' => depth += 1,
            '>' => depth -= 1,
            _ => {}
        }
        pos += 1;
    }

    if depth != 0 {
        return Err(Error::Config(format!("Unmatched brackets in type: {}", s)));
    }

    Ok(pos - 1)
}

/// Find the top-level comma in a type definition
///
/// For "STRING, INT", returns 6 (position of comma)
/// For "ARRAY<INT>, STRING", returns 11 (skips the comma inside ARRAY)
fn find_top_level_comma(s: &str) -> Result<usize> {
    let chars: Vec<char> = s.chars().collect();
    let mut depth = 0;

    for (i, &ch) in chars.iter().enumerate() {
        match ch {
            '<' => depth += 1,
            '>' => depth -= 1,
            ',' if depth == 0 => return Ok(i),
            _ => {}
        }
    }

    Err(Error::Config(format!("No top-level comma found in: {}", s)))
}

/// Find the next field separator (comma) at top level for struct fields
///
/// Returns the position of the next comma, or None if no more commas
fn find_next_field_separator(s: &str, start: usize) -> Option<usize> {
    let chars: Vec<char> = s.chars().collect();
    let mut depth = 0;

    for i in start..chars.len() {
        match chars[i] {
            '<' => depth += 1,
            '>' => depth -= 1,
            ',' if depth == 0 => return Some(i),
            _ => {}
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::models::ColumnInfo;

    #[test]
    fn test_spark_type_mapping_basic_types() {
        // Boolean
        assert_eq!(spark_type_to_arrow("BOOLEAN").unwrap(), DataType::Boolean);

        // Integer types
        assert_eq!(spark_type_to_arrow("TINYINT").unwrap(), DataType::Int8);
        assert_eq!(spark_type_to_arrow("BYTE").unwrap(), DataType::Int8);
        assert_eq!(spark_type_to_arrow("SMALLINT").unwrap(), DataType::Int16);
        assert_eq!(spark_type_to_arrow("SHORT").unwrap(), DataType::Int16);
        assert_eq!(spark_type_to_arrow("INT").unwrap(), DataType::Int32);
        assert_eq!(spark_type_to_arrow("INTEGER").unwrap(), DataType::Int32);
        assert_eq!(spark_type_to_arrow("BIGINT").unwrap(), DataType::Int64);
        assert_eq!(spark_type_to_arrow("LONG").unwrap(), DataType::Int64);

        // Float types
        assert_eq!(spark_type_to_arrow("FLOAT").unwrap(), DataType::Float32);
        assert_eq!(spark_type_to_arrow("REAL").unwrap(), DataType::Float32);
        assert_eq!(spark_type_to_arrow("DOUBLE").unwrap(), DataType::Float64);

        // String types
        assert_eq!(spark_type_to_arrow("STRING").unwrap(), DataType::Utf8);
        assert_eq!(spark_type_to_arrow("VARCHAR").unwrap(), DataType::Utf8);
        assert_eq!(spark_type_to_arrow("CHAR").unwrap(), DataType::Utf8);

        // Binary
        assert_eq!(spark_type_to_arrow("BINARY").unwrap(), DataType::Binary);

        // Date and timestamp
        assert_eq!(spark_type_to_arrow("DATE").unwrap(), DataType::Date32);
        assert_eq!(spark_type_to_arrow("TIMESTAMP").unwrap(), DataType::Timestamp(TimeUnit::Microsecond, None));
        assert_eq!(spark_type_to_arrow("TIMESTAMP_NTZ").unwrap(), DataType::Timestamp(TimeUnit::Microsecond, None));
        assert_eq!(spark_type_to_arrow("TIMESTAMP_LTZ").unwrap(), DataType::Timestamp(TimeUnit::Microsecond, None));
    }

    #[test]
    fn test_spark_type_mapping_case_insensitive() {
        assert_eq!(spark_type_to_arrow("int").unwrap(), DataType::Int32);
        assert_eq!(spark_type_to_arrow("Int").unwrap(), DataType::Int32);
        assert_eq!(spark_type_to_arrow("INT").unwrap(), DataType::Int32);
        assert_eq!(spark_type_to_arrow("string").unwrap(), DataType::Utf8);
        assert_eq!(spark_type_to_arrow("String").unwrap(), DataType::Utf8);
        assert_eq!(spark_type_to_arrow("STRING").unwrap(), DataType::Utf8);
    }

    #[test]
    fn test_spark_type_mapping_decimal() {
        assert_eq!(spark_type_to_arrow("DECIMAL(10,2)").unwrap(), DataType::Decimal128(10, 2));
        assert_eq!(spark_type_to_arrow("DECIMAL(38,18)").unwrap(), DataType::Decimal128(38, 18));
        assert_eq!(spark_type_to_arrow("DECIMAL(10, 2)").unwrap(), DataType::Decimal128(10, 2));
        assert_eq!(spark_type_to_arrow("DECIMAL(5,0)").unwrap(), DataType::Decimal128(5, 0));
    }

    #[test]
    fn test_spark_type_mapping_decimal_invalid() {
        // Invalid precision (> 38)
        assert!(spark_type_to_arrow("DECIMAL(39,2)").is_err());

        // Invalid format
        assert!(spark_type_to_arrow("DECIMAL(10)").is_err());
        assert!(spark_type_to_arrow("DECIMAL").is_err());
        assert!(spark_type_to_arrow("DECIMAL(abc,2)").is_err());
    }

    #[test]
    fn test_spark_type_mapping_array() {
        // Simple array
        let result = spark_type_to_arrow("ARRAY<INT>").unwrap();
        match result {
            DataType::List(field) => {
                assert_eq!(field.name(), "item");
                assert_eq!(field.data_type(), &DataType::Int32);
            }
            _ => panic!("Expected List type"),
        }

        // Array of strings
        let result = spark_type_to_arrow("ARRAY<STRING>").unwrap();
        match result {
            DataType::List(field) => {
                assert_eq!(field.data_type(), &DataType::Utf8);
            }
            _ => panic!("Expected List type"),
        }

        // Nested array
        let result = spark_type_to_arrow("ARRAY<ARRAY<INT>>").unwrap();
        match result {
            DataType::List(outer_field) => {
                match outer_field.data_type() {
                    DataType::List(inner_field) => {
                        assert_eq!(inner_field.data_type(), &DataType::Int32);
                    }
                    _ => panic!("Expected nested List type"),
                }
            }
            _ => panic!("Expected List type"),
        }
    }

    #[test]
    fn test_spark_type_mapping_map() {
        // Simple map
        let result = spark_type_to_arrow("MAP<STRING, INT>").unwrap();
        match result {
            DataType::Map(field, _) => {
                match field.data_type() {
                    DataType::Struct(fields) => {
                        assert_eq!(fields.len(), 2);
                        assert_eq!(fields[0].name(), "key");
                        assert_eq!(fields[0].data_type(), &DataType::Utf8);
                        assert_eq!(fields[1].name(), "value");
                        assert_eq!(fields[1].data_type(), &DataType::Int32);
                    }
                    _ => panic!("Expected Struct type in Map"),
                }
            }
            _ => panic!("Expected Map type"),
        }

        // Map with complex value type
        let result = spark_type_to_arrow("MAP<INT, ARRAY<STRING>>").unwrap();
        match result {
            DataType::Map(field, _) => {
                match field.data_type() {
                    DataType::Struct(fields) => {
                        assert_eq!(fields[0].data_type(), &DataType::Int32);
                        match fields[1].data_type() {
                            DataType::List(inner) => {
                                assert_eq!(inner.data_type(), &DataType::Utf8);
                            }
                            _ => panic!("Expected List type in Map value"),
                        }
                    }
                    _ => panic!("Expected Struct type in Map"),
                }
            }
            _ => panic!("Expected Map type"),
        }
    }

    #[test]
    fn test_spark_type_mapping_struct() {
        // Simple struct
        let result = spark_type_to_arrow("STRUCT<name:STRING, age:INT>").unwrap();
        match result {
            DataType::Struct(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].name(), "name");
                assert_eq!(fields[0].data_type(), &DataType::Utf8);
                assert_eq!(fields[1].name(), "age");
                assert_eq!(fields[1].data_type(), &DataType::Int32);
            }
            _ => panic!("Expected Struct type"),
        }

        // Struct with complex nested type
        let result = spark_type_to_arrow("STRUCT<id:INT, tags:ARRAY<STRING>>").unwrap();
        match result {
            DataType::Struct(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].name(), "id");
                assert_eq!(fields[0].data_type(), &DataType::Int32);
                assert_eq!(fields[1].name(), "tags");
                match fields[1].data_type() {
                    DataType::List(inner) => {
                        assert_eq!(inner.data_type(), &DataType::Utf8);
                    }
                    _ => panic!("Expected List type in Struct field"),
                }
            }
            _ => panic!("Expected Struct type"),
        }

        // Struct with multiple nested complex types
        let result = spark_type_to_arrow("STRUCT<name:STRING, scores:ARRAY<INT>, metadata:MAP<STRING, STRING>>").unwrap();
        match result {
            DataType::Struct(fields) => {
                assert_eq!(fields.len(), 3);
                assert_eq!(fields[0].name(), "name");
                assert_eq!(fields[1].name(), "scores");
                assert_eq!(fields[2].name(), "metadata");
            }
            _ => panic!("Expected Struct type"),
        }
    }

    #[test]
    fn test_spark_type_mapping_unsupported() {
        assert!(spark_type_to_arrow("UNKNOWN_TYPE").is_err());
        assert!(spark_type_to_arrow("CUSTOM").is_err());
    }

    #[test]
    fn test_manifest_to_arrow_schema() {
        let manifest = ManifestSchema {
            columns: vec![
                ColumnInfo {
                    name: "id".to_string(),
                    type_name: "INT".to_string(),
                    type_text: "INT".to_string(),
                    position: 0,
                    nullable: false,
                },
                ColumnInfo {
                    name: "name".to_string(),
                    type_name: "STRING".to_string(),
                    type_text: "STRING".to_string(),
                    position: 1,
                    nullable: true,
                },
                ColumnInfo {
                    name: "price".to_string(),
                    type_name: "DECIMAL".to_string(),
                    type_text: "DECIMAL(10,2)".to_string(),
                    position: 2,
                    nullable: true,
                },
            ],
        };

        let schema = manifest_to_arrow_schema(&manifest).unwrap();

        assert_eq!(schema.fields().len(), 3);

        let field0 = &schema.fields()[0];
        assert_eq!(field0.name(), "id");
        assert_eq!(field0.data_type(), &DataType::Int32);
        assert!(!field0.is_nullable());

        let field1 = &schema.fields()[1];
        assert_eq!(field1.name(), "name");
        assert_eq!(field1.data_type(), &DataType::Utf8);
        assert!(field1.is_nullable());

        let field2 = &schema.fields()[2];
        assert_eq!(field2.name(), "price");
        assert_eq!(field2.data_type(), &DataType::Decimal128(10, 2));
        assert!(field2.is_nullable());
    }

    #[test]
    fn test_manifest_to_arrow_schema_complex_types() {
        let manifest = ManifestSchema {
            columns: vec![
                ColumnInfo {
                    name: "tags".to_string(),
                    type_name: "ARRAY".to_string(),
                    type_text: "ARRAY<STRING>".to_string(),
                    position: 0,
                    nullable: true,
                },
                ColumnInfo {
                    name: "metadata".to_string(),
                    type_name: "MAP".to_string(),
                    type_text: "MAP<STRING, INT>".to_string(),
                    position: 1,
                    nullable: true,
                },
            ],
        };

        let schema = manifest_to_arrow_schema(&manifest).unwrap();
        assert_eq!(schema.fields().len(), 2);

        // Verify array field
        match schema.fields()[0].data_type() {
            DataType::List(field) => {
                assert_eq!(field.data_type(), &DataType::Utf8);
            }
            _ => panic!("Expected List type"),
        }

        // Verify map field
        match schema.fields()[1].data_type() {
            DataType::Map(_, _) => {}
            _ => panic!("Expected Map type"),
        }
    }

    #[test]
    fn test_parse_decimal_type_edge_cases() {
        // Maximum precision
        assert_eq!(parse_decimal_type("DECIMAL(38,0)").unwrap(), DataType::Decimal128(38, 0));

        // Minimum precision
        assert_eq!(parse_decimal_type("DECIMAL(1,0)").unwrap(), DataType::Decimal128(1, 0));

        // Negative scale
        assert_eq!(parse_decimal_type("DECIMAL(10,-2)").unwrap(), DataType::Decimal128(10, -2));
    }

    #[test]
    fn test_find_matching_bracket() {
        let s = "ARRAY<INT>";
        let start = s.find('<').unwrap();
        let end = find_matching_bracket(s, start).unwrap();
        assert_eq!(&s[start + 1..end], "INT");

        let s = "MAP<STRING, ARRAY<INT>>";
        let start = s.find('<').unwrap();
        let end = find_matching_bracket(s, start).unwrap();
        assert_eq!(&s[start + 1..end], "STRING, ARRAY<INT>");

        let s = "ARRAY<ARRAY<INT>>";
        let start = s.find('<').unwrap();
        let end = find_matching_bracket(s, start).unwrap();
        assert_eq!(&s[start + 1..end], "ARRAY<INT>");
    }

    #[test]
    fn test_find_top_level_comma() {
        assert_eq!(find_top_level_comma("STRING, INT").unwrap(), 6);
        assert_eq!(find_top_level_comma("ARRAY<INT>, STRING").unwrap(), 10);
        assert_eq!(find_top_level_comma("MAP<STRING, INT>, BIGINT").unwrap(), 16);
    }

    #[test]
    fn test_parse_struct_fields() {
        let fields = parse_struct_fields("name:STRING, age:INT").unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name(), "name");
        assert_eq!(fields[0].data_type(), &DataType::Utf8);
        assert_eq!(fields[1].name(), "age");
        assert_eq!(fields[1].data_type(), &DataType::Int32);

        // With nested types
        let fields = parse_struct_fields("id:INT, tags:ARRAY<STRING>, metadata:MAP<STRING, INT>").unwrap();
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0].name(), "id");
        assert_eq!(fields[1].name(), "tags");
        assert_eq!(fields[2].name(), "metadata");
    }

    #[test]
    fn test_whitespace_handling() {
        // Type names with extra whitespace
        assert_eq!(spark_type_to_arrow("  INT  ").unwrap(), DataType::Int32);
        assert_eq!(spark_type_to_arrow("DECIMAL(10, 2)").unwrap(), DataType::Decimal128(10, 2));

        // Struct with whitespace
        let result = spark_type_to_arrow("STRUCT< name : STRING , age : INT >").unwrap();
        match result {
            DataType::Struct(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].name(), "name");
                assert_eq!(fields[1].name(), "age");
            }
            _ => panic!("Expected Struct type"),
        }
    }

    // === ChunkFetcher Tests ===

    #[test]
    fn test_chunk_fetcher_new() {
        use crate::client::{SeaClient, SeaClientConfig};

        let config = SeaClientConfig {
            host: "https://test.cloud.databricks.com".to_string(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let sea_client = Arc::new(SeaClient::new(config).unwrap());
        let fetcher = ChunkFetcher::new(
            sea_client,
            "stmt-123".to_string(),
            8,
        );

        assert!(fetcher.is_ok());
        let fetcher = fetcher.unwrap();
        assert_eq!(fetcher.statement_id, "stmt-123");
        assert_eq!(fetcher.concurrency, 8);
    }

    #[test]
    fn test_chunk_fetcher_new_custom_concurrency() {
        use crate::client::{SeaClient, SeaClientConfig};

        let config = SeaClientConfig {
            host: "https://test.cloud.databricks.com".to_string(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let sea_client = Arc::new(SeaClient::new(config).unwrap());

        // Test with different concurrency values
        let fetcher = ChunkFetcher::new(sea_client.clone(), "stmt-1".to_string(), 4).unwrap();
        assert_eq!(fetcher.concurrency, 4);

        let fetcher = ChunkFetcher::new(sea_client.clone(), "stmt-2".to_string(), 16).unwrap();
        assert_eq!(fetcher.concurrency, 16);
    }

    #[tokio::test]
    async fn test_fetch_chunk_success() {
        use crate::client::{SeaClient, SeaClientConfig};
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path};

        let mock_server = MockServer::start().await;

        // Mock successful chunk download
        Mock::given(method("GET"))
            .and(path("/chunk0"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![1, 2, 3, 4, 5]))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: "https://test.cloud.databricks.com".to_string(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let sea_client = Arc::new(SeaClient::new(config).unwrap());
        let fetcher = ChunkFetcher::new(
            sea_client,
            "stmt-123".to_string(),
            8,
        ).unwrap();

        let link = ExternalLink {
            external_link: format!("{}/chunk0", mock_server.uri()),
            chunk_index: 0,
            row_offset: 0,
            row_count: 100,
            byte_count: 5,
            expiration: "2025-12-31T23:59:59Z".to_string(),
        };

        let result = fetcher.fetch_chunk(&link).await;
        assert!(result.is_ok());
        let data = result.unwrap();
        assert_eq!(data, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn test_fetch_chunk_403_forbidden() {
        use crate::client::{SeaClient, SeaClientConfig};
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path};

        let mock_server = MockServer::start().await;

        // Mock 403 Forbidden response (expired URL)
        Mock::given(method("GET"))
            .and(path("/expired-chunk"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: "https://test.cloud.databricks.com".to_string(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let sea_client = Arc::new(SeaClient::new(config).unwrap());
        let fetcher = ChunkFetcher::new(
            sea_client,
            "stmt-456".to_string(),
            8,
        ).unwrap();

        let link = ExternalLink {
            external_link: format!("{}/expired-chunk", mock_server.uri()),
            chunk_index: 5,
            row_offset: 50000,
            row_count: 10000,
            byte_count: 1048576,
            expiration: "2024-01-01T00:00:00Z".to_string(),
        };

        let result = fetcher.fetch_chunk(&link).await;
        assert!(result.is_err());

        match result.unwrap_err() {
            Error::UrlExpired(chunk_index) => {
                assert_eq!(chunk_index, 5);
            }
            e => panic!("Expected UrlExpired error, got {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_fetch_chunk_404_not_found() {
        use crate::client::{SeaClient, SeaClientConfig};
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path};

        let mock_server = MockServer::start().await;

        // Mock 404 Not Found response
        Mock::given(method("GET"))
            .and(path("/missing-chunk"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: "https://test.cloud.databricks.com".to_string(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let sea_client = Arc::new(SeaClient::new(config).unwrap());
        let fetcher = ChunkFetcher::new(
            sea_client,
            "stmt-789".to_string(),
            8,
        ).unwrap();

        let link = ExternalLink {
            external_link: format!("{}/missing-chunk", mock_server.uri()),
            chunk_index: 0,
            row_offset: 0,
            row_count: 100,
            byte_count: 1000,
            expiration: "2025-12-31T23:59:59Z".to_string(),
        };

        let result = fetcher.fetch_chunk(&link).await;
        assert!(result.is_err());

        match result.unwrap_err() {
            Error::Io(e) => {
                assert!(e.to_string().contains("404"));
                assert!(e.to_string().contains("chunk 0"));
            }
            e => panic!("Expected Io error, got {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_fetch_chunk_500_internal_error() {
        use crate::client::{SeaClient, SeaClientConfig};
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path};

        let mock_server = MockServer::start().await;

        // Mock 500 Internal Server Error
        Mock::given(method("GET"))
            .and(path("/error-chunk"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: "https://test.cloud.databricks.com".to_string(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let sea_client = Arc::new(SeaClient::new(config).unwrap());
        let fetcher = ChunkFetcher::new(
            sea_client,
            "stmt-error".to_string(),
            8,
        ).unwrap();

        let link = ExternalLink {
            external_link: format!("{}/error-chunk", mock_server.uri()),
            chunk_index: 3,
            row_offset: 30000,
            row_count: 10000,
            byte_count: 1048576,
            expiration: "2025-12-31T23:59:59Z".to_string(),
        };

        let result = fetcher.fetch_chunk(&link).await;
        assert!(result.is_err());

        match result.unwrap_err() {
            Error::Io(e) => {
                assert!(e.to_string().contains("500"));
                assert!(e.to_string().contains("chunk 3"));
            }
            e => panic!("Expected Io error, got {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_fetch_chunk_large_data() {
        use crate::client::{SeaClient, SeaClientConfig};
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path};

        let mock_server = MockServer::start().await;

        // Create 1MB of test data
        let large_data = vec![42u8; 1_048_576];

        Mock::given(method("GET"))
            .and(path("/large-chunk"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(large_data.clone()))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: "https://test.cloud.databricks.com".to_string(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let sea_client = Arc::new(SeaClient::new(config).unwrap());
        let fetcher = ChunkFetcher::new(
            sea_client,
            "stmt-large".to_string(),
            8,
        ).unwrap();

        let link = ExternalLink {
            external_link: format!("{}/large-chunk", mock_server.uri()),
            chunk_index: 0,
            row_offset: 0,
            row_count: 100000,
            byte_count: 1048576,
            expiration: "2025-12-31T23:59:59Z".to_string(),
        };

        let result = fetcher.fetch_chunk(&link).await;
        assert!(result.is_ok());
        let data = result.unwrap();
        assert_eq!(data.len(), 1_048_576);
        assert_eq!(data, large_data);
    }

    #[tokio::test]
    async fn test_fetch_chunk_empty_response() {
        use crate::client::{SeaClient, SeaClientConfig};
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path};

        let mock_server = MockServer::start().await;

        // Mock empty response (0 bytes)
        Mock::given(method("GET"))
            .and(path("/empty-chunk"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![]))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: "https://test.cloud.databricks.com".to_string(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let sea_client = Arc::new(SeaClient::new(config).unwrap());
        let fetcher = ChunkFetcher::new(
            sea_client,
            "stmt-empty".to_string(),
            8,
        ).unwrap();

        let link = ExternalLink {
            external_link: format!("{}/empty-chunk", mock_server.uri()),
            chunk_index: 0,
            row_offset: 0,
            row_count: 0,
            byte_count: 0,
            expiration: "2025-12-31T23:59:59Z".to_string(),
        };

        let result = fetcher.fetch_chunk(&link).await;
        assert!(result.is_ok());
        let data = result.unwrap();
        assert_eq!(data.len(), 0);
    }

    #[test]
    fn test_chunk_fetcher_concurrency_boundary_values() {
        use crate::client::{SeaClient, SeaClientConfig};

        let config = SeaClientConfig {
            host: "https://test.cloud.databricks.com".to_string(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let sea_client = Arc::new(SeaClient::new(config).unwrap());

        // Test with concurrency = 1 (minimum)
        let fetcher = ChunkFetcher::new(sea_client.clone(), "stmt-1".to_string(), 1);
        assert!(fetcher.is_ok());
        assert_eq!(fetcher.unwrap().concurrency, 1);

        // Test with concurrency = 100 (very high)
        let fetcher = ChunkFetcher::new(sea_client.clone(), "stmt-2".to_string(), 100);
        assert!(fetcher.is_ok());
        assert_eq!(fetcher.unwrap().concurrency, 100);

        // Test with concurrency = 0 (edge case - should still work)
        let fetcher = ChunkFetcher::new(sea_client.clone(), "stmt-3".to_string(), 0);
        assert!(fetcher.is_ok());
        assert_eq!(fetcher.unwrap().concurrency, 0);
    }
}
