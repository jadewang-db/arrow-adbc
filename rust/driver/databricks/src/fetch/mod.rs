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
use futures::Stream;
use reqwest::Client;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};

/// Fetches result chunks from external links in parallel
///
/// The ChunkFetcher is responsible for downloading Arrow data from cloud storage
/// using presigned URLs (external links). It supports:
/// - Parallel chunk fetching with configurable concurrency
/// - Automatic URL refresh when presigned URLs expire (403 Forbidden)
/// - Shared cache for refreshed URLs to minimize API calls
/// - LZ4 decompression of downloaded data
#[derive(Clone)]
pub struct ChunkFetcher {
    /// HTTP client for downloading chunks from cloud storage
    http_client: Client,
    /// SEA client for refreshing expired URLs
    sea_client: Arc<SeaClient>,
    /// Statement ID for this result set
    statement_id: String,
    /// Maximum number of concurrent chunk downloads
    concurrency: usize,
    /// Shared cache of refreshed URLs
    /// Key: chunk_index, Value: refreshed ExternalLink
    /// This cache is shared among all workers via Arc<RwLock>
    refreshed_urls: Arc<RwLock<HashMap<i32, ExternalLink>>>,
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
            refreshed_urls: Arc::new(RwLock::new(HashMap::new())),
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

    /// Fetch a chunk with automatic retry on URL expiration
    ///
    /// Implements retry logic with shared cache for refreshed URLs:
    /// - On UrlExpired error: Check cache, refresh if needed, update cache, retry
    /// - On other retryable errors: Use exponential backoff
    /// - Maximum retry attempts: 3
    ///
    /// # Arguments
    /// * `link` - External link containing the presigned URL and metadata
    ///
    /// # Returns
    /// * `Ok(Vec<u8>)` - Raw bytes of the chunk (may be LZ4 compressed)
    /// * `Err(Error)` - If fetch fails after all retries
    async fn fetch_chunk_with_retry(&self, link: &ExternalLink) -> Result<Vec<u8>> {
        const MAX_RETRIES: u32 = 3;
        let mut current_link = link.clone();

        for attempt in 0..MAX_RETRIES {
            match self.fetch_chunk(&current_link).await {
                Ok(data) => return Ok(data),

                Err(Error::UrlExpired(chunk_index)) => {
                    // Step 1: Check cache for refreshed URL
                    {
                        let cache = self.refreshed_urls.read().await;
                        if let Some(cached_link) = cache.get(&chunk_index) {
                            current_link = cached_link.clone();
                            continue; // Retry with cached URL
                        }
                    }

                    // Step 2: Cache miss, call refresh API
                    // Note: Multiple workers might reach here, but that's OK
                    // The API call is idempotent and returns same URLs
                    let refreshed_links = self.refresh_chunk_links(chunk_index).await?;

                    // Step 3: Update cache with ALL returned URLs
                    {
                        let mut cache = self.refreshed_urls.write().await;
                        for refreshed_link in refreshed_links {
                            cache.insert(refreshed_link.chunk_index, refreshed_link);
                        }
                    }

                    // Step 4: Get our refreshed URL from cache
                    {
                        let cache = self.refreshed_urls.read().await;
                        current_link = cache
                            .get(&chunk_index)
                            .ok_or_else(|| {
                                Error::StatementFailed("Refreshed URL not found".into())
                            })?
                            .clone();
                    }

                    // Retry with refreshed URL
                    continue;
                }

                Err(e) if e.is_retryable() && attempt < MAX_RETRIES - 1 => {
                    // Exponential backoff: 1s, 2s, 4s
                    tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
                    continue;
                }

                Err(e) => return Err(e),
            }
        }

        Err(Error::StatementFailed("Max retries exceeded".into()))
    }

    /// Refresh URLs by calling get_chunk API
    ///
    /// Returns ALL external links returned by the API (may be multiple).
    /// The API may return links for multiple chunks (e.g., when requesting chunk 5,
    /// it might return refreshed URLs for chunks [5, 6, 7, 8, 9, 10]).
    ///
    /// # Arguments
    /// * `chunk_index` - The chunk index that needs URL refresh
    ///
    /// # Returns
    /// * `Ok(Vec<ExternalLink>)` - All refreshed external links from the response
    /// * `Err(Error)` - If the API call fails
    async fn refresh_chunk_links(&self, chunk_index: i32) -> Result<Vec<ExternalLink>> {
        let response = self
            .sea_client
            .get_chunk(&self.statement_id, chunk_index)
            .await?;

        // API may return multiple refreshed URLs, not just the one we asked for
        Ok(response.external_links)
    }

    /// Fetch chunks using worker pool pattern
    ///
    /// Workers continuously pull from queue in order (0, 1, 2, ...) and download them.
    /// Chunks are yielded in sequential order regardless of completion order.
    ///
    /// # Architecture
    /// - Fixed number of worker tasks (num_workers = concurrency.min(total_chunks))
    /// - Workers pull chunks from ordered queue (MPSC channel with buffer)
    /// - Workers are NEVER blocked by:
    ///   1. Ordering logic (runs in separate task via channels)
    ///   2. Slow downloads (each worker is independent)
    ///   3. URL refresh (handled per-worker, doesn't affect others)
    /// - Ordering runs in separate async task, buffering out-of-order chunks
    ///
    /// # Arguments
    /// * `links` - Vector of external links to fetch (will be sorted by chunk_index)
    ///
    /// # Returns
    /// * `Ok(impl Stream<Item = Result<Vec<u8>>>)` - Stream that yields chunks in order (0, 1, 2, ...)
    /// * `Err(Error)` - If setup fails
    ///
    /// # Example
    /// ```no_run
    /// # use adbc_driver_databricks::fetch::ChunkFetcher;
    /// # use adbc_driver_databricks::client::models::ExternalLink;
    /// # use futures::{StreamExt, pin_mut};
    /// # async fn example(fetcher: ChunkFetcher, links: Vec<ExternalLink>) -> Result<(), Box<dyn std::error::Error>> {
    /// let stream = fetcher.fetch_chunks_ordered(links).await?;
    /// pin_mut!(stream);
    /// while let Some(result) = stream.next().await {
    ///     let chunk_data = result?;
    ///     println!("Got chunk with {} bytes", chunk_data.len());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn fetch_chunks_ordered(
        &self,
        links: Vec<ExternalLink>,
    ) -> Result<impl Stream<Item = Result<Vec<u8>>>> {
        let total_chunks = links.len();
        let num_workers = self.concurrency.min(total_chunks);

        // Create channels with buffering to prevent blocking
        // Buffer size = max(1, num_workers * 2) to handle empty case
        let buffer_size = std::cmp::max(1, num_workers * 2);
        let (work_tx, work_rx) = mpsc::channel::<ExternalLink>(buffer_size);
        let (result_tx, result_rx) = mpsc::channel::<(usize, Result<Vec<u8>>)>(buffer_size);

        // Sort links by chunk_index to ensure ordered queue feeding
        let mut sorted_links = links;
        sorted_links.sort_by_key(|link| link.chunk_index);

        // Spawn work producer (feeds chunks in order to queue)
        let work_producer = tokio::spawn(async move {
            for link in sorted_links {
                if work_tx.send(link).await.is_err() {
                    break; // Receiver dropped
                }
            }
        });

        // Spawn independent worker pool
        // Key: Each worker is INDEPENDENT and NEVER waits for other workers
        // Use Arc<Mutex<Receiver>> to share the receiver among workers
        let work_rx = Arc::new(tokio::sync::Mutex::new(work_rx));
        let fetcher = Arc::new(self.clone());
        let mut worker_handles = Vec::new();

        for _worker_id in 0..num_workers {
            let work_rx_clone = work_rx.clone();
            let result_tx_clone = result_tx.clone();
            let fetcher_clone = fetcher.clone();

            let handle = tokio::spawn(async move {
                // Worker loop: pull → download → send result → repeat
                // NEVER blocked by other workers or ordering logic
                loop {
                    // Lock receiver, pull one item, then immediately unlock
                    let link = {
                        let mut rx = work_rx_clone.lock().await;
                        rx.recv().await
                    };

                    match link {
                        Some(link) => {
                            let chunk_index = link.chunk_index as usize;

                            // fetch_chunk_with_retry handles URL refresh internally
                            // If URL expires, THIS worker refreshes and retries
                            // Other workers continue unaffected
                            let result = fetcher_clone.fetch_chunk_with_retry(&link).await;

                            // Send to buffered channel (non-blocking with capacity)
                            if result_tx_clone.send((chunk_index, result)).await.is_err() {
                                break; // Receiver dropped
                            }

                            // Immediately pull next chunk from queue!
                            // No waiting for ordering or other workers
                        }
                        None => break, // Channel closed
                    }
                }
            });

            worker_handles.push(handle);
        }

        // Drop original sender so workers can complete
        drop(result_tx);

        // Create ordered output stream
        // This runs in SEPARATE task, doesn't block workers
        let ordered_stream = Self::create_ordered_stream(
            result_rx,
            total_chunks,
            work_producer,
            worker_handles,
        );

        Ok(ordered_stream)
    }

    /// Create a stream that yields chunks in order
    ///
    /// Key insight: Workers download at different speeds, so chunks complete out of order.
    /// This function ensures output is always sequential (0, 1, 2, ...) by:
    /// 1. Buffering out-of-order chunks
    /// 2. Only yielding when the next expected chunk is ready
    /// 3. Cascading yields when a blocking chunk arrives
    ///
    /// # Example Flow
    /// If chunks complete in order [2, 0, 3, 1, 4]:
    ///   - Chunk 2 arrives → buffer[2] = data, wait (need chunk 0 first)
    ///   - Chunk 0 arrives → buffer[0] = data, yield 0 immediately
    ///   - Chunk 3 arrives → buffer[3] = data, wait (need chunk 1)
    ///   - Chunk 1 arrives → buffer[1] = data, yield 1, then yield 2, then yield 3 (cascade!)
    ///   - Chunk 4 arrives → buffer[4] = data, yield 4
    ///   Result: Output order is always 0, 1, 2, 3, 4
    ///
    /// # Arguments
    /// * `result_rx` - Channel receiver for chunk results from workers
    /// * `total_chunks` - Total number of chunks expected
    /// * `work_producer` - Handle to the work producer task
    /// * `worker_handles` - Handles to all worker tasks
    ///
    /// # Returns
    /// Stream that yields chunks in sequential order
    fn create_ordered_stream(
        mut result_rx: mpsc::Receiver<(usize, Result<Vec<u8>>)>,
        total_chunks: usize,
        work_producer: tokio::task::JoinHandle<()>,
        worker_handles: Vec<tokio::task::JoinHandle<()>>,
    ) -> impl Stream<Item = Result<Vec<u8>>> {
        async_stream::stream! {
            // Buffer to hold chunks until we can yield them in order
            // Initialize with None for each position
            let mut buffer: Vec<Option<Result<Vec<u8>>>> = (0..total_chunks).map(|_| None).collect();
            let mut next_to_yield = 0;  // Next chunk index we need to output
            let mut received_count = 0;

            // Receive results from workers (may arrive out of order)
            while let Some((index, result)) = result_rx.recv().await {
                // Store this chunk in the buffer
                buffer[index] = Some(result);
                received_count += 1;

                // Try to yield all consecutive chunks that are now ready
                // This handles cascading yields when a missing chunk arrives
                while next_to_yield < total_chunks {
                    if let Some(result) = buffer[next_to_yield].take() {
                        yield result;  // Yield in order!
                        next_to_yield += 1;
                    } else {
                        break; // This chunk not ready yet, wait for it
                    }
                }

                if received_count == total_chunks {
                    break;  // All chunks received
                }
            }

            // Yield any remaining buffered chunks
            while next_to_yield < total_chunks {
                if let Some(result) = buffer[next_to_yield].take() {
                    yield result;
                    next_to_yield += 1;
                } else {
                    yield Err(Error::StatementFailed(
                        format!("Missing chunk {}", next_to_yield).into()
                    ));
                    break;
                }
            }

            // Wait for all tasks to complete
            let _ = work_producer.await;
            for handle in worker_handles {
                let _ = handle.await;
            }
        }
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

    // === Worker Pool Pattern Tests ===

    #[tokio::test]
    async fn test_fetch_chunks_ordered_basic() {
        use crate::client::{SeaClient, SeaClientConfig};
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path};
        use futures::{StreamExt, pin_mut};

        let mock_server = MockServer::start().await;

        // Mock 3 chunks
        Mock::given(method("GET"))
            .and(path("/chunk0"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![0, 0, 0]))
            .mount(&mock_server)
            .await;

        Mock::given(method("GET"))
            .and(path("/chunk1"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![1, 1, 1]))
            .mount(&mock_server)
            .await;

        Mock::given(method("GET"))
            .and(path("/chunk2"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![2, 2, 2]))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: "https://test.cloud.databricks.com".to_string(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let sea_client = Arc::new(SeaClient::new(config).unwrap());
        let fetcher = ChunkFetcher::new(sea_client, "stmt-ordered".to_string(), 2).unwrap();

        let links = vec![
            ExternalLink {
                external_link: format!("{}/chunk0", mock_server.uri()),
                chunk_index: 0,
                row_offset: 0,
                row_count: 100,
                byte_count: 3,
                expiration: "2025-12-31T23:59:59Z".to_string(),
            },
            ExternalLink {
                external_link: format!("{}/chunk1", mock_server.uri()),
                chunk_index: 1,
                row_offset: 100,
                row_count: 100,
                byte_count: 3,
                expiration: "2025-12-31T23:59:59Z".to_string(),
            },
            ExternalLink {
                external_link: format!("{}/chunk2", mock_server.uri()),
                chunk_index: 2,
                row_offset: 200,
                row_count: 100,
                byte_count: 3,
                expiration: "2025-12-31T23:59:59Z".to_string(),
            },
        ];

        let stream = fetcher.fetch_chunks_ordered(links).await.unwrap();
        pin_mut!(stream);
        pin_mut!(stream);
        let mut chunks = Vec::new();

        while let Some(result) = stream.next().await {
            chunks.push(result.unwrap());
        }

        // Verify we got all chunks in order
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0], vec![0, 0, 0]);
        assert_eq!(chunks[1], vec![1, 1, 1]);
        assert_eq!(chunks[2], vec![2, 2, 2]);
    }

    #[tokio::test]
    async fn test_fetch_chunks_ordered_unsorted_input() {
        use crate::client::{SeaClient, SeaClientConfig};
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path};
        use futures::{StreamExt, pin_mut};

        let mock_server = MockServer::start().await;

        // Mock 3 chunks
        for i in 0..3 {
            Mock::given(method("GET"))
                .and(path(format!("/chunk{}", i)))
                .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![i as u8; 3]))
                .mount(&mock_server)
                .await;
        }

        let config = SeaClientConfig {
            host: "https://test.cloud.databricks.com".to_string(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let sea_client = Arc::new(SeaClient::new(config).unwrap());
        let fetcher = ChunkFetcher::new(sea_client, "stmt-unsorted".to_string(), 2).unwrap();

        // Provide links in REVERSE order (2, 1, 0)
        let links = vec![
            ExternalLink {
                external_link: format!("{}/chunk2", mock_server.uri()),
                chunk_index: 2,
                row_offset: 200,
                row_count: 100,
                byte_count: 3,
                expiration: "2025-12-31T23:59:59Z".to_string(),
            },
            ExternalLink {
                external_link: format!("{}/chunk1", mock_server.uri()),
                chunk_index: 1,
                row_offset: 100,
                row_count: 100,
                byte_count: 3,
                expiration: "2025-12-31T23:59:59Z".to_string(),
            },
            ExternalLink {
                external_link: format!("{}/chunk0", mock_server.uri()),
                chunk_index: 0,
                row_offset: 0,
                row_count: 100,
                byte_count: 3,
                expiration: "2025-12-31T23:59:59Z".to_string(),
            },
        ];

        let stream = fetcher.fetch_chunks_ordered(links).await.unwrap();
        pin_mut!(stream);
        let mut chunks = Vec::new();

        while let Some(result) = stream.next().await {
            chunks.push(result.unwrap());
        }

        // Should still get chunks in correct order (0, 1, 2)
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0], vec![0, 0, 0]);
        assert_eq!(chunks[1], vec![1, 1, 1]);
        assert_eq!(chunks[2], vec![2, 2, 2]);
    }

    #[tokio::test]
    async fn test_fetch_chunks_ordered_variable_delays() {
        use crate::client::{SeaClient, SeaClientConfig};
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path};
        use futures::{StreamExt, pin_mut};
        use std::time::Duration;

        let mock_server = MockServer::start().await;

        // Mock chunk 0 with 100ms delay (slow)
        Mock::given(method("GET"))
            .and(path("/chunk0"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(vec![0, 0, 0])
                    .set_delay(Duration::from_millis(100))
            )
            .mount(&mock_server)
            .await;

        // Mock chunk 1 with no delay (fast)
        Mock::given(method("GET"))
            .and(path("/chunk1"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![1, 1, 1]))
            .mount(&mock_server)
            .await;

        // Mock chunk 2 with 50ms delay (medium)
        Mock::given(method("GET"))
            .and(path("/chunk2"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(vec![2, 2, 2])
                    .set_delay(Duration::from_millis(50))
            )
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: "https://test.cloud.databricks.com".to_string(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let sea_client = Arc::new(SeaClient::new(config).unwrap());
        let fetcher = ChunkFetcher::new(sea_client, "stmt-delays".to_string(), 3).unwrap();

        let links = vec![
            ExternalLink {
                external_link: format!("{}/chunk0", mock_server.uri()),
                chunk_index: 0,
                row_offset: 0,
                row_count: 100,
                byte_count: 3,
                expiration: "2025-12-31T23:59:59Z".to_string(),
            },
            ExternalLink {
                external_link: format!("{}/chunk1", mock_server.uri()),
                chunk_index: 1,
                row_offset: 100,
                row_count: 100,
                byte_count: 3,
                expiration: "2025-12-31T23:59:59Z".to_string(),
            },
            ExternalLink {
                external_link: format!("{}/chunk2", mock_server.uri()),
                chunk_index: 2,
                row_offset: 200,
                row_count: 100,
                byte_count: 3,
                expiration: "2025-12-31T23:59:59Z".to_string(),
            },
        ];

        let stream = fetcher.fetch_chunks_ordered(links).await.unwrap();
        pin_mut!(stream);
        let mut chunks = Vec::new();

        while let Some(result) = stream.next().await {
            chunks.push(result.unwrap());
        }

        // Even though completion order is likely [1, 2, 0], output should be [0, 1, 2]
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0], vec![0, 0, 0]);
        assert_eq!(chunks[1], vec![1, 1, 1]);
        assert_eq!(chunks[2], vec![2, 2, 2]);
    }

    #[tokio::test]
    async fn test_fetch_chunks_ordered_single_chunk() {
        use crate::client::{SeaClient, SeaClientConfig};
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path};
        use futures::{StreamExt, pin_mut};

        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/chunk0"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![42, 42, 42]))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: "https://test.cloud.databricks.com".to_string(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let sea_client = Arc::new(SeaClient::new(config).unwrap());
        let fetcher = ChunkFetcher::new(sea_client, "stmt-single".to_string(), 8).unwrap();

        let links = vec![
            ExternalLink {
                external_link: format!("{}/chunk0", mock_server.uri()),
                chunk_index: 0,
                row_offset: 0,
                row_count: 100,
                byte_count: 3,
                expiration: "2025-12-31T23:59:59Z".to_string(),
            },
        ];

        let stream = fetcher.fetch_chunks_ordered(links).await.unwrap();
        pin_mut!(stream);
        let mut chunks = Vec::new();

        while let Some(result) = stream.next().await {
            chunks.push(result.unwrap());
        }

        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], vec![42, 42, 42]);
    }

    #[tokio::test]
    async fn test_fetch_chunks_ordered_many_chunks() {
        use crate::client::{SeaClient, SeaClientConfig};
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path};
        use futures::{StreamExt, pin_mut};

        let mock_server = MockServer::start().await;
        let num_chunks = 20;

        // Mock many chunks
        for i in 0..num_chunks {
            Mock::given(method("GET"))
                .and(path(format!("/chunk{}", i)))
                .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![i as u8; 10]))
                .mount(&mock_server)
                .await;
        }

        let config = SeaClientConfig {
            host: "https://test.cloud.databricks.com".to_string(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let sea_client = Arc::new(SeaClient::new(config).unwrap());
        let fetcher = ChunkFetcher::new(sea_client, "stmt-many".to_string(), 4).unwrap();

        let links: Vec<ExternalLink> = (0..num_chunks)
            .map(|i| ExternalLink {
                external_link: format!("{}/chunk{}", mock_server.uri(), i),
                chunk_index: i as i32,
                row_offset: i as i64 * 100,
                row_count: 100,
                byte_count: 10,
                expiration: "2025-12-31T23:59:59Z".to_string(),
            })
            .collect();

        let stream = fetcher.fetch_chunks_ordered(links).await.unwrap();
        pin_mut!(stream);
        let mut chunks = Vec::new();

        while let Some(result) = stream.next().await {
            chunks.push(result.unwrap());
        }

        // Verify we got all chunks in order
        assert_eq!(chunks.len(), num_chunks);
        for i in 0..num_chunks {
            assert_eq!(chunks[i], vec![i as u8; 10]);
        }
    }

    #[tokio::test]
    async fn test_fetch_chunks_ordered_concurrency_limit() {
        use crate::client::{SeaClient, SeaClientConfig};
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path};
        use futures::{StreamExt, pin_mut};

        let mock_server = MockServer::start().await;

        // Mock 10 chunks
        for i in 0..10 {
            Mock::given(method("GET"))
                .and(path(format!("/chunk{}", i)))
                .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![i as u8; 5]))
                .mount(&mock_server)
                .await;
        }

        let config = SeaClientConfig {
            host: "https://test.cloud.databricks.com".to_string(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let sea_client = Arc::new(SeaClient::new(config).unwrap());

        // Set concurrency to 3, but have 10 chunks
        let fetcher = ChunkFetcher::new(sea_client, "stmt-limit".to_string(), 3).unwrap();

        let links: Vec<ExternalLink> = (0..10)
            .map(|i| ExternalLink {
                external_link: format!("{}/chunk{}", mock_server.uri(), i),
                chunk_index: i as i32,
                row_offset: i as i64 * 100,
                row_count: 100,
                byte_count: 5,
                expiration: "2025-12-31T23:59:59Z".to_string(),
            })
            .collect();

        let stream = fetcher.fetch_chunks_ordered(links).await.unwrap();
        pin_mut!(stream);
        let mut chunks = Vec::new();

        while let Some(result) = stream.next().await {
            chunks.push(result.unwrap());
        }

        // Should still get all chunks in order
        assert_eq!(chunks.len(), 10);
        for i in 0..10 {
            assert_eq!(chunks[i], vec![i as u8; 5]);
        }
    }

    #[tokio::test]
    async fn test_fetch_chunks_ordered_error_propagation() {
        use crate::client::{SeaClient, SeaClientConfig};
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path};
        use futures::{StreamExt, pin_mut};

        let mock_server = MockServer::start().await;

        // Mock chunk 0 - success
        Mock::given(method("GET"))
            .and(path("/chunk0"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![0, 0, 0]))
            .mount(&mock_server)
            .await;

        // Mock chunk 1 - 404 error
        Mock::given(method("GET"))
            .and(path("/chunk1"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server)
            .await;

        // Mock chunk 2 - success
        Mock::given(method("GET"))
            .and(path("/chunk2"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![2, 2, 2]))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: "https://test.cloud.databricks.com".to_string(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let sea_client = Arc::new(SeaClient::new(config).unwrap());
        let fetcher = ChunkFetcher::new(sea_client, "stmt-error".to_string(), 2).unwrap();

        let links = vec![
            ExternalLink {
                external_link: format!("{}/chunk0", mock_server.uri()),
                chunk_index: 0,
                row_offset: 0,
                row_count: 100,
                byte_count: 3,
                expiration: "2025-12-31T23:59:59Z".to_string(),
            },
            ExternalLink {
                external_link: format!("{}/chunk1", mock_server.uri()),
                chunk_index: 1,
                row_offset: 100,
                row_count: 100,
                byte_count: 3,
                expiration: "2025-12-31T23:59:59Z".to_string(),
            },
            ExternalLink {
                external_link: format!("{}/chunk2", mock_server.uri()),
                chunk_index: 2,
                row_offset: 200,
                row_count: 100,
                byte_count: 3,
                expiration: "2025-12-31T23:59:59Z".to_string(),
            },
        ];

        let stream = fetcher.fetch_chunks_ordered(links).await.unwrap();
        pin_mut!(stream);
        let mut results = Vec::new();

        while let Some(result) = stream.next().await {
            results.push(result);
        }

        // First chunk should succeed
        assert!(results[0].is_ok());
        assert_eq!(results[0].as_ref().unwrap(), &vec![0, 0, 0]);

        // Second chunk should fail with IO error
        assert!(results[1].is_err());
        match &results[1] {
            Err(Error::Io(e)) => {
                assert!(e.to_string().contains("404"));
            }
            _ => panic!("Expected IO error for chunk 1"),
        }

        // Third chunk should still be returned (though might fail or succeed)
        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn test_fetch_chunks_ordered_empty_list() {
        use crate::client::{SeaClient, SeaClientConfig};
        use futures::{StreamExt, pin_mut};

        let config = SeaClientConfig {
            host: "https://test.cloud.databricks.com".to_string(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let sea_client = Arc::new(SeaClient::new(config).unwrap());
        let fetcher = ChunkFetcher::new(sea_client, "stmt-empty".to_string(), 8).unwrap();

        let links: Vec<ExternalLink> = vec![];

        let stream = fetcher.fetch_chunks_ordered(links).await.unwrap();
        pin_mut!(stream);
        let mut chunks = Vec::new();

        while let Some(result) = stream.next().await {
            chunks.push(result.unwrap());
        }

        // Should get no chunks
        assert_eq!(chunks.len(), 0);
    }

    #[tokio::test]
    async fn test_fetch_chunk_with_retry_success_on_first_attempt() {
        use crate::client::{SeaClient, SeaClientConfig};
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path};

        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/chunk0"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![1, 2, 3]))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: "https://test.cloud.databricks.com".to_string(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let sea_client = Arc::new(SeaClient::new(config).unwrap());
        let fetcher = ChunkFetcher::new(sea_client, "stmt-retry".to_string(), 8).unwrap();

        let link = ExternalLink {
            external_link: format!("{}/chunk0", mock_server.uri()),
            chunk_index: 0,
            row_offset: 0,
            row_count: 100,
            byte_count: 3,
            expiration: "2025-12-31T23:59:59Z".to_string(),
        };

        let result = fetcher.fetch_chunk_with_retry(&link).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_fetch_chunk_with_retry_url_expired_single_refresh() {
        use crate::client::{SeaClient, SeaClientConfig};
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path, header};

        let mock_server = MockServer::start().await;

        // First attempt: 403 Forbidden (expired URL)
        Mock::given(method("GET"))
            .and(path("/expired-chunk"))
            .respond_with(ResponseTemplate::new(403))
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;

        // Refreshed URL endpoint succeeds
        Mock::given(method("GET"))
            .and(path("/refreshed-chunk"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![5, 6, 7]))
            .mount(&mock_server)
            .await;

        // Mock get_chunk API call for URL refresh
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-refresh/result/chunks/5"))
            .and(header("Authorization", "Bearer test-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "external_links": [{
                    "external_link": format!("{}/refreshed-chunk", mock_server.uri()),
                    "chunk_index": 5,
                    "row_offset": 50000,
                    "row_count": 10000,
                    "byte_count": 3,
                    "expiration": "2025-12-31T23:59:59Z"
                }]
            })))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: mock_server.uri(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let sea_client = Arc::new(SeaClient::new(config).unwrap());
        let fetcher = ChunkFetcher::new(sea_client, "stmt-refresh".to_string(), 8).unwrap();

        let link = ExternalLink {
            external_link: format!("{}/expired-chunk", mock_server.uri()),
            chunk_index: 5,
            row_offset: 50000,
            row_count: 10000,
            byte_count: 3,
            expiration: "2024-01-01T00:00:00Z".to_string(),
        };

        let result = fetcher.fetch_chunk_with_retry(&link).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec![5, 6, 7]);
    }

    #[tokio::test]
    async fn test_fetch_chunk_with_retry_url_expired_multiple_urls_cached() {
        use crate::client::{SeaClient, SeaClientConfig};
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path, header};

        let mock_server = MockServer::start().await;

        // Mock expired URLs for chunks 5 and 6
        Mock::given(method("GET"))
            .and(path("/expired-chunk5"))
            .respond_with(ResponseTemplate::new(403))
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;

        Mock::given(method("GET"))
            .and(path("/expired-chunk6"))
            .respond_with(ResponseTemplate::new(403))
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;

        // Refreshed URLs succeed
        Mock::given(method("GET"))
            .and(path("/refreshed-chunk5"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![5, 5, 5]))
            .mount(&mock_server)
            .await;

        Mock::given(method("GET"))
            .and(path("/refreshed-chunk6"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![6, 6, 6]))
            .mount(&mock_server)
            .await;

        // Mock get_chunk API - returns BOTH chunk 5 and 6 refreshed URLs
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-multi/result/chunks/5"))
            .and(header("Authorization", "Bearer test-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "external_links": [
                    {
                        "external_link": format!("{}/refreshed-chunk5", mock_server.uri()),
                        "chunk_index": 5,
                        "row_offset": 50000,
                        "row_count": 10000,
                        "byte_count": 3,
                        "expiration": "2025-12-31T23:59:59Z"
                    },
                    {
                        "external_link": format!("{}/refreshed-chunk6", mock_server.uri()),
                        "chunk_index": 6,
                        "row_offset": 60000,
                        "row_count": 10000,
                        "byte_count": 3,
                        "expiration": "2025-12-31T23:59:59Z"
                    }
                ]
            })))
            .expect(1) // Should only be called once
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: mock_server.uri(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let sea_client = Arc::new(SeaClient::new(config).unwrap());
        let fetcher = ChunkFetcher::new(sea_client, "stmt-multi".to_string(), 8).unwrap();

        let link5 = ExternalLink {
            external_link: format!("{}/expired-chunk5", mock_server.uri()),
            chunk_index: 5,
            row_offset: 50000,
            row_count: 10000,
            byte_count: 3,
            expiration: "2024-01-01T00:00:00Z".to_string(),
        };

        let link6 = ExternalLink {
            external_link: format!("{}/expired-chunk6", mock_server.uri()),
            chunk_index: 6,
            row_offset: 60000,
            row_count: 10000,
            byte_count: 3,
            expiration: "2024-01-01T00:00:00Z".to_string(),
        };

        // Fetch chunk 5 - will refresh and cache both 5 and 6
        let result5 = fetcher.fetch_chunk_with_retry(&link5).await;
        assert!(result5.is_ok());
        assert_eq!(result5.unwrap(), vec![5, 5, 5]);

        // Fetch chunk 6 - should use cached URL, no API call
        let result6 = fetcher.fetch_chunk_with_retry(&link6).await;
        assert!(result6.is_ok());
        assert_eq!(result6.unwrap(), vec![6, 6, 6]);

        // Verify get_chunk was only called once (for chunk 5)
        // This is validated by the .expect(1) above
    }

    #[tokio::test]
    async fn test_fetch_chunk_with_retry_cache_hit() {
        use crate::client::{SeaClient, SeaClientConfig};
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path};

        let mock_server = MockServer::start().await;

        // Expired URL
        Mock::given(method("GET"))
            .and(path("/expired-chunk"))
            .respond_with(ResponseTemplate::new(403))
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;

        // Refreshed URL
        Mock::given(method("GET"))
            .and(path("/refreshed-chunk"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![9, 9, 9]))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: "https://test.cloud.databricks.com".to_string(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let sea_client = Arc::new(SeaClient::new(config).unwrap());
        let fetcher = ChunkFetcher::new(sea_client, "stmt-cache".to_string(), 8).unwrap();

        // Pre-populate cache
        {
            let mut cache = fetcher.refreshed_urls.write().await;
            cache.insert(
                9,
                ExternalLink {
                    external_link: format!("{}/refreshed-chunk", mock_server.uri()),
                    chunk_index: 9,
                    row_offset: 90000,
                    row_count: 10000,
                    byte_count: 3,
                    expiration: "2025-12-31T23:59:59Z".to_string(),
                },
            );
        }

        let link = ExternalLink {
            external_link: format!("{}/expired-chunk", mock_server.uri()),
            chunk_index: 9,
            row_offset: 90000,
            row_count: 10000,
            byte_count: 3,
            expiration: "2024-01-01T00:00:00Z".to_string(),
        };

        // Should hit 403, check cache, find refreshed URL, and succeed
        let result = fetcher.fetch_chunk_with_retry(&link).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec![9, 9, 9]);
    }

    #[tokio::test]
    async fn test_fetch_chunk_with_retry_max_retries_exceeded() {
        use crate::client::{SeaClient, SeaClientConfig};
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path};

        let mock_server = MockServer::start().await;

        // Always return 500 (retryable error)
        Mock::given(method("GET"))
            .and(path("/failing-chunk"))
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
        let fetcher = ChunkFetcher::new(sea_client, "stmt-fail".to_string(), 8).unwrap();

        let link = ExternalLink {
            external_link: format!("{}/failing-chunk", mock_server.uri()),
            chunk_index: 0,
            row_offset: 0,
            row_count: 100,
            byte_count: 3,
            expiration: "2025-12-31T23:59:59Z".to_string(),
        };

        let result = fetcher.fetch_chunk_with_retry(&link).await;
        assert!(result.is_err());

        match result.unwrap_err() {
            Error::Io(e) => {
                assert!(e.to_string().contains("500"));
            }
            e => panic!("Expected Io error, got {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_fetch_chunk_with_retry_exponential_backoff() {
        use crate::client::{SeaClient, SeaClientConfig};
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path};
        use std::time::Instant;

        let mock_server = MockServer::start().await;

        // Return 500 twice, then succeed
        Mock::given(method("GET"))
            .and(path("/retry-chunk"))
            .respond_with(ResponseTemplate::new(500))
            .up_to_n_times(2)
            .mount(&mock_server)
            .await;

        Mock::given(method("GET"))
            .and(path("/retry-chunk"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![7, 8, 9]))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: "https://test.cloud.databricks.com".to_string(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let sea_client = Arc::new(SeaClient::new(config).unwrap());
        let fetcher = ChunkFetcher::new(sea_client, "stmt-backoff".to_string(), 8).unwrap();

        let link = ExternalLink {
            external_link: format!("{}/retry-chunk", mock_server.uri()),
            chunk_index: 0,
            row_offset: 0,
            row_count: 100,
            byte_count: 3,
            expiration: "2025-12-31T23:59:59Z".to_string(),
        };

        let start = Instant::now();
        let result = fetcher.fetch_chunk_with_retry(&link).await;
        let elapsed = start.elapsed();

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec![7, 8, 9]);

        // Should take at least 3 seconds (1s + 2s backoff)
        assert!(elapsed.as_secs() >= 3);
    }

    #[tokio::test]
    async fn test_fetch_chunk_with_retry_concurrent_cache_access() {
        use crate::client::{SeaClient, SeaClientConfig};
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path, header};
        use tokio::task::JoinSet;

        let mock_server = MockServer::start().await;

        // Multiple expired URLs
        for i in 10..15 {
            Mock::given(method("GET"))
                .and(path(format!("/expired-chunk{}", i)))
                .respond_with(ResponseTemplate::new(403))
                .up_to_n_times(1)
                .mount(&mock_server)
                .await;

            Mock::given(method("GET"))
                .and(path(format!("/refreshed-chunk{}", i)))
                .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![i as u8; 3]))
                .mount(&mock_server)
                .await;
        }

        // Mock get_chunk API for any chunk - returns all refreshed URLs (10-14)
        // Due to race conditions, any chunk might call refresh first
        for chunk_idx in 10..15 {
            Mock::given(method("GET"))
                .and(path(format!("/api/2.0/sql/statements/stmt-concurrent/result/chunks/{}", chunk_idx)))
                .and(header("Authorization", "Bearer test-token"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "external_links": (10..15).map(|i| {
                        serde_json::json!({
                            "external_link": format!("{}/refreshed-chunk{}", mock_server.uri(), i),
                            "chunk_index": i,
                            "row_offset": (i as i64) * 10000,
                            "row_count": 10000,
                            "byte_count": 3,
                            "expiration": "2025-12-31T23:59:59Z"
                        })
                    }).collect::<Vec<_>>()
                })))
                .up_to_n_times(1) // Each can be called at most once
                .mount(&mock_server)
                .await;
        }

        let config = SeaClientConfig {
            host: mock_server.uri(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let sea_client = Arc::new(SeaClient::new(config).unwrap());
        let fetcher = Arc::new(ChunkFetcher::new(sea_client, "stmt-concurrent".to_string(), 8).unwrap());

        // Spawn 5 concurrent tasks, all hitting expired URLs
        let mut tasks = JoinSet::new();
        for i in 10..15 {
            let fetcher_clone = fetcher.clone();
            let mock_uri = mock_server.uri();
            tasks.spawn(async move {
                let link = ExternalLink {
                    external_link: format!("{}/expired-chunk{}", mock_uri, i),
                    chunk_index: i,
                    row_offset: (i as i64) * 10000,
                    row_count: 10000,
                    byte_count: 3,
                    expiration: "2024-01-01T00:00:00Z".to_string(),
                };
                fetcher_clone.fetch_chunk_with_retry(&link).await
            });
        }

        // Wait for all tasks to complete
        let mut results = Vec::new();
        while let Some(result) = tasks.join_next().await {
            results.push(result.unwrap());
        }

        // All should succeed
        assert_eq!(results.len(), 5);
        for (idx, result) in results.iter().enumerate() {
            if let Err(e) = result {
                eprintln!("Error for chunk {}: {:?}", 10 + idx, e);
            }
            assert!(result.is_ok(), "Failed for chunk {}: {:?}", 10 + idx, result);
            let i = 10 + idx;
            assert_eq!(result.as_ref().unwrap(), &vec![i as u8; 3]);
        }

        // Key behavior verified: Cache sharing allows all chunks to succeed
        // Even though chunks hit 403 concurrently, the shared cache reduces API calls
    }

    #[tokio::test]
    async fn test_refresh_chunk_links() {
        use crate::client::{SeaClient, SeaClientConfig};
        use wiremock::{MockServer, Mock, ResponseTemplate};
        use wiremock::matchers::{method, path, header};

        let mock_server = MockServer::start().await;

        // Mock get_chunk API
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-refresh-test/result/chunks/5"))
            .and(header("Authorization", "Bearer test-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "external_links": [
                    {
                        "external_link": "https://s3.amazonaws.com/bucket/chunk5",
                        "chunk_index": 5,
                        "row_offset": 50000,
                        "row_count": 10000,
                        "byte_count": 1048576,
                        "expiration": "2025-12-31T23:59:59Z"
                    },
                    {
                        "external_link": "https://s3.amazonaws.com/bucket/chunk6",
                        "chunk_index": 6,
                        "row_offset": 60000,
                        "row_count": 10000,
                        "byte_count": 1048576,
                        "expiration": "2025-12-31T23:59:59Z"
                    }
                ]
            })))
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: mock_server.uri(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            ..Default::default()
        };

        let sea_client = Arc::new(SeaClient::new(config).unwrap());
        let fetcher = ChunkFetcher::new(sea_client, "stmt-refresh-test".to_string(), 8).unwrap();

        let links = fetcher.refresh_chunk_links(5).await.unwrap();

        assert_eq!(links.len(), 2);
        assert_eq!(links[0].chunk_index, 5);
        assert_eq!(links[1].chunk_index, 6);
        assert_eq!(links[0].external_link, "https://s3.amazonaws.com/bucket/chunk5");
        assert_eq!(links[1].external_link, "https://s3.amazonaws.com/bucket/chunk6");
    }
}
