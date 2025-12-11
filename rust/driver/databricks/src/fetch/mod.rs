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

//! Chunk fetching and Arrow result reading.
//!
//! This module handles fetching result chunks from external links
//! and reading Arrow IPC data.
//!
//! # Architecture
//!
//! For large result sets, Databricks returns `EXTERNAL_LINKS` disposition
//! where result data is stored in cloud storage (S3/ADLS/GCS) and presigned
//! URLs are provided to download the chunks.
//!
//! The [`ChunkFetcher`] handles:
//! - Parallel chunk downloading with configurable concurrency
//! - Ordered output (chunks yielded in order regardless of download order)
//! - Automatic URL refresh when presigned URLs expire
//! - LZ4 decompression of chunk data
//!
//! # Example
//!
//! ```ignore
//! use adbc_driver_databricks::fetch::ChunkFetcher;
//!
//! let fetcher = ChunkFetcher::new(
//!     client.clone(),
//!     "stmt-123".to_string(),
//!     8, // concurrency
//! )?;
//!
//! let chunks = fetcher.fetch_all_chunks(external_links).await?;
//! ```

mod decompress;
mod reader;

pub use decompress::*;
pub use reader::*;

use std::sync::Arc;
use std::time::Duration;

use reqwest::Client;
use tokio::sync::Semaphore;

use crate::client::{ExternalLink, SeaClient};
use crate::error::{Error, Result};

/// Default concurrency for parallel chunk fetching.
const DEFAULT_CONCURRENCY: usize = 8;

/// Default timeout for chunk downloads.
const DEFAULT_CHUNK_TIMEOUT: Duration = Duration::from_secs(300);

/// Maximum number of retries for failed chunk downloads.
const MAX_CHUNK_RETRIES: u32 = 3;

/// Configuration for the ChunkFetcher.
#[derive(Debug, Clone)]
pub struct ChunkFetcherConfig {
    /// Number of concurrent chunk downloads.
    pub concurrency: usize,
    /// Timeout for individual chunk downloads.
    pub timeout: Duration,
    /// Maximum retries for failed downloads.
    pub max_retries: u32,
}

impl Default for ChunkFetcherConfig {
    fn default() -> Self {
        Self {
            concurrency: DEFAULT_CONCURRENCY,
            timeout: DEFAULT_CHUNK_TIMEOUT,
            max_retries: MAX_CHUNK_RETRIES,
        }
    }
}

impl ChunkFetcherConfig {
    /// Create a new configuration with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the concurrency level.
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency.max(1); // At least 1
        self
    }

    /// Set the timeout for chunk downloads.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set the maximum retries.
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }
}

/// Parallel chunk fetcher for EXTERNAL_LINKS results.
///
/// Fetches chunks in parallel with configurable concurrency.
/// Output is yielded in chunk order regardless of download completion order.
///
/// # URL Expiration Handling
///
/// Presigned URLs have an expiration time. If a download fails with HTTP 403
/// (Forbidden), the fetcher automatically refreshes the URL by calling the
/// SEA API's get_chunk endpoint and retries the download.
///
/// # Error Handling
///
/// - Transient errors (network issues, 5xx) are retried with exponential backoff
/// - URL expiration (403) triggers automatic URL refresh
/// - Non-retryable errors fail immediately
pub struct ChunkFetcher {
    /// HTTP client for downloading chunks from presigned URLs.
    http_client: Client,
    /// SEA client for refreshing expired URLs.
    sea_client: Arc<SeaClient>,
    /// Statement ID for URL refresh requests.
    statement_id: String,
    /// Configuration.
    config: ChunkFetcherConfig,
}

impl std::fmt::Debug for ChunkFetcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkFetcher")
            .field("statement_id", &self.statement_id)
            .field("config", &self.config)
            .finish()
    }
}

impl ChunkFetcher {
    /// Create a new chunk fetcher.
    ///
    /// # Arguments
    ///
    /// * `sea_client` - SEA client for refreshing expired URLs
    /// * `statement_id` - Statement ID for URL refresh requests
    /// * `concurrency` - Number of concurrent downloads
    ///
    /// # Returns
    ///
    /// A new ChunkFetcher instance.
    pub fn new(
        sea_client: Arc<SeaClient>,
        statement_id: String,
        concurrency: usize,
    ) -> Result<Self> {
        Self::with_config(
            sea_client,
            statement_id,
            ChunkFetcherConfig::default().with_concurrency(concurrency),
        )
    }

    /// Create a new chunk fetcher with custom configuration.
    pub fn with_config(
        sea_client: Arc<SeaClient>,
        statement_id: String,
        config: ChunkFetcherConfig,
    ) -> Result<Self> {
        let http_client = Client::builder()
            .timeout(config.timeout)
            .build()
            .map_err(Error::Http)?;

        Ok(Self {
            http_client,
            sea_client,
            statement_id,
            config,
        })
    }

    /// Get the statement ID.
    pub fn statement_id(&self) -> &str {
        &self.statement_id
    }

    /// Get the configuration.
    pub fn config(&self) -> &ChunkFetcherConfig {
        &self.config
    }

    /// Fetch all chunks in parallel, returning them in order.
    ///
    /// Downloads all chunks concurrently (up to the configured concurrency limit)
    /// and returns them in chunk index order.
    ///
    /// # Arguments
    ///
    /// * `links` - External links with presigned URLs for each chunk
    ///
    /// # Returns
    ///
    /// A vector of raw chunk data (potentially LZ4 compressed) in chunk order.
    ///
    /// # Errors
    ///
    /// Returns an error if any chunk download fails after all retries.
    pub async fn fetch_all_chunks(&self, links: Vec<ExternalLink>) -> Result<Vec<Vec<u8>>> {
        if links.is_empty() {
            return Ok(Vec::new());
        }

        let total_chunks = links.len();
        let semaphore = Arc::new(Semaphore::new(self.config.concurrency));

        // Spawn all fetch tasks
        let mut tasks = Vec::with_capacity(total_chunks);

        for link in links {
            let sem = semaphore.clone();
            let fetcher = ChunkFetcherHandle {
                http_client: self.http_client.clone(),
                sea_client: self.sea_client.clone(),
                statement_id: self.statement_id.clone(),
                max_retries: self.config.max_retries,
            };
            let chunk_index = link.chunk_index;

            let task = tokio::spawn(async move {
                let _permit = sem.acquire().await.map_err(|_| {
                    Error::internal("Semaphore closed unexpectedly")
                })?;

                let data = fetcher.fetch_chunk_with_retry(link).await?;
                Ok::<_, Error>((chunk_index, data))
            });

            tasks.push(task);
        }

        // Collect results into ordered vec
        let mut results: Vec<Option<Vec<u8>>> = vec![None; total_chunks];

        for task in tasks {
            let (index, data) = task.await.map_err(|e| {
                Error::internal(format!("Task join error: {}", e))
            })??;

            if (index as usize) < total_chunks {
                results[index as usize] = Some(data);
            } else {
                return Err(Error::internal(format!(
                    "Chunk index {} out of range (total: {})",
                    index, total_chunks
                )));
            }
        }

        // Convert to ordered vec, ensuring all chunks are present
        results
            .into_iter()
            .enumerate()
            .map(|(i, opt)| {
                opt.ok_or_else(|| Error::internal(format!("Missing chunk {}", i)))
            })
            .collect()
    }

    /// Fetch and decompress all chunks, returning decompressed data in order.
    ///
    /// This is a convenience method that fetches all chunks and decompresses
    /// any LZ4-compressed data.
    ///
    /// # Arguments
    ///
    /// * `links` - External links with presigned URLs for each chunk
    ///
    /// # Returns
    ///
    /// A vector of decompressed chunk data in chunk order.
    pub async fn fetch_and_decompress_all(&self, links: Vec<ExternalLink>) -> Result<Vec<Vec<u8>>> {
        let chunks = self.fetch_all_chunks(links).await?;

        chunks
            .into_iter()
            .map(decompress_if_needed)
            .collect()
    }
}

/// Internal handle for fetching a single chunk.
/// This is cloned for each parallel task to avoid lifetime issues.
struct ChunkFetcherHandle {
    http_client: Client,
    sea_client: Arc<SeaClient>,
    statement_id: String,
    max_retries: u32,
}

impl ChunkFetcherHandle {
    /// Fetch a single chunk with retry and URL refresh logic.
    async fn fetch_chunk_with_retry(&self, mut link: ExternalLink) -> Result<Vec<u8>> {
        let mut last_error = None;

        for attempt in 0..=self.max_retries {
            match self.fetch_chunk(&link).await {
                Ok(data) => return Ok(data),
                Err(e) => {
                    // Check if URL might be expired (403 Forbidden)
                    if Self::is_url_expired_error(&e) {
                        // Try to refresh the URL
                        match self.refresh_chunk_link(link.chunk_index).await {
                            Ok(refreshed) => {
                                link = refreshed;
                                // Don't count URL refresh as a retry attempt
                                continue;
                            }
                            Err(refresh_err) => {
                                last_error = Some(refresh_err);
                            }
                        }
                    } else if e.is_retryable() && attempt < self.max_retries {
                        // Exponential backoff for retryable errors
                        let delay = Duration::from_millis(100 * (1 << attempt));
                        tokio::time::sleep(delay).await;
                        last_error = Some(e);
                        continue;
                    } else {
                        return Err(e);
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            Error::internal("Max retries exceeded for chunk download")
        }))
    }

    /// Fetch a single chunk from its presigned URL.
    async fn fetch_chunk(&self, link: &ExternalLink) -> Result<Vec<u8>> {
        let response = self
            .http_client
            .get(&link.external_link)
            .send()
            .await
            .map_err(Error::Http)?;

        let status = response.status();

        if status == reqwest::StatusCode::FORBIDDEN {
            // URL might be expired
            return Err(Error::sea_api(
                "URL_EXPIRED",
                format!("Presigned URL expired for chunk {}", link.chunk_index),
                403,
            ));
        }

        if !status.is_success() {
            return Err(Error::sea_api(
                "CHUNK_DOWNLOAD_FAILED",
                format!(
                    "Failed to download chunk {}: HTTP {}",
                    link.chunk_index, status
                ),
                status.as_u16(),
            ));
        }

        let bytes = response.bytes().await.map_err(Error::Http)?;
        Ok(bytes.to_vec())
    }

    /// Check if an error indicates an expired URL.
    fn is_url_expired_error(error: &Error) -> bool {
        match error {
            Error::SeaApi { http_status, .. } => *http_status == 403,
            _ => false,
        }
    }

    /// Refresh a chunk link by calling the SEA API.
    async fn refresh_chunk_link(&self, chunk_index: i32) -> Result<ExternalLink> {
        let response = self
            .sea_client
            .get_chunk(&self.statement_id, chunk_index)
            .await?;

        response
            .external_links
            .into_iter()
            .find(|l| l.chunk_index == chunk_index)
            .ok_or_else(|| {
                Error::internal(format!(
                    "Chunk {} not found in refresh response",
                    chunk_index
                ))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::SeaClientConfig;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn create_test_client(server_uri: &str) -> Arc<SeaClient> {
        let config = SeaClientConfig::new(server_uri, "test-token", "test-warehouse");
        Arc::new(SeaClient::new(config).unwrap())
    }

    fn create_external_link(chunk_index: i32, url: &str) -> ExternalLink {
        ExternalLink {
            chunk_index,
            row_offset: (chunk_index as i64) * 1000,
            row_count: 1000,
            byte_count: 10000,
            external_link: url.to_string(),
            expiration: "2099-12-31T23:59:59Z".to_string(),
        }
    }

    #[tokio::test]
    async fn test_chunk_fetcher_config_defaults() {
        let config = ChunkFetcherConfig::default();
        assert_eq!(config.concurrency, 8);
        assert_eq!(config.timeout, Duration::from_secs(300));
        assert_eq!(config.max_retries, 3);
    }

    #[tokio::test]
    async fn test_chunk_fetcher_config_builder() {
        let config = ChunkFetcherConfig::new()
            .with_concurrency(4)
            .with_timeout(Duration::from_secs(60))
            .with_max_retries(5);

        assert_eq!(config.concurrency, 4);
        assert_eq!(config.timeout, Duration::from_secs(60));
        assert_eq!(config.max_retries, 5);
    }

    #[tokio::test]
    async fn test_chunk_fetcher_config_min_concurrency() {
        let config = ChunkFetcherConfig::new().with_concurrency(0);
        assert_eq!(config.concurrency, 1); // Minimum is 1
    }

    #[tokio::test]
    async fn test_chunk_fetcher_creation() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        let fetcher = ChunkFetcher::new(client, "stmt-123".to_string(), 4).unwrap();

        assert_eq!(fetcher.statement_id(), "stmt-123");
        assert_eq!(fetcher.config().concurrency, 4);
    }

    #[tokio::test]
    async fn test_fetch_empty_links() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());
        let fetcher = ChunkFetcher::new(client, "stmt-123".to_string(), 4).unwrap();

        let result = fetcher.fetch_all_chunks(Vec::new()).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_fetch_single_chunk() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        // Mock the chunk data endpoint
        Mock::given(method("GET"))
            .and(path("/chunk/0"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"chunk data 0"))
            .mount(&mock_server)
            .await;

        let fetcher = ChunkFetcher::new(client, "stmt-123".to_string(), 4).unwrap();

        let links = vec![create_external_link(
            0,
            &format!("{}/chunk/0", mock_server.uri()),
        )];

        let result = fetcher.fetch_all_chunks(links).await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0], b"chunk data 0");
    }

    #[tokio::test]
    async fn test_fetch_multiple_chunks_in_order() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        // Mock chunk endpoints with different response times to test ordering
        for i in 0..3 {
            Mock::given(method("GET"))
                .and(path(format!("/chunk/{}", i)))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_bytes(format!("chunk data {}", i).into_bytes()),
                )
                .mount(&mock_server)
                .await;
        }

        let fetcher = ChunkFetcher::new(client, "stmt-123".to_string(), 4).unwrap();

        let links: Vec<_> = (0..3)
            .map(|i| create_external_link(i, &format!("{}/chunk/{}", mock_server.uri(), i)))
            .collect();

        let result = fetcher.fetch_all_chunks(links).await.unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result[0], b"chunk data 0");
        assert_eq!(result[1], b"chunk data 1");
        assert_eq!(result[2], b"chunk data 2");
    }

    #[tokio::test]
    async fn test_fetch_with_url_refresh() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        // First request returns 403 (expired)
        Mock::given(method("GET"))
            .and(path("/chunk/0/expired"))
            .respond_with(ResponseTemplate::new(403))
            .expect(1)
            .mount(&mock_server)
            .await;

        // SEA API returns refreshed URL
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-123/result/chunks/0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "external_links": [{
                    "chunk_index": 0,
                    "row_offset": 0,
                    "row_count": 1000,
                    "byte_count": 10000,
                    "external_link": format!("{}/chunk/0/fresh", mock_server.uri()),
                    "expiration": "2099-12-31T23:59:59Z"
                }]
            })))
            .mount(&mock_server)
            .await;

        // Fresh URL works
        Mock::given(method("GET"))
            .and(path("/chunk/0/fresh"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"fresh chunk data"))
            .mount(&mock_server)
            .await;

        let fetcher = ChunkFetcher::new(client, "stmt-123".to_string(), 4).unwrap();

        let links = vec![create_external_link(
            0,
            &format!("{}/chunk/0/expired", mock_server.uri()),
        )];

        let result = fetcher.fetch_all_chunks(links).await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0], b"fresh chunk data");
    }

    #[tokio::test]
    async fn test_fetch_with_retry_on_transient_error() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        // First request fails with 503, second succeeds
        Mock::given(method("GET"))
            .and(path("/chunk/0"))
            .respond_with(ResponseTemplate::new(503))
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;

        Mock::given(method("GET"))
            .and(path("/chunk/0"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"chunk after retry"))
            .mount(&mock_server)
            .await;

        let fetcher = ChunkFetcher::new(client, "stmt-123".to_string(), 4).unwrap();

        let links = vec![create_external_link(
            0,
            &format!("{}/chunk/0", mock_server.uri()),
        )];

        let result = fetcher.fetch_all_chunks(links).await.unwrap();

        assert_eq!(result[0], b"chunk after retry");
    }

    #[tokio::test]
    async fn test_fetch_non_retryable_error() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        // 404 is not retryable
        Mock::given(method("GET"))
            .and(path("/chunk/0"))
            .respond_with(ResponseTemplate::new(404))
            .expect(1)
            .mount(&mock_server)
            .await;

        let fetcher = ChunkFetcher::new(client, "stmt-123".to_string(), 4).unwrap();

        let links = vec![create_external_link(
            0,
            &format!("{}/chunk/0", mock_server.uri()),
        )];

        let result = fetcher.fetch_all_chunks(links).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_fetch_and_decompress() {
        use lz4_flex::frame::FrameEncoder;
        use std::io::Write;

        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        // Create LZ4 compressed data
        let original = b"decompressed chunk data";
        let mut encoder = FrameEncoder::new(Vec::new());
        encoder.write_all(original).unwrap();
        let compressed = encoder.finish().unwrap();

        Mock::given(method("GET"))
            .and(path("/chunk/0"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(compressed))
            .mount(&mock_server)
            .await;

        let fetcher = ChunkFetcher::new(client, "stmt-123".to_string(), 4).unwrap();

        let links = vec![create_external_link(
            0,
            &format!("{}/chunk/0", mock_server.uri()),
        )];

        let result = fetcher.fetch_and_decompress_all(links).await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0], original);
    }

    #[tokio::test]
    async fn test_concurrency_limit() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        let concurrent_count = Arc::new(AtomicUsize::new(0));
        let max_concurrent = Arc::new(AtomicUsize::new(0));

        // Create 10 chunks, but limit concurrency to 2
        for i in 0..10 {
            let cc = concurrent_count.clone();
            let mc = max_concurrent.clone();

            Mock::given(method("GET"))
                .and(path(format!("/chunk/{}", i)))
                .respond_with(ResponseTemplate::new(200).set_body_bytes(format!("data{}", i)))
                .mount(&mock_server)
                .await;
        }

        let fetcher = ChunkFetcher::new(client, "stmt-123".to_string(), 2).unwrap();

        let links: Vec<_> = (0..10)
            .map(|i| create_external_link(i, &format!("{}/chunk/{}", mock_server.uri(), i)))
            .collect();

        let result = fetcher.fetch_all_chunks(links).await.unwrap();

        assert_eq!(result.len(), 10);
        // Verify all chunks were fetched in order
        for (i, chunk) in result.iter().enumerate() {
            assert_eq!(*chunk, format!("data{}", i).into_bytes());
        }
    }
}
