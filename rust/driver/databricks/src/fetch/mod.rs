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

//! Chunk fetching for external links.
//!
//! This module provides parallel chunk fetching capabilities for
//! downloading large result sets from cloud storage.

pub mod decompress;
pub mod reader;

use std::sync::Arc;

use arrow_array::RecordBatch;

use crate::client::{ExternalLink, ResultManifest, SeaClient};
use crate::error::Result;

pub use decompress::decompress_lz4;
pub use reader::ArrowResultReader;

/// Chunk fetcher for downloading result chunks in parallel.
///
/// Uses a worker pool pattern with ordered output to efficiently
/// download chunks while maintaining result order.
#[derive(Debug)]
pub struct ChunkFetcher {
    /// SEA client for refreshing expired URLs.
    client: Arc<SeaClient>,
    /// HTTP client for downloading chunks.
    http_client: reqwest::Client,
    /// Number of parallel workers.
    concurrency: usize,
}

impl ChunkFetcher {
    /// Create a new chunk fetcher.
    pub fn new(client: Arc<SeaClient>, concurrency: usize) -> Result<Self> {
        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(300))
            .build()
            .map_err(|e| crate::error::Error::Http(e))?;

        Ok(Self {
            client,
            http_client,
            concurrency,
        })
    }

    /// Fetch all chunks for a result set sequentially.
    ///
    /// This is a basic implementation that fetches chunks one at a time.
    /// A parallel implementation will be added in Sprint 3 for better performance.
    ///
    /// Returns the concatenated record batches from all chunks.
    pub async fn fetch_chunks(
        &self,
        _statement_id: &str,
        _manifest: &ResultManifest,
        external_links: &[ExternalLink],
    ) -> Result<Vec<RecordBatch>> {
        let mut all_batches = Vec::new();

        for link in external_links {
            let bytes = self.download_chunk(link).await?;
            if bytes.is_empty() {
                continue;
            }

            // Parse Arrow IPC data
            let (_, batches) = ArrowResultReader::parse_ipc_stream(&bytes)?;
            all_batches.extend(batches);
        }

        Ok(all_batches)
    }

    /// Download a single chunk from a presigned URL.
    ///
    /// This method fetches the Arrow IPC data from the external link URL.
    pub async fn download_chunk(&self, link: &ExternalLink) -> Result<Vec<u8>> {
        let mut request = self.http_client.get(&link.external_link);

        // Add custom headers if provided (e.g., for Azure blob storage)
        if let Some(ref headers) = link.http_headers {
            for (key, value) in headers {
                request = request.header(key, value);
            }
        }

        let response = request.send().await.map_err(crate::error::Error::Http)?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(crate::error::Error::Config(format!(
                "Failed to download chunk: HTTP {} - {}",
                status, body
            )));
        }

        let bytes = response.bytes().await.map_err(crate::error::Error::Http)?;
        Ok(bytes.to_vec())
    }

    /// Refresh an expired external link.
    ///
    /// Note: This will be implemented in Sprint 3 when parallel fetching is added.
    /// For now, links should not expire during sequential fetching.
    #[allow(dead_code)]
    async fn refresh_link(
        &self,
        _statement_id: &str,
        _chunk_index: usize,
    ) -> Result<ExternalLink> {
        // TODO: Implement link refresh using get_chunk API
        // This will be needed for parallel fetching where links may expire
        // before all chunks are downloaded.
        unimplemented!("refresh_link not yet implemented - will be added in Sprint 3")
    }
}
