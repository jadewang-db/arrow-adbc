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

//! LZ4 decompression for result chunks.
//!
//! This module handles decompressing LZ4_FRAME compressed result data
//! from Databricks SQL Warehouses. The SEA API returns Arrow IPC data
//! that may be compressed using LZ4 frame format.
//!
//! # LZ4 Frame Format
//!
//! LZ4 frame format is identified by the magic number `0x184D2204` at the
//! start of the data (stored as little-endian: `[0x04, 0x22, 0x4D, 0x18]`).
//!
//! # Example
//!
//! ```ignore
//! use adbc_driver_databricks::fetch::decompress_if_needed;
//!
//! // Automatically detect and decompress if needed
//! let data = download_chunk_from_presigned_url();
//! let decompressed = decompress_if_needed(data)?;
//! ```

use crate::error::Result;

/// LZ4 frame magic number (little-endian).
/// The magic number is 0x184D2204, which is stored as [0x04, 0x22, 0x4D, 0x18].
const LZ4_FRAME_MAGIC: [u8; 4] = [0x04, 0x22, 0x4D, 0x18];

/// Check if data appears to be LZ4 frame compressed.
///
/// Detects LZ4 frame format by checking for the magic number at the
/// start of the data.
///
/// # Arguments
///
/// * `data` - The data to check.
///
/// # Returns
///
/// `true` if the data starts with the LZ4 frame magic number.
///
/// # Example
///
/// ```ignore
/// let compressed_data = vec![0x04, 0x22, 0x4D, 0x18, /* more data */];
/// assert!(is_lz4_compressed(&compressed_data));
///
/// let uncompressed_data = vec![0x41, 0x52, 0x52, 0x4F]; // ARRO
/// assert!(!is_lz4_compressed(&uncompressed_data));
/// ```
pub fn is_lz4_compressed(data: &[u8]) -> bool {
    data.len() >= 4 && data[0..4] == LZ4_FRAME_MAGIC
}

/// Decompress LZ4 frame data.
///
/// # Arguments
///
/// * `data` - The LZ4 frame compressed data.
///
/// # Returns
///
/// The decompressed data.
///
/// # Errors
///
/// Returns an error if:
/// - The data is not valid LZ4 frame format
/// - Decompression fails (corrupted data)
pub fn decompress_lz4_frame(data: &[u8]) -> Result<Vec<u8>> {
    use lz4_flex::frame::FrameDecoder;
    use std::io::Read;

    let mut decoder = FrameDecoder::new(data);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed).map_err(|e| {
        crate::error::Error::internal(format!("LZ4 decompression failed: {}", e))
    })?;
    Ok(decompressed)
}

/// Decompress data if it is LZ4 compressed, otherwise return the original data.
///
/// This function automatically detects whether the input data is LZ4 frame
/// compressed by checking for the magic number. If compressed, it decompresses
/// the data; otherwise, it returns the original data unchanged.
///
/// This is useful when the compression status of the data is unknown or when
/// the server may or may not compress the response based on configuration.
///
/// # Arguments
///
/// * `data` - The data to potentially decompress.
///
/// # Returns
///
/// - If the data is LZ4 compressed: the decompressed data
/// - If the data is not compressed: the original data unchanged
///
/// # Errors
///
/// Returns an error if the data appears to be LZ4 compressed but decompression fails.
///
/// # Example
///
/// ```ignore
/// // Compressed data will be decompressed
/// let compressed = create_lz4_compressed_data(b"Hello, World!");
/// let result = decompress_if_needed(compressed)?;
/// assert_eq!(result, b"Hello, World!");
///
/// // Uncompressed data passes through unchanged
/// let uncompressed = b"Hello, World!".to_vec();
/// let result = decompress_if_needed(uncompressed)?;
/// assert_eq!(result, b"Hello, World!");
/// ```
pub fn decompress_if_needed(data: Vec<u8>) -> Result<Vec<u8>> {
    if is_lz4_compressed(&data) {
        decompress_lz4_frame(&data)
    } else {
        Ok(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lz4_flex::frame::FrameEncoder;
    use std::io::Write;

    /// Helper to create LZ4 compressed data for testing.
    fn compress_lz4(data: &[u8]) -> Vec<u8> {
        let mut encoder = FrameEncoder::new(Vec::new());
        encoder.write_all(data).unwrap();
        encoder.finish().unwrap()
    }

    #[test]
    fn test_lz4_magic_constant() {
        // Verify the magic number is correct
        assert_eq!(LZ4_FRAME_MAGIC, [0x04, 0x22, 0x4D, 0x18]);
    }

    #[test]
    fn test_is_lz4_compressed_with_compressed_data() {
        let compressed = compress_lz4(b"test data");
        assert!(is_lz4_compressed(&compressed));
    }

    #[test]
    fn test_is_lz4_compressed_with_uncompressed_data() {
        let uncompressed = b"ARRO"; // Arrow magic
        assert!(!is_lz4_compressed(uncompressed));
    }

    #[test]
    fn test_is_lz4_compressed_with_short_data() {
        // Less than 4 bytes - cannot have magic number
        assert!(!is_lz4_compressed(&[]));
        assert!(!is_lz4_compressed(&[0x04]));
        assert!(!is_lz4_compressed(&[0x04, 0x22]));
        assert!(!is_lz4_compressed(&[0x04, 0x22, 0x4D]));
    }

    #[test]
    fn test_is_lz4_compressed_with_exact_magic() {
        // Exactly 4 bytes matching magic
        assert!(is_lz4_compressed(&LZ4_FRAME_MAGIC));
    }

    #[test]
    fn test_decompress_lz4_frame_success() {
        let original = b"Hello, World! This is test data for compression.";
        let compressed = compress_lz4(original);

        let decompressed = decompress_lz4_frame(&compressed).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_decompress_lz4_frame_empty() {
        // Empty data returns empty output (no valid LZ4 frame header to read)
        let result = decompress_lz4_frame(&[]);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_decompress_lz4_frame_invalid() {
        // Invalid LZ4 frame data should fail
        let invalid_data = b"not a valid lz4 frame";
        let result = decompress_lz4_frame(invalid_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_decompress_if_needed_compressed() {
        let original = b"Test data for decompression";
        let compressed = compress_lz4(original);

        let result = decompress_if_needed(compressed).unwrap();
        assert_eq!(result, original);
    }

    #[test]
    fn test_decompress_if_needed_uncompressed() {
        let original = b"Uncompressed data without LZ4 magic".to_vec();

        let result = decompress_if_needed(original.clone()).unwrap();
        assert_eq!(result, original);
    }

    #[test]
    fn test_decompress_if_needed_empty() {
        let result = decompress_if_needed(Vec::new()).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_decompress_if_needed_arrow_ipc_data() {
        // Arrow IPC stream starts with the schema message, not a specific magic
        // But we can test with a fake Arrow-like header
        let arrow_like = vec![0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00];

        let result = decompress_if_needed(arrow_like.clone()).unwrap();
        assert_eq!(result, arrow_like); // Should pass through unchanged
    }

    #[test]
    fn test_roundtrip_various_sizes() {
        // Test with various data sizes
        for size in [0, 1, 10, 100, 1000, 10000] {
            let original: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
            let compressed = compress_lz4(&original);
            let decompressed = decompress_if_needed(compressed).unwrap();
            assert_eq!(decompressed, original, "Failed for size {}", size);
        }
    }

    #[test]
    fn test_decompress_highly_compressible_data() {
        // Data with lots of repetition compresses very well
        let original: Vec<u8> = vec![0xAB; 100000];
        let compressed = compress_lz4(&original);

        // Verify compression actually happened
        assert!(compressed.len() < original.len());

        let decompressed = decompress_if_needed(compressed).unwrap();
        assert_eq!(decompressed, original);
    }
}
