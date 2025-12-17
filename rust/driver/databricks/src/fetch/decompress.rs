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

//! LZ4 decompression utilities

use crate::error::Result;
use lz4_flex::frame::FrameDecoder;
use std::io::Read;

/// Decompress LZ4_FRAME compressed data
///
/// # Arguments
/// * `compressed` - LZ4 frame compressed byte slice
///
/// # Returns
/// * `Ok(Vec<u8>)` - Decompressed data
/// * `Err(Error)` - If decompression fails (corrupt data, invalid format, etc.)
///
/// # Example
/// ```ignore
/// let compressed_data = get_compressed_chunk();
/// let decompressed = decompress_lz4(&compressed_data)?;
/// ```
pub fn decompress_lz4(compressed: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = FrameDecoder::new(compressed);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed)?;
    Ok(decompressed)
}

/// Check if data appears to be LZ4 compressed
pub fn is_lz4_compressed(data: &[u8]) -> bool {
    // LZ4 frame magic number: 0x184D2204
    data.len() >= 4 && data[0..4] == [0x04, 0x22, 0x4D, 0x18]
}

/// Decompress if needed, return original if not compressed
///
/// # Arguments
/// * `data` - Data that may or may not be LZ4 compressed
///
/// # Returns
/// * `Ok(Vec<u8>)` - Decompressed data if LZ4 compressed, original data otherwise
/// * `Err(Error)` - If decompression fails
///
/// # Example
/// ```ignore
/// let chunk_data = download_chunk();
/// let data = decompress_if_needed(chunk_data)?;
/// ```
pub fn decompress_if_needed(data: Vec<u8>) -> Result<Vec<u8>> {
    if is_lz4_compressed(&data) {
        decompress_lz4(&data)
    } else {
        Ok(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lz4_flex::frame::FrameEncoder;
    use std::io::Write;

    /// Test basic LZ4 compression and decompression
    #[test]
    fn test_lz4_compression_decompression() {
        let original = b"Hello, World! This is test data for LZ4 compression.";

        // Compress using lz4_flex
        let mut encoder = FrameEncoder::new(Vec::new());
        encoder.write_all(original).unwrap();
        let compressed = encoder.finish().unwrap();

        // Verify it's recognized as LZ4
        assert!(is_lz4_compressed(&compressed));

        // Decompress
        let decompressed = decompress_lz4(&compressed).unwrap();
        assert_eq!(decompressed, original);
    }

    /// Test LZ4 magic number detection with valid LZ4 frame
    #[test]
    fn test_is_lz4_compressed_valid_frame() {
        // LZ4 frame magic number: 0x184D2204 (little-endian)
        let lz4_header = [0x04, 0x22, 0x4D, 0x18, 0x00, 0x00];
        assert!(is_lz4_compressed(&lz4_header));
    }

    /// Test LZ4 magic number detection with non-LZ4 data
    #[test]
    fn test_is_lz4_compressed_non_lz4() {
        let not_lz4 = [0x00, 0x00, 0x00, 0x00];
        assert!(!is_lz4_compressed(&not_lz4));
    }

    /// Test LZ4 magic number detection with empty data
    #[test]
    fn test_is_lz4_compressed_empty() {
        let empty: &[u8] = &[];
        assert!(!is_lz4_compressed(empty));
    }

    /// Test LZ4 magic number detection with short data
    #[test]
    fn test_is_lz4_compressed_too_short() {
        let short_data = [0x04, 0x22, 0x4D]; // Only 3 bytes
        assert!(!is_lz4_compressed(&short_data));
    }

    /// Test decompress_if_needed with compressed data
    #[test]
    fn test_decompress_if_needed_compressed() {
        let original = b"Test data that will be compressed";

        // Compress
        let mut encoder = FrameEncoder::new(Vec::new());
        encoder.write_all(original).unwrap();
        let compressed = encoder.finish().unwrap();

        // Decompress using decompress_if_needed
        let result = decompress_if_needed(compressed).unwrap();
        assert_eq!(result, original);
    }

    /// Test decompress_if_needed with uncompressed data
    #[test]
    fn test_decompress_if_needed_uncompressed() {
        let original = b"Uncompressed data";
        let result = decompress_if_needed(original.to_vec()).unwrap();
        assert_eq!(result, original);
    }

    /// Test decompression with larger data to verify streaming works correctly
    #[test]
    fn test_lz4_large_data() {
        // Create 1MB of test data
        let original: Vec<u8> = (0..1_000_000)
            .map(|i| (i % 256) as u8)
            .collect();

        // Compress
        let mut encoder = FrameEncoder::new(Vec::new());
        encoder.write_all(&original).unwrap();
        let compressed = encoder.finish().unwrap();

        // Verify compression actually reduced size
        assert!(compressed.len() < original.len());

        // Decompress
        let decompressed = decompress_lz4(&compressed).unwrap();
        assert_eq!(decompressed, original);
    }

    /// Test decompression with repeated data for high compression ratio
    #[test]
    fn test_lz4_highly_compressible() {
        // Create highly compressible data (repeated pattern)
        let original = vec![0x42u8; 100_000]; // 100KB of same byte

        // Compress
        let mut encoder = FrameEncoder::new(Vec::new());
        encoder.write_all(&original).unwrap();
        let compressed = encoder.finish().unwrap();

        // Verify high compression ratio (should be much smaller)
        assert!(compressed.len() < original.len() / 10);

        // Decompress
        let decompressed = decompress_lz4(&compressed).unwrap();
        assert_eq!(decompressed, original);
    }

    /// Test decompression error handling with corrupt data
    #[test]
    fn test_lz4_decompress_corrupt_data() {
        // Create data that looks like LZ4 but is corrupt
        let corrupt_data = vec![0x04, 0x22, 0x4D, 0x18, 0xFF, 0xFF, 0xFF, 0xFF];
        let result = decompress_lz4(&corrupt_data);
        assert!(result.is_err());
    }

    /// Test decompression with empty compressed data
    #[test]
    fn test_lz4_decompress_empty() {
        let empty: &[u8] = &[];
        let result = decompress_lz4(empty);
        // Empty data should either fail or return empty result
        // The lz4_flex decoder may handle this gracefully
        match result {
            Ok(data) => assert!(data.is_empty(), "Empty input should produce empty output"),
            Err(_) => {} // Also acceptable - invalid frame
        }
    }

    /// Test decompress_if_needed with various data patterns
    #[test]
    fn test_decompress_if_needed_various_patterns() {
        // Test with arrow IPC-like data (which would not be LZ4 compressed)
        let arrow_ipc_header = b"ARROW1\x00\x00";
        let result = decompress_if_needed(arrow_ipc_header.to_vec()).unwrap();
        assert_eq!(result, arrow_ipc_header);

        // Test with JSON-like data
        let json_data = br#"{"key": "value"}"#;
        let result = decompress_if_needed(json_data.to_vec()).unwrap();
        assert_eq!(result, json_data);
    }

    /// Test that LZ4 magic number is in little-endian format
    #[test]
    fn test_lz4_magic_number_endianness() {
        // 0x184D2204 in little-endian is [0x04, 0x22, 0x4D, 0x18]
        let magic_number_le = [0x04, 0x22, 0x4D, 0x18];
        assert!(is_lz4_compressed(&magic_number_le));

        // Big-endian format should NOT be recognized
        let magic_number_be = [0x18, 0x4D, 0x22, 0x04];
        assert!(!is_lz4_compressed(&magic_number_be));
    }

    /// Test roundtrip with real-world Arrow IPC-like data
    #[test]
    fn test_lz4_arrow_ipc_roundtrip() {
        // Simulate Arrow IPC stream data
        let arrow_data = b"ARROW1\x00\x00\xFF\xFF\xFF\xFF\x00\x00\x00\x00\x00\x00\x00\x00";

        // Compress it
        let mut encoder = FrameEncoder::new(Vec::new());
        encoder.write_all(arrow_data).unwrap();
        let compressed = encoder.finish().unwrap();

        // Verify it's compressed
        assert!(is_lz4_compressed(&compressed));
        assert!(compressed.len() < arrow_data.len() + 20); // Account for frame overhead

        // Decompress and verify
        let decompressed = decompress_lz4(&compressed).unwrap();
        assert_eq!(decompressed, arrow_data);
    }
}
