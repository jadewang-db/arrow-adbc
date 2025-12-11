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

//! Unit tests for LZ4 decompression functionality.
//!
//! Tests cover:
//! - LZ4 frame magic number detection
//! - Decompression of valid LZ4 data
//! - Pass-through of non-compressed data
//! - Error handling for invalid compressed data
//! - Various data sizes and edge cases

use adbc_driver_databricks::fetch::{decompress_if_needed, decompress_lz4_frame, is_lz4_compressed};
use lz4_flex::frame::FrameEncoder;
use std::io::Write;

// =============================================================================
// Test Helpers
// =============================================================================

/// Helper to create LZ4 compressed data for testing.
fn compress_lz4(data: &[u8]) -> Vec<u8> {
    let mut encoder = FrameEncoder::new(Vec::new());
    encoder.write_all(data).unwrap();
    encoder.finish().unwrap()
}

/// LZ4 frame magic number constant for reference.
const LZ4_FRAME_MAGIC: [u8; 4] = [0x04, 0x22, 0x4D, 0x18];

// =============================================================================
// is_lz4_compressed Tests
// =============================================================================

mod is_lz4_compressed_tests {
    use super::*;

    #[test]
    fn test_with_valid_compressed_data() {
        let compressed = compress_lz4(b"test data");
        assert!(is_lz4_compressed(&compressed));
    }

    #[test]
    fn test_with_uncompressed_data() {
        let uncompressed = b"ARRO"; // Arrow magic
        assert!(!is_lz4_compressed(uncompressed));
    }

    #[test]
    fn test_with_empty_data() {
        assert!(!is_lz4_compressed(&[]));
    }

    #[test]
    fn test_with_one_byte() {
        assert!(!is_lz4_compressed(&[0x04]));
    }

    #[test]
    fn test_with_two_bytes() {
        assert!(!is_lz4_compressed(&[0x04, 0x22]));
    }

    #[test]
    fn test_with_three_bytes() {
        assert!(!is_lz4_compressed(&[0x04, 0x22, 0x4D]));
    }

    #[test]
    fn test_with_exact_magic_number() {
        assert!(is_lz4_compressed(&LZ4_FRAME_MAGIC));
    }

    #[test]
    fn test_with_magic_plus_data() {
        let mut data = LZ4_FRAME_MAGIC.to_vec();
        data.extend_from_slice(b"extra data");
        assert!(is_lz4_compressed(&data));
    }

    #[test]
    fn test_with_partial_magic_mismatch() {
        // First byte matches but rest don't
        assert!(!is_lz4_compressed(&[0x04, 0x00, 0x00, 0x00]));
        // First two bytes match
        assert!(!is_lz4_compressed(&[0x04, 0x22, 0x00, 0x00]));
        // First three bytes match
        assert!(!is_lz4_compressed(&[0x04, 0x22, 0x4D, 0x00]));
    }

    #[test]
    fn test_with_random_data() {
        let random_data: Vec<u8> = (0..100).map(|i| i as u8).collect();
        // Very unlikely to match magic number
        assert!(!is_lz4_compressed(&random_data));
    }

    #[test]
    fn test_with_all_zeros() {
        let zeros = vec![0u8; 100];
        assert!(!is_lz4_compressed(&zeros));
    }

    #[test]
    fn test_with_arrow_ipc_header() {
        // Arrow IPC stream continuation marker
        let arrow_continuation = vec![0xFF, 0xFF, 0xFF, 0xFF];
        assert!(!is_lz4_compressed(&arrow_continuation));
    }
}

// =============================================================================
// decompress_lz4_frame Tests
// =============================================================================

mod decompress_lz4_frame_tests {
    use super::*;

    #[test]
    fn test_basic_decompression() {
        let original = b"Hello, World!";
        let compressed = compress_lz4(original);

        let decompressed = decompress_lz4_frame(&compressed).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_empty_data() {
        let result = decompress_lz4_frame(&[]);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_compressed_empty_data() {
        let compressed = compress_lz4(&[]);
        let decompressed = decompress_lz4_frame(&compressed).unwrap();
        assert!(decompressed.is_empty());
    }

    #[test]
    fn test_invalid_data() {
        let invalid_data = b"not a valid lz4 frame";
        let result = decompress_lz4_frame(invalid_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_corrupted_compressed_data() {
        let mut compressed = compress_lz4(b"test data");
        // Corrupt some bytes in the middle
        if compressed.len() > 10 {
            compressed[8] ^= 0xFF;
            compressed[9] ^= 0xFF;
        }
        let result = decompress_lz4_frame(&compressed);
        // May succeed or fail depending on where corruption is
        // The important thing is it doesn't panic
        let _ = result;
    }

    #[test]
    fn test_truncated_compressed_data() {
        let compressed = compress_lz4(b"test data that is longer");
        // Truncate the data
        let truncated = &compressed[..compressed.len() / 2];
        let result = decompress_lz4_frame(truncated);
        // Should fail gracefully
        assert!(result.is_err());
    }

    #[test]
    fn test_small_data() {
        let original = b"x";
        let compressed = compress_lz4(original);
        let decompressed = decompress_lz4_frame(&compressed).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_medium_data() {
        let original: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
        let compressed = compress_lz4(&original);
        let decompressed = decompress_lz4_frame(&compressed).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_large_data() {
        let original: Vec<u8> = (0..100000).map(|i| (i % 256) as u8).collect();
        let compressed = compress_lz4(&original);
        let decompressed = decompress_lz4_frame(&compressed).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_highly_compressible_data() {
        // Repeated data compresses very well
        let original = vec![0xAB; 100000];
        let compressed = compress_lz4(&original);

        // Verify compression is effective
        assert!(compressed.len() < original.len());

        let decompressed = decompress_lz4_frame(&compressed).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_incompressible_data() {
        // Random-like data doesn't compress well
        let mut original = Vec::new();
        let mut val: u32 = 12345;
        for _ in 0..10000 {
            val = val.wrapping_mul(1103515245).wrapping_add(12345);
            original.push((val >> 16) as u8);
        }

        let compressed = compress_lz4(&original);
        let decompressed = decompress_lz4_frame(&compressed).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_binary_data() {
        // All possible byte values
        let original: Vec<u8> = (0..256).map(|i| i as u8).collect();
        let compressed = compress_lz4(&original);
        let decompressed = decompress_lz4_frame(&compressed).unwrap();
        assert_eq!(decompressed, original);
    }
}

// =============================================================================
// decompress_if_needed Tests
// =============================================================================

mod decompress_if_needed_tests {
    use super::*;

    #[test]
    fn test_compressed_data_is_decompressed() {
        let original = b"Test data for decompression";
        let compressed = compress_lz4(original);

        let result = decompress_if_needed(compressed).unwrap();
        assert_eq!(result, original);
    }

    #[test]
    fn test_uncompressed_data_passes_through() {
        let original = b"Uncompressed data without LZ4 magic".to_vec();

        let result = decompress_if_needed(original.clone()).unwrap();
        assert_eq!(result, original);
    }

    #[test]
    fn test_empty_data() {
        let result = decompress_if_needed(Vec::new()).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_arrow_ipc_data_passes_through() {
        // Arrow IPC stream continuation marker
        let arrow_like = vec![0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00];

        let result = decompress_if_needed(arrow_like.clone()).unwrap();
        assert_eq!(result, arrow_like);
    }

    #[test]
    fn test_plain_text_passes_through() {
        let text = b"SELECT * FROM table WHERE id = 1".to_vec();
        let result = decompress_if_needed(text.clone()).unwrap();
        assert_eq!(result, text);
    }

    #[test]
    fn test_binary_data_without_magic() {
        let binary: Vec<u8> = (0..256).map(|i| i as u8).collect();
        let result = decompress_if_needed(binary.clone()).unwrap();
        assert_eq!(result, binary);
    }

    #[test]
    fn test_data_starting_with_partial_magic() {
        // Starts with first byte of magic but isn't LZ4
        let data = vec![0x04, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03];
        let result = decompress_if_needed(data.clone()).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_roundtrip_various_sizes() {
        for size in [0, 1, 10, 100, 1000, 10000] {
            let original: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
            let compressed = compress_lz4(&original);

            let decompressed = decompress_if_needed(compressed).unwrap();
            assert_eq!(decompressed, original, "Failed for size {}", size);
        }
    }

    #[test]
    fn test_double_decompression_safe() {
        // Ensure that decompressing already-decompressed data doesn't break
        let original = b"test data";
        let compressed = compress_lz4(original);

        let first_decompress = decompress_if_needed(compressed).unwrap();
        assert_eq!(first_decompress, original);

        // Second call should pass through unchanged (not LZ4 compressed anymore)
        let second_result = decompress_if_needed(first_decompress).unwrap();
        assert_eq!(second_result, original);
    }
}

// =============================================================================
// Integration-Style Tests
// =============================================================================

mod integration_tests {
    use super::*;

    #[test]
    fn test_simulated_chunk_flow() {
        // Simulate the flow of fetching and processing chunks
        let original_arrow_data = b"Arrow IPC stream data here...";

        // Simulate server compressing the data
        let compressed_chunk = compress_lz4(original_arrow_data);

        // Verify it's detected as compressed
        assert!(is_lz4_compressed(&compressed_chunk));

        // Decompress
        let decompressed = decompress_if_needed(compressed_chunk).unwrap();

        // Verify we get back the original
        assert_eq!(decompressed, original_arrow_data);

        // Verify it's no longer detected as compressed
        assert!(!is_lz4_compressed(&decompressed));
    }

    #[test]
    fn test_uncompressed_chunk_flow() {
        // Some servers may send uncompressed data
        let original_arrow_data = b"Uncompressed Arrow IPC stream".to_vec();

        // Not compressed
        assert!(!is_lz4_compressed(&original_arrow_data));

        // Pass through decompress_if_needed
        let result = decompress_if_needed(original_arrow_data.clone()).unwrap();

        // Should be unchanged
        assert_eq!(result, original_arrow_data);
    }

    #[test]
    fn test_multiple_chunks_processing() {
        // Simulate processing multiple chunks
        let chunks_original: Vec<Vec<u8>> = vec![
            b"chunk 0 data".to_vec(),
            b"chunk 1 data with more content".to_vec(),
            b"chunk 2 final data".to_vec(),
        ];

        // Compress all chunks
        let compressed_chunks: Vec<Vec<u8>> =
            chunks_original.iter().map(|c| compress_lz4(c)).collect();

        // Verify all are detected as compressed
        for compressed in &compressed_chunks {
            assert!(is_lz4_compressed(compressed));
        }

        // Decompress all
        let decompressed_chunks: Vec<Vec<u8>> = compressed_chunks
            .into_iter()
            .map(|c| decompress_if_needed(c).unwrap())
            .collect();

        // Verify we get back the originals
        for (original, decompressed) in chunks_original.iter().zip(decompressed_chunks.iter()) {
            assert_eq!(original, decompressed);
        }
    }
}

// =============================================================================
// Edge Case Tests
// =============================================================================

mod edge_case_tests {
    use super::*;

    #[test]
    fn test_exact_four_bytes_magic() {
        // Exactly the magic number with nothing else
        let data = LZ4_FRAME_MAGIC.to_vec();
        assert!(is_lz4_compressed(&data));
        // Decompression should handle this (may fail or return empty)
        let _ = decompress_if_needed(data);
    }

    #[test]
    fn test_magic_at_wrong_offset() {
        // Magic number present but not at start
        let mut data = vec![0x00; 10];
        data.extend_from_slice(&LZ4_FRAME_MAGIC);
        assert!(!is_lz4_compressed(&data));
    }

    #[test]
    fn test_very_large_compression_ratio() {
        // Data that compresses to tiny size
        let original = vec![0x00; 1_000_000]; // 1MB of zeros
        let compressed = compress_lz4(&original);

        // Should decompress back correctly
        let decompressed = decompress_if_needed(compressed).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_unicode_content() {
        // Unicode text data
        let original = "Hello, \u{4E16}\u{754C}! \u{1F600}".as_bytes();
        let compressed = compress_lz4(original);
        let decompressed = decompress_if_needed(compressed).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_null_bytes_in_data() {
        // Data with embedded null bytes
        let original = vec![0x00, 0x01, 0x00, 0x02, 0x00, 0x03];
        let compressed = compress_lz4(&original);
        let decompressed = decompress_if_needed(compressed).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_all_same_bytes() {
        for byte_val in [0x00, 0x7F, 0xFF] {
            let original = vec![byte_val; 10000];
            let compressed = compress_lz4(&original);
            let decompressed = decompress_if_needed(compressed).unwrap();
            assert_eq!(
                decompressed, original,
                "Failed for byte value {:02X}",
                byte_val
            );
        }
    }
}

// =============================================================================
// Performance Characteristics Tests
// =============================================================================

mod performance_tests {
    use super::*;

    #[test]
    fn test_compression_ratio_repetitive_data() {
        let original = vec![0xAB; 10000];
        let compressed = compress_lz4(&original);

        // Highly repetitive data should compress very well
        let ratio = original.len() as f64 / compressed.len() as f64;
        assert!(
            ratio > 10.0,
            "Expected high compression ratio for repetitive data, got {}",
            ratio
        );
    }

    #[test]
    fn test_compression_ratio_random_data() {
        // Pseudo-random data
        let mut original = Vec::new();
        let mut val: u32 = 42;
        for _ in 0..10000 {
            val = val.wrapping_mul(1103515245).wrapping_add(12345);
            original.push((val >> 16) as u8);
        }

        let compressed = compress_lz4(&original);

        // Random data typically doesn't compress well and may even expand
        // Just verify it works
        let decompressed = decompress_if_needed(compressed).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_small_overhead_for_tiny_data() {
        // Very small data - overhead of LZ4 frame format
        let original = b"x";
        let compressed = compress_lz4(original);

        // LZ4 has some fixed overhead for frame headers
        // But data should still decompress correctly
        let decompressed = decompress_if_needed(compressed).unwrap();
        assert_eq!(decompressed, original);
    }
}
