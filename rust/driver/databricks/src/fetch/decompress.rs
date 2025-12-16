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

//! LZ4 decompression for chunk data.
//!
//! This module provides LZ4 frame decompression utilities for
//! decompressing result chunks downloaded from cloud storage.

use lz4_flex::frame::FrameDecoder;
use std::io::Read;

use crate::error::{Error, Result};

/// Decompress LZ4 frame compressed data.
///
/// # Arguments
///
/// * `data` - The compressed data to decompress.
///
/// # Returns
///
/// The decompressed data as a byte vector.
///
/// # Errors
///
/// Returns an error if decompression fails.
pub fn decompress_lz4(data: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = FrameDecoder::new(data);
    let mut decompressed = Vec::new();
    decoder
        .read_to_end(&mut decompressed)
        .map_err(|e| Error::Io(std::io::Error::new(e.kind(), format!("LZ4 decompression failed: {}", e))))?;
    Ok(decompressed)
}

/// Check if data appears to be LZ4 frame compressed.
///
/// LZ4 frame format starts with the magic number 0x184D2204.
///
/// # Arguments
///
/// * `data` - The data to check.
///
/// # Returns
///
/// `true` if the data appears to be LZ4 frame compressed.
pub fn is_lz4_frame(data: &[u8]) -> bool {
    if data.len() < 4 {
        return false;
    }
    // LZ4 frame magic number (little-endian): 0x184D2204
    data[0] == 0x04 && data[1] == 0x22 && data[2] == 0x4D && data[3] == 0x18
}

#[cfg(test)]
mod tests {
    use super::*;
    use lz4_flex::frame::FrameEncoder;
    use std::io::Write;

    #[test]
    fn test_decompress_lz4() {
        // Create some test data
        let original = b"Hello, world! This is a test of LZ4 compression.";

        // Compress it
        let mut encoder = FrameEncoder::new(Vec::new());
        encoder.write_all(original).unwrap();
        let compressed = encoder.finish().unwrap();

        // Decompress it
        let decompressed = decompress_lz4(&compressed).unwrap();

        // Verify
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_is_lz4_frame() {
        // LZ4 frame magic number
        let lz4_data = [0x04, 0x22, 0x4D, 0x18, 0x00, 0x00];
        assert!(is_lz4_frame(&lz4_data));

        // Not LZ4
        let not_lz4 = [0x00, 0x00, 0x00, 0x00];
        assert!(!is_lz4_frame(&not_lz4));

        // Too short
        let short = [0x04, 0x22];
        assert!(!is_lz4_frame(&short));
    }
}
