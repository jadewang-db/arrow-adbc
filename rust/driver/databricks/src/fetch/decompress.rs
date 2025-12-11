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
//! from Databricks.

use crate::error::Result;

/// Decompress LZ4 frame data.
///
/// # Arguments
///
/// * `data` - The compressed data.
///
/// # Returns
///
/// The decompressed data.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decompress_empty() {
        // Empty data returns empty output (no valid LZ4 frame header to read)
        let result = decompress_lz4_frame(&[]);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_decompress_invalid() {
        // Invalid LZ4 frame data should fail
        let invalid_data = b"not a valid lz4 frame";
        let result = decompress_lz4_frame(invalid_data);
        assert!(result.is_err());
    }
}
