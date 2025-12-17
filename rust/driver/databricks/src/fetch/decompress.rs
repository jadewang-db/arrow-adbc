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

/// Decompress LZ4_FRAME compressed data
pub fn decompress_lz4(_compressed: &[u8]) -> Result<Vec<u8>> {
    todo!("decompress_lz4 implementation in work item 3.2")
}

/// Check if data appears to be LZ4 compressed
pub fn is_lz4_compressed(data: &[u8]) -> bool {
    // LZ4 frame magic number: 0x184D2204
    data.len() >= 4 && data[0..4] == [0x04, 0x22, 0x4D, 0x18]
}

/// Decompress if needed, return original if not compressed
pub fn decompress_if_needed(data: Vec<u8>) -> Result<Vec<u8>> {
    if is_lz4_compressed(&data) {
        decompress_lz4(&data)
    } else {
        Ok(data)
    }
}
