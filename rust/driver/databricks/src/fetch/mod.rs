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

use crate::error::Result;

/// Fetches result chunks from external links in parallel
pub struct ChunkFetcher;

impl ChunkFetcher {
    pub fn new() -> Result<Self> {
        todo!("ChunkFetcher::new implementation in work item 3.3")
    }
}

impl Default for ChunkFetcher {
    fn default() -> Self {
        Self
    }
}
