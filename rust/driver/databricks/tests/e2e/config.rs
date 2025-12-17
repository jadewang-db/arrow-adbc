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

use serde::{Deserialize, Serialize};
use std::error::Error;

/// E2E test configuration matching C# ADBC driver format
/// Loaded from JSON file via DATABRICKS_TEST_CONFIG_FILE environment variable
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct E2EConfig {
    /// Environment name (e.g., "Databricks")
    pub environment: String,

    /// Full URI including warehouse path
    /// Format: https://{host}/sql/1.0/warehouses/{warehouse_id}
    pub uri: String,

    /// Personal Access Token
    pub token: String,

    /// Optional test query
    #[serde(default)]
    pub query: String,

    /// Driver type (e.g., "databricks")
    #[serde(rename = "type")]
    pub driver_type: String,

    /// Trace logging flag
    #[serde(default)]
    pub trace: String,

    /// Expected number of results for test query
    #[serde(rename = "expectedResults", default)]
    pub expected_results: i64,

    /// Test metadata
    #[serde(default)]
    pub metadata: TestMetadata,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TestMetadata {
    /// Catalog name for tests
    #[serde(default)]
    pub catalog: String,

    /// Schema name for tests
    #[serde(default)]
    pub schema: String,

    /// Table name for tests
    #[serde(default)]
    pub table: String,

    /// Expected column count for metadata tests
    #[serde(rename = "expectedColumnCount", default)]
    pub expected_column_count: i32,
}

impl E2EConfig {
    /// Load configuration from file specified by DATABRICKS_TEST_CONFIG_FILE
    pub fn from_env() -> Result<Self, Box<dyn Error>> {
        let config_path = std::env::var("DATABRICKS_TEST_CONFIG_FILE")?;
        let content = std::fs::read_to_string(config_path)?;
        let config: E2EConfig = serde_json::from_str(&content)?;
        Ok(config)
    }

    /// Parse host and warehouse_id from uri
    pub fn parse_uri(&self) -> Result<(String, String), Box<dyn Error>> {
        // Parse: https://{host}/sql/1.0/warehouses/{warehouse_id}
        let url = url::Url::parse(&self.uri)?;
        let host = url.host_str().ok_or("No host in URI")?.to_string();
        let warehouse_id = url
            .path_segments()
            .and_then(|segments| segments.last())
            .ok_or("No warehouse_id in URI")?
            .to_string();
        Ok((format!("https://{}", host), warehouse_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_parse_valid_json() {
        let json = r#"{
            "environment": "Databricks",
            "uri": "https://my-workspace.cloud.databricks.com/sql/1.0/warehouses/abc123def456",
            "token": "dapi1234567890",
            "query": "SELECT 1",
            "type": "databricks",
            "trace": "true",
            "expectedResults": 1,
            "metadata": {
                "catalog": "main",
                "schema": "default",
                "table": "test_table",
                "expectedColumnCount": 3
            }
        }"#;

        let config: E2EConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.environment, "Databricks");
        assert_eq!(
            config.uri,
            "https://my-workspace.cloud.databricks.com/sql/1.0/warehouses/abc123def456"
        );
        assert_eq!(config.token, "dapi1234567890");
        assert_eq!(config.query, "SELECT 1");
        assert_eq!(config.driver_type, "databricks");
        assert_eq!(config.trace, "true");
        assert_eq!(config.expected_results, 1);
        assert_eq!(config.metadata.catalog, "main");
        assert_eq!(config.metadata.schema, "default");
        assert_eq!(config.metadata.table, "test_table");
        assert_eq!(config.metadata.expected_column_count, 3);
    }

    #[test]
    fn test_config_parse_minimal_json() {
        let json = r#"{
            "environment": "Databricks",
            "uri": "https://my-workspace.cloud.databricks.com/sql/1.0/warehouses/abc123",
            "token": "dapi123",
            "type": "databricks"
        }"#;

        let config: E2EConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.environment, "Databricks");
        assert_eq!(config.query, "");
        assert_eq!(config.trace, "");
        assert_eq!(config.expected_results, 0);
        assert_eq!(config.metadata.catalog, "");
        assert_eq!(config.metadata.schema, "");
        assert_eq!(config.metadata.table, "");
        assert_eq!(config.metadata.expected_column_count, 0);
    }

    #[test]
    fn test_parse_uri_valid() {
        let config = E2EConfig {
            environment: "Databricks".to_string(),
            uri: "https://my-workspace.cloud.databricks.com/sql/1.0/warehouses/abc123def456"
                .to_string(),
            token: "token".to_string(),
            query: String::new(),
            driver_type: "databricks".to_string(),
            trace: String::new(),
            expected_results: 0,
            metadata: TestMetadata::default(),
        };

        let (host, warehouse_id) = config.parse_uri().unwrap();
        assert_eq!(host, "https://my-workspace.cloud.databricks.com");
        assert_eq!(warehouse_id, "abc123def456");
    }

    #[test]
    fn test_parse_uri_invalid_url() {
        let config = E2EConfig {
            environment: "Databricks".to_string(),
            uri: "not-a-valid-url".to_string(),
            token: "token".to_string(),
            query: String::new(),
            driver_type: "databricks".to_string(),
            trace: String::new(),
            expected_results: 0,
            metadata: TestMetadata::default(),
        };

        assert!(config.parse_uri().is_err());
    }

    #[test]
    fn test_parse_uri_no_host() {
        let config = E2EConfig {
            environment: "Databricks".to_string(),
            uri: "file:///local/path".to_string(),
            token: "token".to_string(),
            query: String::new(),
            driver_type: "databricks".to_string(),
            trace: String::new(),
            expected_results: 0,
            metadata: TestMetadata::default(),
        };

        assert!(config.parse_uri().is_err());
    }

    #[test]
    fn test_parse_uri_no_warehouse_id() {
        let config = E2EConfig {
            environment: "Databricks".to_string(),
            uri: "https://my-workspace.cloud.databricks.com/".to_string(),
            token: "token".to_string(),
            query: String::new(),
            driver_type: "databricks".to_string(),
            trace: String::new(),
            expected_results: 0,
            metadata: TestMetadata::default(),
        };

        let result = config.parse_uri();
        // This will parse, but warehouse_id will be empty string
        assert!(result.is_ok());
        let (_, warehouse_id) = result.unwrap();
        assert_eq!(warehouse_id, "");
    }
}
