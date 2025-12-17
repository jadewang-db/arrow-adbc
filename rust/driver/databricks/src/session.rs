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

//! Session management for Databricks connections

use crate::client::SeaClient;
use crate::error::Result;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Session manager for managing SQL Warehouse sessions
///
/// This manager handles creating and caching session IDs for use with
/// SQL Warehouse statements. It ensures that only one session is created
/// per connection and handles proper cleanup when the connection is closed.
pub struct SessionManager {
    client: Arc<SeaClient>,
    session_id: Mutex<Option<String>>,
    catalog: Option<String>,
    schema: Option<String>,
}

impl SessionManager {
    /// Create a new session manager
    ///
    /// # Arguments
    /// * `client` - The SeaClient to use for API calls
    /// * `catalog` - Optional default catalog for the session
    /// * `schema` - Optional default schema for the session
    pub fn new(
        client: Arc<SeaClient>,
        catalog: Option<String>,
        schema: Option<String>,
    ) -> Self {
        Self {
            client,
            session_id: Mutex::new(None),
            catalog,
            schema,
        }
    }

    /// Get or create a session ID
    ///
    /// If a session has already been created, returns the cached session ID.
    /// Otherwise, creates a new session and caches the ID for future use.
    ///
    /// # Returns
    /// The session ID to use for SQL statements
    pub async fn get_session_id(&self) -> Result<String> {
        let mut guard = self.session_id.lock().await;

        if let Some(ref id) = *guard {
            return Ok(id.clone());
        }

        let session_id = self
            .client
            .create_session(self.catalog.clone(), self.schema.clone())
            .await?;

        *guard = Some(session_id.clone());
        Ok(session_id)
    }

    /// Terminate the session if active
    ///
    /// This should be called when the connection is closed to clean up
    /// the session on the server side.
    pub async fn terminate(&self) -> Result<()> {
        let mut guard = self.session_id.lock().await;

        if let Some(ref id) = *guard {
            self.client.delete_session(id).await?;
            *guard = None;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::SeaClientConfig;
    use std::time::Duration;
    use wiremock::{MockServer, Mock, ResponseTemplate};
    use wiremock::matchers::{method, path, body_json};

    #[tokio::test]
    async fn test_session_manager_lazy_creation() {
        let mock_server = MockServer::start().await;

        // Session should be created on first access
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .and(body_json(serde_json::json!({
                "warehouse_id": "test-warehouse",
                "catalog": "main",
                "schema": "default"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "test-session-123"
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: mock_server.uri(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(300),
        };

        let client = Arc::new(SeaClient::new(config).unwrap());
        let manager = SessionManager::new(
            client,
            Some("main".to_string()),
            Some("default".to_string()),
        );

        // First call should create session
        let session_id = manager.get_session_id().await;
        assert!(session_id.is_ok());
        assert_eq!(session_id.unwrap(), "test-session-123");

        // Second call should return cached session (no additional API call)
        let session_id = manager.get_session_id().await;
        assert!(session_id.is_ok());
        assert_eq!(session_id.unwrap(), "test-session-123");
    }

    #[tokio::test]
    async fn test_session_manager_caches_session_id() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "cached-session-456"
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: mock_server.uri(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(300),
        };

        let client = Arc::new(SeaClient::new(config).unwrap());
        let manager = SessionManager::new(client, None, None);

        // Multiple calls should only create one session
        for _ in 0..5 {
            let session_id = manager.get_session_id().await;
            assert!(session_id.is_ok());
            assert_eq!(session_id.unwrap(), "cached-session-456");
        }
    }

    #[tokio::test]
    async fn test_session_manager_terminate() {
        let mock_server = MockServer::start().await;

        // First session creation
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "terminate-session-789"
            })))
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;

        // Session deletion
        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/terminate-session-789"))
            .respond_with(ResponseTemplate::new(200))
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;

        // Second session creation after termination
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "new-session-after-terminate"
            })))
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: mock_server.uri(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(300),
        };

        let client = Arc::new(SeaClient::new(config).unwrap());
        let manager = SessionManager::new(client, None, None);

        // Create session
        let session_id = manager.get_session_id().await.unwrap();
        assert_eq!(session_id, "terminate-session-789");

        // Terminate session
        let result = manager.terminate().await;
        assert!(result.is_ok());

        // After termination, session_id should be cleared
        // Next call should create a new session
        let new_session_id = manager.get_session_id().await.unwrap();
        assert_eq!(new_session_id, "new-session-after-terminate");
    }

    #[tokio::test]
    async fn test_session_manager_terminate_no_session() {
        let config = SeaClientConfig {
            host: "https://test.databricks.com".to_string(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(300),
        };

        let client = Arc::new(SeaClient::new(config).unwrap());
        let manager = SessionManager::new(client, None, None);

        // Terminating without a session should not error
        let result = manager.terminate().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_session_manager_without_catalog_schema() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .and(body_json(serde_json::json!({
                "warehouse_id": "test-warehouse"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "no-catalog-schema-session"
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let config = SeaClientConfig {
            host: mock_server.uri(),
            token: "test-token".to_string(),
            warehouse_id: "test-warehouse".to_string(),
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(300),
        };

        let client = Arc::new(SeaClient::new(config).unwrap());
        let manager = SessionManager::new(client, None, None);

        let session_id = manager.get_session_id().await;
        assert!(session_id.is_ok());
        assert_eq!(session_id.unwrap(), "no-catalog-schema-session");
    }
}
