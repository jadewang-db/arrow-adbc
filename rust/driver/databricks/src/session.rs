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

//! Session management for Databricks connections.
//!
//! Sessions maintain connection state with the SQL Warehouse and enable
//! features like temporary tables and session-scoped configurations.
//!
//! # Session Lifecycle
//!
//! - Sessions are created lazily on first use (when `get_session_id` is called)
//! - Sessions are kept alive automatically (statements refresh the idle timeout)
//! - Sessions are terminated when `terminate` is called or the manager is dropped
//!
//! # Example
//!
//! ```ignore
//! use std::sync::Arc;
//! use adbc_driver_databricks::client::SeaClient;
//! use adbc_driver_databricks::session::SessionManager;
//!
//! let client = Arc::new(SeaClient::new(config)?);
//! let session_manager = SessionManager::new(
//!     client,
//!     Some("main".to_string()),
//!     Some("default".to_string()),
//! );
//!
//! // Session is created lazily
//! let session_id = session_manager.get_session_id().await?;
//!
//! // Use the session...
//!
//! // Terminate when done
//! session_manager.terminate().await?;
//! ```

use std::sync::Arc;
use tokio::sync::Mutex;

use crate::client::SeaClient;
use crate::error::Result;

/// Session manager for creating and managing SQL Warehouse sessions.
///
/// This struct manages the lifecycle of a session with a Databricks SQL Warehouse.
/// It provides lazy session creation (sessions are created on first use) and
/// handles proper cleanup when the session is no longer needed.
///
/// # Thread Safety
///
/// `SessionManager` is thread-safe and can be shared across multiple async tasks.
/// It uses interior mutability with `Mutex` to protect the session state.
///
/// # Session Caching
///
/// Once a session is created, it is cached and reused for subsequent calls.
/// This avoids the overhead of creating multiple sessions.
#[derive(Debug)]
pub struct SessionManager {
    /// The SEA client used for API calls.
    client: Arc<SeaClient>,
    /// The cached session ID, protected by a mutex for thread safety.
    session_id: Mutex<Option<String>>,
    /// Default catalog for the session.
    catalog: Option<String>,
    /// Default schema for the session.
    schema: Option<String>,
}

impl SessionManager {
    /// Create a new session manager.
    ///
    /// Note: This does not create a session immediately. The session is created
    /// lazily when `get_session_id` is first called.
    ///
    /// # Arguments
    ///
    /// * `client` - The SEA client to use for API calls
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

    /// Get the session ID, creating a new session if necessary.
    ///
    /// This method is idempotent - calling it multiple times will return the
    /// same session ID without creating additional sessions.
    ///
    /// # Returns
    ///
    /// The session ID for this connection.
    ///
    /// # Errors
    ///
    /// Returns an error if session creation fails.
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

    /// Check if a session is currently active.
    ///
    /// # Returns
    ///
    /// `true` if a session has been created and is active.
    pub async fn has_session(&self) -> bool {
        self.session_id.lock().await.is_some()
    }

    /// Terminate the session if active.
    ///
    /// This method safely terminates the current session and clears the cached
    /// session ID. It is safe to call even if no session exists.
    ///
    /// # Errors
    ///
    /// Returns an error if the session deletion fails. Note that if the session
    /// has already expired or been deleted server-side, this may return a 404
    /// error, which is typically safe to ignore.
    pub async fn terminate(&self) -> Result<()> {
        let mut guard = self.session_id.lock().await;

        if let Some(ref id) = *guard {
            // Attempt to delete the session
            // We proceed with clearing the local state even if deletion fails
            let result = self.client.delete_session(id).await;
            *guard = None;
            result?;
        }

        Ok(())
    }

    /// Get the default catalog for this session.
    pub fn catalog(&self) -> Option<&str> {
        self.catalog.as_deref()
    }

    /// Get the default schema for this session.
    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{SeaClientConfig, SessionResponse};
    use wiremock::matchers::{body_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Helper to create a SeaClient pointing to a mock server.
    fn create_test_client(server_uri: &str) -> Arc<SeaClient> {
        let config = SeaClientConfig::new(server_uri, "test-token", "test-warehouse");
        Arc::new(SeaClient::new(config).unwrap())
    }

    #[tokio::test]
    async fn test_session_manager_new() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        let manager = SessionManager::new(
            client,
            Some("test_catalog".to_string()),
            Some("test_schema".to_string()),
        );

        assert_eq!(manager.catalog(), Some("test_catalog"));
        assert_eq!(manager.schema(), Some("test_schema"));
        assert!(!manager.has_session().await);
    }

    #[tokio::test]
    async fn test_session_manager_new_without_catalog_schema() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        let manager = SessionManager::new(client, None, None);

        assert_eq!(manager.catalog(), None);
        assert_eq!(manager.schema(), None);
        assert!(!manager.has_session().await);
    }

    #[tokio::test]
    async fn test_get_session_id_creates_session() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(SessionResponse {
                session_id: "test-session-123".to_string(),
            }))
            .expect(1) // Expect exactly one call
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri());
        let manager = SessionManager::new(client, None, None);

        let session_id = manager.get_session_id().await.unwrap();
        assert_eq!(session_id, "test-session-123");
        assert!(manager.has_session().await);
    }

    #[tokio::test]
    async fn test_get_session_id_returns_cached_session() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(SessionResponse {
                session_id: "cached-session-456".to_string(),
            }))
            .expect(1) // Should only be called once despite multiple get_session_id calls
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri());
        let manager = SessionManager::new(client, None, None);

        // Call get_session_id multiple times
        let session_id_1 = manager.get_session_id().await.unwrap();
        let session_id_2 = manager.get_session_id().await.unwrap();
        let session_id_3 = manager.get_session_id().await.unwrap();

        // All should return the same session ID
        assert_eq!(session_id_1, "cached-session-456");
        assert_eq!(session_id_2, "cached-session-456");
        assert_eq!(session_id_3, "cached-session-456");
    }

    #[tokio::test]
    async fn test_get_session_id_with_catalog_and_schema() {
        let mock_server = MockServer::start().await;

        // Use body_json to verify the request body
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .and(body_json(&serde_json::json!({
                "warehouse_id": "test-warehouse",
                "catalog": "main",
                "schema": "default"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(SessionResponse {
                session_id: "session-with-catalog".to_string(),
            }))
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri());
        let manager = SessionManager::new(
            client,
            Some("main".to_string()),
            Some("default".to_string()),
        );

        let session_id = manager.get_session_id().await.unwrap();
        assert_eq!(session_id, "session-with-catalog");
    }

    #[tokio::test]
    async fn test_terminate_deletes_session() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(SessionResponse {
                session_id: "session-to-delete".to_string(),
            }))
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/session-to-delete"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri());
        let manager = SessionManager::new(client, None, None);

        // Create a session
        let session_id = manager.get_session_id().await.unwrap();
        assert_eq!(session_id, "session-to-delete");
        assert!(manager.has_session().await);

        // Terminate the session
        manager.terminate().await.unwrap();
        assert!(!manager.has_session().await);
    }

    #[tokio::test]
    async fn test_terminate_without_session_is_noop() {
        let mock_server = MockServer::start().await;

        // No mocks needed - DELETE should not be called
        let client = create_test_client(&mock_server.uri());
        let manager = SessionManager::new(client, None, None);

        // Should succeed without making any API calls
        manager.terminate().await.unwrap();
        assert!(!manager.has_session().await);
    }

    #[tokio::test]
    async fn test_terminate_clears_session_even_on_api_error() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(SessionResponse {
                session_id: "session-with-error".to_string(),
            }))
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/session-with-error"))
            .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                "error_code": "NOT_FOUND",
                "message": "Session not found"
            })))
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri());
        let manager = SessionManager::new(client, None, None);

        // Create a session
        manager.get_session_id().await.unwrap();
        assert!(manager.has_session().await);

        // Terminate should fail but still clear the session
        let result = manager.terminate().await;
        assert!(result.is_err());
        assert!(!manager.has_session().await);
    }

    #[tokio::test]
    async fn test_create_session_error_propagates() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
                "error_code": "UNAUTHENTICATED",
                "message": "Invalid token"
            })))
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri());
        let manager = SessionManager::new(client, None, None);

        let result = manager.get_session_id().await;
        assert!(result.is_err());
        assert!(!manager.has_session().await);
    }

    #[tokio::test]
    async fn test_session_manager_can_recreate_after_terminate() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(SessionResponse {
                session_id: "new-session".to_string(),
            }))
            .expect(2) // Called twice: once initially, once after terminate
            .mount(&mock_server)
            .await;

        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/new-session"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri());
        let manager = SessionManager::new(client, None, None);

        // Create first session
        let session_id_1 = manager.get_session_id().await.unwrap();
        assert_eq!(session_id_1, "new-session");

        // Terminate
        manager.terminate().await.unwrap();

        // Create new session
        let session_id_2 = manager.get_session_id().await.unwrap();
        assert_eq!(session_id_2, "new-session");
    }
}
