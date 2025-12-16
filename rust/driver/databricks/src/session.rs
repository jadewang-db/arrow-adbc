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

//! Session management for the Databricks ADBC driver.
//!
//! This module provides session lifecycle management for Databricks SQL Warehouse
//! connections. Sessions maintain connection state and enable features like
//! temporary tables.
//!
//! # Session Lifecycle
//!
//! Sessions are lazily initialized when first requested via [`SessionManager::get_session_id`].
//! Once created, the session ID is cached and reused for subsequent calls. Sessions
//! should be explicitly terminated via [`SessionManager::terminate`] when no longer needed.
//!
//! # Thread Safety
//!
//! `SessionManager` uses `tokio::sync::Mutex` for internal state management, making it
//! safe to share across async tasks. The `SeaClient` is wrapped in `Arc` for efficient
//! sharing.
//!
//! # Example
//!
//! ```ignore
//! use std::sync::Arc;
//! use adbc_databricks::client::{SeaClient, SeaClientConfig};
//! use adbc_databricks::session::SessionManager;
//!
//! let client = Arc::new(SeaClient::new(config)?);
//! let session_manager = SessionManager::new(
//!     client,
//!     Some("main".to_string()),
//!     Some("default".to_string()),
//! );
//!
//! // Lazy session creation
//! let session_id = session_manager.get_session_id().await?;
//!
//! // Session is cached - returns same ID
//! let same_id = session_manager.get_session_id().await?;
//! assert_eq!(session_id, same_id);
//!
//! // Explicit termination
//! session_manager.terminate().await?;
//! ```

use std::sync::Arc;

use tokio::sync::Mutex;

use crate::client::{CreateSessionRequest, SeaClient};
use crate::error::Result;

/// Session manager for Databricks connections.
///
/// Manages the lifecycle of sessions with Databricks SQL Warehouses.
/// Sessions are lazily initialized on first access and cached for subsequent use.
///
/// # Design
///
/// The session manager uses lazy initialization to defer session creation until
/// it's actually needed. This allows for more efficient resource usage when
/// connections might be created but not immediately used.
///
/// The internal state is protected by an async mutex to ensure thread-safe access
/// in concurrent async contexts.
#[derive(Debug)]
pub struct SessionManager {
    /// The SEA client for API calls (shared reference).
    client: Arc<SeaClient>,
    /// Current session ID, protected by async mutex for thread safety.
    session_id: Mutex<Option<String>>,
    /// Default catalog for the session.
    catalog: Option<String>,
    /// Default schema for the session.
    schema: Option<String>,
}

impl SessionManager {
    /// Create a new session manager.
    ///
    /// Creates a session manager with the given client and optional default
    /// catalog/schema. The session is not created immediately - it will be
    /// lazily initialized when [`get_session_id`] is first called.
    ///
    /// # Arguments
    ///
    /// * `client` - The SEA client for API calls, wrapped in Arc for sharing
    /// * `catalog` - Optional default catalog for the session
    /// * `schema` - Optional default schema for the session
    ///
    /// # Example
    ///
    /// ```ignore
    /// let client = Arc::new(SeaClient::new(config)?);
    /// let session_manager = SessionManager::new(
    ///     client,
    ///     Some("main".to_string()),
    ///     Some("default".to_string()),
    /// );
    /// ```
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

    /// Get or create a session ID (lazy initialization).
    ///
    /// If a session has already been created, returns the cached session ID.
    /// Otherwise, creates a new session via the SEA API and caches the ID.
    ///
    /// # Returns
    ///
    /// The session ID string.
    ///
    /// # Errors
    ///
    /// Returns an error if session creation fails (e.g., network error,
    /// authentication failure, warehouse unavailable).
    ///
    /// # Example
    ///
    /// ```ignore
    /// // First call creates the session
    /// let session_id = session_manager.get_session_id().await?;
    ///
    /// // Subsequent calls return cached ID
    /// let same_id = session_manager.get_session_id().await?;
    /// assert_eq!(session_id, same_id);
    /// ```
    pub async fn get_session_id(&self) -> Result<String> {
        let mut guard = self.session_id.lock().await;

        // Return cached session ID if available
        if let Some(ref id) = *guard {
            return Ok(id.clone());
        }

        // Create a new session
        let request = CreateSessionRequest {
            warehouse_id: self.client.warehouse_id().to_string(),
            session_alias: None,
            catalog: self.catalog.clone(),
            schema: self.schema.clone(),
        };

        let response = self.client.create_session(&request).await?;

        // Cache and return the session ID
        *guard = Some(response.session_id.clone());
        Ok(response.session_id)
    }

    /// Terminate the session if active.
    ///
    /// Deletes the session via the SEA API if one is currently active.
    /// After termination, [`is_active`] will return `false` and the next call
    /// to [`get_session_id`] will create a new session.
    ///
    /// # Errors
    ///
    /// Returns an error if session deletion fails. Note that even if deletion
    /// fails, the local session ID is still cleared to avoid inconsistent state.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Terminate the session
    /// session_manager.terminate().await?;
    ///
    /// // Session is no longer active
    /// assert!(!session_manager.is_active().await);
    ///
    /// // Next get_session_id will create a new session
    /// let new_session_id = session_manager.get_session_id().await?;
    /// ```
    pub async fn terminate(&self) -> Result<()> {
        let mut guard = self.session_id.lock().await;

        if let Some(ref id) = *guard {
            // Attempt to delete the session
            // Clear local state regardless of API result to avoid inconsistency
            let result = self.client.delete_session(id).await;
            *guard = None;
            result?;
        }

        Ok(())
    }

    /// Check if a session is currently active.
    ///
    /// Returns `true` if a session has been created and not yet terminated.
    /// Note that this only checks local state - it does not verify the session
    /// is still valid on the server side.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // No session initially
    /// assert!(!session_manager.is_active().await);
    ///
    /// // After getting session ID, it's active
    /// let _ = session_manager.get_session_id().await?;
    /// assert!(session_manager.is_active().await);
    ///
    /// // After termination, it's not active
    /// session_manager.terminate().await?;
    /// assert!(!session_manager.is_active().await);
    /// ```
    pub async fn is_active(&self) -> bool {
        self.session_id.lock().await.is_some()
    }

    /// Get the SEA client.
    ///
    /// Returns a reference to the underlying SEA client for direct API access
    /// when needed.
    pub fn client(&self) -> &Arc<SeaClient> {
        &self.client
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
    use crate::client::SeaClientConfig;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use wiremock::matchers::{method, path, path_regex};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Create a test client pointing to a mock server.
    fn create_test_client(mock_server_uri: &str, warehouse_id: &str) -> Arc<SeaClient> {
        let config = SeaClientConfig::new(mock_server_uri, "test_token", warehouse_id);
        Arc::new(SeaClient::new(config).expect("Failed to create test client"))
    }

    /// Test that session manager starts with no active session.
    #[tokio::test]
    async fn test_session_manager_initial_state() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri(), "test_warehouse");

        let session_manager = SessionManager::new(client, None, None);

        assert!(!session_manager.is_active().await);
        assert!(session_manager.catalog().is_none());
        assert!(session_manager.schema().is_none());
    }

    /// Test that session manager stores catalog and schema.
    #[tokio::test]
    async fn test_session_manager_with_catalog_schema() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri(), "test_warehouse");

        let session_manager = SessionManager::new(
            client,
            Some("main".to_string()),
            Some("default".to_string()),
        );

        assert_eq!(session_manager.catalog(), Some("main"));
        assert_eq!(session_manager.schema(), Some("default"));
    }

    /// Test that get_session_id creates a session and returns the ID.
    #[tokio::test]
    async fn test_session_manager_creates_session() {
        let mock_server = MockServer::start().await;

        // Set up mock for session creation
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "test-session-123"
            })))
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri(), "test_warehouse");
        let session_manager = SessionManager::new(client, None, None);

        // Get session ID should create a session
        let session_id = session_manager.get_session_id().await.unwrap();
        assert_eq!(session_id, "test-session-123");
        assert!(session_manager.is_active().await);
    }

    /// Test that get_session_id caches the session ID.
    #[tokio::test]
    async fn test_session_manager_caches_session_id() {
        let mock_server = MockServer::start().await;

        // Track how many times the API is called
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(move |_: &wiremock::Request| {
                call_count_clone.fetch_add(1, Ordering::SeqCst);
                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "session_id": "cached-session-456"
                }))
            })
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri(), "test_warehouse");
        let session_manager = SessionManager::new(client, None, None);

        // First call creates the session
        let session_id_1 = session_manager.get_session_id().await.unwrap();
        assert_eq!(call_count.load(Ordering::SeqCst), 1);

        // Second call returns cached session ID without API call
        let session_id_2 = session_manager.get_session_id().await.unwrap();
        assert_eq!(call_count.load(Ordering::SeqCst), 1);

        // Both should return the same session ID
        assert_eq!(session_id_1, session_id_2);
        assert_eq!(session_id_1, "cached-session-456");
    }

    /// Test that terminate deletes the session.
    #[tokio::test]
    async fn test_session_manager_terminates_session() {
        let mock_server = MockServer::start().await;

        // Set up mock for session creation
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "terminate-test-session"
            })))
            .mount(&mock_server)
            .await;

        // Set up mock for session deletion
        Mock::given(method("DELETE"))
            .and(path("/api/2.0/sql/sessions/terminate-test-session"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri(), "test_warehouse");
        let session_manager = SessionManager::new(client, None, None);

        // Create a session
        let _ = session_manager.get_session_id().await.unwrap();
        assert!(session_manager.is_active().await);

        // Terminate the session
        session_manager.terminate().await.unwrap();
        assert!(!session_manager.is_active().await);
    }

    /// Test that terminate on inactive session is a no-op.
    #[tokio::test]
    async fn test_session_manager_terminate_no_session() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri(), "test_warehouse");
        let session_manager = SessionManager::new(client, None, None);

        // Terminate when no session exists should succeed
        session_manager.terminate().await.unwrap();
        assert!(!session_manager.is_active().await);
    }

    /// Test that session can be recreated after termination.
    #[tokio::test]
    async fn test_session_manager_recreate_after_terminate() {
        let mock_server = MockServer::start().await;

        let session_count = Arc::new(AtomicUsize::new(0));
        let session_count_clone = session_count.clone();

        // Set up mock for session creation that generates unique IDs
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(move |_: &wiremock::Request| {
                let count = session_count_clone.fetch_add(1, Ordering::SeqCst);
                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "session_id": format!("session-{}", count)
                }))
            })
            .mount(&mock_server)
            .await;

        // Set up mock for session deletion (match session paths using regex)
        Mock::given(method("DELETE"))
            .and(path_regex(r"/api/2\.0/sql/sessions/session-\d+"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri(), "test_warehouse");
        let session_manager = SessionManager::new(client, None, None);

        // Create first session
        let session_id_1 = session_manager.get_session_id().await.unwrap();
        assert_eq!(session_id_1, "session-0");

        // Terminate
        session_manager.terminate().await.unwrap();
        assert!(!session_manager.is_active().await);

        // Create second session
        let session_id_2 = session_manager.get_session_id().await.unwrap();
        assert_eq!(session_id_2, "session-1");

        // Should be a different session
        assert_ne!(session_id_1, session_id_2);
    }

    /// Test that session creation includes catalog and schema.
    #[tokio::test]
    async fn test_session_manager_sends_catalog_schema() {
        let mock_server = MockServer::start().await;

        // Set up mock that responds to session creation requests
        // We verify catalog/schema are set in the SessionManager accessors
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "catalog-schema-session"
            })))
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri(), "test_warehouse");
        let session_manager = SessionManager::new(
            client,
            Some("test_catalog".to_string()),
            Some("test_schema".to_string()),
        );

        // Verify catalog and schema are stored
        assert_eq!(session_manager.catalog(), Some("test_catalog"));
        assert_eq!(session_manager.schema(), Some("test_schema"));

        // Session creation should succeed
        let session_id = session_manager.get_session_id().await.unwrap();
        assert_eq!(session_id, "catalog-schema-session");
    }

    /// Test error handling when session creation fails.
    #[tokio::test]
    async fn test_session_manager_creation_error() {
        let mock_server = MockServer::start().await;

        // Set up mock to return an error
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/sessions"))
            .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
                "error_code": "UNAUTHENTICATED",
                "message": "Invalid token"
            })))
            .mount(&mock_server)
            .await;

        let client = create_test_client(&mock_server.uri(), "test_warehouse");
        let session_manager = SessionManager::new(client, None, None);

        // Session creation should fail
        let result = session_manager.get_session_id().await;
        assert!(result.is_err());

        // Session should not be active after failure
        assert!(!session_manager.is_active().await);
    }

    /// Test that client reference is accessible.
    #[tokio::test]
    async fn test_session_manager_client_access() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri(), "test_warehouse");

        let session_manager = SessionManager::new(client.clone(), None, None);

        // Client should be accessible and be the same reference
        assert_eq!(session_manager.client().warehouse_id(), "test_warehouse");
    }
}
