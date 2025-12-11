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

//! Statement polling functionality with exponential backoff.
//!
//! This module provides utilities for polling statement execution status
//! until completion.

use std::time::Duration;

use crate::client::{SeaClient, StatementResponse};
use crate::error::{Error, Result};

/// Configuration for polling behavior.
#[derive(Debug, Clone)]
pub struct PollingConfig {
    /// Initial delay between poll attempts.
    pub initial_delay: Duration,
    /// Maximum delay between poll attempts.
    pub max_delay: Duration,
    /// Backoff multiplier for exponential backoff.
    pub backoff_multiplier: f64,
    /// Maximum total time to wait for statement completion.
    /// If None, polling will continue indefinitely.
    pub max_wait_time: Option<Duration>,
}

impl Default for PollingConfig {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(10),
            backoff_multiplier: 2.0,
            max_wait_time: None,
        }
    }
}

impl PollingConfig {
    /// Create a new polling configuration with specified initial delay.
    pub fn new(initial_delay: Duration) -> Self {
        Self {
            initial_delay,
            ..Default::default()
        }
    }

    /// Set the maximum delay between polls.
    pub fn with_max_delay(mut self, max_delay: Duration) -> Self {
        self.max_delay = max_delay;
        self
    }

    /// Set the maximum total wait time.
    pub fn with_max_wait_time(mut self, max_wait_time: Duration) -> Self {
        self.max_wait_time = Some(max_wait_time);
        self
    }

    /// Set the backoff multiplier.
    pub fn with_backoff_multiplier(mut self, multiplier: f64) -> Self {
        self.backoff_multiplier = multiplier;
        self
    }
}

/// Poll a statement until it completes (succeeds, fails, or is cancelled).
///
/// Uses exponential backoff with the following sequence:
/// 1s, 2s, 4s, 8s, 10s (capped at max_delay), ...
///
/// # Arguments
///
/// * `client` - The SEA client to use for polling
/// * `statement_id` - The statement ID to poll
/// * `config` - Polling configuration
///
/// # Returns
///
/// The final statement response when the statement completes.
///
/// # Errors
///
/// Returns an error if:
/// - The statement fails with an error
/// - The statement is cancelled
/// - The maximum wait time is exceeded
/// - A network error occurs
pub async fn poll_until_complete(
    client: &SeaClient,
    statement_id: &str,
    config: &PollingConfig,
) -> Result<StatementResponse> {
    let start_time = std::time::Instant::now();
    let mut current_delay = config.initial_delay;

    loop {
        // Check if we've exceeded the maximum wait time
        if let Some(max_wait) = config.max_wait_time {
            if start_time.elapsed() > max_wait {
                return Err(Error::Timeout);
            }
        }

        // Fetch current status
        let response = client.get_statement_with_retry(statement_id).await?;

        // Check the status using helper methods
        if response.status.is_succeeded() {
            // Statement completed successfully
            return Ok(response);
        }

        if response.status.is_failed() {
            // Statement failed - extract error message
            let error_msg = response
                .status
                .error_message()
                .unwrap_or_else(|| "Statement execution failed".to_string());
            return Err(Error::statement_failed(error_msg));
        }

        if response.status.is_cancelled() {
            return Err(Error::cancelled("Statement was cancelled"));
        }

        if response.status.is_running() {
            // Still running, wait and retry
            tokio::time::sleep(current_delay).await;

            // Calculate next delay with exponential backoff
            let next_delay_secs = current_delay.as_secs_f64() * config.backoff_multiplier;
            let next_delay = Duration::from_secs_f64(next_delay_secs);
            current_delay = next_delay.min(config.max_delay);
        } else {
            // Unknown state - treat as success
            return Ok(response);
        }
    }
}

/// Check if a statement response indicates a terminal state.
pub fn is_terminal_state(response: &StatementResponse) -> bool {
    response.status.is_terminal()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{StatementState, StatementStatus};

    #[test]
    fn test_polling_config_default() {
        let config = PollingConfig::default();
        assert_eq!(config.initial_delay, Duration::from_secs(1));
        assert_eq!(config.max_delay, Duration::from_secs(10));
        assert_eq!(config.backoff_multiplier, 2.0);
        assert!(config.max_wait_time.is_none());
    }

    #[test]
    fn test_polling_config_builder() {
        let config = PollingConfig::new(Duration::from_millis(500))
            .with_max_delay(Duration::from_secs(5))
            .with_max_wait_time(Duration::from_secs(60))
            .with_backoff_multiplier(1.5);

        assert_eq!(config.initial_delay, Duration::from_millis(500));
        assert_eq!(config.max_delay, Duration::from_secs(5));
        assert_eq!(config.max_wait_time, Some(Duration::from_secs(60)));
        assert_eq!(config.backoff_multiplier, 1.5);
    }

    #[test]
    fn test_is_terminal_state_succeeded() {
        let response = StatementResponse {
            statement_id: "test".to_string(),
            status: StatementStatus {
                state: StatementState::Succeeded,
                error: None,
            },
            manifest: None,
            result: None,
        };
        assert!(is_terminal_state(&response));
    }

    #[test]
    fn test_is_terminal_state_failed() {
        let response = StatementResponse {
            statement_id: "test".to_string(),
            status: StatementStatus {
                state: StatementState::Failed,
                error: None,
            },
            manifest: None,
            result: None,
        };
        assert!(is_terminal_state(&response));
    }

    #[test]
    fn test_is_terminal_state_cancelled() {
        let response = StatementResponse {
            statement_id: "test".to_string(),
            status: StatementStatus {
                state: StatementState::Cancelled,
                error: None,
            },
            manifest: None,
            result: None,
        };
        assert!(is_terminal_state(&response));
    }

    #[test]
    fn test_is_terminal_state_pending() {
        let response = StatementResponse {
            statement_id: "test".to_string(),
            status: StatementStatus {
                state: StatementState::Pending,
                error: None,
            },
            manifest: None,
            result: None,
        };
        assert!(!is_terminal_state(&response));
    }

    #[test]
    fn test_is_terminal_state_running() {
        let response = StatementResponse {
            statement_id: "test".to_string(),
            status: StatementStatus {
                state: StatementState::Running,
                error: None,
            },
            manifest: None,
            result: None,
        };
        assert!(!is_terminal_state(&response));
    }
}
