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

//! Retry logic with exponential backoff for transient errors.
//!
//! This module provides retry functionality for handling transient errors
//! such as rate limiting (429), server errors (500), and service unavailable (503).
//!
//! # Retry Strategy
//!
//! The retry strategy uses exponential backoff with jitter:
//! - Delay = min(base_delay * 2^attempt, max_delay) * (1 + random(0, jitter))
//! - 429 errors respect the `Retry-After` header if present
//! - Network errors (timeout, connection) are retried with backoff
//!
//! # Example
//!
//! ```ignore
//! use adbc_driver_databricks::client::RetryConfig;
//! use std::time::Duration;
//!
//! let config = RetryConfig::default();
//! assert_eq!(config.max_retries, 3);
//! assert_eq!(config.base_delay, Duration::from_secs(1));
//!
//! // Calculate delay for first retry (attempt 0)
//! let delay = config.delay_for_attempt(0);
//! assert!(delay >= Duration::from_secs(1));
//! ```

use rand::Rng;
use std::future::Future;
use std::time::Duration;

use crate::error::{Error, Result};

/// Configuration for retry behavior with exponential backoff.
///
/// This struct controls how transient errors are retried, including
/// the number of retries, backoff timing, and jitter to avoid thundering herd.
#[derive(Clone, Debug)]
pub struct RetryConfig {
    /// Maximum number of retry attempts (0 means no retries, only initial attempt).
    pub max_retries: u32,
    /// Base delay for exponential backoff.
    pub base_delay: Duration,
    /// Maximum delay between retries (caps exponential growth).
    pub max_delay: Duration,
    /// Jitter factor (0.0 - 1.0) to randomize delays.
    /// A value of 0.5 means delays will vary up to 50% above the calculated value.
    pub jitter: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            jitter: 0.5,
        }
    }
}

impl RetryConfig {
    /// Create a new RetryConfig with custom settings.
    pub fn new(max_retries: u32, base_delay: Duration, max_delay: Duration, jitter: f64) -> Self {
        Self {
            max_retries,
            base_delay,
            max_delay,
            jitter: jitter.clamp(0.0, 1.0),
        }
    }

    /// Create a RetryConfig with no retries (fail fast).
    pub fn no_retry() -> Self {
        Self {
            max_retries: 0,
            ..Default::default()
        }
    }

    /// Calculate delay for a given attempt (0-indexed).
    ///
    /// Uses exponential backoff with optional jitter:
    /// - delay = min(base_delay * 2^attempt, max_delay) * (1 + random(0, jitter))
    ///
    /// # Arguments
    ///
    /// * `attempt` - The retry attempt number (0 for first retry)
    ///
    /// # Returns
    ///
    /// The duration to wait before the next retry.
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        self.delay_for_attempt_with_rng(attempt, &mut rand::thread_rng())
    }

    /// Calculate delay with a custom RNG (useful for testing).
    fn delay_for_attempt_with_rng<R: Rng>(&self, attempt: u32, rng: &mut R) -> Duration {
        let base_ms = self.base_delay.as_millis() as f64;

        // Exponential backoff: base_delay * 2^attempt
        let exponential = base_ms * 2_f64.powi(attempt as i32);

        // Cap at max_delay
        let capped = exponential.min(self.max_delay.as_millis() as f64);

        // Add jitter: delay * (1 + random(0, jitter))
        let jitter_factor = if self.jitter > 0.0 {
            1.0 + rng.gen::<f64>() * self.jitter
        } else {
            1.0
        };
        let final_ms = capped * jitter_factor;

        Duration::from_millis(final_ms as u64)
    }

    /// Calculate delay respecting a Retry-After header value.
    ///
    /// If `retry_after` is provided and greater than the calculated delay,
    /// it will be used instead (with jitter applied).
    ///
    /// # Arguments
    ///
    /// * `attempt` - The retry attempt number (0 for first retry)
    /// * `retry_after` - Optional Retry-After value from server response
    ///
    /// # Returns
    ///
    /// The duration to wait, taking Retry-After into account.
    pub fn delay_with_retry_after(&self, attempt: u32, retry_after: Option<Duration>) -> Duration {
        let base_delay = self.delay_for_attempt(attempt);

        match retry_after {
            Some(server_delay) if server_delay > base_delay => {
                // Use server-specified delay, but still add some jitter
                let jitter_factor = if self.jitter > 0.0 {
                    1.0 + rand::thread_rng().gen::<f64>() * (self.jitter / 2.0)
                } else {
                    1.0
                };
                Duration::from_millis((server_delay.as_millis() as f64 * jitter_factor) as u64)
            }
            _ => base_delay,
        }
    }
}

/// Execute an async operation with retry logic.
///
/// This function will retry the operation according to the retry configuration
/// when retryable errors occur (429, 500, 503, network errors).
///
/// # Arguments
///
/// * `config` - Retry configuration controlling backoff behavior
/// * `operation` - A closure that returns a Future producing a Result
///
/// # Returns
///
/// The result of the operation if successful, or the last error if all retries failed.
///
/// # Example
///
/// ```ignore
/// use adbc_driver_databricks::client::{RetryConfig, retry_with_backoff};
///
/// let config = RetryConfig::default();
/// let result = retry_with_backoff(&config, || async {
///     // Your async operation here
///     Ok::<_, Error>(42)
/// }).await;
/// ```
pub async fn retry_with_backoff<F, Fut, T>(config: &RetryConfig, mut operation: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut last_error: Option<Error> = None;

    for attempt in 0..=config.max_retries {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(e) => {
                // Check if this error is retryable and we have retries left
                if !e.is_retryable() || attempt == config.max_retries {
                    return Err(e);
                }

                // Calculate delay
                let delay = config.delay_for_attempt(attempt);
                tokio::time::sleep(delay).await;

                last_error = Some(e);
            }
        }
    }

    // This should be unreachable, but handle it gracefully
    Err(last_error.unwrap_or_else(|| {
        Error::internal("Retry loop completed without result or error")
    }))
}

/// Execute an async operation with retry logic, supporting Retry-After headers.
///
/// Similar to `retry_with_backoff`, but the operation can return an optional
/// `Retry-After` duration hint that will be respected during backoff calculation.
///
/// # Arguments
///
/// * `config` - Retry configuration controlling backoff behavior
/// * `operation` - A closure that returns a Future producing (Result<T>, Option<Duration>)
///
/// # Returns
///
/// The result of the operation if successful, or the last error if all retries failed.
pub async fn retry_with_backoff_and_retry_after<F, Fut, T>(
    config: &RetryConfig,
    mut operation: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = (Result<T>, Option<Duration>)>,
{
    let mut last_error: Option<Error> = None;

    for attempt in 0..=config.max_retries {
        let (result, retry_after) = operation().await;

        match result {
            Ok(value) => return Ok(value),
            Err(e) => {
                // Check if this error is retryable and we have retries left
                if !e.is_retryable() || attempt == config.max_retries {
                    return Err(e);
                }

                // Calculate delay with optional Retry-After
                let delay = config.delay_with_retry_after(attempt, retry_after);
                tokio::time::sleep(delay).await;

                last_error = Some(e);
            }
        }
    }

    // This should be unreachable, but handle it gracefully
    Err(last_error.unwrap_or_else(|| {
        Error::internal("Retry loop completed without result or error")
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.base_delay, Duration::from_secs(1));
        assert_eq!(config.max_delay, Duration::from_secs(30));
        assert!((config.jitter - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_retry_config_new() {
        let config = RetryConfig::new(
            5,
            Duration::from_millis(500),
            Duration::from_secs(60),
            0.3,
        );
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.base_delay, Duration::from_millis(500));
        assert_eq!(config.max_delay, Duration::from_secs(60));
        assert!((config.jitter - 0.3).abs() < f64::EPSILON);
    }

    #[test]
    fn test_retry_config_no_retry() {
        let config = RetryConfig::no_retry();
        assert_eq!(config.max_retries, 0);
    }

    #[test]
    fn test_retry_config_jitter_clamped() {
        let config = RetryConfig::new(3, Duration::from_secs(1), Duration::from_secs(30), 1.5);
        assert!((config.jitter - 1.0).abs() < f64::EPSILON);

        let config = RetryConfig::new(3, Duration::from_secs(1), Duration::from_secs(30), -0.5);
        assert!((config.jitter - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_exponential_backoff_no_jitter() {
        let config = RetryConfig {
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            jitter: 0.0, // Disable jitter for deterministic test
            ..Default::default()
        };

        // Use a seeded RNG for deterministic testing
        let mut rng = rand::thread_rng();

        assert_eq!(
            config.delay_for_attempt_with_rng(0, &mut rng),
            Duration::from_secs(1)
        );
        assert_eq!(
            config.delay_for_attempt_with_rng(1, &mut rng),
            Duration::from_secs(2)
        );
        assert_eq!(
            config.delay_for_attempt_with_rng(2, &mut rng),
            Duration::from_secs(4)
        );
        assert_eq!(
            config.delay_for_attempt_with_rng(3, &mut rng),
            Duration::from_secs(8)
        );
        assert_eq!(
            config.delay_for_attempt_with_rng(4, &mut rng),
            Duration::from_secs(16)
        );
        // Should be capped at max_delay
        assert_eq!(
            config.delay_for_attempt_with_rng(5, &mut rng),
            Duration::from_secs(30)
        );
        assert_eq!(
            config.delay_for_attempt_with_rng(10, &mut rng),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn test_jitter_range() {
        let config = RetryConfig {
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            jitter: 0.5,
            ..Default::default()
        };

        // Test that delays with jitter fall within expected range
        for _ in 0..100 {
            let delay = config.delay_for_attempt(0);
            // With jitter 0.5, delay should be between 1s and 1.5s
            assert!(
                delay >= Duration::from_secs(1),
                "delay {:?} is less than 1s",
                delay
            );
            assert!(
                delay <= Duration::from_millis(1500),
                "delay {:?} is greater than 1.5s",
                delay
            );
        }
    }

    #[test]
    fn test_max_delay_cap() {
        let config = RetryConfig {
            base_delay: Duration::from_secs(10),
            max_delay: Duration::from_secs(30),
            jitter: 0.0,
            ..Default::default()
        };

        // After a few attempts, should be capped at max_delay
        let delay = config.delay_for_attempt(5); // 10 * 2^5 = 320, capped to 30
        assert_eq!(delay, Duration::from_secs(30));
    }

    #[test]
    fn test_delay_with_retry_after_uses_server_value() {
        let config = RetryConfig {
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            jitter: 0.0,
            ..Default::default()
        };

        // Server says retry after 10 seconds
        let delay = config.delay_with_retry_after(0, Some(Duration::from_secs(10)));
        // Should use server value since it's greater than calculated (1s)
        assert!(delay >= Duration::from_secs(10));
    }

    #[test]
    fn test_delay_with_retry_after_ignores_small_server_value() {
        let config = RetryConfig {
            base_delay: Duration::from_secs(10),
            max_delay: Duration::from_secs(30),
            jitter: 0.0,
            ..Default::default()
        };

        // Server says retry after 1 second, but our calculated delay is 10s
        let delay = config.delay_with_retry_after(0, Some(Duration::from_secs(1)));
        // Should use our calculated value since it's greater
        assert_eq!(delay, Duration::from_secs(10));
    }

    #[test]
    fn test_delay_with_retry_after_none() {
        let config = RetryConfig {
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            jitter: 0.0,
            ..Default::default()
        };

        let delay = config.delay_with_retry_after(0, None);
        assert_eq!(delay, Duration::from_secs(1));
    }

    #[tokio::test]
    async fn test_retry_succeeds_immediately() {
        let config = RetryConfig::default();
        let call_count = Arc::new(AtomicU32::new(0));
        let count = call_count.clone();

        let result = retry_with_backoff(&config, || {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok::<_, Error>(42)
            }
        })
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_retry_succeeds_after_retries() {
        let config = RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_millis(10), // Short delay for tests
            max_delay: Duration::from_millis(100),
            jitter: 0.0,
        };
        let call_count = Arc::new(AtomicU32::new(0));
        let count = call_count.clone();

        let result = retry_with_backoff(&config, || {
            let count = count.clone();
            async move {
                let calls = count.fetch_add(1, Ordering::SeqCst);
                if calls < 2 {
                    // Fail first 2 times with retryable error
                    Err(Error::sea_api("TEMPORARILY_UNAVAILABLE", "retry", 503))
                } else {
                    Ok::<_, Error>(42)
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(call_count.load(Ordering::SeqCst), 3); // Initial + 2 retries
    }

    #[tokio::test]
    async fn test_retry_fails_after_max_retries() {
        let config = RetryConfig {
            max_retries: 2,
            base_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(100),
            jitter: 0.0,
        };
        let call_count = Arc::new(AtomicU32::new(0));
        let count = call_count.clone();

        let result = retry_with_backoff(&config, || {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Err::<i32, Error>(Error::sea_api("INTERNAL_ERROR", "always fails", 500))
            }
        })
        .await;

        assert!(result.is_err());
        // Initial attempt + max_retries
        assert_eq!(call_count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_retry_non_retryable_fails_immediately() {
        let config = RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(100),
            jitter: 0.0,
        };
        let call_count = Arc::new(AtomicU32::new(0));
        let count = call_count.clone();

        let result = retry_with_backoff(&config, || {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                // 400 is not retryable
                Err::<i32, Error>(Error::sea_api("BAD_REQUEST", "invalid", 400))
            }
        })
        .await;

        assert!(result.is_err());
        // Should fail immediately without retries
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_retry_no_retry_config() {
        let config = RetryConfig::no_retry();
        let call_count = Arc::new(AtomicU32::new(0));
        let count = call_count.clone();

        let result = retry_with_backoff(&config, || {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Err::<i32, Error>(Error::sea_api("TEMPORARILY_UNAVAILABLE", "retry", 503))
            }
        })
        .await;

        assert!(result.is_err());
        // Should only try once with no_retry
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_retry_with_backoff_and_retry_after() {
        let config = RetryConfig {
            max_retries: 2,
            base_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(100),
            jitter: 0.0,
        };
        let call_count = Arc::new(AtomicU32::new(0));
        let count = call_count.clone();

        let result = retry_with_backoff_and_retry_after(&config, || {
            let count = count.clone();
            async move {
                let calls = count.fetch_add(1, Ordering::SeqCst);
                if calls < 1 {
                    // Return error with retry-after hint
                    (
                        Err(Error::sea_api("REQUEST_LIMIT_EXCEEDED", "rate limited", 429)),
                        Some(Duration::from_millis(5)),
                    )
                } else {
                    (Ok::<_, Error>(42), None)
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_retry_different_retryable_errors() {
        let config = RetryConfig {
            max_retries: 5,
            base_delay: Duration::from_millis(5),
            max_delay: Duration::from_millis(50),
            jitter: 0.0,
        };
        let call_count = Arc::new(AtomicU32::new(0));
        let count = call_count.clone();

        let result = retry_with_backoff(&config, || {
            let count = count.clone();
            async move {
                let calls = count.fetch_add(1, Ordering::SeqCst);
                match calls {
                    0 => Err(Error::sea_api("REQUEST_LIMIT_EXCEEDED", "429", 429)),
                    1 => Err(Error::sea_api("INTERNAL_ERROR", "500", 500)),
                    2 => Err(Error::sea_api("TEMPORARILY_UNAVAILABLE", "503", 503)),
                    _ => Ok::<_, Error>(42),
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(call_count.load(Ordering::SeqCst), 4);
    }
}
