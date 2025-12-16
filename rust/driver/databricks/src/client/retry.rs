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

//! Retry logic with exponential backoff and jitter for transient errors.
//!
//! This module provides retry configuration and utilities for handling
//! transient errors in the Databricks ADBC driver.
//!
//! # Retry Strategy
//!
//! The retry strategy uses exponential backoff with jitter:
//! - Base delay doubles with each attempt (1s, 2s, 4s, 8s, ...)
//! - Maximum delay caps the exponential growth (default: 30s)
//! - Jitter adds randomness to prevent thundering herd (default: 50%)
//!
//! # Retryable Errors
//!
//! The following errors are considered retryable:
//! - HTTP 429 (Rate Limited) - with Retry-After header support
//! - HTTP 500 (Internal Error)
//! - HTTP 503 (Temporarily Unavailable)
//! - Network timeout or connection errors
//!
//! # Example
//!
//! ```ignore
//! use adbc_databricks::client::retry::RetryConfig;
//! use std::time::Duration;
//!
//! let config = RetryConfig::default();
//! assert_eq!(config.max_retries, 3);
//! assert_eq!(config.base_delay, Duration::from_secs(1));
//!
//! // Calculate delay for first retry attempt
//! let delay = config.delay_for_attempt(0);
//! assert!(delay >= Duration::from_secs(1));
//! assert!(delay <= Duration::from_millis(1500)); // With 50% jitter
//! ```

use std::time::Duration;

use rand::Rng;

/// Configuration for retry behavior with exponential backoff.
///
/// This struct configures how the client handles transient errors
/// by specifying retry limits and backoff timing.
#[derive(Clone, Debug)]
pub struct RetryConfig {
    /// Maximum number of retry attempts (default: 3).
    ///
    /// A value of 0 means no retries (fail immediately).
    /// A value of 3 means up to 4 total attempts (1 initial + 3 retries).
    pub max_retries: u32,

    /// Base delay for exponential backoff (default: 1 second).
    ///
    /// The actual delay for attempt N is: base_delay * 2^N * (1 + jitter_factor)
    pub base_delay: Duration,

    /// Maximum delay between retries (default: 30 seconds).
    ///
    /// The exponential backoff is capped at this value to prevent
    /// excessively long waits.
    pub max_delay: Duration,

    /// Jitter factor (0.0 - 1.0, default: 0.5).
    ///
    /// Adds randomness to prevent thundering herd problems.
    /// A jitter of 0.5 means delays can be up to 50% longer than
    /// the calculated exponential delay.
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
    /// Create a new retry configuration with custom values.
    ///
    /// # Arguments
    ///
    /// * `max_retries` - Maximum number of retry attempts
    /// * `base_delay` - Base delay for exponential backoff
    /// * `max_delay` - Maximum delay cap
    /// * `jitter` - Jitter factor (0.0 - 1.0)
    pub fn new(max_retries: u32, base_delay: Duration, max_delay: Duration, jitter: f64) -> Self {
        Self {
            max_retries,
            base_delay,
            max_delay,
            jitter: jitter.clamp(0.0, 1.0),
        }
    }

    /// Create a configuration with no retries (fail immediately).
    pub fn no_retry() -> Self {
        Self {
            max_retries: 0,
            ..Default::default()
        }
    }

    /// Create a configuration with custom max retries.
    pub fn with_max_retries(max_retries: u32) -> Self {
        Self {
            max_retries,
            ..Default::default()
        }
    }

    /// Calculate delay for a given attempt (0-indexed).
    ///
    /// The delay is calculated as:
    /// 1. Exponential base: `base_delay * 2^attempt`
    /// 2. Capped at `max_delay`
    /// 3. Multiplied by `(1 + random(0, jitter))`
    ///
    /// # Arguments
    ///
    /// * `attempt` - The attempt number (0-indexed, 0 = first retry)
    ///
    /// # Returns
    ///
    /// The duration to wait before the next retry attempt.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let config = RetryConfig {
    ///     base_delay: Duration::from_secs(1),
    ///     max_delay: Duration::from_secs(30),
    ///     jitter: 0.0, // No jitter for predictable values
    ///     ..Default::default()
    /// };
    ///
    /// assert_eq!(config.delay_for_attempt(0), Duration::from_secs(1));  // 1 * 2^0 = 1
    /// assert_eq!(config.delay_for_attempt(1), Duration::from_secs(2));  // 1 * 2^1 = 2
    /// assert_eq!(config.delay_for_attempt(2), Duration::from_secs(4));  // 1 * 2^2 = 4
    /// assert_eq!(config.delay_for_attempt(3), Duration::from_secs(8));  // 1 * 2^3 = 8
    /// assert_eq!(config.delay_for_attempt(4), Duration::from_secs(16)); // 1 * 2^4 = 16
    /// assert_eq!(config.delay_for_attempt(5), Duration::from_secs(30)); // Capped at max_delay
    /// ```
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let base_ms = self.base_delay.as_millis() as f64;
        let exponential = base_ms * 2_f64.powi(attempt as i32);
        let capped = exponential.min(self.max_delay.as_millis() as f64);

        // Add jitter: delay * (1 + random(0, jitter))
        let jitter_factor = if self.jitter > 0.0 {
            let mut rng = rand::thread_rng();
            1.0 + rng.gen::<f64>() * self.jitter
        } else {
            1.0
        };

        let final_ms = capped * jitter_factor;
        Duration::from_millis(final_ms as u64)
    }

    /// Calculate delay for a given attempt with a specific Retry-After value.
    ///
    /// When the server provides a Retry-After header (e.g., for 429 responses),
    /// this method uses the server-specified delay instead of the calculated
    /// exponential backoff, but still applies jitter.
    ///
    /// # Arguments
    ///
    /// * `retry_after` - The server-specified retry delay
    ///
    /// # Returns
    ///
    /// The duration to wait, with jitter applied.
    pub fn delay_with_retry_after(&self, retry_after: Duration) -> Duration {
        // Apply jitter to the server-specified delay
        let jitter_factor = if self.jitter > 0.0 {
            let mut rng = rand::thread_rng();
            1.0 + rng.gen::<f64>() * self.jitter
        } else {
            1.0
        };

        let base_ms = retry_after.as_millis() as f64;
        let final_ms = base_ms * jitter_factor;

        // Cap at max_delay even for server-specified delays
        let capped_ms = final_ms.min(self.max_delay.as_millis() as f64);
        Duration::from_millis(capped_ms as u64)
    }
}

/// Parse the Retry-After header value.
///
/// The Retry-After header can be either:
/// - A number of seconds: "120"
/// - An HTTP-date: "Wed, 21 Oct 2015 07:28:00 GMT"
///
/// This function currently only supports the seconds format.
///
/// # Arguments
///
/// * `value` - The Retry-After header value
///
/// # Returns
///
/// The parsed duration, or None if parsing fails.
pub fn parse_retry_after(value: &str) -> Option<Duration> {
    // Try parsing as seconds
    value
        .trim()
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
}

#[cfg(test)]
mod tests {
    use super::*;

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
            0.25,
        );
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.base_delay, Duration::from_millis(500));
        assert_eq!(config.max_delay, Duration::from_secs(60));
        assert!((config.jitter - 0.25).abs() < f64::EPSILON);
    }

    #[test]
    fn test_retry_config_jitter_clamped() {
        // Jitter should be clamped to [0.0, 1.0]
        let config = RetryConfig::new(3, Duration::from_secs(1), Duration::from_secs(30), 2.0);
        assert!((config.jitter - 1.0).abs() < f64::EPSILON);

        let config = RetryConfig::new(3, Duration::from_secs(1), Duration::from_secs(30), -0.5);
        assert!(config.jitter.abs() < f64::EPSILON);
    }

    #[test]
    fn test_no_retry() {
        let config = RetryConfig::no_retry();
        assert_eq!(config.max_retries, 0);
    }

    #[test]
    fn test_with_max_retries() {
        let config = RetryConfig::with_max_retries(10);
        assert_eq!(config.max_retries, 10);
        // Other values should be defaults
        assert_eq!(config.base_delay, Duration::from_secs(1));
    }

    #[test]
    fn test_backoff_exponential_no_jitter() {
        let config = RetryConfig {
            max_retries: 5,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            jitter: 0.0, // No jitter for predictable test
        };

        // Verify delays: 1s, 2s, 4s, 8s, 16s
        assert_eq!(config.delay_for_attempt(0), Duration::from_secs(1));
        assert_eq!(config.delay_for_attempt(1), Duration::from_secs(2));
        assert_eq!(config.delay_for_attempt(2), Duration::from_secs(4));
        assert_eq!(config.delay_for_attempt(3), Duration::from_secs(8));
        assert_eq!(config.delay_for_attempt(4), Duration::from_secs(16));
    }

    #[test]
    fn test_backoff_max_cap() {
        let config = RetryConfig {
            max_retries: 5,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(10),
            jitter: 0.0,
        };

        // After attempt 3 (8s), it should cap at 10s
        assert_eq!(config.delay_for_attempt(3), Duration::from_secs(8));
        assert_eq!(config.delay_for_attempt(4), Duration::from_secs(10));
        assert_eq!(config.delay_for_attempt(5), Duration::from_secs(10));
        assert_eq!(config.delay_for_attempt(10), Duration::from_secs(10));
    }

    #[test]
    fn test_backoff_jitter_within_range() {
        let config = RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            jitter: 0.5,
        };

        // With 50% jitter, delay should be between 1.0s and 1.5s for attempt 0
        for _ in 0..100 {
            let delay = config.delay_for_attempt(0);
            assert!(
                delay >= Duration::from_millis(1000),
                "Delay {:?} should be >= 1000ms",
                delay
            );
            assert!(
                delay <= Duration::from_millis(1500),
                "Delay {:?} should be <= 1500ms",
                delay
            );
        }
    }

    #[test]
    fn test_backoff_jitter_for_later_attempts() {
        let config = RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            jitter: 0.5,
        };

        // For attempt 2, base is 4s, with 50% jitter should be 4.0s - 6.0s
        for _ in 0..100 {
            let delay = config.delay_for_attempt(2);
            assert!(
                delay >= Duration::from_millis(4000),
                "Delay {:?} should be >= 4000ms",
                delay
            );
            assert!(
                delay <= Duration::from_millis(6000),
                "Delay {:?} should be <= 6000ms",
                delay
            );
        }
    }

    #[test]
    fn test_delay_with_retry_after() {
        let config = RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            jitter: 0.0, // No jitter for predictable test
        };

        let retry_after = Duration::from_secs(5);
        let delay = config.delay_with_retry_after(retry_after);
        assert_eq!(delay, Duration::from_secs(5));
    }

    #[test]
    fn test_delay_with_retry_after_capped() {
        let config = RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            jitter: 0.0,
        };

        // Server says wait 60s, but we cap at 30s
        let retry_after = Duration::from_secs(60);
        let delay = config.delay_with_retry_after(retry_after);
        assert_eq!(delay, Duration::from_secs(30));
    }

    #[test]
    fn test_delay_with_retry_after_jitter() {
        let config = RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            jitter: 0.5,
        };

        let retry_after = Duration::from_secs(10);
        for _ in 0..100 {
            let delay = config.delay_with_retry_after(retry_after);
            assert!(
                delay >= Duration::from_millis(10000),
                "Delay {:?} should be >= 10000ms",
                delay
            );
            assert!(
                delay <= Duration::from_millis(15000),
                "Delay {:?} should be <= 15000ms",
                delay
            );
        }
    }

    #[test]
    fn test_parse_retry_after_seconds() {
        assert_eq!(
            parse_retry_after("120"),
            Some(Duration::from_secs(120))
        );
        assert_eq!(parse_retry_after("0"), Some(Duration::from_secs(0)));
        assert_eq!(parse_retry_after("1"), Some(Duration::from_secs(1)));
        assert_eq!(
            parse_retry_after("  60  "),
            Some(Duration::from_secs(60))
        );
    }

    #[test]
    fn test_parse_retry_after_invalid() {
        assert_eq!(parse_retry_after("not-a-number"), None);
        assert_eq!(parse_retry_after(""), None);
        assert_eq!(parse_retry_after("-1"), None);
        // HTTP-date format not supported yet
        assert_eq!(
            parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT"),
            None
        );
    }

    #[test]
    fn test_backoff_millisecond_base_delay() {
        // Test with millisecond base delay
        let config = RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(1),
            jitter: 0.0,
        };

        assert_eq!(config.delay_for_attempt(0), Duration::from_millis(100));
        assert_eq!(config.delay_for_attempt(1), Duration::from_millis(200));
        assert_eq!(config.delay_for_attempt(2), Duration::from_millis(400));
        assert_eq!(config.delay_for_attempt(3), Duration::from_millis(800));
        // Capped at 1000ms
        assert_eq!(config.delay_for_attempt(4), Duration::from_millis(1000));
    }
}
