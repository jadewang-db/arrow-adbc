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

//! Unit tests for retry logic with exponential backoff.
//!
//! Tests cover:
//! - RetryConfig default values and builder pattern
//! - Exponential backoff delay calculation
//! - Jitter application and bounds
//! - Max delay capping
//! - Retry-After header handling
//! - retry_with_backoff behavior for various scenarios

use adbc_driver_databricks::client::{retry_with_backoff, RetryConfig};
use adbc_driver_databricks::Error;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

// =============================================================================
// RetryConfig Tests
// =============================================================================

mod retry_config_tests {
    use super::*;

    #[test]
    fn test_default_values() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.base_delay, Duration::from_secs(1));
        assert_eq!(config.max_delay, Duration::from_secs(30));
        assert!((config.jitter - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_new_with_custom_values() {
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
    fn test_no_retry_config() {
        let config = RetryConfig::no_retry();
        assert_eq!(config.max_retries, 0);
        // Other values should be defaults
        assert_eq!(config.base_delay, Duration::from_secs(1));
        assert_eq!(config.max_delay, Duration::from_secs(30));
    }

    #[test]
    fn test_jitter_clamped_to_max() {
        let config = RetryConfig::new(3, Duration::from_secs(1), Duration::from_secs(30), 1.5);
        assert!((config.jitter - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_jitter_clamped_to_min() {
        let config = RetryConfig::new(3, Duration::from_secs(1), Duration::from_secs(30), -0.5);
        assert!((config.jitter - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_jitter_at_boundaries() {
        let config_zero = RetryConfig::new(3, Duration::from_secs(1), Duration::from_secs(30), 0.0);
        assert!((config_zero.jitter - 0.0).abs() < f64::EPSILON);

        let config_one = RetryConfig::new(3, Duration::from_secs(1), Duration::from_secs(30), 1.0);
        assert!((config_one.jitter - 1.0).abs() < f64::EPSILON);
    }
}

// =============================================================================
// Exponential Backoff Tests
// =============================================================================

mod exponential_backoff_tests {
    use super::*;

    #[test]
    fn test_exponential_growth_no_jitter() {
        let config = RetryConfig {
            max_retries: 10,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(1000), // High cap to test exponential growth
            jitter: 0.0,
        };

        // Test exponential growth: 1s, 2s, 4s, 8s, 16s, ...
        assert_eq!(config.delay_for_attempt(0), Duration::from_secs(1));
        assert_eq!(config.delay_for_attempt(1), Duration::from_secs(2));
        assert_eq!(config.delay_for_attempt(2), Duration::from_secs(4));
        assert_eq!(config.delay_for_attempt(3), Duration::from_secs(8));
        assert_eq!(config.delay_for_attempt(4), Duration::from_secs(16));
    }

    #[test]
    fn test_delay_capped_at_max() {
        let config = RetryConfig {
            max_retries: 10,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            jitter: 0.0,
        };

        // After a few attempts, should hit max_delay
        assert_eq!(config.delay_for_attempt(5), Duration::from_secs(30)); // 2^5 = 32 > 30
        assert_eq!(config.delay_for_attempt(10), Duration::from_secs(30));
        assert_eq!(config.delay_for_attempt(100), Duration::from_secs(30));
    }

    #[test]
    fn test_delay_with_different_base() {
        let config = RetryConfig {
            max_retries: 5,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            jitter: 0.0,
        };

        assert_eq!(config.delay_for_attempt(0), Duration::from_millis(100));
        assert_eq!(config.delay_for_attempt(1), Duration::from_millis(200));
        assert_eq!(config.delay_for_attempt(2), Duration::from_millis(400));
    }

    #[test]
    fn test_jitter_bounds() {
        let config = RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            jitter: 0.5,
        };

        // Run multiple times to test jitter range
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
    fn test_jitter_max_bound() {
        let config = RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            jitter: 1.0, // Maximum jitter
        };

        // With jitter 1.0, delay should be between 1s and 2s
        for _ in 0..100 {
            let delay = config.delay_for_attempt(0);
            assert!(delay >= Duration::from_secs(1));
            assert!(delay <= Duration::from_secs(2));
        }
    }

    #[test]
    fn test_zero_jitter_is_deterministic() {
        let config = RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            jitter: 0.0,
        };

        // Without jitter, delays should be deterministic
        let delay1 = config.delay_for_attempt(2);
        let delay2 = config.delay_for_attempt(2);
        assert_eq!(delay1, delay2);
    }
}

// =============================================================================
// Retry-After Header Tests
// =============================================================================

mod retry_after_tests {
    use super::*;

    #[test]
    fn test_uses_server_value_when_larger() {
        let config = RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            jitter: 0.0,
        };

        // Server says retry after 10 seconds, our calculation is 1s
        let delay = config.delay_with_retry_after(0, Some(Duration::from_secs(10)));
        // Should use server value
        assert!(delay >= Duration::from_secs(10));
    }

    #[test]
    fn test_ignores_server_value_when_smaller() {
        let config = RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_secs(10),
            max_delay: Duration::from_secs(30),
            jitter: 0.0,
        };

        // Server says retry after 1 second, our calculation is 10s
        let delay = config.delay_with_retry_after(0, Some(Duration::from_secs(1)));
        // Should use our calculated value
        assert_eq!(delay, Duration::from_secs(10));
    }

    #[test]
    fn test_handles_none_retry_after() {
        let config = RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            jitter: 0.0,
        };

        let delay = config.delay_with_retry_after(0, None);
        assert_eq!(delay, Duration::from_secs(1));
    }

    #[test]
    fn test_retry_after_with_jitter() {
        let config = RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            jitter: 0.5,
        };

        // With server value and jitter, delay should be slightly above server value
        for _ in 0..100 {
            let delay = config.delay_with_retry_after(0, Some(Duration::from_secs(10)));
            // Server value with up to half jitter applied (jitter/2 = 0.25)
            assert!(delay >= Duration::from_secs(10));
            assert!(delay <= Duration::from_millis(12500)); // 10s * 1.25
        }
    }

    #[test]
    fn test_retry_after_zero_duration() {
        let config = RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            jitter: 0.0,
        };

        // Zero duration from server should use calculated delay
        let delay = config.delay_with_retry_after(0, Some(Duration::from_secs(0)));
        assert_eq!(delay, Duration::from_secs(1));
    }
}

// =============================================================================
// retry_with_backoff Function Tests
// =============================================================================

mod retry_with_backoff_tests {
    use super::*;

    #[tokio::test]
    async fn test_succeeds_immediately() {
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
    async fn test_succeeds_after_retries() {
        let config = RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_millis(1), // Very short for tests
            max_delay: Duration::from_millis(10),
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
    async fn test_fails_after_max_retries() {
        let config = RetryConfig {
            max_retries: 2,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(10),
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
        // Initial attempt + max_retries = 3
        assert_eq!(call_count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_non_retryable_fails_immediately() {
        let config = RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(10),
            jitter: 0.0,
        };
        let call_count = Arc::new(AtomicU32::new(0));
        let count = call_count.clone();

        let result = retry_with_backoff(&config, || {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                // 400 Bad Request is not retryable
                Err::<i32, Error>(Error::sea_api("BAD_REQUEST", "invalid", 400))
            }
        })
        .await;

        assert!(result.is_err());
        // Should fail immediately without retries
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_no_retry_config_fails_immediately() {
        let config = RetryConfig::no_retry();
        let call_count = Arc::new(AtomicU32::new(0));
        let count = call_count.clone();

        let result: Result<i32, Error> = retry_with_backoff(&config, || {
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
    async fn test_retries_different_retryable_errors() {
        let config = RetryConfig {
            max_retries: 5,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(10),
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

    #[tokio::test]
    async fn test_stops_retrying_on_non_retryable_after_retryable() {
        let config = RetryConfig {
            max_retries: 5,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(10),
            jitter: 0.0,
        };
        let call_count = Arc::new(AtomicU32::new(0));
        let count = call_count.clone();

        let result = retry_with_backoff(&config, || {
            let count = count.clone();
            async move {
                let calls = count.fetch_add(1, Ordering::SeqCst);
                if calls == 0 {
                    // First call: retryable error
                    Err(Error::sea_api("TEMPORARILY_UNAVAILABLE", "503", 503))
                } else {
                    // Second call: non-retryable error
                    Err::<i32, Error>(Error::sea_api("BAD_REQUEST", "400", 400))
                }
            }
        })
        .await;

        assert!(result.is_err());
        // First attempt + one retry, then stop on non-retryable
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_preserves_error_type() {
        let config = RetryConfig::no_retry();

        let result = retry_with_backoff(&config, || async {
            Err::<i32, Error>(Error::not_implemented("test feature"))
        })
        .await;

        match result.unwrap_err() {
            Error::NotImplemented(msg) => assert_eq!(msg, "test feature"),
            other => panic!("Unexpected error type: {:?}", other),
        }
    }
}

// =============================================================================
// Integration-Style Tests
// =============================================================================

mod integration_tests {
    use super::*;

    #[tokio::test]
    async fn test_realistic_retry_scenario() {
        // Simulate a realistic scenario where server is temporarily overloaded
        let config = RetryConfig {
            max_retries: 5,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(20),
            jitter: 0.1,
        };

        let call_count = Arc::new(AtomicU32::new(0));
        let count = call_count.clone();

        let result = retry_with_backoff(&config, || {
            let count = count.clone();
            async move {
                let calls = count.fetch_add(1, Ordering::SeqCst);
                // Simulate server recovering after 3 attempts
                if calls < 3 {
                    Err(Error::sea_api(
                        "REQUEST_LIMIT_EXCEEDED",
                        "Too many requests",
                        429,
                    ))
                } else {
                    Ok::<_, Error>("Success!")
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), "Success!");
        assert_eq!(call_count.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn test_authentication_error_not_retried() {
        let config = RetryConfig::default();
        let call_count = Arc::new(AtomicU32::new(0));
        let count = call_count.clone();

        let result = retry_with_backoff(&config, || {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Err::<i32, Error>(Error::sea_api("UNAUTHENTICATED", "Invalid token", 401))
            }
        })
        .await;

        assert!(result.is_err());
        // Auth errors should not be retried
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_permission_error_not_retried() {
        let config = RetryConfig::default();
        let call_count = Arc::new(AtomicU32::new(0));
        let count = call_count.clone();

        let result = retry_with_backoff(&config, || {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Err::<i32, Error>(Error::sea_api("PERMISSION_DENIED", "Access denied", 403))
            }
        })
        .await;

        assert!(result.is_err());
        // Permission errors should not be retried
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_not_found_error_not_retried() {
        let config = RetryConfig::default();
        let call_count = Arc::new(AtomicU32::new(0));
        let count = call_count.clone();

        let result = retry_with_backoff(&config, || {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Err::<i32, Error>(Error::sea_api("NOT_FOUND", "Resource not found", 404))
            }
        })
        .await;

        assert!(result.is_err());
        // Not found errors should not be retried
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }
}

// =============================================================================
// Edge Case Tests
// =============================================================================

mod edge_case_tests {
    use super::*;

    #[test]
    fn test_very_high_attempt_number() {
        let config = RetryConfig {
            max_retries: 100,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            jitter: 0.0,
        };

        // Even with very high attempt number, should be capped at max_delay
        let delay = config.delay_for_attempt(1000);
        assert_eq!(delay, Duration::from_secs(30));
    }

    #[test]
    fn test_zero_base_delay() {
        let config = RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_secs(0),
            max_delay: Duration::from_secs(30),
            jitter: 0.0,
        };

        // Zero base delay should result in zero delay
        assert_eq!(config.delay_for_attempt(0), Duration::from_secs(0));
        assert_eq!(config.delay_for_attempt(5), Duration::from_secs(0));
    }

    #[test]
    fn test_base_delay_equals_max_delay() {
        let config = RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_secs(10),
            max_delay: Duration::from_secs(10),
            jitter: 0.0,
        };

        // All delays should be the same
        assert_eq!(config.delay_for_attempt(0), Duration::from_secs(10));
        assert_eq!(config.delay_for_attempt(5), Duration::from_secs(10));
    }

    #[test]
    fn test_very_small_jitter() {
        let config = RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            jitter: 0.001, // Very small jitter
        };

        for _ in 0..100 {
            let delay = config.delay_for_attempt(0);
            // Should be very close to 1s with tiny variation
            assert!(delay >= Duration::from_secs(1));
            assert!(delay <= Duration::from_millis(1001));
        }
    }

    #[tokio::test]
    async fn test_zero_max_retries() {
        let config = RetryConfig {
            max_retries: 0,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(10),
            jitter: 0.0,
        };
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
        // Only initial attempt, no retries
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }
}
