//! Authentication rate limiting with exponential backoff and lockout.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::error::SchemaError;

/// Rate limiter for authentication attempts.
///
/// Tracks failed login attempts per username and enforces exponential
/// backoff followed by account lockout after too many failures.
pub struct AuthRateLimiter {
    attempts: Mutex<HashMap<String, FailedAttempts>>,
    config: RateLimitConfig,
}

/// Tracks failed authentication attempts for a single user.
pub struct FailedAttempts {
    /// Number of consecutive failed attempts.
    pub count: u32,
    /// When the first failure in this window occurred.
    pub first_failure: Instant,
    /// When the most recent failure occurred.
    pub last_failure: Instant,
}

/// Configuration for authentication rate limiting.
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Maximum failed attempts before lockout.
    pub max_attempts: u32,
    /// Base backoff duration (doubled each attempt).
    pub base_backoff: Duration,
    /// Maximum backoff duration cap.
    pub max_backoff: Duration,
    /// How long an account stays locked after exceeding max_attempts.
    pub lockout_duration: Duration,
    /// Sliding window for counting failures.
    pub window: Duration,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
            lockout_duration: Duration::from_secs(900), // 15 min
            window: Duration::from_secs(900),
        }
    }
}

impl AuthRateLimiter {
    /// Create a new rate limiter with the given configuration.
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            attempts: Mutex::new(HashMap::new()),
            config,
        }
    }

    /// Compute the backoff duration for the nth failure.
    /// Formula: `min(base_backoff * 2^(n-1), max_backoff)`
    pub fn compute_backoff(config: &RateLimitConfig, failure_count: u32) -> Duration {
        let shift = failure_count.saturating_sub(1);
        let multiplier = if shift >= 32 {
            u32::MAX
        } else {
            1u32.wrapping_shl(shift)
        };
        let backoff = config.base_backoff.saturating_mul(multiplier);
        if backoff > config.max_backoff {
            config.max_backoff
        } else {
            backoff
        }
    }

    /// Check if a username is rate-limited. Returns `AuthenticationThrottled` if blocked.
    pub fn check_rate_limit(&self, username: &str) -> crate::Result<()> {
        let attempts = self.attempts.lock().expect("rate limiter lock poisoned");
        let entry = match attempts.get(username) {
            Some(e) => e,
            None => return Ok(()),
        };

        let now = Instant::now();

        // If the window has expired, the entry is stale — allow access
        if now.duration_since(entry.first_failure) > self.config.window {
            return Ok(());
        }

        // If the user has exceeded max_attempts, check if lockout has expired
        if entry.count >= self.config.max_attempts {
            let since_last = now.duration_since(entry.last_failure);
            if since_last < self.config.lockout_duration {
                return Err(SchemaError::AuthenticationThrottled);
            }
            // Lockout expired — allow
            return Ok(());
        }

        // Under max_attempts: check backoff since last failure.
        // Backoff kicks in after 2+ failures — the first failure is a freebie.
        if entry.count > 1 {
            let backoff = Self::compute_backoff(&self.config, entry.count);
            let since_last = now.duration_since(entry.last_failure);
            if since_last < backoff {
                return Err(SchemaError::AuthenticationThrottled);
            }
        }

        Ok(())
    }

    /// Record a failed authentication attempt.
    pub fn record_failure(&self, username: &str) {
        let mut attempts = self.attempts.lock().expect("rate limiter lock poisoned");
        let now = Instant::now();

        let entry = attempts
            .entry(username.to_string())
            .or_insert(FailedAttempts {
                count: 0,
                first_failure: now,
                last_failure: now,
            });

        // If the window has expired, start fresh
        if now.duration_since(entry.first_failure) > self.config.window {
            entry.count = 0;
            entry.first_failure = now;
        }

        entry.count += 1;
        entry.last_failure = now;
    }

    /// Record a successful authentication — resets the failure counter.
    pub fn record_success(&self, username: &str) {
        let mut attempts = self.attempts.lock().expect("rate limiter lock poisoned");
        attempts.remove(username);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fast_config(max_attempts: u32) -> RateLimitConfig {
        RateLimitConfig {
            max_attempts,
            base_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(500),
            lockout_duration: Duration::from_millis(100),
            window: Duration::from_millis(500),
            ..Default::default()
        }
    }

    #[test]
    fn first_attempt_is_allowed() {
        let limiter = AuthRateLimiter::new(fast_config(3));
        assert!(limiter.check_rate_limit("alice").is_ok());
    }

    #[test]
    fn successful_auth_resets_counter() {
        let limiter = AuthRateLimiter::new(fast_config(3));
        limiter.record_failure("bob");
        limiter.record_failure("bob");
        // Two failures, but then a success resets the counter
        limiter.record_success("bob");
        // Should be allowed again (counter reset)
        assert!(limiter.check_rate_limit("bob").is_ok());
        // Can fail again without being locked out
        limiter.record_failure("bob");
        assert!(limiter.check_rate_limit("bob").is_ok());
    }

    #[test]
    fn lockout_after_max_attempts() {
        let limiter = AuthRateLimiter::new(fast_config(3));
        limiter.record_failure("charlie");
        limiter.record_failure("charlie");
        limiter.record_failure("charlie");
        // Should now be locked out
        let result = limiter.check_rate_limit("charlie");
        assert!(result.is_err());
        match result.unwrap_err() {
            SchemaError::AuthenticationThrottled => {}
            other => panic!("expected AuthenticationThrottled, got: {other}"),
        }
    }

    #[test]
    fn default_config_values() {
        let config = RateLimitConfig::default();
        assert_eq!(config.max_attempts, 5);
        assert_eq!(config.base_backoff, Duration::from_secs(1));
        assert_eq!(config.max_backoff, Duration::from_secs(60));
        assert_eq!(config.lockout_duration, Duration::from_secs(900));
        assert_eq!(config.window, Duration::from_secs(900));
    }

    #[test]
    fn backoff_increases_exponentially() {
        // Verify the backoff computation: min(base * 2^(n-1), max)
        let config = RateLimitConfig {
            base_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_millis(1000),
            ..Default::default()
        };

        // n=1: 100 * 2^0 = 100ms
        let b1 = AuthRateLimiter::compute_backoff(&config, 1);
        assert_eq!(b1, Duration::from_millis(100));

        // n=2: 100 * 2^1 = 200ms
        let b2 = AuthRateLimiter::compute_backoff(&config, 2);
        assert_eq!(b2, Duration::from_millis(200));

        // n=3: 100 * 2^2 = 400ms
        let b3 = AuthRateLimiter::compute_backoff(&config, 3);
        assert_eq!(b3, Duration::from_millis(400));

        // n=4: 100 * 2^3 = 800ms
        let b4 = AuthRateLimiter::compute_backoff(&config, 4);
        assert_eq!(b4, Duration::from_millis(800));

        // n=5: 100 * 2^4 = 1600ms, capped to 1000ms
        let b5 = AuthRateLimiter::compute_backoff(&config, 5);
        assert_eq!(b5, Duration::from_millis(1000));
    }
}
