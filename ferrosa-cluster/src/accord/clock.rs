//! Clock-skew validation for the Accord consensus protocol.
//!
//! Accord timestamps are hybrid logical clock (HLC) values in **microseconds**.
//! When a coordinator proposes a timestamp, other nodes validate that the
//! proposed timestamp is within an acceptable skew window relative to their
//! local clock.  Timestamps too far in the past or future are rejected to
//! prevent clock-skewed nodes from corrupting Accord's ordering guarantees.
//!
//! # Units
//!
//! All timestamps passed to [`ClockValidator`] are **microseconds since the
//! Unix epoch** (i.e., Accord's HLC unit).  The `max_skew` field is a
//! [`std::time::Duration`], which is converted internally.
//!
//! # Default skew window
//!
//! The default [`ClockValidator`] (via `Default`) uses a 5-second window,
//! matching the Accord specification's recommended upper bound for NTP drift
//! in production clusters.
//!
//! # Example
//!
//! ```rust
//! use ferrosa_cluster::accord::clock::{ClockValidator, ClockError};
//! let v = ClockValidator::default();
//! let local_ts: i64 = 1_000_000_000_000;
//! assert!(v.validate_timestamp(local_ts + 1_000, local_ts).is_ok());
//! assert!(matches!(
//!     v.validate_timestamp(local_ts - 10_000_000, local_ts),
//!     Err(ClockError::TooFarInPast { .. })
//! ));
//! ```

use std::time::Duration;

// ---------------------------------------------------------------------------
// ClockError
// ---------------------------------------------------------------------------

/// Validation error returned when a timestamp lies outside the acceptable
/// skew window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClockError {
    /// The proposed timestamp is more than `max_skew` microseconds in the
    /// past relative to the local clock.
    TooFarInPast {
        /// Absolute skew in microseconds (`local_ts - proposed_ts`).
        skew_ms: i64,
    },
    /// The proposed timestamp is more than `max_skew` microseconds in the
    /// future relative to the local clock.
    TooFarInFuture {
        /// Absolute skew in microseconds (`proposed_ts - local_ts`).
        skew_ms: i64,
    },
}

impl std::fmt::Display for ClockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClockError::TooFarInPast { skew_ms } => {
                write!(f, "timestamp too far in past: skew={skew_ms}µs")
            }
            ClockError::TooFarInFuture { skew_ms } => {
                write!(f, "timestamp too far in future: skew={skew_ms}µs")
            }
        }
    }
}

impl std::error::Error for ClockError {}

// ---------------------------------------------------------------------------
// ClockValidator
// ---------------------------------------------------------------------------

/// Validates that an Accord HLC timestamp falls within an acceptable clock
/// skew window relative to the local node's clock.
///
/// Construct with [`ClockValidator::default`] for the standard 5-second
/// window, or set `max_skew` explicitly.
#[derive(Debug, Clone)]
pub struct ClockValidator {
    /// Maximum allowed absolute clock difference.
    pub max_skew: Duration,
}

impl Default for ClockValidator {
    fn default() -> Self {
        // Allow override via FERROSA_CLOCK_MAX_SKEW_SECS for test harnesses
        // (e.g., Jepsen) that may introduce clock drift. Default 5s matches
        // the Accord spec's recommended upper bound for NTP drift.
        let secs = std::env::var("FERROSA_CLOCK_MAX_SKEW_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(5);
        Self {
            max_skew: Duration::from_secs(secs),
        }
    }
}

impl ClockValidator {
    /// Validate that `ts` (a proposed HLC timestamp in **microseconds**) is
    /// within `±max_skew` of `local_ts` (the local node's current time in
    /// **microseconds**).
    ///
    /// # Errors
    ///
    /// Returns [`ClockError::TooFarInPast`] if the timestamp is more than
    /// `max_skew` in the past, or [`ClockError::TooFarInFuture`] if it is
    /// more than `max_skew` in the future.
    pub fn validate_timestamp(&self, ts: i64, local_ts: i64) -> Result<(), ClockError> {
        let max_skew_us = self.max_skew.as_micros() as i64;
        let diff = ts - local_ts;

        if diff < -max_skew_us {
            return Err(ClockError::TooFarInPast { skew_ms: -diff });
        }
        if diff > max_skew_us {
            return Err(ClockError::TooFarInFuture { skew_ms: diff });
        }
        Ok(())
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_zero_skew() {
        let v = ClockValidator::default();
        let ts: i64 = 1_000_000_000_000;
        assert!(v.validate_timestamp(ts, ts).is_ok());
    }

    #[test]
    fn accepts_within_skew() {
        let v = ClockValidator::default();
        let local: i64 = 1_000_000_000_000;
        // 1 second within the 5-second window — should pass.
        assert!(v.validate_timestamp(local + 1_000_000, local).is_ok());
        assert!(v.validate_timestamp(local - 1_000_000, local).is_ok());
    }

    #[test]
    fn accepts_at_exact_boundary() {
        let v = ClockValidator::default();
        let local: i64 = 1_000_000_000_000;
        let max = 5_000_000_i64; // 5 seconds in microseconds
        assert!(v.validate_timestamp(local + max, local).is_ok());
        assert!(v.validate_timestamp(local - max, local).is_ok());
    }

    #[test]
    fn rejects_one_us_past_boundary() {
        let v = ClockValidator::default();
        let local: i64 = 1_000_000_000_000;
        let max = 5_000_000_i64;

        let result = v.validate_timestamp(local + max + 1, local);
        assert!(matches!(
            result,
            Err(ClockError::TooFarInFuture { skew_ms: 5_000_001 })
        ));

        let result = v.validate_timestamp(local - max - 1, local);
        assert!(matches!(
            result,
            Err(ClockError::TooFarInPast { skew_ms: 5_000_001 })
        ));
    }
}
