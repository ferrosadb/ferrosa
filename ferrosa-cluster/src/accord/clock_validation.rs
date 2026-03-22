//! Clock drift validation for Accord PreAccept messages.
//!
//! Accord requires nodes to have roughly synchronized clocks. When a node
//! receives a PreAccept with a proposed timestamp far in the future relative
//! to its local clock, it should reject the message to prevent clock-skewed
//! nodes from poisoning the timestamp ordering.
//!
//! Past timestamps are always accepted -- a slow clock is harmless because
//! the receiving node will bump the timestamp forward via the HLC max rule.

use ferrosa_common::accord::Timestamp;

/// Maximum allowed clock drift before rejecting a PreAccept (500ms).
pub const DEFAULT_MAX_CLOCK_DRIFT_NS: u64 = 500_000_000;

/// Check if a proposed timestamp is within acceptable clock drift.
///
/// Returns `Ok(())` if the proposed timestamp is not too far in the future
/// relative to the local clock. Past timestamps are always accepted.
///
/// # Errors
///
/// Returns [`ClockDriftRejection`] if `proposed_t0.time` exceeds
/// `local_time_ns + max_drift_ns`.
pub fn validate_timestamp_drift(
    proposed_t0: &Timestamp,
    local_time_ns: u64,
    max_drift_ns: u64,
) -> Result<(), ClockDriftRejection> {
    // Only reject future timestamps that exceed the drift threshold.
    // Past timestamps are fine -- the HLC max rule handles them.
    if proposed_t0.time > local_time_ns + max_drift_ns {
        return Err(ClockDriftRejection {
            proposed_ns: proposed_t0.time,
            local_ns: local_time_ns,
            drift_ns: proposed_t0.time - local_time_ns,
            max_drift_ns,
        });
    }
    Ok(())
}

/// Rejection details when a proposed timestamp exceeds the maximum clock drift.
#[derive(Debug)]
pub struct ClockDriftRejection {
    /// The proposed timestamp in nanoseconds.
    pub proposed_ns: u64,
    /// The local clock time in nanoseconds.
    pub local_ns: u64,
    /// The actual drift: `proposed_ns - local_ns`.
    pub drift_ns: u64,
    /// The configured maximum allowed drift.
    pub max_drift_ns: u64,
}

impl std::fmt::Display for ClockDriftRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "clock drift {}ns exceeds max {}ns (proposed={}, local={})",
            self.drift_ns, self.max_drift_ns, self.proposed_ns, self.local_ns
        )
    }
}

impl std::error::Error for ClockDriftRejection {}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::accord::Timestamp;

    #[test]
    fn reject_future_timestamp_preaccept() {
        // local_time=1000ns, proposed t0.time=1000+500_000_001 (1ns over max drift).
        let local_time_ns = 1000;
        let proposed = Timestamp::new(0, local_time_ns + DEFAULT_MAX_CLOCK_DRIFT_NS + 1, 1);

        let result = validate_timestamp_drift(&proposed, local_time_ns, DEFAULT_MAX_CLOCK_DRIFT_NS);
        assert!(result.is_err());

        let rejection = result.unwrap_err();
        assert_eq!(
            rejection.proposed_ns,
            local_time_ns + DEFAULT_MAX_CLOCK_DRIFT_NS + 1
        );
        assert_eq!(rejection.local_ns, local_time_ns);
        assert_eq!(rejection.drift_ns, DEFAULT_MAX_CLOCK_DRIFT_NS + 1);
        assert_eq!(rejection.max_drift_ns, DEFAULT_MAX_CLOCK_DRIFT_NS);
    }

    #[test]
    fn hlc_ntp_step_change_guard() {
        // local_time=1000ns, proposed t0.time=500ns (in the past).
        // Past timestamps are fine -- only future is rejected.
        let local_time_ns = 1000;
        let proposed = Timestamp::new(0, 500, 1);

        let result = validate_timestamp_drift(&proposed, local_time_ns, DEFAULT_MAX_CLOCK_DRIFT_NS);
        assert!(result.is_ok());
    }

    #[test]
    fn accept_exact_boundary() {
        // Exactly at max drift should be accepted.
        let local_time_ns = 1000;
        let proposed = Timestamp::new(0, local_time_ns + DEFAULT_MAX_CLOCK_DRIFT_NS, 1);

        let result = validate_timestamp_drift(&proposed, local_time_ns, DEFAULT_MAX_CLOCK_DRIFT_NS);
        assert!(result.is_ok());
    }

    #[test]
    fn accept_within_drift() {
        // Well within drift should be accepted.
        let local_time_ns = 1_000_000_000;
        let proposed = Timestamp::new(0, local_time_ns + 100_000_000, 1); // 100ms ahead

        let result = validate_timestamp_drift(&proposed, local_time_ns, DEFAULT_MAX_CLOCK_DRIFT_NS);
        assert!(result.is_ok());
    }
}
