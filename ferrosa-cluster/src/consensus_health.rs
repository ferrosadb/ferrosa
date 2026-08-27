//! Module: process-wide consensus health gate.
//! Responsibility: retain the first fatal consensus diagnostic and make the
//! failed state synchronously visible to readiness and CQL admission.
//! Correctness: failure is monotonic, diagnostics are fixed-capacity, and no
//! failure path waits on the failed Raft runtime.
//! Last revised: 2026-08-27
//! Last changed: Added bounded supervision for Raft panic and fatal states.

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::OnceLock;

/// Maximum retained diagnostic bytes. The public health/CQL responses never
/// expose this text; it exists solely for bounded operator logging.
pub const CONSENSUS_DIAGNOSTIC_CAPACITY: usize = 1024;

#[derive(Debug)]
struct BoundedDiagnostic {
    bytes: [u8; CONSENSUS_DIAGNOSTIC_CAPACITY],
    len: usize,
}

impl BoundedDiagnostic {
    fn from_args(args: fmt::Arguments<'_>) -> Self {
        let mut diagnostic = Self {
            bytes: [0; CONSENSUS_DIAGNOSTIC_CAPACITY],
            len: 0,
        };
        let _ = fmt::write(&mut diagnostic, args);
        diagnostic
    }

    fn as_str(&self) -> &str {
        std::str::from_utf8(&self.bytes[..self.len])
            .expect("BoundedDiagnostic only accepts UTF-8 string fragments")
    }
}

impl fmt::Write for BoundedDiagnostic {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        let available = CONSENSUS_DIAGNOSTIC_CAPACITY.saturating_sub(self.len);
        if available == 0 {
            return Err(fmt::Error);
        }

        let mut take = available.min(value.len());
        while take > 0 && !value.is_char_boundary(take) {
            take -= 1;
        }
        self.bytes[self.len..self.len + take].copy_from_slice(&value.as_bytes()[..take]);
        self.len += take;

        if take == value.len() {
            Ok(())
        } else {
            Err(fmt::Error)
        }
    }
}

/// First fatal consensus failure retained for operator diagnostics.
#[derive(Debug)]
pub struct ConsensusFailure {
    kind: &'static str,
    detail: BoundedDiagnostic,
}

impl ConsensusFailure {
    /// Stable failure category suitable for metrics and logs.
    pub fn kind(&self) -> &'static str {
        self.kind
    }

    /// Bounded internal detail. Never return this text to clients.
    pub fn detail(&self) -> &str {
        self.detail.as_str()
    }
}

/// Monotonic shared health state for the process's consensus lane.
#[derive(Debug, Default)]
pub struct ConsensusHealth {
    failed: AtomicBool,
    failure_count: AtomicU64,
    first_failure: OnceLock<ConsensusFailure>,
}

impl ConsensusHealth {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a fatal condition without allocating from its diagnostic size.
    /// The first detail is retained; later failures only increment the counter.
    /// Returns `true` only to the caller that installed the first failure, which
    /// grants ownership of the single operator-facing FATAL emission.
    pub fn fail(&self, kind: &'static str, detail: fmt::Arguments<'_>) -> bool {
        let _ = self
            .failure_count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                Some(count.saturating_add(1))
            });
        let first = self
            .first_failure
            .set(ConsensusFailure {
                kind,
                detail: BoundedDiagnostic::from_args(detail),
            })
            .is_ok();
        self.failed.store(true, Ordering::Release);
        first
    }

    pub fn is_healthy(&self) -> bool {
        !self.failed.load(Ordering::Acquire)
    }

    pub fn failure(&self) -> Option<&ConsensusFailure> {
        self.first_failure.get()
    }

    pub fn failure_count(&self) -> u64 {
        self.failure_count.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostic_is_utf8_safe_and_fixed_capacity() {
        let health = ConsensusHealth::new();
        let oversized = "🦀".repeat(CONSENSUS_DIAGNOSTIC_CAPACITY);
        assert!(health.fail("test", format_args!("{oversized}")));

        let detail = health.failure().unwrap().detail();
        assert!(detail.len() <= CONSENSUS_DIAGNOSTIC_CAPACITY);
        assert!(std::str::from_utf8(detail.as_bytes()).is_ok());
    }

    #[test]
    fn first_failure_is_stable_and_count_is_monotonic() {
        let health = ConsensusHealth::new();
        assert!(health.fail("first", format_args!("one")));
        for attempt in 0..10_000 {
            assert!(!health.fail("repeat", format_args!("failure {attempt}")));
        }

        assert!(!health.is_healthy());
        assert_eq!(health.failure_count(), 10_001);
        assert_eq!(health.failure().unwrap().kind(), "first");
        assert_eq!(health.failure().unwrap().detail(), "one");
    }
}
