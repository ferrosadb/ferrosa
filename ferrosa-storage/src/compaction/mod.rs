//! Compaction strategies for SSTables.
//!
//! - **STCS**: Size-Tiered — groups SSTables by similar size, merges when
//!   a bucket reaches `min_threshold`.
//! - **UCS**: Unified — density-based levels with configurable fan factor.
//!   Subsumes STCS (W=large), LCS (W=2), and TWCS behavior.

/// `cancel_crash_sweep_*` tests (CS2): the crash-twin subprocess harness.
#[cfg(test)]
mod cancel_crash_sweep_tests;
/// Compaction cancel-point test harness (T-020): `CancelPoint`, the hook
/// production code calls at each lifecycle step, and install/clear helpers.
/// See `compaction-cancel-safety.md`. No cancellation exists yet (T-021) —
/// this packet only records or crashes, for tests.
#[cfg(any(test, feature = "test-support"))]
pub mod cancel_harness;
/// `cancel_harness_*` tests: proves the hook fires at every documented point
/// during a real, uncancelled compaction, and the oracle/invariant checker
/// self-tests on a clean table.
#[cfg(test)]
mod cancel_harness_integration;
/// Acknowledged-write oracle + `assert_cancel_invariants` (I1-I4), built on
/// [`cancel_harness`]. See `test-specification.md` L10.
#[cfg(any(test, feature = "test-support"))]
pub mod cancel_oracle;
/// `cancel_token_*` tests (T-021): real cancellation. CS1 (rollback at every
/// honoured checkpoint), CS3 (latency), CS4 (shutdown), CS14 (re-compaction
/// after cancel), CD1 (a parked worker exits shutdown immediately).
#[cfg(test)]
mod cancel_token_tests;
pub(crate) mod control;
pub use control::{CompactionStopReport, TableCompactionPause};
pub mod executor;
pub mod finalize;
pub mod intent;
pub mod metadata;
pub mod purge;
pub(crate) mod retire;
pub(crate) mod retry;
/// Stateful writes/compactions/restarts checked against a small reference model.
#[cfg(test)]
mod stateful_model_tests;
pub mod strategy;
pub mod strategy_ucs;

/// Compaction correctness validator (oracle + differential checks). Compiled
/// only for tests or when the `compaction-validator` feature is enabled.
#[cfg(any(test, feature = "compaction-validator"))]
pub mod validator;

pub(crate) use executor::{compaction_planning_deferred, compaction_pressure};
pub use executor::{CompactionExecutor, CompactionResult};
pub use metadata::{CompactionTask, SSTableMetadata};
pub use strategy::{CompactionConfig, CompactionStrategy, SizeTieredStrategy};
pub use strategy_ucs::{UcsConfig, UnifiedCompactionStrategy};

/// Fires a [`cancel_harness::CancelPoint`] hook at a named step in the
/// compaction lifecycle, scoped to `$scope` (a table id string — see
/// `cancel_harness`'s module docs for why hooks are scoped rather than
/// process-global). Compiles to nothing outside
/// `cfg(any(test, feature = "test-support"))`: both `$scope` and `$point`
/// are captured as `expr` fragments but never emitted, so neither needs an
/// import and neither is type-checked in production builds — call sites are
/// written unconditionally and stay one line regardless of build
/// configuration.
///
/// In this packet the hook only records or (in the crash-twin harness)
/// aborts the process; cancellation itself is T-021.
#[cfg(any(test, feature = "test-support"))]
macro_rules! cancel_point {
    ($scope:expr, $point:expr) => {
        $crate::compaction::cancel_harness::record_cancel_point($scope, $point)
    };
}
#[cfg(not(any(test, feature = "test-support")))]
macro_rules! cancel_point {
    ($scope:expr, $point:expr) => {};
}
pub(crate) use cancel_point;
