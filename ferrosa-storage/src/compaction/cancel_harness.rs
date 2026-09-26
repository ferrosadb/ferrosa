//! Compaction cancel-point test harness (T-020).
//!
//! `compaction-cancel-safety.md` describes a compaction lifecycle with no
//! durable commit point and five non-atomic windows. Before that design can
//! be built (T-021 onward) or verified, the lifecycle needs a name for every
//! step a cancellation or a crash can land on, and a way for a test to
//! observe or interrupt the task at exactly one of them.
//!
//! This module provides:
//! - [`CancelPoint`]: every named step in the lifecycle table and the C1
//!   check-point list.
//! - [`record_cancel_point`]: the hook production code calls at each step.
//!   In this packet it only invokes a test-installed callback (record or
//!   crash) — no cancellation exists yet, that is T-021.
//! - [`set_cancel_hook`] / [`clear_cancel_hook`]: install/remove the
//!   callback for one **scope** (a table id string).
//!
//! Hooks are keyed by scope, not a single process-wide slot: `cargo test`
//! runs many unrelated tests concurrently in one process, and essentially
//! any of them can drive a real compaction (which reaches every
//! `cancel_point!` call site regardless of which test triggered it). A
//! single global hook would record — or worse, abort on — points reached by
//! a completely unrelated concurrently-running test's compaction. Scoping by
//! the table id each call site already has in scope means a test that picks
//! its own dedicated table name is naturally isolated from every other test,
//! with no cross-test locking required.
//!
//! The [`cancel_point!`](crate::compaction::cancel_point) macro (defined in
//! `compaction::mod`) is what production code actually calls; it expands to
//! nothing outside `cfg(any(test, feature = "test-support"))`, so call sites
//! carry zero cost and zero code in production builds.
//!
//! See also `crate::compaction::cancel_oracle` (the acknowledged-write model
//! and `assert_cancel_invariants`) and `cancel_crash_sweep_tests` (the CS2
//! crash-twin subprocess harness, the first real use of this module).

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, OnceLock};

use parking_lot::RwLock;

/// Every step in the compaction lifecycle a cancel (T-021) or a crash can
/// land on. Ordering follows `compaction-cancel-safety.md`'s lifecycle
/// table: merge/staging, promote, finalize, input retirement, S3.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CancelPoint {
    /// Opening one input SSTable (`executor.rs` input-open loop).
    InputOpen,
    /// The first partition popped off the k-way merge heap.
    MergePartitionFirst,
    /// Every partition after the first and before the loop ends.
    MergePartitionMiddle,
    /// Right after the merge loop has written its last partition.
    MergePartitionLast,
    /// Immediately before `SSTableWriter::finish_to_directory`.
    BeforeFinish,
    /// Immediately before `FlushTarget::flush_files` promotes the staged
    /// output into the table's local `compaction/` directory.
    BeforeFlushFiles,
    /// Inside the streaming readback verification, once per partition read.
    VerifyPartition,
    /// Immediately before `promote_compaction_output` renames the output
    /// into `sstables/<table>/<gen>` (window A/B boundary).
    BeforePromote,
    /// Immediately after the promote rename + directory fsync has returned
    /// `Ok` (window C: output live, swap not yet applied).
    AfterPromote,
    /// Building full-text/index sidecars for the promoted output, before
    /// the view swap.
    SidecarBuild,
    /// Immediately before `TableStore::swap_compacted_sstables`.
    BeforeSwap,
    /// Immediately after the swap has applied (window D: swapped in
    /// memory, inputs not yet deleted).
    AfterSwap,
    /// Retiring input generation `k` (its index in the task's input list),
    /// before that generation's component files are unlinked (window E).
    RetireInput(usize),
    /// Writing the fsynced pending-upload log entry before S3 upload.
    S3PendingLog,
    /// Submitting/awaiting the S3 upload.
    S3Upload,
    /// The manifest compare-and-swap update after upload confirmation.
    S3ManifestCas,
    /// Enqueuing the grace-period S3 delete of a retired input.
    S3Delete,
}

impl fmt::Display for CancelPoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CancelPoint::InputOpen => write!(f, "InputOpen"),
            CancelPoint::MergePartitionFirst => write!(f, "MergePartitionFirst"),
            CancelPoint::MergePartitionMiddle => write!(f, "MergePartitionMiddle"),
            CancelPoint::MergePartitionLast => write!(f, "MergePartitionLast"),
            CancelPoint::BeforeFinish => write!(f, "BeforeFinish"),
            CancelPoint::BeforeFlushFiles => write!(f, "BeforeFlushFiles"),
            CancelPoint::VerifyPartition => write!(f, "VerifyPartition"),
            CancelPoint::BeforePromote => write!(f, "BeforePromote"),
            CancelPoint::AfterPromote => write!(f, "AfterPromote"),
            CancelPoint::SidecarBuild => write!(f, "SidecarBuild"),
            CancelPoint::BeforeSwap => write!(f, "BeforeSwap"),
            CancelPoint::AfterSwap => write!(f, "AfterSwap"),
            CancelPoint::RetireInput(k) => write!(f, "RetireInput:{k}"),
            CancelPoint::S3PendingLog => write!(f, "S3PendingLog"),
            CancelPoint::S3Upload => write!(f, "S3Upload"),
            CancelPoint::S3ManifestCas => write!(f, "S3ManifestCas"),
            CancelPoint::S3Delete => write!(f, "S3Delete"),
        }
    }
}

impl std::str::FromStr for CancelPoint {
    type Err = String;

    /// Parses the [`Display`](fmt::Display) form back into a [`CancelPoint`].
    /// Used by the crash-twin subprocess harness to pass the target point
    /// through an environment variable across the `exec` boundary.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(idx) = s.strip_prefix("RetireInput:") {
            let idx: usize = idx
                .parse()
                .map_err(|e| format!("bad RetireInput index in {s:?}: {e}"))?;
            return Ok(CancelPoint::RetireInput(idx));
        }
        Ok(match s {
            "InputOpen" => CancelPoint::InputOpen,
            "MergePartitionFirst" => CancelPoint::MergePartitionFirst,
            "MergePartitionMiddle" => CancelPoint::MergePartitionMiddle,
            "MergePartitionLast" => CancelPoint::MergePartitionLast,
            "BeforeFinish" => CancelPoint::BeforeFinish,
            "BeforeFlushFiles" => CancelPoint::BeforeFlushFiles,
            "VerifyPartition" => CancelPoint::VerifyPartition,
            "BeforePromote" => CancelPoint::BeforePromote,
            "AfterPromote" => CancelPoint::AfterPromote,
            "SidecarBuild" => CancelPoint::SidecarBuild,
            "BeforeSwap" => CancelPoint::BeforeSwap,
            "AfterSwap" => CancelPoint::AfterSwap,
            "S3PendingLog" => CancelPoint::S3PendingLog,
            "S3Upload" => CancelPoint::S3Upload,
            "S3ManifestCas" => CancelPoint::S3ManifestCas,
            "S3Delete" => CancelPoint::S3Delete,
            other => return Err(format!("unknown CancelPoint: {other:?}")),
        })
    }
}

/// A test-installed callback invoked from [`record_cancel_point`]. Boxed as
/// `Arc<dyn Fn>` (not `FnMut`) because compaction can call it from more than
/// one worker thread concurrently; a recording hook owns its own interior
/// mutability (e.g. a `Mutex<Vec<_>>`).
pub type CancelHookFn = Arc<dyn Fn(CancelPoint) + Send + Sync>;

static HOOKS: OnceLock<RwLock<HashMap<String, CancelHookFn>>> = OnceLock::new();

fn hooks() -> &'static RwLock<HashMap<String, CancelHookFn>> {
    HOOKS.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Installs a cancel-point hook for `scope` (a table id string). Tests call
/// this before driving a compaction on that table and must pair it with
/// [`clear_cancel_hook`] (`#[test]` bodies that can panic mid-test should use
/// [`CancelHookGuard`] instead).
pub fn set_cancel_hook(scope: impl Into<String>, hook: CancelHookFn) {
    hooks().write().insert(scope.into(), hook);
}

/// Removes the cancel-point hook for `scope`.
pub fn clear_cancel_hook(scope: &str) {
    hooks().write().remove(scope);
}

/// Fires the hook registered for `scope`, if any, with `point`. Called only
/// through the [`cancel_point!`](super::cancel_point) macro, never directly,
/// so every call site stays a one-line, self-documenting statement and the
/// macro is the single place that decides whether the call exists at all.
pub fn record_cancel_point(scope: &str, point: CancelPoint) {
    let hook = hooks().read().get(scope).cloned();
    if let Some(hook) = hook {
        hook(point);
    }
}

/// RAII guard that clears its scope's hook on drop, including on an early
/// return or a panic unwind, so one test's hook can never leak into another
/// test that happens to reuse the same table name later in the same process.
pub struct CancelHookGuard {
    scope: String,
}

impl CancelHookGuard {
    pub fn install(scope: impl Into<String>, hook: CancelHookFn) -> Self {
        let scope = scope.into();
        set_cancel_hook(scope.clone(), hook);
        CancelHookGuard { scope }
    }
}

impl Drop for CancelHookGuard {
    fn drop(&mut self) {
        clear_cancel_hook(&self.scope);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use std::sync::Mutex;

    #[test]
    fn display_and_parse_round_trip_every_point() {
        let points = [
            CancelPoint::InputOpen,
            CancelPoint::MergePartitionFirst,
            CancelPoint::MergePartitionMiddle,
            CancelPoint::MergePartitionLast,
            CancelPoint::BeforeFinish,
            CancelPoint::BeforeFlushFiles,
            CancelPoint::VerifyPartition,
            CancelPoint::BeforePromote,
            CancelPoint::AfterPromote,
            CancelPoint::SidecarBuild,
            CancelPoint::BeforeSwap,
            CancelPoint::AfterSwap,
            CancelPoint::RetireInput(0),
            CancelPoint::RetireInput(3),
            CancelPoint::S3PendingLog,
            CancelPoint::S3Upload,
            CancelPoint::S3ManifestCas,
            CancelPoint::S3Delete,
        ];
        for point in points {
            let s = point.to_string();
            let parsed = CancelPoint::from_str(&s).unwrap_or_else(|e| {
                panic!("round-trip failed for {point:?} (rendered {s:?}): {e}")
            });
            assert_eq!(parsed, point, "round-trip mismatch for {s:?}");
        }
    }

    #[test]
    fn unknown_point_name_is_rejected() {
        assert!(CancelPoint::from_str("NotAPoint").is_err());
        assert!(CancelPoint::from_str("RetireInput:notanumber").is_err());
    }

    #[test]
    fn hook_fires_exactly_for_installed_points_and_guard_clears_it() {
        // A unique scope per test: hooks are keyed by scope, so this cannot
        // collide with any other test running concurrently in the same
        // process.
        let scope = "cancel_harness_unit_test::hook_fires";
        let seen: Arc<Mutex<Vec<CancelPoint>>> = Arc::new(Mutex::new(Vec::new()));
        let seen_for_hook = Arc::clone(&seen);
        {
            let _guard = CancelHookGuard::install(
                scope,
                Arc::new(move |p| {
                    seen_for_hook.lock().unwrap().push(p);
                }),
            );
            record_cancel_point(scope, CancelPoint::InputOpen);
            record_cancel_point(scope, CancelPoint::RetireInput(2));
        }
        // Guard dropped: the hook must be gone, so this call records nothing.
        record_cancel_point(scope, CancelPoint::AfterSwap);

        let recorded = seen.lock().unwrap();
        assert_eq!(
            *recorded,
            vec![CancelPoint::InputOpen, CancelPoint::RetireInput(2)]
        );
    }

    #[test]
    fn no_hook_installed_is_a_silent_no_op() {
        // A scope no test ever installs a hook for: proves the default
        // (nobody driving this scope) state does nothing rather than
        // panicking or blocking.
        record_cancel_point(
            "cancel_harness_unit_test::no_hook_installed",
            CancelPoint::S3Delete,
        );
    }
}
