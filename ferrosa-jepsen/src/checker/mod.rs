pub mod elle;
pub mod knossos;
pub mod membership;

use std::collections::BTreeSet;
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::history::{History, Op, OpResult, Operation};

/// Result of a linearizability check for a single key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckResult {
    pub valid: bool,
    pub key: String,
    pub total_ops: usize,
    /// If invalid, a minimal counterexample.
    pub counterexample: Option<Counterexample>,
    pub check_duration_ms: u64,
}

/// A counterexample demonstrating non-linearizability.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Counterexample {
    /// The operations that cannot be linearized.
    pub operations: Vec<Operation>,
    /// Human-readable explanation.
    pub explanation: String,
}

/// Sequential specification for a single-value register.
struct RegisterModel {
    /// Current value (`None` means never written).
    value: Option<i64>,
}

impl RegisterModel {
    fn new() -> Self {
        Self { value: None }
    }

    /// Try to apply an operation against this model state.
    /// Returns `true` if the recorded result is consistent with the model.
    fn apply(&mut self, op: &Op, result: &OpResult) -> bool {
        match (op, result) {
            // Read must see the current value.
            (Op::Read { .. } | Op::SerialRead { .. }, OpResult::Value(v)) => *v == self.value,

            // Write always succeeds; update model.
            (Op::Write { value, .. }, OpResult::Ok) => {
                self.value = Some(*value);
                true
            }

            // CAS succeeds when model matches expected.
            (
                Op::Cas {
                    expected, value, ..
                },
                OpResult::Ok | OpResult::Applied(true),
            ) if self.value == Some(*expected) => {
                self.value = Some(*value);
                true
            }

            // CAS fails when model does NOT match expected.
            (Op::Cas { expected, .. }, OpResult::Applied(false)) => self.value != Some(*expected),

            // Timeouts and errors are indeterminate — always linearizable.
            (_, OpResult::Timeout | OpResult::Err(_)) => true,

            _ => false,
        }
    }

    fn snapshot(&self) -> Option<i64> {
        self.value
    }

    fn restore(&mut self, value: Option<i64>) {
        self.value = value;
    }
}

/// Maximum number of backtracking nodes before we give up.
const SEARCH_LIMIT: u64 = 100_000;

/// Check linearizability of a complete history.
///
/// Each key is checked independently. Returns one `CheckResult` per key.
pub fn check_linearizability(history: &History) -> Vec<CheckResult> {
    let keys = extract_keys(history);
    keys.into_iter()
        .map(|key| {
            let filtered = history.filter_key(&key);
            check_key(&key, &filtered.operations)
        })
        .collect()
}

/// Extract all unique keys mentioned in the history.
fn extract_keys(history: &History) -> Vec<String> {
    let mut keys = BTreeSet::new();
    for op in &history.operations {
        collect_keys(&op.op, &mut keys);
    }
    keys.into_iter().collect()
}

/// Recursively collect keys from an `Op`.
fn collect_keys(op: &Op, keys: &mut BTreeSet<String>) {
    match op {
        Op::Read { key } | Op::Write { key, .. } | Op::Cas { key, .. } | Op::SerialRead { key } => {
            keys.insert(key.clone());
        }
        Op::Transaction { statements } => {
            for s in statements {
                collect_keys(s, keys);
            }
        }
        Op::InsertIfNotExists { .. } | Op::UpdateIf { .. } | Op::DeleteIf { .. } => {}
    }
}

/// Check a single key's operations for linearizability using backtracking.
fn check_key(key: &str, ops: &[Operation]) -> CheckResult {
    let start = Instant::now();

    if ops.is_empty() {
        return CheckResult {
            valid: true,
            key: key.to_string(),
            total_ops: 0,
            counterexample: None,
            check_duration_ms: start.elapsed().as_millis() as u64,
        };
    }

    // Sort by invoke time — we process in temporal order.
    let mut sorted_ops: Vec<&Operation> = ops.iter().collect();
    sorted_ops.sort_by_key(|o| o.invoke_us);

    let n = sorted_ops.len();
    let mut linearized = vec![false; n];
    let mut model = RegisterModel::new();
    let mut nodes_visited: u64 = 0;

    let valid = backtrack(
        &sorted_ops,
        &mut linearized,
        &mut model,
        0,
        &mut nodes_visited,
    );

    let elapsed = start.elapsed().as_millis() as u64;

    if valid {
        CheckResult {
            valid: true,
            key: key.to_string(),
            total_ops: n,
            counterexample: None,
            check_duration_ms: elapsed,
        }
    } else {
        CheckResult {
            valid: false,
            key: key.to_string(),
            total_ops: n,
            counterexample: Some(Counterexample {
                operations: ops.to_vec(),
                explanation: format!(
                    "No valid linearization found for key '{}' after exploring {} nodes ({} ops)",
                    key, nodes_visited, n
                ),
            }),
            check_duration_ms: elapsed,
        }
    }
}

/// Recursive backtracking search for a valid linearization.
///
/// `linearized_count` tracks how many operations have been placed so far (derived
/// from the `linearized` bitvec). An operation is eligible to be linearized next
/// if:
///   - It has not yet been linearized.
///   - Its invoke_us is <= the complete_us of every already-linearized operation
///     that precedes it in real time (i.e., it could have happened at this point).
///
/// In the WGL approach we conceptually pick a linearization point inside each
/// operation's `[invoke_us, complete_us]` interval. An operation `o` is eligible
/// if all previously linearized operations could have their linearization points
/// before `o`'s. The simplest sufficient condition: `o` has been invoked (its
/// `invoke_us` is at most the latest `complete_us` we have committed so far, or
/// it is the earliest un-linearized invocation).
fn backtrack(
    ops: &[&Operation],
    linearized: &mut [bool],
    model: &mut RegisterModel,
    linearized_count: usize,
    nodes_visited: &mut u64,
) -> bool {
    // All operations linearized — success.
    if linearized_count == ops.len() {
        return true;
    }

    *nodes_visited += 1;
    if *nodes_visited > SEARCH_LIMIT {
        return false;
    }

    // Determine the earliest invoke time among un-linearized operations.
    // Only operations whose invoke_us <= the minimum un-linearized complete_us
    // (i.e., they overlap with or precede the earliest pending operation) are
    // eligible candidates.
    let min_pending_complete = ops
        .iter()
        .enumerate()
        .filter(|(i, _)| !linearized[*i])
        .map(|(_, o)| o.complete_us)
        .min()
        .expect("at least one un-linearized op");

    for i in 0..ops.len() {
        if linearized[i] {
            continue;
        }

        // An operation can only be linearized if it could have happened
        // before the earliest pending operation completes (they overlap in time).
        if ops[i].invoke_us > min_pending_complete {
            continue;
        }

        let saved = model.snapshot();

        if model.apply(&ops[i].op, &ops[i].result) {
            linearized[i] = true;

            if backtrack(ops, linearized, model, linearized_count + 1, nodes_visited) {
                return true;
            }

            linearized[i] = false;
        }

        model.restore(saved);
    }

    false
}

/// Unified checker that can run Rust-native, Knossos, and Elle checkers.
pub struct UnifiedChecker {
    pub jepsen_dir: Option<std::path::PathBuf>,
    /// Membership snapshots collected from each node, used by Sprint 2 W2.4
    /// structural-invariant checks. Empty when the orchestrator did not
    /// expose the `/admin/membership-snapshot` endpoint (pre-W2.3 setups).
    pub membership_snapshots: Vec<membership::MembershipSnapshot>,
}

impl UnifiedChecker {
    pub fn new() -> Self {
        Self {
            jepsen_dir: None,
            membership_snapshots: Vec::new(),
        }
    }

    pub fn with_jepsen_dir(jepsen_dir: impl Into<std::path::PathBuf>) -> Self {
        Self {
            jepsen_dir: Some(jepsen_dir.into()),
            membership_snapshots: Vec::new(),
        }
    }

    /// Attach membership snapshots collected from `/admin/membership-snapshot`.
    /// Sprint 2 W2.4 — fed by the orchestrator after a workload completes.
    pub fn with_membership_snapshots(
        mut self,
        snapshots: Vec<membership::MembershipSnapshot>,
    ) -> Self {
        self.membership_snapshots = snapshots;
        self
    }

    /// Run all applicable checkers on a history.
    pub fn check_all(&self, history: &History) -> AllCheckResults {
        let linear = check_linearizability(history);

        // Knossos and Elle require a running Jepsen cluster with lein;
        // return None when the subprocess checkers are not available.
        let knossos_result: Option<knossos::KnossosResult> = None;
        let elle_result: Option<elle::ElleResult> = None;

        // Acknowledge jepsen_dir for future use.
        let _ = &self.jepsen_dir;

        // Run Sprint 2 W2.4 structural-invariant checks if snapshots present.
        let membership_violations = if self.membership_snapshots.is_empty() {
            Vec::new()
        } else {
            membership::check_membership_invariants(&self.membership_snapshots)
        };

        AllCheckResults {
            linearizability: linear,
            knossos: knossos_result,
            elle: elle_result,
            membership_violations,
        }
    }
}

impl Default for UnifiedChecker {
    fn default() -> Self {
        Self::new()
    }
}

/// Results from all checker backends.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllCheckResults {
    pub linearizability: Vec<CheckResult>,
    pub knossos: Option<knossos::KnossosResult>,
    pub elle: Option<elle::ElleResult>,
    /// Sprint 2 W2.4 — structural-invariant violations across the
    /// membership snapshots collected from each node. Empty when no
    /// snapshots were attached.
    #[serde(default)]
    pub membership_violations: Vec<membership::InvariantViolation>,
}

impl AllCheckResults {
    /// True when every checker that ran reports clean.
    pub fn all_passed(&self) -> bool {
        self.linearizability.iter().all(|r| r.valid)
            && self.knossos.as_ref().is_none_or(|r| r.valid)
            && self.elle.as_ref().is_none_or(|r| r.valid)
            && self.membership_violations.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::{Op, OpResult, Operation};

    /// Helper: build an Operation with explicit timestamps.
    fn make_op(client: &str, invoke: u64, complete: u64, op: Op, result: OpResult) -> Operation {
        Operation {
            client_id: client.to_string(),
            invoke_us: invoke,
            complete_us: complete,
            op,
            result,
        }
    }

    #[test]
    fn test_linearizable_sequential() {
        // w(x, 1) at [100, 200], then r(x) = 1 at [300, 400] — sequential, should pass.
        let history = History {
            operations: vec![
                make_op(
                    "c1",
                    100,
                    200,
                    Op::Write {
                        key: "x".into(),
                        value: 1,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c1",
                    300,
                    400,
                    Op::Read { key: "x".into() },
                    OpResult::Value(Some(1)),
                ),
            ],
        };

        let results = check_linearizability(&history);
        assert_eq!(results.len(), 1);
        assert!(
            results[0].valid,
            "sequential w(1) -> r(1) should be linearizable"
        );
    }

    #[test]
    fn test_linearizable_concurrent() {
        // w(x, 1) at [100, 300] and r(x) = 1 at [200, 400] — overlapping, r sees 1, should pass.
        let history = History {
            operations: vec![
                make_op(
                    "c1",
                    100,
                    300,
                    Op::Write {
                        key: "x".into(),
                        value: 1,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c2",
                    200,
                    400,
                    Op::Read { key: "x".into() },
                    OpResult::Value(Some(1)),
                ),
            ],
        };

        let results = check_linearizability(&history);
        assert_eq!(results.len(), 1);
        assert!(
            results[0].valid,
            "concurrent w(1) || r(1) should be linearizable"
        );
    }

    #[test]
    fn test_not_linearizable_stale_read() {
        // w(x, 1) at [100, 200], w(x, 2) at [300, 400], r(x) = 1 at [500, 600]
        // The read happens after both writes complete, so it must see 2. Seeing 1 is stale.
        let history = History {
            operations: vec![
                make_op(
                    "c1",
                    100,
                    200,
                    Op::Write {
                        key: "x".into(),
                        value: 1,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c1",
                    300,
                    400,
                    Op::Write {
                        key: "x".into(),
                        value: 2,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c2",
                    500,
                    600,
                    Op::Read { key: "x".into() },
                    OpResult::Value(Some(1)),
                ),
            ],
        };

        let results = check_linearizability(&history);
        assert_eq!(results.len(), 1);
        assert!(
            !results[0].valid,
            "stale read after sequential writes should NOT be linearizable"
        );
        assert!(results[0].counterexample.is_some());
    }

    #[test]
    fn test_cas_linearizable() {
        // w(x, 0) at [100, 200], CAS(x, 0->1) succeeds at [300, 400], r(x) = 1 at [500, 600]
        let history = History {
            operations: vec![
                make_op(
                    "c1",
                    100,
                    200,
                    Op::Write {
                        key: "x".into(),
                        value: 0,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c1",
                    300,
                    400,
                    Op::Cas {
                        key: "x".into(),
                        expected: 0,
                        value: 1,
                    },
                    OpResult::Applied(true),
                ),
                make_op(
                    "c2",
                    500,
                    600,
                    Op::Read { key: "x".into() },
                    OpResult::Value(Some(1)),
                ),
            ],
        };

        let results = check_linearizability(&history);
        assert_eq!(results.len(), 1);
        assert!(
            results[0].valid,
            "w(0) -> CAS(0->1) -> r(1) should be linearizable"
        );
    }

    #[test]
    fn test_cas_not_linearizable() {
        // w(x, 0) at [100, 200], CAS(x, 0->1) succeeds at [300, 400], r(x) = 0 at [500, 600]
        // After CAS succeeds, value is 1, but read sees 0 — not linearizable.
        let history = History {
            operations: vec![
                make_op(
                    "c1",
                    100,
                    200,
                    Op::Write {
                        key: "x".into(),
                        value: 0,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c1",
                    300,
                    400,
                    Op::Cas {
                        key: "x".into(),
                        expected: 0,
                        value: 1,
                    },
                    OpResult::Applied(true),
                ),
                make_op(
                    "c2",
                    500,
                    600,
                    Op::Read { key: "x".into() },
                    OpResult::Value(Some(0)),
                ),
            ],
        };

        let results = check_linearizability(&history);
        assert_eq!(results.len(), 1);
        assert!(
            !results[0].valid,
            "r(0) after successful CAS(0->1) should NOT be linearizable"
        );
        assert!(results[0].counterexample.is_some());
    }

    #[test]
    fn test_concurrent_writes_linearizable() {
        // Two overlapping writes: w(x, 1) at [100, 300], w(x, 2) at [200, 400]
        // Then r(x) = 2 at [500, 600] and r(x) = 2 at [700, 800]
        // Linearization order: w(1) then w(2) — reads see 2, consistent.
        let history = History {
            operations: vec![
                make_op(
                    "c1",
                    100,
                    300,
                    Op::Write {
                        key: "x".into(),
                        value: 1,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c2",
                    200,
                    400,
                    Op::Write {
                        key: "x".into(),
                        value: 2,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c1",
                    500,
                    600,
                    Op::Read { key: "x".into() },
                    OpResult::Value(Some(2)),
                ),
                make_op(
                    "c2",
                    700,
                    800,
                    Op::Read { key: "x".into() },
                    OpResult::Value(Some(2)),
                ),
            ],
        };

        let results = check_linearizability(&history);
        assert_eq!(results.len(), 1);
        assert!(
            results[0].valid,
            "concurrent writes with consistent reads should be linearizable"
        );
    }

    #[test]
    fn test_empty_history() {
        let history = History { operations: vec![] };

        let results = check_linearizability(&history);
        assert!(
            results.is_empty(),
            "empty history should produce no results"
        );
    }

    #[test]
    fn test_timeout_operations_linearizable() {
        // w(x, 1) at [100, 200] succeeds, w(x, 2) at [300, 400] times out,
        // r(x) = 1 at [500, 600] — the timed-out write may or may not have applied.
        // Since r sees 1 and the timeout is indeterminate, this is linearizable
        // (linearize the timeout as a no-op).
        let history = History {
            operations: vec![
                make_op(
                    "c1",
                    100,
                    200,
                    Op::Write {
                        key: "x".into(),
                        value: 1,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c2",
                    300,
                    400,
                    Op::Write {
                        key: "x".into(),
                        value: 2,
                    },
                    OpResult::Timeout,
                ),
                make_op(
                    "c1",
                    500,
                    600,
                    Op::Read { key: "x".into() },
                    OpResult::Value(Some(1)),
                ),
            ],
        };

        let results = check_linearizability(&history);
        assert_eq!(results.len(), 1);
        assert!(
            results[0].valid,
            "timed-out write followed by read of prior value should be linearizable"
        );
    }

    #[test]
    fn unified_checker_no_jepsen() {
        let checker = UnifiedChecker::new();
        let history = History { operations: vec![] };
        let results = checker.check_all(&history);
        assert!(results.linearizability.is_empty());
        assert!(results.knossos.is_none());
        assert!(results.elle.is_none());
    }

    #[test]
    fn all_check_results_serialization() {
        let results = AllCheckResults {
            linearizability: vec![],
            knossos: None,
            elle: None,
            membership_violations: vec![],
        };
        let json = serde_json::to_string(&results).unwrap();
        let back: AllCheckResults = serde_json::from_str(&json).unwrap();
        assert!(back.linearizability.is_empty());
        assert!(back.membership_violations.is_empty());
    }

    // -----------------------------------------------------------------------
    // JP-002: Checker correctness logic unit tests
    // -----------------------------------------------------------------------

    // --- Known-good histories (checker should pass) ---

    /// A single write with no reads is trivially linearizable.
    #[test]
    fn linearizable_single_write() {
        let history = History {
            operations: vec![make_op(
                "c1",
                100,
                200,
                Op::Write {
                    key: "x".into(),
                    value: 42,
                },
                OpResult::Ok,
            )],
        };
        let results = check_linearizability(&history);
        assert_eq!(results.len(), 1);
        assert!(results[0].valid, "single write must be linearizable");
        assert_eq!(results[0].total_ops, 1);
    }

    /// A read of None from an unwritten key is linearizable.
    #[test]
    fn linearizable_read_none_before_write() {
        let history = History {
            operations: vec![make_op(
                "c1",
                100,
                200,
                Op::Read { key: "x".into() },
                OpResult::Value(None),
            )],
        };
        let results = check_linearizability(&history);
        assert_eq!(results.len(), 1);
        assert!(
            results[0].valid,
            "reading None from unwritten register must be linearizable"
        );
    }

    /// Multiple independent keys should each be checked independently.
    #[test]
    fn linearizable_multi_key_independent() {
        let history = History {
            operations: vec![
                make_op(
                    "c1",
                    100,
                    200,
                    Op::Write {
                        key: "x".into(),
                        value: 1,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c1",
                    100,
                    200,
                    Op::Write {
                        key: "y".into(),
                        value: 2,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c2",
                    300,
                    400,
                    Op::Read { key: "x".into() },
                    OpResult::Value(Some(1)),
                ),
                make_op(
                    "c2",
                    300,
                    400,
                    Op::Read { key: "y".into() },
                    OpResult::Value(Some(2)),
                ),
            ],
        };
        let results = check_linearizability(&history);
        assert_eq!(results.len(), 2, "should produce one result per key");
        assert!(
            results.iter().all(|r| r.valid),
            "both keys should be independently linearizable"
        );
    }

    /// CAS that fails (not applied) because the expected value does not match.
    #[test]
    fn linearizable_cas_fails_correctly() {
        let history = History {
            operations: vec![
                make_op(
                    "c1",
                    100,
                    200,
                    Op::Write {
                        key: "x".into(),
                        value: 5,
                    },
                    OpResult::Ok,
                ),
                // CAS expects 0 but value is 5, so it fails.
                make_op(
                    "c2",
                    300,
                    400,
                    Op::Cas {
                        key: "x".into(),
                        expected: 0,
                        value: 10,
                    },
                    OpResult::Applied(false),
                ),
                // Value is still 5.
                make_op(
                    "c1",
                    500,
                    600,
                    Op::Read { key: "x".into() },
                    OpResult::Value(Some(5)),
                ),
            ],
        };
        let results = check_linearizability(&history);
        assert_eq!(results.len(), 1);
        assert!(
            results[0].valid,
            "failed CAS followed by read of original value should be linearizable"
        );
    }

    /// All operations are timeouts/errors -- always linearizable.
    #[test]
    fn linearizable_all_timeouts() {
        let history = History {
            operations: vec![
                make_op(
                    "c1",
                    100,
                    200,
                    Op::Write {
                        key: "x".into(),
                        value: 1,
                    },
                    OpResult::Timeout,
                ),
                make_op(
                    "c2",
                    300,
                    400,
                    Op::Read { key: "x".into() },
                    OpResult::Timeout,
                ),
                make_op(
                    "c1",
                    500,
                    600,
                    Op::Write {
                        key: "x".into(),
                        value: 2,
                    },
                    OpResult::Err("connection lost".into()),
                ),
            ],
        };
        let results = check_linearizability(&history);
        assert_eq!(results.len(), 1);
        assert!(
            results[0].valid,
            "all timeout/error operations should be trivially linearizable"
        );
    }

    /// SerialRead variant works the same as Read for the checker.
    #[test]
    fn linearizable_serial_read() {
        let history = History {
            operations: vec![
                make_op(
                    "c1",
                    100,
                    200,
                    Op::Write {
                        key: "x".into(),
                        value: 7,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c2",
                    300,
                    400,
                    Op::SerialRead { key: "x".into() },
                    OpResult::Value(Some(7)),
                ),
            ],
        };
        let results = check_linearizability(&history);
        assert_eq!(results.len(), 1);
        assert!(results[0].valid, "serial read of last write should pass");
    }

    /// Multiple concurrent overlapping writes with reads that see either value.
    #[test]
    fn linearizable_concurrent_writes_either_order() {
        // w(x,1) at [100, 400], w(x,2) at [200, 500]
        // r(x)=1 at [600, 700] -- linearize as w(2) then w(1)
        let history = History {
            operations: vec![
                make_op(
                    "c1",
                    100,
                    400,
                    Op::Write {
                        key: "x".into(),
                        value: 1,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c2",
                    200,
                    500,
                    Op::Write {
                        key: "x".into(),
                        value: 2,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c1",
                    600,
                    700,
                    Op::Read { key: "x".into() },
                    OpResult::Value(Some(1)),
                ),
            ],
        };
        let results = check_linearizability(&history);
        assert_eq!(results.len(), 1);
        // Linearization: w(2) at t=250, w(1) at t=350 -> read sees 1. Valid.
        assert!(
            results[0].valid,
            "concurrent overlapping writes allow either final value"
        );
    }

    // --- Known-bad histories (checker should detect violation) ---

    /// Read returns a value that was never written.
    #[test]
    fn not_linearizable_phantom_read() {
        let history = History {
            operations: vec![
                make_op(
                    "c1",
                    100,
                    200,
                    Op::Write {
                        key: "x".into(),
                        value: 1,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c2",
                    300,
                    400,
                    Op::Read { key: "x".into() },
                    OpResult::Value(Some(99)),
                ),
            ],
        };
        let results = check_linearizability(&history);
        assert_eq!(results.len(), 1);
        assert!(
            !results[0].valid,
            "reading a never-written value must fail linearizability"
        );
        assert!(results[0].counterexample.is_some());
    }

    /// CAS succeeds but the expected value doesn't match the model.
    #[test]
    fn not_linearizable_impossible_cas_success() {
        let history = History {
            operations: vec![
                make_op(
                    "c1",
                    100,
                    200,
                    Op::Write {
                        key: "x".into(),
                        value: 5,
                    },
                    OpResult::Ok,
                ),
                // CAS expects 0, but value is 5. This should fail, but claims success.
                make_op(
                    "c2",
                    300,
                    400,
                    Op::Cas {
                        key: "x".into(),
                        expected: 0,
                        value: 10,
                    },
                    OpResult::Applied(true),
                ),
                // If CAS actually applied, value should be 10. But we read 5.
                make_op(
                    "c1",
                    500,
                    600,
                    Op::Read { key: "x".into() },
                    OpResult::Value(Some(5)),
                ),
            ],
        };
        let results = check_linearizability(&history);
        assert_eq!(results.len(), 1);
        assert!(
            !results[0].valid,
            "CAS that claims success with wrong precondition + contradictory read should fail"
        );
    }

    /// Two sequential reads see values in impossible order.
    #[test]
    fn not_linearizable_backward_read() {
        // w(x,1), w(x,2) both sequential. Then r(x)=2, r(x)=1 both sequential.
        // The second read is after the first and must also see 2 or later.
        let history = History {
            operations: vec![
                make_op(
                    "c1",
                    100,
                    200,
                    Op::Write {
                        key: "x".into(),
                        value: 1,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c1",
                    300,
                    400,
                    Op::Write {
                        key: "x".into(),
                        value: 2,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c2",
                    500,
                    600,
                    Op::Read { key: "x".into() },
                    OpResult::Value(Some(2)),
                ),
                make_op(
                    "c2",
                    700,
                    800,
                    Op::Read { key: "x".into() },
                    OpResult::Value(Some(1)),
                ),
            ],
        };
        let results = check_linearizability(&history);
        assert_eq!(results.len(), 1);
        assert!(
            !results[0].valid,
            "reading older value after newer value in sequential order should fail"
        );
    }

    /// Read returns None after a successful sequential write.
    #[test]
    fn not_linearizable_read_none_after_write() {
        let history = History {
            operations: vec![
                make_op(
                    "c1",
                    100,
                    200,
                    Op::Write {
                        key: "x".into(),
                        value: 1,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c1",
                    300,
                    400,
                    Op::Read { key: "x".into() },
                    OpResult::Value(None),
                ),
            ],
        };
        let results = check_linearizability(&history);
        assert_eq!(results.len(), 1);
        assert!(
            !results[0].valid,
            "reading None after a successful write should fail linearizability"
        );
    }

    // --- Edge cases ---

    /// One key is linearizable, another is not -- results are per-key.
    #[test]
    fn mixed_keys_partial_failure() {
        let history = History {
            operations: vec![
                // Key "a": linearizable
                make_op(
                    "c1",
                    100,
                    200,
                    Op::Write {
                        key: "a".into(),
                        value: 1,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c1",
                    300,
                    400,
                    Op::Read { key: "a".into() },
                    OpResult::Value(Some(1)),
                ),
                // Key "b": NOT linearizable (phantom read)
                make_op(
                    "c2",
                    100,
                    200,
                    Op::Write {
                        key: "b".into(),
                        value: 1,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c2",
                    300,
                    400,
                    Op::Read { key: "b".into() },
                    OpResult::Value(Some(99)),
                ),
            ],
        };
        let results = check_linearizability(&history);
        assert_eq!(results.len(), 2);

        let a_result = results.iter().find(|r| r.key == "a").unwrap();
        let b_result = results.iter().find(|r| r.key == "b").unwrap();
        assert!(a_result.valid, "key 'a' should be linearizable");
        assert!(!b_result.valid, "key 'b' should NOT be linearizable");
    }

    /// CheckResult and Counterexample serialize/deserialize correctly.
    #[test]
    fn check_result_serialization() {
        let result = CheckResult {
            valid: false,
            key: "x".into(),
            total_ops: 3,
            counterexample: Some(Counterexample {
                operations: vec![make_op(
                    "c1",
                    100,
                    200,
                    Op::Write {
                        key: "x".into(),
                        value: 1,
                    },
                    OpResult::Ok,
                )],
                explanation: "stale read".into(),
            }),
            check_duration_ms: 42,
        };
        let json = serde_json::to_string(&result).unwrap();
        let back: CheckResult = serde_json::from_str(&json).unwrap();
        assert!(!back.valid);
        assert_eq!(back.key, "x");
        assert_eq!(back.total_ops, 3);
        assert!(back.counterexample.is_some());
        assert_eq!(back.counterexample.unwrap().explanation, "stale read");
    }

    /// UnifiedChecker produces per-key results.
    #[test]
    fn unified_checker_detects_violation() {
        let checker = UnifiedChecker::new();
        let history = History {
            operations: vec![
                make_op(
                    "c1",
                    100,
                    200,
                    Op::Write {
                        key: "x".into(),
                        value: 1,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c1",
                    300,
                    400,
                    Op::Write {
                        key: "x".into(),
                        value: 2,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c2",
                    500,
                    600,
                    Op::Read { key: "x".into() },
                    OpResult::Value(Some(1)),
                ),
            ],
        };
        let results = checker.check_all(&history);
        assert_eq!(results.linearizability.len(), 1);
        assert!(
            !results.linearizability[0].valid,
            "UnifiedChecker should detect stale read violation"
        );
    }

    /// UnifiedChecker passes on a known-good history.
    #[test]
    fn unified_checker_passes_good_history() {
        let checker = UnifiedChecker::new();
        let history = History {
            operations: vec![
                make_op(
                    "c1",
                    100,
                    200,
                    Op::Write {
                        key: "x".into(),
                        value: 1,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c1",
                    300,
                    400,
                    Op::Read { key: "x".into() },
                    OpResult::Value(Some(1)),
                ),
            ],
        };
        let results = checker.check_all(&history);
        assert_eq!(results.linearizability.len(), 1);
        assert!(results.linearizability[0].valid);
    }

    /// Sprint 2 W2.4: UnifiedChecker runs the membership invariants and surfaces
    /// violations through `AllCheckResults.membership_violations`.
    #[test]
    fn unified_checker_runs_membership_invariants() {
        use crate::checker::membership::{MembershipSnapshot, NodeStateLabel, NodeView};

        // Construct a snapshot with a deliberate I-07 (empty addr) violation.
        let mut state_members = std::collections::BTreeMap::new();
        state_members.insert(
            "node1".to_string(),
            NodeView {
                host_id: "node1".into(),
                addr: String::new(), // I-07 violation
                state: NodeStateLabel::Normal,
            },
        );
        let voters: std::collections::BTreeSet<String> =
            ["node1".to_string()].into_iter().collect();
        let snap = MembershipSnapshot {
            reporter_host_id: "node1".into(),
            state_members,
            openraft_voters: voters.clone(),
            openraft_learners: Default::default(),
            node_map: voters.clone(),
            peer_manager_peers: voters,
            committed_cluster_size: 1,
            live_peer_count: 1,
        };

        let checker = UnifiedChecker::new().with_membership_snapshots(vec![snap]);
        let results = checker.check_all(&History { operations: vec![] });
        assert!(
            !results.membership_violations.is_empty(),
            "UnifiedChecker must surface membership violations"
        );
        assert!(
            !results.all_passed(),
            "all_passed must be false when membership_violations are present"
        );
    }

    /// `all_passed` is true on a fully clean run.
    #[test]
    fn unified_checker_all_passed_on_clean_run() {
        let checker = UnifiedChecker::new();
        let results = checker.check_all(&History { operations: vec![] });
        assert!(results.all_passed());
    }

    /// UnifiedChecker with_jepsen_dir stores the path for future use.
    #[test]
    fn unified_checker_with_jepsen_dir() {
        let checker = UnifiedChecker::with_jepsen_dir("/tmp/jepsen");
        assert_eq!(
            checker.jepsen_dir,
            Some(std::path::PathBuf::from("/tmp/jepsen"))
        );
        // Still works without external checkers.
        let history = History { operations: vec![] };
        let results = checker.check_all(&history);
        assert!(results.linearizability.is_empty());
        assert!(results.knossos.is_none());
        assert!(results.elle.is_none());
    }

    /// Keys are extracted in sorted order (BTreeSet).
    #[test]
    fn extract_keys_sorted_order() {
        let history = History {
            operations: vec![
                make_op(
                    "c1",
                    100,
                    200,
                    Op::Write {
                        key: "z".into(),
                        value: 1,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c1",
                    200,
                    300,
                    Op::Write {
                        key: "a".into(),
                        value: 1,
                    },
                    OpResult::Ok,
                ),
                make_op(
                    "c1",
                    300,
                    400,
                    Op::Write {
                        key: "m".into(),
                        value: 1,
                    },
                    OpResult::Ok,
                ),
            ],
        };
        let keys = extract_keys(&history);
        assert_eq!(keys, vec!["a", "m", "z"]);
    }

    /// Transaction ops collect keys from nested statements.
    #[test]
    fn extract_keys_from_transaction() {
        let history = History {
            operations: vec![make_op(
                "c1",
                100,
                200,
                Op::Transaction {
                    statements: vec![
                        Op::Write {
                            key: "x".into(),
                            value: 1,
                        },
                        Op::Read { key: "y".into() },
                    ],
                },
                OpResult::Ok,
            )],
        };
        let keys = extract_keys(&history);
        assert_eq!(keys, vec!["x", "y"]);
    }

    /// InsertIfNotExists/UpdateIf/DeleteIf ops do not contribute keys
    /// (they use table/pk addressing, not the key field).
    #[test]
    fn extract_keys_ignores_lwt_ops() {
        let history = History {
            operations: vec![
                make_op(
                    "c1",
                    100,
                    200,
                    Op::InsertIfNotExists {
                        table: "t1".into(),
                        pk: "pk-0".into(),
                        values: vec![],
                    },
                    OpResult::Applied(true),
                ),
                make_op(
                    "c1",
                    200,
                    300,
                    Op::UpdateIf {
                        table: "t1".into(),
                        pk: "pk-0".into(),
                        condition: "val = 0".into(),
                        assignments: vec![],
                    },
                    OpResult::Applied(true),
                ),
                make_op(
                    "c1",
                    300,
                    400,
                    Op::DeleteIf {
                        table: "t1".into(),
                        pk: "pk-0".into(),
                        condition: "EXISTS".into(),
                    },
                    OpResult::Applied(true),
                ),
            ],
        };
        let keys = extract_keys(&history);
        assert!(
            keys.is_empty(),
            "LWT ops (InsertIfNotExists, UpdateIf, DeleteIf) should not contribute keys"
        );
    }

    /// Many concurrent operations on the same key -- stress the backtracking.
    #[test]
    fn linearizable_many_concurrent_writes_and_reads() {
        // 5 concurrent writes at overlapping times, then a read of the last value.
        let mut ops = Vec::new();
        for i in 0..5 {
            ops.push(make_op(
                &format!("c{i}"),
                100 + i * 10,
                500,
                Op::Write {
                    key: "x".into(),
                    value: i as i64,
                },
                OpResult::Ok,
            ));
        }
        // After all writes complete, read sees value 4 (last writer wins in some linearization).
        ops.push(make_op(
            "reader",
            600,
            700,
            Op::Read { key: "x".into() },
            OpResult::Value(Some(4)),
        ));

        let history = History { operations: ops };
        let results = check_linearizability(&history);
        assert_eq!(results.len(), 1);
        assert!(
            results[0].valid,
            "any value from the concurrent writers is valid if it's the last in some linearization"
        );
    }
}
