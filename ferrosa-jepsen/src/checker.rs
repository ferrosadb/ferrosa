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
            ) => {
                if self.value == Some(*expected) {
                    self.value = Some(*value);
                    true
                } else {
                    false
                }
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
}
