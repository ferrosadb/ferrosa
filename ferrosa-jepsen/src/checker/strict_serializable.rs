//! Bounded transaction-history checker for PostgreSQL strict serializability.
//!
//! Unlike the register checker, this model treats a transaction's operations
//! as one atomic unit and preserves real-time order between non-overlapping
//! transactions. Aborted transactions have no effect on the committed history.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};

const MAX_TRANSACTIONS: usize = 64;
const SEARCH_LIMIT: usize = 200_000;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum TransactionOperation {
    Read { key: String, value: Option<i64> },
    Write { key: String, value: Option<i64> },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct RecordedTransaction {
    pub id: u64,
    /// Monotonic event sequence captured at invocation.
    pub invoked: u64,
    /// Monotonic event sequence captured at completion.
    pub completed: u64,
    pub committed: bool,
    pub operations: Vec<TransactionOperation>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CheckError {
    Invalid(String),
    Inconclusive(String),
}

impl std::fmt::Display for CheckError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(reason) => write!(f, "strict-serializability violation: {reason}"),
            Self::Inconclusive(reason) => {
                write!(f, "strict-serializability check inconclusive: {reason}")
            }
        }
    }
}

impl std::error::Error for CheckError {}

/// Checks whether committed transactions admit a sequential execution that
/// matches every recorded read, final value, and real-time precedence edge.
///
/// The search is bounded and fails inconclusively instead of treating an
/// exhausted search budget as a passing history.
pub fn check_strict_serializable(
    initial: &BTreeMap<String, i64>,
    final_state: &BTreeMap<String, i64>,
    history: &[RecordedTransaction],
) -> Result<(), CheckError> {
    let committed: Vec<_> = history.iter().filter(|txn| txn.committed).collect();
    if committed.len() > MAX_TRANSACTIONS {
        return Err(CheckError::Inconclusive(format!(
            "{} committed transactions exceed the search limit of {MAX_TRANSACTIONS}",
            committed.len()
        )));
    }

    let mut ids = HashSet::with_capacity(committed.len());
    for txn in &committed {
        if !ids.insert(txn.id) {
            return Err(CheckError::Invalid(format!(
                "duplicate committed transaction id {}",
                txn.id
            )));
        }
        if txn.completed <= txn.invoked {
            return Err(CheckError::Invalid(format!(
                "transaction {} has an invalid invocation/completion interval",
                txn.id
            )));
        }
    }

    let mut predecessors = vec![0_u64; committed.len()];
    for (later_index, later) in committed.iter().enumerate() {
        for (earlier_index, earlier) in committed.iter().enumerate() {
            if earlier.completed < later.invoked {
                predecessors[later_index] |= 1_u64 << earlier_index;
            }
        }
    }

    let all_done = if committed.len() == 64 {
        u64::MAX
    } else {
        (1_u64 << committed.len()) - 1
    };
    let mut visited = HashSet::new();
    let mut explored = 0;
    match search_order(
        0,
        all_done,
        &initial.clone(),
        final_state,
        &committed,
        &predecessors,
        &mut visited,
        &mut explored,
    ) {
        SearchResult::Found => Ok(()),
        SearchResult::Exhausted => Err(CheckError::Invalid(
            "no real-time-respecting serial order reproduces the observed reads and final state"
                .into(),
        )),
        SearchResult::Limit => Err(CheckError::Inconclusive(format!(
            "search exceeded {SEARCH_LIMIT} states"
        ))),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SearchResult {
    Found,
    Exhausted,
    Limit,
}

#[allow(clippy::too_many_arguments)]
fn search_order(
    done: u64,
    all_done: u64,
    state: &BTreeMap<String, i64>,
    final_state: &BTreeMap<String, i64>,
    txns: &[&RecordedTransaction],
    predecessors: &[u64],
    visited: &mut HashSet<(u64, Vec<(String, i64)>)>,
    explored: &mut usize,
) -> SearchResult {
    if done == all_done {
        return if state == final_state {
            SearchResult::Found
        } else {
            SearchResult::Exhausted
        };
    }

    let fingerprint = (done, state.iter().map(|(k, v)| (k.clone(), *v)).collect());
    if !visited.insert(fingerprint) {
        return SearchResult::Exhausted;
    }

    *explored += 1;
    if *explored > SEARCH_LIMIT {
        return SearchResult::Limit;
    }

    for (index, txn) in txns.iter().enumerate() {
        let bit = 1_u64 << index;
        if done & bit != 0 || predecessors[index] & !done != 0 {
            continue;
        }

        let Some(next_state) = apply_transaction(state, txn) else {
            continue;
        };
        match search_order(
            done | bit,
            all_done,
            &next_state,
            final_state,
            txns,
            predecessors,
            visited,
            explored,
        ) {
            SearchResult::Found => return SearchResult::Found,
            SearchResult::Limit => return SearchResult::Limit,
            SearchResult::Exhausted => {}
        }
    }
    SearchResult::Exhausted
}

fn apply_transaction(
    state: &BTreeMap<String, i64>,
    txn: &RecordedTransaction,
) -> Option<BTreeMap<String, i64>> {
    let mut next = state.clone();
    for operation in &txn.operations {
        match operation {
            TransactionOperation::Read { key, value } => {
                if next.get(key).copied() != *value {
                    return None;
                }
            }
            TransactionOperation::Write { key, value } => match value {
                Some(value) => {
                    next.insert(key.clone(), *value);
                }
                None => {
                    next.remove(key);
                }
            },
        }
    }
    Some(next)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn txn(
        id: u64,
        invoked: u64,
        completed: u64,
        committed: bool,
        operations: Vec<TransactionOperation>,
    ) -> RecordedTransaction {
        RecordedTransaction {
            id,
            invoked,
            completed,
            committed,
            operations,
        }
    }

    fn read(key: &str, value: i64) -> TransactionOperation {
        TransactionOperation::Read {
            key: key.into(),
            value: Some(value),
        }
    }

    fn write(key: &str, value: i64) -> TransactionOperation {
        TransactionOperation::Write {
            key: key.into(),
            value: Some(value),
        }
    }

    #[test]
    fn accepts_overlapping_transactions_when_a_serial_order_exists() {
        let initial = BTreeMap::from([("x".into(), 0)]);
        let final_state = BTreeMap::from([("x".into(), 1)]);
        let history = [
            txn(1, 1, 5, true, vec![write("x", 1)]),
            txn(2, 2, 4, true, vec![read("x", 0)]),
        ];
        assert!(check_strict_serializable(&initial, &final_state, &history).is_ok());
    }

    #[test]
    fn rejects_a_stale_read_after_its_writer_completed() {
        let initial = BTreeMap::from([("x".into(), 0)]);
        let final_state = BTreeMap::from([("x".into(), 1)]);
        let history = [
            txn(1, 1, 2, true, vec![write("x", 1)]),
            txn(2, 3, 4, true, vec![read("x", 0)]),
        ];
        assert!(matches!(
            check_strict_serializable(&initial, &final_state, &history),
            Err(CheckError::Invalid(_))
        ));
    }

    #[test]
    fn accepts_a_predicate_read_before_a_later_phantom_insert() {
        let initial = BTreeMap::from([("doctor-a".into(), 1), ("doctor-b".into(), 1)]);
        let final_state = BTreeMap::from([
            ("doctor-a".into(), 1),
            ("doctor-b".into(), 1),
            ("phantom".into(), 1),
        ]);
        let history = [
            txn(
                1,
                1,
                2,
                true,
                vec![
                    read("doctor-a", 1),
                    read("doctor-b", 1),
                    TransactionOperation::Read {
                        key: "phantom".into(),
                        value: None,
                    },
                ],
            ),
            txn(
                2,
                3,
                4,
                true,
                vec![
                    TransactionOperation::Read {
                        key: "phantom".into(),
                        value: None,
                    },
                    write("phantom", 1),
                ],
            ),
        ];
        assert!(check_strict_serializable(&initial, &final_state, &history).is_ok());
    }

    #[test]
    fn rejects_a_predicate_read_that_hides_a_real_time_predecessor_insert() {
        let initial = BTreeMap::from([("doctor-a".into(), 1), ("doctor-b".into(), 1)]);
        let final_state = BTreeMap::from([
            ("doctor-a".into(), 1),
            ("doctor-b".into(), 1),
            ("phantom".into(), 1),
        ]);
        let history = [
            txn(1, 1, 2, true, vec![write("phantom", 1)]),
            txn(
                2,
                3,
                4,
                true,
                vec![TransactionOperation::Read {
                    key: "phantom".into(),
                    value: None,
                }],
            ),
        ];
        assert!(matches!(
            check_strict_serializable(&initial, &final_state, &history),
            Err(CheckError::Invalid(_))
        ));
    }

    #[test]
    fn rejects_write_skew_when_both_transactions_commit() {
        let initial = BTreeMap::from([("x".into(), 1), ("y".into(), 1)]);
        let final_state = BTreeMap::from([("x".into(), 0), ("y".into(), 0)]);
        let history = [
            txn(
                1,
                1,
                5,
                true,
                vec![read("x", 1), read("y", 1), write("x", 0)],
            ),
            txn(
                2,
                2,
                6,
                true,
                vec![read("x", 1), read("y", 1), write("y", 0)],
            ),
        ];
        assert!(matches!(
            check_strict_serializable(&initial, &final_state, &history),
            Err(CheckError::Invalid(_))
        ));
    }

    #[test]
    fn rejects_a_mixed_multi_row_snapshot_after_real_time_predecessor() {
        let initial = BTreeMap::from([("x".into(), 0), ("y".into(), 0)]);
        let final_state = BTreeMap::from([("x".into(), 1), ("y".into(), 1)]);
        let history = [
            txn(1, 1, 2, true, vec![write("x", 1), write("y", 1)]),
            txn(2, 3, 4, true, vec![read("x", 1), read("y", 0)]),
        ];
        assert!(matches!(
            check_strict_serializable(&initial, &final_state, &history),
            Err(CheckError::Invalid(_))
        ));
    }

    #[test]
    fn ignores_aborted_transactions_and_rejects_search_overflow() {
        let initial = BTreeMap::from([("x".into(), 0)]);
        let history = [txn(1, 1, 2, false, vec![write("x", 7)])];
        assert!(check_strict_serializable(&initial, &initial, &history).is_ok());

        let too_many = (0..=MAX_TRANSACTIONS as u64)
            .map(|id| txn(id, id * 2, id * 2 + 1, true, vec![]))
            .collect::<Vec<_>>();
        assert!(matches!(
            check_strict_serializable(&initial, &initial, &too_many),
            Err(CheckError::Inconclusive(_))
        ));
    }
}
