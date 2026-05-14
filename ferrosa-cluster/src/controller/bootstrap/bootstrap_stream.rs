//! Phase 6 — BootstrapStream (W4.7).
//!
//! Pre-condition: schema replay complete on every node.
//! Post-condition: every owning replica has streamed its share of the
//! token-redistribution payload.  Operationally, the leader iterates
//! the [`crate::ring::TokenRing`] to determine which replicas owe
//! data to which joiners and tracks completion via
//! `BootstrapComplete` RPC acks.

use std::collections::{BTreeMap, BTreeSet};

use super::phase::{BootstrapError, BootstrapPhase};

/// Small-table row fallback cap. Row fallback is only selected when no SSTable
/// directories exist for the table; SSTable-backed tables must use bulk streaming
/// or be left for retry/repair.
pub const BOUNDED_ROW_FALLBACK_LIMIT: usize = 1_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TableStreamPlanInput {
    pub sstable_dir_count: usize,
    pub row_fallback_limit: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableStreamPlan {
    SstableBulk { sstable_dir_count: usize },
    BoundedRows { limit: usize },
    RetryRequired,
}

impl TableStreamPlan {
    pub fn allows_row_materialization(self) -> bool {
        matches!(self, Self::BoundedRows { .. })
    }

    pub fn row_materialization_limit(self) -> Option<usize> {
        match self {
            Self::BoundedRows { limit } => Some(limit),
            Self::SstableBulk { .. } | Self::RetryRequired => None,
        }
    }

    pub fn requires_retry(self) -> bool {
        matches!(self, Self::RetryRequired)
    }

    pub fn after_sstable_stream_failure(self, _reason: impl AsRef<str>) -> Self {
        match self {
            Self::SstableBulk { .. } => Self::RetryRequired,
            other => other,
        }
    }
}

pub fn plan_table_stream(input: TableStreamPlanInput) -> TableStreamPlan {
    if input.sstable_dir_count > 0 {
        TableStreamPlan::SstableBulk {
            sstable_dir_count: input.sstable_dir_count,
        }
    } else {
        TableStreamPlan::BoundedRows {
            limit: input.row_fallback_limit,
        }
    }
}

/// Per-replica streaming progress.  `expected_owners` is every
/// replica that owes data to the joining set; `completed_owners` is
/// every replica that has sent `BootstrapComplete`.
#[derive(Clone, Debug)]
pub struct BootstrapStreamState {
    pub expected_owners: BTreeSet<u64>,
    pub completed_owners: BTreeSet<u64>,
    /// For diagnostics: per-replica byte counter (zero for empty
    /// keyspaces — still counts as "completed" once the ack lands).
    pub bytes_streamed: BTreeMap<u64, u64>,
}

pub fn precondition(schema_replayed: bool) -> Result<(), BootstrapError> {
    if schema_replayed {
        Ok(())
    } else {
        Err(BootstrapError::phase(
            BootstrapPhase::BootstrapStream,
            "ReplaySchema post-condition not satisfied",
        ))
    }
}

pub fn postcondition(state: &BootstrapStreamState) -> Result<(), BootstrapError> {
    let missing: Vec<u64> = state
        .expected_owners
        .difference(&state.completed_owners)
        .copied()
        .collect();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(BootstrapError::phase(
            BootstrapPhase::BootstrapStream,
            format!(
                "{n} replica(s) did not finish streaming: {missing:?}",
                n = missing.len()
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_stream_postcondition_holds_when_all_owners_complete() {
        let state = BootstrapStreamState {
            expected_owners: [1, 2, 3].into_iter().collect(),
            completed_owners: [1, 2, 3].into_iter().collect(),
            bytes_streamed: BTreeMap::new(),
        };
        precondition(true).expect("replay ok");
        postcondition(&state).expect("all owners completed");
    }

    #[test]
    fn bootstrap_stream_flags_uncompleted_replica() {
        let state = BootstrapStreamState {
            expected_owners: [1, 2, 3].into_iter().collect(),
            completed_owners: [1, 2].into_iter().collect(),
            bytes_streamed: BTreeMap::new(),
        };
        let err = postcondition(&state).expect_err("missing replica → fail");
        assert_eq!(err.name(), BootstrapPhase::BootstrapStream);
    }

    #[test]
    fn bootstrap_stream_precondition_requires_replay() {
        assert!(precondition(false).is_err());
    }

    #[test]
    fn sstable_backed_table_uses_sstable_stream_before_row_materialization() {
        let plan = plan_table_stream(TableStreamPlanInput {
            sstable_dir_count: 3,
            row_fallback_limit: BOUNDED_ROW_FALLBACK_LIMIT,
        });

        assert_eq!(
            plan,
            TableStreamPlan::SstableBulk {
                sstable_dir_count: 3,
            }
        );
        assert!(
            !plan.allows_row_materialization(),
            "SSTable-backed bootstrap must attempt bulk SSTable transfer before row materialization"
        );
    }

    #[test]
    fn failed_sstable_stream_does_not_fall_back_to_unbounded_rows() {
        let plan = TableStreamPlan::SstableBulk {
            sstable_dir_count: 2,
        };

        let retry = plan.after_sstable_stream_failure("network partition");

        assert!(retry.requires_retry());
        assert!(
            !retry.allows_row_materialization(),
            "SSTable stream failure must not switch to row materialization, bounded or unbounded"
        );
    }

    #[test]
    fn small_table_row_fallback_is_partition_bounded() {
        let plan = plan_table_stream(TableStreamPlanInput {
            sstable_dir_count: 0,
            row_fallback_limit: 64,
        });

        assert_eq!(plan, TableStreamPlan::BoundedRows { limit: 64 });
        assert!(plan.allows_row_materialization());
        assert_eq!(plan.row_materialization_limit(), Some(64));
    }

    #[test]
    fn stream_failure_reports_retry_required() {
        let retry = TableStreamPlan::SstableBulk {
            sstable_dir_count: 1,
        }
        .after_sstable_stream_failure("send_sstable_files failed");

        assert_eq!(retry, TableStreamPlan::RetryRequired);
        assert!(retry.requires_retry());
    }
}
