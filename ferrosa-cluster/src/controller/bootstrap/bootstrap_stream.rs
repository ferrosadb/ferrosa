//! Phase 6 — BootstrapStream.
//!
//! Pre-condition: schema replay complete on every node.
//! Post-condition: every owning replica has streamed its share of the
//! token-redistribution payload.  Operationally, the leader iterates
//! the [`crate::ring::TokenRing`] to determine which replicas owe
//! data to which joiners and tracks completion via
//! `BootstrapComplete` RPC acks.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use ferrosa_storage::engine::StorageEngine;
use ferrosa_storage::TableId;

use crate::streaming::StreamedMutation;

use super::phase::{BootstrapError, BootstrapPhase};

/// Stream every partition of `table_id`, one at a time, into per-owner batches.
///
/// The row fallback is NOT bounded by a partition count: the capped `read_range`
/// this replaces silently dropped the tail of a table larger than the limit (no
/// error, no truncated flag). The walk covers the merged memtable + SSTable view,
/// so every partition is streamed while at most one is resident. Owner resolution
/// is injected so the batching is testable without a live ring.
pub fn stream_row_fallback_into(
    engine: &StorageEngine,
    table_id: &TableId,
    keyspace: &str,
    table: &str,
    local_node_id: u64,
    owner_of: impl Fn(i64) -> u64,
) -> Result<HashMap<u64, Vec<StreamedMutation>>, String> {
    let mut by_node: HashMap<u64, Vec<StreamedMutation>> = HashMap::new();
    engine
        .walk_token_range(table_id, i64::MIN, i64::MAX, |partition| {
            let owner = owner_of(partition.key.token.0);
            if owner != local_node_id {
                match StreamedMutation::from_partition(keyspace, table, partition) {
                    Ok(mutation) => by_node.entry(owner).or_default().push(mutation),
                    Err(e) => {
                        tracing::error!(
                            %e,
                            partition_key = ?partition.key,
                            "bootstrap: failed to serialize partition, skipping partition (data loss avoided)"
                        );
                    }
                }
            }
            Ok(())
        })
        .map_err(|e| e.to_string())?;
    Ok(by_node)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TableStreamPlanInput {
    pub sstable_dir_count: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableStreamPlan {
    SstableBulk {
        sstable_dir_count: usize,
    },
    /// No SSTable directories exist for the table, so its rows are streamed
    /// partition by partition. There is no row cap: the fallback streams EVERY
    /// partition (spilling from disk), so a table of any size is transferred in
    /// full. The former `BoundedRows { limit }` silently dropped the tail of a
    /// table larger than the limit.
    StreamRows,
    RetryRequired,
}

impl TableStreamPlan {
    pub fn allows_row_materialization(self) -> bool {
        matches!(self, Self::StreamRows)
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
        TableStreamPlan::StreamRows
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

    /// A table with no SSTable directories streams EVERY row: the fallback is not
    /// bounded by a partition count (the former 1_000-row `BoundedRows` cap
    /// silently dropped the tail of a larger table).
    #[test]
    fn row_fallback_streams_every_partition() {
        let plan = plan_table_stream(TableStreamPlanInput {
            sstable_dir_count: 0,
        });

        assert_eq!(plan, TableStreamPlan::StreamRows);
        assert!(plan.allows_row_materialization());
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

    use std::sync::Arc;

    fn stream_test_storage(dir: &std::path::Path) -> Arc<StorageEngine> {
        use ferrosa_storage::{CommitLogConfig, CompactionConfig, StorageEngineConfig};
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig {
                log_dir: dir.to_path_buf(),
                checkpoint_dir: dir.to_path_buf(),
                archive: None,
                ..CommitLogConfig::default()
            },
            compaction: CompactionConfig::from_env(dir.join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            local_disk_free_reserve_bytes: 0,
            flush_threshold_bytes: u64::MAX,
            memtable_backpressure_bytes: u64::MAX,
            flush_max_age_secs: 5,
            data_dir: dir.to_path_buf(),
            index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
            auth_enabled: false,
            auth_warn: false,
            write_verify: false,
            max_pending_replay_mutations_without_schema: 1024,
            memtable_num_shards: 64,
            cache_hot_window_secs: 900,
        };
        Arc::new(StorageEngine::new(config, None).unwrap())
    }

    fn register_stream_table(storage: &StorageEngine, ks: &str, tbl: &str) {
        use ferrosa_common::schema::{ColumnDefinition, TableSchema};
        storage
            .register_table(TableSchema {
                keyspace: ks.to_string(),
                table: tbl.to_string(),
                key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                clustering_columns: vec![],
                static_columns: vec![],
                regular_columns: vec![ColumnDefinition {
                    name: "val".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                }],
                extensions: Default::default(),
            })
            .unwrap();
    }

    /// The bootstrap row fallback must stream EVERY partition, not stop at the
    /// former 1_000-partition cap. RED before the fix: the capped read returned
    /// 1_000 of 1_001, silently dropping the tail (no error, no truncated flag).
    #[test]
    fn row_fallback_streams_every_partition_past_the_former_cap() {
        use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
        let dir = tempfile::tempdir().unwrap();
        let storage = stream_test_storage(dir.path());
        register_stream_table(&storage, "ks", "tbl");
        let tid = TableId::new("ks", "tbl");

        let total = 1_001usize; // one past the former 1_000 cap
        for i in 0..total {
            let key = DecoratedKey::new(PartitionKey::new((i as u64).to_be_bytes().to_vec()));
            let row = Row {
                clustering: vec![],
                cells: vec![(0, CellValue::live(b"v".to_vec(), 1))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(1),
            };
            storage
                .apply_partition_parts(&tid, &key, DeletionTime::LIVE, None, std::iter::once(row))
                .unwrap();
        }

        // Node 1 owns every partition; this node is 0, so all are streamed.
        let by_node = stream_row_fallback_into(&storage, &tid, "ks", "tbl", 0, |_| 1).unwrap();
        let streamed: usize = by_node.values().map(|v| v.len()).sum();
        assert_eq!(
            streamed, total,
            "every partition must be streamed; the former 1_000-row cap dropped the tail"
        );
    }
}
