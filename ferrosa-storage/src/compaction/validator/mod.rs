//! Compaction validator: deterministic data run through compaction and checked
//! against an independent oracle and across strategies.
//!
//! Ported in spirit from the Cassandra `compaction-validator` differential tool
//! (the Java original is pipeline-specific and is not vendored). Compiled only
//! under `cfg(test)` or the `compaction-validator` feature, so it never enters
//! the production library; the feature lets `ferrosa-loadgen` reuse the same
//! oracle/auditor for soak testing.

pub mod oracle;

#[cfg(test)]
mod tests {
    use super::oracle::oracle_merge;
    use crate::merge::merge_partitions;
    use ferrosa_common::key::{DecoratedKey, PartitionKey};
    use ferrosa_common::CellValue;
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};

    fn partition(cells: Vec<(u16, CellValue)>, ts: i64) -> Partition {
        Partition {
            key: DecoratedKey::new(PartitionKey::new(b"k".to_vec())),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![Row {
                clustering: b"c".to_vec(),
                cells,
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(ts),
            }],
        }
    }

    /// The read-path merge must agree with the oracle and be order-independent
    /// when two sources write the same column at the same timestamp. This is the
    /// "inverted timestamp tie-breaker" errata class: last-write-wins is only
    /// well defined if equal-timestamp conflicts converge regardless of the
    /// order SSTables are grouped (which differs between STCS and UCS).
    #[test]
    fn merge_matches_oracle_on_equal_timestamp_conflict() {
        let a = partition(vec![(0, CellValue::live(b"aaa".to_vec(), 5))], 5);
        let b = partition(vec![(0, CellValue::live(b"bbb".to_vec(), 5))], 5);

        let oracle = oracle_merge(&[a.clone(), b.clone()]);
        let ab = merge_partitions(vec![a.clone(), b.clone()]);
        let ba = merge_partitions(vec![b, a]);

        assert_eq!(ab, ba, "merge_partitions must be order-independent");
        assert_eq!(ab, oracle, "merge_partitions must match the oracle");
    }
}
