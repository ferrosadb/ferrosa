//! Compaction validator: deterministic data run through compaction and checked
//! against an independent oracle and across strategies.
//!
//! Ported in spirit from the Cassandra `compaction-validator` differential tool
//! (the Java original is pipeline-specific and is not vendored). Compiled only
//! under `cfg(test)` or the `compaction-validator` feature, so it never enters
//! the production library; the feature lets `ferrosa-loadgen` reuse the same
//! oracle/auditor for soak testing.

pub mod auditor;
pub mod driver;
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

    /// When a write and a delete carry the same timestamp, the write is favored
    /// and the data is preserved (Ferrosa's chosen equal-timestamp reconciliation).
    #[test]
    fn write_wins_over_tombstone_on_equal_timestamp() {
        let write = partition(vec![(0, CellValue::live(b"v".to_vec(), 7))], 7);
        let del = partition(vec![(0, CellValue::tombstone(7, 100))], 7);

        let oracle = oracle_merge(&[write.clone(), del.clone()]);
        let ab = merge_partitions(vec![write.clone(), del.clone()]);
        let ba = merge_partitions(vec![del, write]);

        assert_eq!(ab, ba, "merge must be order-independent");
        assert_eq!(ab, oracle, "merge must match the oracle");
        assert!(
            ab.rows[0].cells[0].1.value.is_some(),
            "the write must survive the equal-timestamp tie, got a tombstone"
        );
    }
}

#[cfg(test)]
mod prop_tests {
    use super::auditor::audit_partition;
    use super::oracle::oracle_merge;
    use crate::merge::merge_partitions;
    use ferrosa_common::key::{DecoratedKey, PartitionKey};
    use ferrosa_common::CellValue;
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};
    use proptest::prelude::*;
    use std::collections::BTreeMap;

    // A live cell or a tombstone, at a small timestamp to force conflicts.
    fn arb_cell() -> impl Strategy<Value = CellValue> {
        prop_oneof![
            (0u8..3, 1i64..4).prop_map(|(v, ts)| CellValue::live(vec![v], ts)),
            (1i64..4, 50i32..150).prop_map(|(ts, ldt)| CellValue::tombstone(ts, ldt)),
        ]
    }

    // A row keyed by a small clustering, with a pk timestamp, an optional row
    // deletion, and cells deduplicated by column (a row holds one cell/column).
    fn arb_row() -> impl Strategy<Value = Row> {
        (
            0u8..2,
            1i64..4,
            prop::option::of(1i64..4),
            prop::collection::vec((0u16..3, arb_cell()), 0..3),
        )
            .prop_map(|(clustering, pk_ts, row_del, cells)| {
                let mut by_col: BTreeMap<u16, CellValue> = BTreeMap::new();
                for (col, cell) in cells {
                    by_col.insert(col, cell);
                }
                Row {
                    clustering: vec![b'c', clustering],
                    cells: by_col.into_iter().collect(),
                    deletion: row_del
                        .map_or(DeletionTime::LIVE, |t| DeletionTime::new(t, t as u32)),
                    primary_key_liveness: LivenessInfo::with_timestamp(pk_ts),
                }
            })
    }

    // One source version of the shared partition (rows deduped by clustering).
    fn arb_partition() -> impl Strategy<Value = Partition> {
        (
            prop::option::of(1i64..4),
            prop::collection::vec(arb_row(), 0..3),
        )
            .prop_map(|(part_del, rows)| {
                let mut by_clustering: BTreeMap<Vec<u8>, Row> = BTreeMap::new();
                for row in rows {
                    by_clustering.insert(row.clustering.clone(), row);
                }
                Partition {
                    key: DecoratedKey::new(PartitionKey::new(b"k".to_vec())),
                    deletion: part_del
                        .map_or(DeletionTime::LIVE, |t| DeletionTime::new(t, t as u32)),
                    static_row: None,
                    rows: by_clustering.into_values().collect(),
                }
            })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// Across many random multi-SSTable partition sets, the read-path merge
        /// must be order-independent and agree with the independent oracle.
        #[test]
        fn merge_is_order_independent_and_matches_oracle(
            sources in prop::collection::vec(arb_partition(), 1..4)
        ) {
            let oracle = oracle_merge(&sources);
            let forward = merge_partitions(sources.clone());
            let mut reversed = sources.clone();
            reversed.reverse();
            let backward = merge_partitions(reversed);

            prop_assert_eq!(&forward, &backward, "merge must be order-independent");
            prop_assert_eq!(&forward, &oracle, "merge must match the oracle");

            // The merged output (and the oracle) must satisfy format invariants.
            prop_assert!(
                audit_partition(&forward).is_empty(),
                "merge output violated format invariants: {:?}",
                audit_partition(&forward)
            );
            prop_assert!(
                audit_partition(&oracle).is_empty(),
                "oracle output violated format invariants: {:?}",
                audit_partition(&oracle)
            );
        }
    }
}
