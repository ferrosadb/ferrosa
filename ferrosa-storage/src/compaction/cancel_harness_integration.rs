//! Proves the T-020 harness itself: the `cancel_point!` hook actually fires
//! at every documented step of a real, uncancelled compaction, and the
//! oracle/invariant checker agree with each other on a clean table.
//! `test-specification.md` L10: "hook fires at every point in a normal
//! compaction (first pass); oracle + invariant checker self-tests on a
//! clean table."
//!
//! This is deliberately the *recording* half of the harness — no process is
//! crashed here. `cancel_crash_sweep_*` (in `cancel_crash_sweep_tests`) is
//! the crash-twin half (CS2).

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use ferrosa_common::cell::CellValue;
    use ferrosa_common::key::{DecoratedKey, PartitionKey};
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

    use crate::compaction::cancel_harness::{CancelHookGuard, CancelPoint};
    use crate::compaction::cancel_oracle::{assert_cancel_invariants, WriteOracle};
    use crate::engine::{StorageEngine, StorageEngineConfig};
    use crate::TableId;

    fn test_schema(table: &str, compressed: bool) -> TableSchema {
        let mut extensions = std::collections::HashMap::new();
        if !compressed {
            extensions.insert("compression.class".to_string(), "none".to_string());
        }
        TableSchema {
            keyspace: "ks".to_string(),
            table: table.to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "ck".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "val".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions,
        }
    }

    fn key(s: &str) -> DecoratedKey {
        DecoratedKey::new(PartitionKey::new(s.as_bytes().to_vec()))
    }

    fn row(value: &[u8], ts: i64) -> Row {
        Row {
            clustering: 1i32.to_be_bytes().to_vec(),
            cells: vec![(0, CellValue::live(value.to_vec(), ts))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(ts),
        }
    }

    /// Writes 2 SSTables of 2 partitions each (4 total) on a dedicated table
    /// named `table`, submits a compaction merging all 4, and drives it to
    /// completion while recording every `CancelPoint` reached for that
    /// table. Returns the recorded sequence and the oracle of every
    /// acknowledged write.
    ///
    /// `table` must be unique per call: hooks are scoped by table id
    /// (`cancel_harness`'s module docs), and cargo runs tests concurrently —
    /// two calls sharing a table name would record each other's points.
    async fn run_full_compaction_recording_points(
        table: &str,
        compressed: bool,
    ) -> (
        Vec<CancelPoint>,
        WriteOracle,
        StorageEngine,
        TableId,
        tempfile::TempDir,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let engine = StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None)
            .expect("engine open");
        let tid = TableId::new("ks", table);
        engine
            .register_table(test_schema(table, compressed))
            .expect("register table");

        let mut oracle = WriteOracle::new();
        for (gen, keys) in [["p1", "p2"], ["p3", "p4"]].into_iter().enumerate() {
            for (i, k) in keys.into_iter().enumerate() {
                let ts = 1000 + (gen * 10 + i) as i64;
                let value = format!("v-{k}").into_bytes();
                engine
                    .write(&tid, &key(k), row(&value, ts), ts)
                    .expect("write");
                oracle.record_write(&key(k), &1i32.to_be_bytes(), &value, ts);
            }
            engine.flush(&tid).expect("flush");
        }
        assert_eq!(engine.sstable_count(&tid), 2, "expected 2 flushed SSTables");

        let recorded: Arc<Mutex<Vec<CancelPoint>>> = Arc::new(Mutex::new(Vec::new()));
        let recorded_for_hook = Arc::clone(&recorded);
        let scope = tid.to_string();
        let guard = CancelHookGuard::install(
            scope,
            Arc::new(move |p| {
                recorded_for_hook.lock().unwrap().push(p);
            }),
        );

        // Submits a compaction task for every table with >=2 SSTables,
        // ignoring size/threshold bucketing — exactly the 2 inputs this
        // scenario just flushed.
        engine.force_compact_all();

        let mut done = false;
        for _ in 0..500 {
            engine.poll_compactions().await;
            if engine.sstable_count(&tid) == 1 {
                done = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(done, "compaction did not converge to 1 SSTable in time");
        drop(guard);

        let points = recorded.lock().unwrap().clone();
        (points, oracle, engine, tid, dir)
    }

    #[tokio::test]
    async fn cancel_harness_fires_at_every_point_in_normal_compaction_compressed() {
        let (points, ..) =
            run_full_compaction_recording_points("cancel_harness_compressed", true).await;
        assert_points_cover_every_no_s3_step(&points);
    }

    #[tokio::test]
    async fn cancel_harness_fires_at_every_point_in_normal_compaction_uncompressed() {
        let (points, ..) =
            run_full_compaction_recording_points("cancel_harness_uncompressed", false).await;
        assert_points_cover_every_no_s3_step(&points);
    }

    /// Every point on the executor + finalize path that a no-S3 compaction
    /// must reach exactly because it ran to completion. S3 points
    /// (`S3PendingLog`/`S3Upload`/`S3ManifestCas`/`S3Delete`) are excluded:
    /// this scenario has no object store configured, so those call sites
    /// are never reached, by design (`poll_compactions` returns early on
    /// `upload_mgr.is_none()`).
    fn assert_points_cover_every_no_s3_step(points: &[CancelPoint]) {
        let seen: HashSet<CancelPoint> = points.iter().copied().collect();
        for expected in [
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
            CancelPoint::RetireInput(1),
        ] {
            assert!(
                seen.contains(&expected),
                "expected {expected} in recorded points, got {points:?}"
            );
        }
        // InputOpen fires once per input (2 inputs).
        assert_eq!(
            points
                .iter()
                .filter(|p| **p == CancelPoint::InputOpen)
                .count(),
            2
        );
        // First fires exactly once; the loop merges 4 distinct partitions,
        // so Middle fires for the remaining 3 and Last fires once after the
        // loop.
        assert_eq!(
            points
                .iter()
                .filter(|p| **p == CancelPoint::MergePartitionFirst)
                .count(),
            1
        );
        assert_eq!(
            points
                .iter()
                .filter(|p| **p == CancelPoint::MergePartitionLast)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn cancel_harness_oracle_and_invariants_hold_on_a_clean_flushed_table() {
        let dir = tempfile::tempdir().unwrap();
        let engine = StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None)
            .expect("engine open");
        let tid = TableId::new("ks", "cancel_harness_clean");
        engine
            .register_table(test_schema("cancel_harness_clean", true))
            .unwrap();

        let mut oracle = WriteOracle::new();
        let k = key("clean");
        engine.write(&tid, &k, row(b"v1", 1000), 1000).unwrap();
        oracle.record_write(&k, &1i32.to_be_bytes(), b"v1", 1000);
        engine.flush(&tid).unwrap();

        let input_gens = StorageEngine::list_generations_in_dir(&engine.table_sstable_dir(&tid));
        assert_eq!(input_gens.len(), 1);
        let report = assert_cancel_invariants(&engine, &tid, &oracle, &input_gens);
        report.assert_all();
    }
}
