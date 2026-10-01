//! `cancel_token_*` tests (T-021): real cancellation via `CancelToken`.
//!
//! - **CS1** — rollback at every honoured checkpoint (`InputOpen` through
//!   `BeforePromote`): cancelling returns `Err`, leaves the original inputs
//!   live and untouched, and leaks nothing (I1, I2, I4 hold, including after
//!   a reopen).
//! - **CS3** — cancel latency is bounded, not proportional to whatever
//!   remained of the merge.
//! - **CS4** — `CompactionExecutor::shutdown()` cancels a task stuck
//!   mid-merge promptly, rather than waiting for it to finish.
//! - **CS14** — folded into each CS1 case (I5 liveness): re-compacting the
//!   same inputs after a cancel succeeds with identical content.
//! - **CD1** — a worker parked on an empty task channel exits shutdown
//!   immediately, not on a poll interval.
//!
//! Reuses the fixture shape `cancel_harness_integration.rs` established
//! (a dedicated table per case, `force_compact_all` + `poll_compactions`)
//! and the invariant checker from `cancel_oracle.rs`.

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use ferrosa_common::cell::CellValue;
    use ferrosa_common::key::{DecoratedKey, PartitionKey};
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
    use ferrosa_common::CancelReason;
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

    use crate::compaction::cancel_harness::{cancel_now, CancelHookGuard, CancelPoint};
    use crate::compaction::cancel_oracle::{assert_cancel_invariants, WriteOracle};
    use crate::compaction::executor::CompactionExecutor;
    use crate::engine::{StorageEngine, StorageEngineConfig};
    use crate::TableId;

    fn test_schema(table: &str) -> TableSchema {
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
            extensions: Default::default(),
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

    /// Writes `partitions_per_input` partitions to each of two flushed
    /// SSTables. Returns the oracle of every acknowledged write and the
    /// resulting input generations.
    fn seed_two_inputs(
        engine: &StorageEngine,
        tid: &TableId,
        partitions_per_input: usize,
    ) -> (WriteOracle, Vec<u64>) {
        let mut oracle = WriteOracle::new();
        let mut ts = 1000i64;
        for input in 0..2 {
            for p in 0..partitions_per_input {
                let k = format!("p{input}-{p}");
                let value = format!("v-{k}").into_bytes();
                engine
                    .write(tid, &key(&k), row(&value, ts), ts)
                    .expect("write");
                oracle.record_write(&key(&k), &1i32.to_be_bytes(), &value, ts);
                ts += 1;
            }
            engine.flush(tid).expect("flush");
        }
        let input_gens = StorageEngine::list_generations_in_dir(&engine.table_sstable_dir(tid));
        assert_eq!(input_gens.len(), 2, "expected 2 flushed input SSTables");
        (oracle, input_gens)
    }

    /// Upper bound on a compaction that never settles. A hang guard that turns
    /// a stuck worker into a loud failure, not a convergence budget: settling
    /// is awaited on the executor's own state transitions, so how slow the host
    /// is never decides whether these tests pass.
    const SETTLE_HANG_GUARD: Duration = Duration::from_secs(120);

    /// CS1 + CS14 for one checkpoint: installs a hook that cancels exactly
    /// at `point`, drives the task to its (cancelled) conclusion, and
    /// asserts I1/I2 hold immediately (no restart needed — cancelling is
    /// free up to `BeforePromote`), I1-I4 hold again after a reopen (the
    /// same restart proof `cancel_crash_sweep_tests` uses), and — CS14 — a
    /// fresh compaction of the same inputs afterward converges to identical
    /// content, proving the claim was released (I5).
    async fn assert_cancel_at_point_rolls_back_and_allows_recompaction(
        table: &str,
        point: CancelPoint,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let engine = StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None)
            .expect("engine open");
        let tid = TableId::new("ks", table);
        engine
            .register_table(test_schema(table))
            .expect("register table");
        let (oracle, input_gens) = seed_two_inputs(&engine, &tid, 2);

        let scope = tid.to_string();
        let cancel_scope = scope.clone();
        let guard = CancelHookGuard::install(
            scope,
            Arc::new(move |p| {
                if p == point {
                    cancel_now(&cancel_scope, CancelReason::Operator);
                }
            }),
        );

        engine.force_compact_all();
        engine
            .drive_compactions_until_idle(&tid, SETTLE_HANG_GUARD)
            .await;
        // The hook must not fire again during the CS14 re-compaction below,
        // or that compaction would be cancelled too and never converge.
        drop(guard);

        // I1/I2, no restart: cancelling anywhere up to and including
        // `BeforePromote` is free (compaction-cancel-safety.md C2) — the
        // inputs were never touched and nothing was published.
        let observed: HashSet<u64> =
            StorageEngine::list_generations_in_dir(&engine.table_sstable_dir(&tid))
                .into_iter()
                .collect();
        let expected: HashSet<u64> = input_gens.iter().copied().collect();
        assert_eq!(
            observed, expected,
            "cancel at {point} must leave exactly the original inputs live, got {observed:?}"
        );
        let mismatches = oracle.diff_against_engine(&engine, &tid);
        assert!(mismatches.is_empty(), "cancel at {point}: {mismatches:?}");

        // Reopen: proves I1-I4 hold after startup reconciliation too, and
        // that whatever this cancel staged (if anything) is gone — either
        // this checkpoint's own proactive cleanup already removed it, or
        // the startup sweep of `compaction/` did.
        drop(engine);
        let reopened =
            StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None).expect("reopen");
        let report = assert_cancel_invariants(&reopened, &tid, &oracle, &input_gens);
        report.assert_all();

        // CS14 / I5 liveness: the claim was released, so the same inputs
        // compact again and converge to identical content.
        reopened.force_compact_all();
        reopened
            .drive_compactions_until_idle(&tid, SETTLE_HANG_GUARD)
            .await;
        assert_eq!(
            reopened.sstable_count(&tid),
            1,
            "re-compaction after cancel at {point} did not converge"
        );
        let mismatches = oracle.diff_against_engine(&reopened, &tid);
        assert!(
            mismatches.is_empty(),
            "re-compaction after cancel at {point} changed content: {mismatches:?}"
        );
    }

    /// ST-57: a re-compaction whose worker is slower than any fixed poll
    /// budget (a loaded host: fsync-bound merge, descheduled worker thread)
    /// still converges, because the test waits on the executor settling, not
    /// on a count of 10 ms polls. The hook stalls the worker once at its first
    /// checkpoint, well past the old 500 x 10 ms poll budget.
    #[cfg(feature = "slow-tests")]
    mod slow {
        use super::*;

        const STALL: Duration = Duration::from_secs(12);

        #[tokio::test]
        async fn cancel_token_recompaction_converges_with_stalled_worker() {
            let dir = tempfile::tempdir().unwrap();
            let engine = StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None)
                .expect("engine open");
            let tid = TableId::new("ks", "cancel_token_stalled_worker");
            engine
                .register_table(test_schema("cancel_token_stalled_worker"))
                .expect("register table");
            let (oracle, _input_gens) = seed_two_inputs(&engine, &tid, 2);

            let stalled = Arc::new(AtomicBool::new(false));
            let hook_stalled = Arc::clone(&stalled);
            let _guard = CancelHookGuard::install(
                tid.to_string(),
                Arc::new(move |p| {
                    if p == CancelPoint::InputOpen && !hook_stalled.swap(true, Ordering::SeqCst) {
                        std::thread::sleep(STALL);
                    }
                }),
            );

            engine.force_compact_all();
            engine
                .drive_compactions_until_idle(&tid, SETTLE_HANG_GUARD)
                .await;

            assert!(
                stalled.load(Ordering::SeqCst),
                "the stall hook never fired; the test did not exercise a slow worker"
            );
            assert_eq!(
                engine.sstable_count(&tid),
                1,
                "compaction with a stalled worker did not converge"
            );
            let mismatches = oracle.diff_against_engine(&engine, &tid);
            assert!(mismatches.is_empty(), "content changed: {mismatches:?}");
        }
    }

    #[tokio::test]
    async fn cancel_token_rolls_back_at_input_open() {
        assert_cancel_at_point_rolls_back_and_allows_recompaction(
            "cancel_token_input_open",
            CancelPoint::InputOpen,
        )
        .await;
    }

    #[tokio::test]
    async fn cancel_token_rolls_back_at_merge_partition_first() {
        assert_cancel_at_point_rolls_back_and_allows_recompaction(
            "cancel_token_merge_first",
            CancelPoint::MergePartitionFirst,
        )
        .await;
    }

    #[tokio::test]
    async fn cancel_token_rolls_back_at_merge_partition_middle() {
        assert_cancel_at_point_rolls_back_and_allows_recompaction(
            "cancel_token_merge_middle",
            CancelPoint::MergePartitionMiddle,
        )
        .await;
    }

    #[tokio::test]
    async fn cancel_token_rolls_back_at_merge_partition_last() {
        assert_cancel_at_point_rolls_back_and_allows_recompaction(
            "cancel_token_merge_last",
            CancelPoint::MergePartitionLast,
        )
        .await;
    }

    #[tokio::test]
    async fn cancel_token_rolls_back_at_before_finish() {
        assert_cancel_at_point_rolls_back_and_allows_recompaction(
            "cancel_token_before_finish",
            CancelPoint::BeforeFinish,
        )
        .await;
    }

    #[tokio::test]
    async fn cancel_token_rolls_back_at_before_flush_files() {
        assert_cancel_at_point_rolls_back_and_allows_recompaction(
            "cancel_token_before_flush_files",
            CancelPoint::BeforeFlushFiles,
        )
        .await;
    }

    #[tokio::test]
    async fn cancel_token_rolls_back_at_verify_partition() {
        assert_cancel_at_point_rolls_back_and_allows_recompaction(
            "cancel_token_verify_partition",
            CancelPoint::VerifyPartition,
        )
        .await;
    }

    #[tokio::test]
    async fn cancel_token_rolls_back_at_before_promote() {
        assert_cancel_at_point_rolls_back_and_allows_recompaction(
            "cancel_token_before_promote",
            CancelPoint::BeforePromote,
        )
        .await;
    }

    /// CS3: cancelling returns within a small, bounded latency, and the
    /// executor's own `compaction_cancel_latency_seconds` observation
    /// reflects it (measured end to end: `cancel()` call to the task
    /// returning `Err`, as `compaction-cancel-safety.md` C1 specifies).
    #[tokio::test]
    async fn cancel_token_latency_is_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let engine = StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None)
            .expect("engine open");
        let tid = TableId::new("ks", "cancel_token_latency");
        engine
            .register_table(test_schema("cancel_token_latency"))
            .expect("register table");
        // Wide enough that the merge loop has several checkpoints to cancel
        // between, not just one.
        let (_oracle, _input_gens) = seed_two_inputs(&engine, &tid, 25);

        let scope = tid.to_string();
        let cancel_scope = scope.clone();
        let _guard = CancelHookGuard::install(
            scope,
            Arc::new(move |p| {
                if p == CancelPoint::MergePartitionFirst {
                    cancel_now(&cancel_scope, CancelReason::Operator);
                }
            }),
        );

        let before_count = crate::metrics::compaction_cancel_latency_count();
        let start = Instant::now();
        engine.force_compact_all();
        // Poll until the cancel is observed (not a fixed-duration drive
        // loop, which would measure the loop's own budget rather than the
        // actual cancel-to-return latency).
        let mut observed = false;
        for _ in 0..300 {
            engine.poll_compactions().await;
            if crate::metrics::compaction_cancel_latency_count() > before_count {
                observed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let elapsed = start.elapsed();

        assert!(
            observed,
            "expected a compaction_cancel_latency_seconds observation within the drive loop"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "cancel-to-return latency was {elapsed:?}; expected well under 2s for a \
             25-partition-per-input merge cancelled at the first partition"
        );
    }

    /// CS4: `CompactionExecutor::shutdown()` cancels a task stuck mid-merge
    /// promptly instead of waiting for it to finish. The hook sleeps on
    /// every partition so the uncancelled merge would take on the order of
    /// two seconds; shutdown must return in a small fraction of that.
    #[tokio::test]
    async fn cancel_token_shutdown_mid_merge_completes_promptly_and_invariants_hold() {
        let dir = tempfile::tempdir().unwrap();
        let engine = StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None)
            .expect("engine open");
        let tid = TableId::new("ks", "cancel_token_shutdown");
        engine
            .register_table(test_schema("cancel_token_shutdown"))
            .expect("register table");
        let (oracle, input_gens) = seed_two_inputs(&engine, &tid, 40);

        let scope = tid.to_string();
        let reached_merge = Arc::new(AtomicBool::new(false));
        let reached_merge_for_hook = Arc::clone(&reached_merge);
        let _guard = CancelHookGuard::install(
            scope,
            Arc::new(move |p| {
                if matches!(
                    p,
                    CancelPoint::MergePartitionFirst | CancelPoint::MergePartitionMiddle
                ) {
                    reached_merge_for_hook.store(true, Ordering::SeqCst);
                    // Slows the merge to roughly 40 * 50ms = 2s if never
                    // cancelled, giving `shutdown()` a wide window to
                    // interrupt it well before it would finish on its own.
                    std::thread::sleep(Duration::from_millis(50));
                }
            }),
        );

        engine.force_compact_all();
        for _ in 0..200 {
            if reached_merge.load(Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            reached_merge.load(Ordering::SeqCst),
            "compaction should have reached the merge loop before shutdown"
        );

        let start = Instant::now();
        engine.compaction_executor_for_test().shutdown();
        let elapsed = start.elapsed();
        drop(_guard);

        assert!(
            elapsed < Duration::from_secs(1),
            "shutdown mid-merge took {elapsed:?}; expected it to cancel the task rather \
             than wait out the ~2s uncancelled merge"
        );

        // Nothing was promoted; inputs untouched (I2), without a restart.
        let observed: HashSet<u64> =
            StorageEngine::list_generations_in_dir(&engine.table_sstable_dir(&tid))
                .into_iter()
                .collect();
        let expected: HashSet<u64> = input_gens.iter().copied().collect();
        assert_eq!(
            observed, expected,
            "shutdown mid-merge must leave exactly the original inputs live, got {observed:?}"
        );

        // I4 (no leaks) is checked after a reopen: `compaction/<table>`
        // exists the instant a merge is staged at all (its parent directory
        // is created as a side effect of allocating the staging dir), so its
        // mere presence is not itself a leak until startup's sweep has had a
        // chance to run — the same convention `cancel_oracle`'s own doc
        // comment and the CS1 cases above use.
        drop(engine);
        let reopened =
            StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None).expect("reopen");
        let report = assert_cancel_invariants(&reopened, &tid, &oracle, &input_gens);
        report.assert_all();
    }

    /// CD1: a worker parked on an empty task channel (no compaction ever
    /// submitted) exits shutdown immediately. It is woken by the shutdown
    /// channel closing, not on a poll interval — the old
    /// `recv_timeout(100ms)` design would let this take up to 100ms.
    #[test]
    fn cancel_token_idle_shutdown_is_immediate_not_on_a_poll_interval() {
        let executor = CompactionExecutor::new();
        let start = Instant::now();
        executor.shutdown();
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(50),
            "shutdown of idle workers took {elapsed:?}; a poll-interval design \
             would take on the order of 100ms"
        );
    }
}
