//! CS2 — the compaction cancel-safety crash sweep (`test-specification.md`
//! L10), and the first real use of the T-020 harness.
//!
//! Each test is a **crash twin**: the same test binary re-execs itself as a
//! child process (selected by test name, matching the pattern documented in
//! `test-specification.md`'s harness section), the child installs a
//! [`CancelHookGuard`] that calls `std::process::abort()` (SIGABRT) the
//! instant its target `CancelPoint` is reached, drives a real compaction
//! into it, and the parent asserts the child actually died by signal (not by
//! panicking, timing out, or completing normally — any of those would mean
//! the point was never reached, which is a harness bug, not a pass). The
//! parent then reopens a fresh [`StorageEngine`] on the same data dir
//! (today's only form of "restart"; startup now runs the T-023 startup
//! reconciliation described in `compaction-cancel-safety.md` C3) and checks
//! I1-I4 with [`assert_cancel_invariants`].
//!
//! Unix-only: signal-based crash detection has no Windows equivalent, and
//! nothing in this workspace targets Windows (macOS-only deps already exist
//! in this crate's `Cargo.toml`).
#![cfg(unix)]

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::os::unix::process::ExitStatusExt;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::Duration;

    use ferrosa_common::cell::CellValue;
    use ferrosa_common::key::{DecoratedKey, PartitionKey};
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

    use crate::compaction::cancel_harness::{CancelHookGuard, CancelPoint};
    use crate::compaction::cancel_oracle::{assert_cancel_invariants, WriteOracle};
    use crate::engine::{StorageEngine, StorageEngineConfig};
    use crate::TableId;

    const SIGABRT: i32 = 6;

    fn table_id() -> TableId {
        TableId::new("ks", "cancel_crash_sweep")
    }

    fn test_schema(compressed: bool) -> TableSchema {
        let mut extensions = std::collections::HashMap::new();
        if !compressed {
            extensions.insert("compression.class".to_string(), "none".to_string());
        }
        TableSchema {
            keyspace: "ks".to_string(),
            table: "cancel_crash_sweep".to_string(),
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

    /// Writes 2 generations (2 keys each) and shuts the engine down cleanly
    /// so the crash-twin child can safely reopen the same data dir. Returns
    /// the oracle of every acknowledged write and the input generation ids.
    fn build_baseline(dir: &Path, compressed: bool) -> (WriteOracle, Vec<u64>) {
        let engine =
            StorageEngine::new(StorageEngineConfig::test_config(dir), None).expect("engine open");
        let tid = table_id();
        engine
            .register_table(test_schema(compressed))
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
        let input_gens = StorageEngine::list_generations_in_dir(&engine.table_sstable_dir(&tid));
        assert_eq!(
            input_gens.len(),
            2,
            "expected exactly 2 baseline generations"
        );
        engine
            .shutdown()
            .expect("clean shutdown before crash-twin child reopens");
        (oracle, input_gens)
    }

    /// The child-process half: installs the abort hook, reopens the engine,
    /// drives a real compaction, and panics (a loud, ordinary test failure —
    /// not a crash) if the target point is never reached. Never returns
    /// normally on the success path: either `std::process::abort()` fires
    /// from inside the hook, or the loop finishes and this panics.
    fn run_child(point: CancelPoint, compressed: bool, dir: &Path) {
        let tid = table_id();
        // The scope only needs to be unique within this process; each
        // crash-twin case runs in its own freshly spawned child process, so
        // there is no cross-test hook collision to worry about here (unlike
        // the in-process `cancel_harness_integration` tests).
        let _guard = CancelHookGuard::install(
            tid.to_string(),
            Arc::new(move |p| {
                if p == point {
                    std::process::abort();
                }
            }),
        );

        let engine = StorageEngine::new(StorageEngineConfig::test_config(dir), None)
            .expect("engine reopen in crash-twin child");
        engine
            .register_table(test_schema(compressed))
            .expect("register table in crash-twin child");
        engine.force_compact_all();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build child tokio runtime");
        rt.block_on(async {
            for _ in 0..500 {
                engine.poll_compactions().await;
                if engine.sstable_count(&tid) == 1 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });

        panic!(
            "crash-twin harness bug: cancel point {point} was never reached (the compaction \
             ran to completion, or stalled, without the hook firing)"
        );
    }

    /// What a crash-twin case must observe once the driver reopens the
    /// engine (i.e. after T-023 startup reconciliation has had a chance to
    /// run).
    #[derive(Clone, Copy)]
    enum ExpectedOutcome {
        /// Crashed strictly before the T-022 commit point
        /// (`compaction-cancel-safety.md` C2): reconciliation finds no
        /// output to roll forward onto, so the inputs are always left
        /// untouched. `assert_all()` covers this fully — there is only one
        /// possible outcome, so the report's generic "exactly one" check
        /// already pins it down.
        CleanRollback,
        /// Crashed at or after the T-022 commit, at a point where the
        /// output is already promoted and digest-verified on disk. T-023's
        /// C3 table (`Promoting`/`Swapped`, output present, digest matches)
        /// always rolls forward from here, so this asserts that specific
        /// outcome instead of "one of the two": every input generation must
        /// be gone and a disjoint, non-empty output generation must be the
        /// only thing left. Closes windows C and D (T-022/T-023) and the
        /// `AfterPromote` sub-window of C (T-022's generation-choice
        /// reservation, forge t_cb6fa288), and closes window E at the
        /// per-generation granularity this harness exercises (T-023's
        /// blanket idempotent retirement retry).
        RolledForward,
    }

    /// Drives one crash-twin case. `test_name` must be this test's own
    /// (unique, crate-wide) function name — the crash-twin child re-execs
    /// the same test binary filtered by this name (substring match; unique
    /// names never collide with `--exact` semantics needed).
    fn run_crash_sweep_case(
        test_name: &str,
        point: CancelPoint,
        compressed: bool,
        expected: ExpectedOutcome,
    ) {
        if std::env::var("FERROSA_CANCEL_CRASH_ROLE").as_deref() == Ok("child") {
            let dir = PathBuf::from(
                std::env::var("FERROSA_CANCEL_CRASH_DIR")
                    .expect("FERROSA_CANCEL_CRASH_DIR must be set for the crash-twin child"),
            );
            run_child(point, compressed, &dir);
            return;
        }

        // ---- driver ----
        let dir = tempfile::tempdir().expect("tempdir");
        let (oracle, input_gens) = build_baseline(dir.path(), compressed);

        let exe = std::env::current_exe().expect("current_exe");
        let status = std::process::Command::new(&exe)
            .arg(test_name)
            .env("FERROSA_CANCEL_CRASH_ROLE", "child")
            .env("FERROSA_CANCEL_CRASH_DIR", dir.path())
            .status()
            .expect("spawn crash-twin child process");

        assert_eq!(
            status.signal(),
            Some(SIGABRT),
            "expected the crash-twin child to die of SIGABRT at {point}; got exit status {status:?} \
             (a non-signal exit means the point was never reached inside the child -- see its \
             stderr above)"
        );

        let engine = StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None)
            .expect("reopen after crash-twin");
        let tid = table_id();
        engine
            .register_table(test_schema(compressed))
            .expect("register table after crash-twin");
        let report = assert_cancel_invariants(&engine, &tid, &oracle, &input_gens);

        match expected {
            ExpectedOutcome::CleanRollback => report.assert_all(),
            ExpectedOutcome::RolledForward => {
                report.assert_i1_content_matches_oracle();
                report.assert_i3_no_corrupt_generation();
                report.assert_i4_no_leaks();
                let observed: HashSet<u64> =
                    StorageEngine::list_generations_in_dir(&engine.table_sstable_dir(&tid))
                        .into_iter()
                        .collect();
                let inputs: HashSet<u64> = input_gens.iter().copied().collect();
                assert!(
                    observed.is_disjoint(&inputs) && !observed.is_empty(),
                    "{point}: expected startup reconciliation to roll FORWARD onto the \
                     promoted output (compaction-cancel-safety.md C3: output present, digest \
                     matches at this point) -- inputs {inputs:?} should all have been \
                     retired and a disjoint, non-empty output generation should be the only \
                     thing left; observed {observed:?}"
                );
            }
        }
        let _ = engine.shutdown();
    }

    /// Generates one `#[test]` per (point, compression) crash-twin case.
    /// `clean` cases crash before the T-022 commit point and always roll
    /// back; `rolled_forward` cases crash at or after it and always roll
    /// forward onto the promoted output. There is no longer a gated
    /// "known-open-window" arm: T-022 (generation reservation, forge
    /// t_cb6fa288) plus T-023 (startup reconciliation) close every window
    /// this harness exercises. The remaining T-024 scope (per-component
    /// retirement atomicity) needs a finer-grained hook than this harness's
    /// per-generation `CancelPoint::RetireInput` provides -- see
    /// `ferrosa-storage/specs/roadmap.md`.
    macro_rules! crash_sweep_test {
        ($name:ident, $point:expr, $compressed:expr, clean) => {
            #[test]
            fn $name() {
                run_crash_sweep_case(
                    stringify!($name),
                    $point,
                    $compressed,
                    ExpectedOutcome::CleanRollback,
                );
            }
        };
        ($name:ident, $point:expr, $compressed:expr, rolled_forward) => {
            #[test]
            fn $name() {
                run_crash_sweep_case(
                    stringify!($name),
                    $point,
                    $compressed,
                    ExpectedOutcome::RolledForward,
                );
            }
        };
    }

    // ---- Green by default: crashes strictly before the C2 commit point
    // (`compaction-cancel-safety.md`). Everything up to and including
    // `BeforePromote` lives under `compaction/<table>/`, which today's
    // startup unconditionally wipes (`cleanup_stale_compaction_staging`),
    // so a crash there always rolls back to the untouched inputs. ----

    crash_sweep_test!(
        cancel_crash_sweep_input_open_compressed,
        CancelPoint::InputOpen,
        true,
        clean
    );
    crash_sweep_test!(
        cancel_crash_sweep_input_open_uncompressed,
        CancelPoint::InputOpen,
        false,
        clean
    );
    crash_sweep_test!(
        cancel_crash_sweep_merge_first_compressed,
        CancelPoint::MergePartitionFirst,
        true,
        clean
    );
    crash_sweep_test!(
        cancel_crash_sweep_merge_first_uncompressed,
        CancelPoint::MergePartitionFirst,
        false,
        clean
    );
    crash_sweep_test!(
        cancel_crash_sweep_merge_middle_compressed,
        CancelPoint::MergePartitionMiddle,
        true,
        clean
    );
    crash_sweep_test!(
        cancel_crash_sweep_merge_middle_uncompressed,
        CancelPoint::MergePartitionMiddle,
        false,
        clean
    );
    crash_sweep_test!(
        cancel_crash_sweep_merge_last_compressed,
        CancelPoint::MergePartitionLast,
        true,
        clean
    );
    crash_sweep_test!(
        cancel_crash_sweep_merge_last_uncompressed,
        CancelPoint::MergePartitionLast,
        false,
        clean
    );
    crash_sweep_test!(
        cancel_crash_sweep_before_finish_compressed,
        CancelPoint::BeforeFinish,
        true,
        clean
    );
    crash_sweep_test!(
        cancel_crash_sweep_before_finish_uncompressed,
        CancelPoint::BeforeFinish,
        false,
        clean
    );
    crash_sweep_test!(
        cancel_crash_sweep_before_flush_files_compressed,
        CancelPoint::BeforeFlushFiles,
        true,
        clean
    );
    crash_sweep_test!(
        cancel_crash_sweep_before_flush_files_uncompressed,
        CancelPoint::BeforeFlushFiles,
        false,
        clean
    );
    crash_sweep_test!(
        cancel_crash_sweep_verify_partition_compressed,
        CancelPoint::VerifyPartition,
        true,
        clean
    );
    crash_sweep_test!(
        cancel_crash_sweep_verify_partition_uncompressed,
        CancelPoint::VerifyPartition,
        false,
        clean
    );
    crash_sweep_test!(
        cancel_crash_sweep_before_promote_compressed,
        CancelPoint::BeforePromote,
        true,
        clean
    );
    crash_sweep_test!(
        cancel_crash_sweep_before_promote_uncompressed,
        CancelPoint::BeforePromote,
        false,
        clean
    );

    // ---- Windows C and D are fully closed by T-022/T-023 and run
    // unconditionally below, asserting the specific roll-forward outcome
    // those packets guarantee. No `known-open-window`-gated case remains in
    // this file (see the macro's doc comment for the remaining T-024
    // scope). ----

    // Window C (compaction-cancel-safety.md): the output is promoted (live
    // under sstables/<table>/) but the view has not swapped yet.
    //
    // `AfterPromote` used to reproduce this window even with T-022/T-023 in
    // place: `poll_compactions` fired this cancel point BEFORE it corrected
    // the intent record's `output_gen` from its pre-promotion placeholder
    // (the staged output's own id) to the actual promoted generation id --
    // `promote_compaction_output` was free to pick a different id to avoid
    // colliding with a concurrent flush, and routinely did in this
    // scenario. A crash in that gap left a durable, fsynced record whose
    // `output_gen` named a generation that no longer existed (it had been
    // renamed away), so `reconcile_one_compaction_intent` found "output
    // missing" and rolled BACK -- deleting the record and leaving both
    // inputs untouched -- while the real promoted output sat live on disk
    // under its true id, an orphan no record pointed to. Both inputs and
    // the output were then discoverable after restart: I2 violated, the
    // original window C shape. Closed by T-022 itself (forge t_cb6fa288):
    // `StorageEngine::reserve_compaction_promotion_target` now chooses and
    // reserves the final generation id BEFORE the intent record is written,
    // so the record's `output_gen` is correct from its first write and
    // `promote_compaction_output` only ever moves data into the id already
    // committed to disk -- there is nothing left to correct after
    // `AfterPromote` fires.
    crash_sweep_test!(
        cancel_crash_sweep_after_promote_compressed,
        CancelPoint::AfterPromote,
        true,
        rolled_forward
    );
    crash_sweep_test!(
        cancel_crash_sweep_after_promote_uncompressed,
        CancelPoint::AfterPromote,
        false,
        rolled_forward
    );

    // SidecarBuild and BeforeSwap: by these points the intent record has
    // named the real promoted id since its first (and now only) write (see
    // `AfterPromote` above), so T-023's C3 reconciliation finds the
    // promoted output under the recorded id and rolls forward. Closed by
    // T-022 (C2) + T-023 (C3).
    crash_sweep_test!(
        cancel_crash_sweep_sidecar_build_compressed,
        CancelPoint::SidecarBuild,
        true,
        rolled_forward
    );
    crash_sweep_test!(
        cancel_crash_sweep_sidecar_build_uncompressed,
        CancelPoint::SidecarBuild,
        false,
        rolled_forward
    );
    crash_sweep_test!(
        cancel_crash_sweep_before_swap_compressed,
        CancelPoint::BeforeSwap,
        true,
        rolled_forward
    );
    crash_sweep_test!(
        cancel_crash_sweep_before_swap_uncompressed,
        CancelPoint::BeforeSwap,
        false,
        rolled_forward
    );

    // Window D ("same as C" per compaction-cancel-safety.md): swapped in
    // memory, inputs not yet deleted. Before T-023, restart discovered both
    // the output and the untouched inputs (I2 violated). Closed by T-023's
    // C3 reconciliation, which rolls forward here for the same reason as
    // window C: the record's output is present and digest-verified.
    crash_sweep_test!(
        cancel_crash_sweep_after_swap_compressed,
        CancelPoint::AfterSwap,
        true,
        rolled_forward
    );
    crash_sweep_test!(
        cancel_crash_sweep_after_swap_uncompressed,
        CancelPoint::AfterSwap,
        false,
        rolled_forward
    );

    // Window E, at this harness's granularity: `RetireInput(0)` fires before
    // generation 0's first component file is unlinked, i.e. zero inputs
    // removed, output already live. The design doc (`compaction-cancel-safety.md`)
    // describes this as open until T-024's atomic per-generation retirement
    // (C4). It turns out T-023's reconciliation already closes it AT THIS
    // GRANULARITY: `reconcile_one_compaction_intent` doesn't ask how much of
    // a generation's retirement completed, it just retires every input the
    // record lists, unconditionally and idempotently
    // (`evict_local_input_sstable_files` treats an already-missing component
    // as a no-op) -- so whether zero, some, or all of a generation's files
    // survived a crash, startup finishes the job the same way. Verified by
    // running this case with `known-open-window` still asserting the
    // documented violation: the assertion itself failed ("I2 held anyway"),
    // which is exactly the self-check that comment was written to catch.
    //
    // What T-024 (C4) still owns: retirement atomicity WITHIN a single
    // generation at per-component granularity -- "one input half-deleted"
    // (some of a generation's own component files gone, some not) -- which
    // needs a hook finer than this harness's per-generation
    // `CancelPoint::RetireInput(k)` can express. See
    // `ferrosa-storage/specs/roadmap.md`.
    crash_sweep_test!(
        cancel_crash_sweep_retire_input_0_compressed,
        CancelPoint::RetireInput(0),
        true,
        rolled_forward
    );
    crash_sweep_test!(
        cancel_crash_sweep_retire_input_0_uncompressed,
        CancelPoint::RetireInput(0),
        false,
        rolled_forward
    );
}
