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
        /// only thing left. Closes windows C and D (T-022/T-023).
        RolledForward,
        /// A documented, `known-open-window`-gated case where I2 is
        /// expected to still fail: window E (T-024, not yet fixed) or the
        /// `AfterPromote` sub-window of C (a gap in T-022's own commit
        /// protocol, also not yet fixed -- see that test's comment).
        KnownOpenWindow,
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
            ExpectedOutcome::KnownOpenWindow => {
                report.assert_i1_content_matches_oracle();
                report.assert_i3_no_corrupt_generation();
                let observed: HashSet<u64> =
                    StorageEngine::list_generations_in_dir(&engine.table_sstable_dir(&tid))
                        .into_iter()
                        .collect();
                let inputs: HashSet<u64> = input_gens.iter().copied().collect();
                assert!(
                    report.duplicate_or_missing_generations.is_some(),
                    "{point} was gated as a known-open-window case (see this test's comment \
                     for which gap -- window E/T-024, or the AfterPromote sub-window of C) \
                     but I2 held anyway (inputs {inputs:?}, observed {observed:?}) -- the \
                     fixing packet may already be in, in which case remove this gate and \
                     this test's `known-open-window` feature guard"
                );
            }
        }
        let _ = engine.shutdown();
    }

    /// Generates one `#[test]` per (point, compression) crash-twin case.
    /// `clean` cases run by default; `known_open_window` cases compile only
    /// under `--features known-open-window` (off by default, so `cargo
    /// test` stays green) and assert the documented I2 violation instead of
    /// a clean pass.
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
        ($name:ident, $point:expr, $compressed:expr, known_open_window) => {
            #[cfg(feature = "known-open-window")]
            #[test]
            fn $name() {
                run_crash_sweep_case(
                    stringify!($name),
                    $point,
                    $compressed,
                    ExpectedOutcome::KnownOpenWindow,
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

    // ---- Window D and most of window C are closed by T-022/T-023 and run
    // unconditionally below, asserting the specific roll-forward outcome
    // those packets guarantee. `AfterPromote` (part of window C) and window
    // E remain gated behind `known-open-window` -- see each section's
    // comment for why. ----

    // Window C (compaction-cancel-safety.md): the output is promoted (live
    // under sstables/<table>/) but the view has not swapped yet.
    //
    // `AfterPromote` itself is a narrower window T-022/T-023 do NOT close,
    // found by running this case with the fix in place: `poll_compactions`
    // fires this cancel point BEFORE it corrects the intent record's
    // `output_gen` from its pre-promotion placeholder (the staged output's
    // own id) to the actual promoted generation id -- `promote_compaction_output`
    // is free to pick a different id to avoid colliding with a concurrent
    // flush, and routinely does in this scenario. A crash in that gap
    // leaves a durable, fsynced record whose `output_gen` names a
    // generation that no longer exists (it was renamed away), so
    // `reconcile_one_compaction_intent` finds "output missing" and rolls
    // BACK -- deleting the record and leaving both inputs untouched --
    // while the real promoted output sits live on disk under its true id,
    // now an orphan no record points to. Both inputs and the output are
    // then discoverable after restart: I2 violated, the original window C
    // shape. This is a gap in T-022's own commit protocol (the two-step
    // intent write around promotion), not something T-024 covers, and needs
    // its own fix packet. Kept `known-open-window`-gated pending that fix.
    crash_sweep_test!(
        cancel_crash_sweep_after_promote_compressed,
        CancelPoint::AfterPromote,
        true,
        known_open_window
    );
    crash_sweep_test!(
        cancel_crash_sweep_after_promote_uncompressed,
        CancelPoint::AfterPromote,
        false,
        known_open_window
    );

    // SidecarBuild and BeforeSwap: by these points `poll_compactions` has
    // already corrected the intent record's `output_gen` to the real
    // promoted id and re-fsynced it (immediately after `AfterPromote` fires,
    // see above), so T-023's C3 reconciliation finds the promoted output
    // under the recorded id and rolls forward. Closed by T-022 (C2) + T-023
    // (C3).
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

    // Window E: input retirement stops after zero inputs are removed (the
    // output is already live, both inputs still are too). NOT YET fixed —
    // T-024 (C4: atomic, fsynced per-generation retirement) is the only
    // remaining `known-open-window` case now that windows C and D are
    // closed by T-022/T-023 above. This packet's `CancelPoint` granularity
    // is per whole input generation, not per component file, so it does not
    // reach the finer-grained "one input half-deleted" shape the design doc
    // also describes under window E — that needs a per-component hook a
    // later packet can add alongside its fix.
    crash_sweep_test!(
        cancel_crash_sweep_retire_input_0_compressed,
        CancelPoint::RetireInput(0),
        true,
        known_open_window
    );
    crash_sweep_test!(
        cancel_crash_sweep_retire_input_0_uncompressed,
        CancelPoint::RetireInput(0),
        false,
        known_open_window
    );
}
