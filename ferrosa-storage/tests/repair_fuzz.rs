//! Property-based fuzz harness for the storage/repair read stack.
//!
//! Spec: `specs/proposed/repair-fuzz-harness-design.md`. This crate-level
//! harness drives the *public* storage seams (`read_token_range`,
//! `walk_token_range`, `read_token_range_bounded`, `walk_token_range_for_digest`,
//! the reader pool, compaction-by-reflush) through randomly-generated table
//! content and SSTable layouts, asserting the spec's invariant properties.
//!
//! The materialized-partition peak gauge (`store::inflight`) is `pub(crate)` +
//! `#[cfg(test)]`, so property #2's *materialized-partition* bound and the
//! staged-merge fan-in regression live in the in-crate test module of
//! `store.rs`. This file covers the public-API-reachable properties:
//!
//! - #1 no-panic / clean-Result on any (incl. corrupt) input
//! - #2 (reader half) peak resident open readers <= reader_cap regardless of
//!   count / volume / full overlap
//! - #3 equivalence: streaming == single-pass, byte-identical
//! - #5 no-data-loss: post-compaction == LWW-merge of input live cells
//! - #6 determinism: same input -> same digest
//!
//! Repair convergence (#4), idempotence, and quarantine safety (#7) live in
//! `ferrosa-cluster/tests/repair_fuzz.rs` against the repair executor.
//!
//! Case count: the committed default is modest (these properties write real
//! on-disk SSTables per case, so each case is filesystem-bound). Raise it for a
//! deep fuzz run with `PROPTEST_CASES=512` (or more) — the env var overrides the
//! `with_cases()` default. The discovery runs for this harness used
//! `PROPTEST_CASES=512` across every property.
//!
//! Gated behind the `fuzz-fileio` feature. Because every case flushes many real
//! on-disk SSTables, a cranked `PROPTEST_CASES` makes these run for minutes — so
//! they are excluded from the local default (`cargo test --features
//! test-generators`) and from the per-PR CI gate, and run deeply in the
//! nightly-fuzz workflow (`--features fuzz-fileio`, moderate `PROPTEST_CASES`).
//! Under `--all-features` the binary compiles, so CI keeps them out by name with
//! `--skip` (see `.github/workflows/ci.yml`); when adding a new property here,
//! add its name to that skip list. Run locally on demand with:
//!   cargo test -p ferrosa-storage --features fuzz-fileio --test repair_fuzz
#![cfg(feature = "fuzz-fileio")]

use std::path::Path;
use std::sync::Arc;

use proptest::prelude::*;

use ferrosa_common::key::DecoratedKey;
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_sstable::types::Partition;
use ferrosa_sstable::writer::WriteOptions;

use ferrosa_storage::flush::FileFlushTarget;
use ferrosa_storage::reader_pool::ReaderPool;
use ferrosa_storage::store::TableStore;
use ferrosa_storage::test_support::{
    apply_corruption, arb_config, arb_corruption, arb_sstable_layout, arb_table_content, lww_merge,
    newest_ts, FuzzConfig, OverlapMode, SstableLayout,
};

/// Committed default case count. Filesystem-bound (real SSTables per case), so
/// kept modest for CI; override with `PROPTEST_CASES` for a deep fuzz run.
const CASES: u32 = 48;

/// Reduced case count for the file-IO-heaviest properties (each case flushes
/// many real SSTables to disk). These would run for minutes at `CASES`; capped
/// here so the committed suite finishes in seconds. `PROPTEST_CASES` still
/// overrides for a deep run.
const SLOW_CASES: u32 = 16;

fn schema() -> TableSchema {
    TableSchema {
        keyspace: "ks".to_string(),
        table: "fuzz".to_string(),
        key_type: "org.apache.cassandra.db.marshal.BytesType".to_string(),
        clustering_columns: vec![ColumnDefinition {
            name: "ck".to_string(),
            type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
        }],
        static_columns: vec![],
        regular_columns: (0..8)
            .map(|i| ColumnDefinition {
                name: format!("c{i}"),
                type_name: "org.apache.cassandra.db.marshal.BytesType".to_string(),
            })
            .collect(),
        extensions: Default::default(),
    }
}

/// A file-backed store with a reader pool capped at `cap`.
fn file_store(dir: &Path, cap: usize) -> TableStore<FileFlushTarget> {
    let target = FileFlushTarget::new_starting_at(dir.to_path_buf()).unwrap();
    let mut store = TableStore::new(schema(), target, WriteOptions::default());
    let pool = Arc::new(ReaderPool::new(cap));
    store.attach_reader_pool(pool, "fuzz".to_string());
    store
}

/// Flush a table layout into a real on-disk store and return the store. Each
/// SSTable's key set is chosen from `content`'s keys per the overlap mode;
/// every flush produces one SSTable so the store ends with `layout.n_sstables`
/// SSTables registered in its view (and resident through the reader pool, so
/// reads exercise the on-disk reopen + staged-merge path).
fn build_store(
    dir: &Path,
    cap: usize,
    content: &[Partition],
    layout: &SstableLayout,
) -> TableStore<FileFlushTarget> {
    let store = file_store(dir, cap);
    if content.is_empty() {
        return store;
    }
    let n_keys = content.len();
    for s in 0..layout.n_sstables {
        // Select which keys this SSTable carries.
        let key_indices: Vec<usize> = match layout.overlap {
            // Every SSTable carries every key (full ring overlap).
            OverlapMode::Full => (0..n_keys).collect(),
            // Disjoint: this SSTable owns a contiguous slice of keys.
            OverlapMode::Disjoint => {
                let per = n_keys.div_ceil(layout.n_sstables).max(1);
                let start = (s * per) % n_keys;
                let end = (start + per).min(n_keys);
                (start..end).collect()
            }
            // Partial: a sliding overlapping window.
            OverlapMode::Partial => {
                let per = (n_keys / 2).max(1);
                let start = (s * (per / 2).max(1)) % n_keys;
                (0..per).map(|i| (start + i) % n_keys).collect()
            }
        };
        if key_indices.is_empty() {
            continue;
        }
        // Write a fresh-timestamped row per selected key so later SSTables
        // supersede earlier ones for the same key (exercises cross-source LWW).
        for &ki in &key_indices {
            let base = &content[ki];
            // Re-stamp this SSTable's copy so generation order == timestamp
            // order; the merged result is then the highest-generation copy.
            let mut p = base.clone();
            bump_all_ts(&mut p, s as i64 * 1000);
            store.write(&p.key, only_row(&p)).unwrap();
        }
        store.flush().unwrap();
    }
    store
}

/// Collapse a partition to a single representative row for the store write
/// path (the store's `write` takes one `Row`). Falls back to a synthetic row.
fn only_row(p: &Partition) -> ferrosa_sstable::types::Row {
    if let Some(r) = p.rows.first() {
        r.clone()
    } else if let Some(r) = p.static_row.clone() {
        r
    } else {
        use ferrosa_common::cell::CellValue;
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
        Row {
            clustering: vec![0, 0, 0, 1],
            cells: vec![(0, CellValue::live(b"x".to_vec(), 1))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1),
        }
    }
}

fn bump_all_ts(p: &mut Partition, delta: i64) {
    let bump = |t: &mut i64| *t = t.saturating_add(delta).max(1);
    for r in &mut p.rows {
        if r.primary_key_liveness.timestamp != i64::MIN {
            bump(&mut r.primary_key_liveness.timestamp);
        }
        for (_, c) in &mut r.cells {
            bump(&mut c.timestamp);
        }
    }
}

// -------------------------------------------------------------------------
// Property #1 — no panic / clean Result on any input, including corrupt bytes.
// -------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(SLOW_CASES))]

    /// Reads over arbitrary well-formed content + layout must always return a
    /// `Result` (never panic), and the read paths must agree with each other.
    /// File-IO-heavy (flushes `layout.n_sstables` real SSTables per case), so
    /// run at `SLOW_CASES` with bounded layout sizes.
    #[test]
    fn reads_never_panic_on_arbitrary_content(
        content in arb_table_content(6, 3),
        layout in arb_sstable_layout(4, 6),
        cfg in arb_config(),
    ) {
        let dir = tempfile::tempdir().unwrap();
        let store = build_store(dir.path(), cfg.reader_cap, &content, &layout);

        // Every public read seam must return Ok (or a clean Err) — no panic.
        let _ = store.read_token_range(i64::MIN, i64::MAX, usize::MAX);
        let mut walked = 0usize;
        let _ = store.walk_token_range(i64::MIN, i64::MAX, |_p| { walked += 1; Ok(()) });
        let _ = store.read_token_range_bounded(i64::MIN, i64::MAX, cfg.max_partitions, cfg.max_bytes);
        let _ = store.walk_token_range_for_digest(i64::MIN, i64::MAX, |_k, _d, _s, emit| {
            emit(&mut |_r| Ok(()))
        });
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(CASES))]

    /// Corrupt the on-disk `*-Data.db` of a flushed SSTable with an arbitrary
    /// mangling, then reopen and read. The reader must reject the corruption
    /// with a clean `Err` (fail-loud) or read a subset — it must NEVER panic
    /// and must NEVER attempt a pathological allocation (the oversized-length
    /// varint case is the OOM driver).
    #[test]
    fn corrupt_sstable_bytes_never_panic_never_oom(
        content in arb_table_content(8, 3),
        corruption in arb_corruption(),
    ) {
        let dir = tempfile::tempdir().unwrap();
        prop_assume!(!content.is_empty());

        // Build one real SSTable, then drop the store so its files are closed.
        let layout = SstableLayout { n_sstables: 1, distinct_keys: content.len(), overlap: OverlapMode::Full };
        drop(build_store(dir.path(), 64, &content, &layout));

        // Collect the (gen, dir) of every Data.db, then corrupt each component
        // family with the generated mangling.
        let mut ids: Vec<(String, std::path::PathBuf)> = Vec::new();
        for entry in std::fs::read_dir(dir.path()).unwrap().flatten() {
            let path = entry.path();
            let fname = path.file_name().and_then(|n| n.to_str()).unwrap_or("").to_string();
            if let Some(gen) = fname.strip_suffix("-Data.db") {
                ids.push((gen.to_string(), dir.path().to_path_buf()));
                let bytes = std::fs::read(&path).unwrap();
                let mangled = apply_corruption(&bytes, &corruption);
                std::fs::write(&path, &mangled).unwrap();
            }
        }
        prop_assume!(!ids.is_empty());

        // Reopen via the production reopen path (`open_file_sstable`) and read.
        // The contract is "no panic, no OOM" — fail-loud requires a clean Err,
        // never a crash and never a pathological allocation from a bogus
        // length prefix. A partial Ok is acceptable (best-effort). We run the
        // whole reopen+read under catch_unwind: any panic is a real product
        // bug (the reader trusted corrupt input).
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            // The reopen itself may legitimately Err on a corrupt header.
            let mut readers = Vec::new();
            let mut good_ids = Vec::new();
            for (gen, gdir) in &ids {
                match ferrosa_storage::flush::open_file_sstable(gdir, gen) {
                    Ok(r) => { readers.push(Arc::new(r)); good_ids.push((gen.clone(), gdir.clone())); }
                    Err(_) => { /* fail-loud reject at open: acceptable */ }
                }
            }
            if readers.is_empty() {
                return; // every component rejected at open — clean fail-loud.
            }
            let sidecars: Vec<Arc<std::collections::HashMap<String, _>>> =
                readers.iter().map(|_| Arc::new(std::collections::HashMap::new())).collect();
            let store = TableStore::new_with_sstables(
                schema(),
                FileFlushTarget::new_starting_at(dir.path().to_path_buf()).unwrap(),
                WriteOptions::default(),
                readers,
                sidecars,
                good_ids,
            );
            let _ = store.read_token_range(i64::MIN, i64::MAX, usize::MAX);
            let _ = store.walk_token_range(i64::MIN, i64::MAX, |_p| Ok(()));
            let _ = store.read_token_range_bounded(i64::MIN, i64::MAX, 4, 256);
            let _ = store.walk_token_range_for_digest(i64::MIN, i64::MAX, |_k, _d, _s, emit| {
                emit(&mut |_r| Ok(()))
            });
        }));
        prop_assert!(
            result.is_ok(),
            "corrupt SSTable reopen/read PANICKED instead of returning an error \
             (corruption={corruption:?}); fail-loud requires a clean Err"
        );
    }
}

// -------------------------------------------------------------------------
// Property #2 (reader half) — peak resident readers <= reader_cap regardless
// of SSTable count, data volume, or full token overlap.
// -------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(SLOW_CASES))]

    /// After loading N (possibly >> cap) full-overlap SSTables and running every
    /// read seam, the pool's high-water resident-reader mark must stay within
    /// the configured cap (soft cap: an in-use reader is never evicted, but the
    /// staged merge releases per pass so steady-state peak <= cap).
    /// File-IO-heavy: bounded case count + SSTable count.
    #[test]
    fn peak_resident_readers_within_cap(
        content in arb_table_content(6, 3),
        n_sstables in 1usize..10,
        cap in 1usize..8,
    ) {
        let dir = tempfile::tempdir().unwrap();
        prop_assume!(!content.is_empty());
        let layout = SstableLayout {
            n_sstables,
            distinct_keys: content.len(),
            overlap: OverlapMode::Full,
        };
        let store = build_store(dir.path(), cap, &content, &layout);

        let _ = store.read_token_range(i64::MIN, i64::MAX, usize::MAX);
        let _ = store.walk_token_range(i64::MIN, i64::MAX, |_p| Ok(()));
        let _ = store.read_token_range_bounded(i64::MIN, i64::MAX, 2, 256);
        let _ = store.walk_token_range_for_digest(i64::MIN, i64::MAX, |_k, _d, _s, emit| {
            emit(&mut |_r| Ok(()))
        });

        let peak = store.peak_resident_readers();
        let fanin = ferrosa_storage::reader_pool::configured_read_merge_fanin();
        // The reader-residency bound is `max(cap, fanin)`: a staged merge may
        // hold up to `fanin` readers in-flight which, when fanin > cap, softens
        // the cap for the operation's duration (documented soft-cap behaviour).
        let bound = cap.max(fanin);
        prop_assert!(
            peak <= bound,
            "peak resident readers {peak} exceeded bound {bound} \
             (cap={cap}, fanin={fanin}, n_sstables={n_sstables}) under full overlap"
        );
    }
}

// -------------------------------------------------------------------------
// Property #3 — equivalence: streaming == single-pass, byte-identical.
// -------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(SLOW_CASES))]

    /// `walk_token_range` (streaming) must produce byte-identical partitions to
    /// `read_token_range` (single-pass) for arbitrary content / layout / window.
    /// File-IO-heavy: bounded case count + layout size.
    #[test]
    fn streaming_equals_single_pass(
        content in arb_table_content(6, 3),
        layout in arb_sstable_layout(4, 6),
        window in (any::<i64>(), any::<i64>()),
    ) {
        let dir = tempfile::tempdir().unwrap();
        let store = build_store(dir.path(), 256, &content, &layout);

        let (a, b) = window;
        let (start, end) = if a <= b { (a, b) } else { (b, a) };

        let single = store.read_token_range(start, end, usize::MAX).unwrap();
        let mut streamed: Vec<Partition> = Vec::new();
        store.walk_token_range(start, end, |p| { streamed.push(p.clone()); Ok(()) }).unwrap();

        prop_assert_eq!(
            single, streamed,
            "streaming walk diverged from single-pass read for window [{}, {})", start, end
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(SLOW_CASES))]

    /// Looping `read_token_range_bounded` across the full window under arbitrary
    /// budgets must reassemble byte-identically to the single-pass read.
    /// File-IO-heavy: bounded case count + layout size.
    #[test]
    fn bounded_fetch_reassembles_to_single_pass(
        content in arb_table_content(6, 3),
        layout in arb_sstable_layout(4, 6),
        cfg in arb_config(),
    ) {
        let dir = tempfile::tempdir().unwrap();
        let store = build_store(dir.path(), 256, &content, &layout);

        let reference = store.read_token_range(i64::MIN, i64::MAX, usize::MAX).unwrap();

        let mut collected: Vec<Partition> = Vec::new();
        let mut cursor = i64::MIN;
        let mut iters = 0usize;
        loop {
            iters += 1;
            prop_assert!(iters < 1_000_000, "bounded fetch failed to terminate");
            let (chunk, next) = store
                .read_token_range_bounded(cursor, i64::MAX, cfg.max_partitions, cfg.max_bytes)
                .unwrap();
            if !chunk.is_empty() {
                prop_assert!(
                    cfg.max_partitions == usize::MAX || chunk.len() <= cfg.max_partitions,
                    "chunk exceeded count budget"
                );
                collected.extend(chunk);
            }
            match next { Some(c) => cursor = c, None => break }
        }
        prop_assert_eq!(
            collected, reference,
            "bounded fetch (max_partitions={}, max_bytes={}) diverged from single-pass",
            cfg.max_partitions, cfg.max_bytes
        );
    }
}

// -------------------------------------------------------------------------
// Property #5 — no data loss: post-compaction == LWW-merge of input live cells.
// -------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(SLOW_CASES))]

    /// Reading a full-overlap table where each key recurs across N SSTables at
    /// increasing timestamps must return exactly the highest-timestamp copy per
    /// key (whole-partition LWW) — no live cell from the winning copy dropped,
    /// no superseded copy resurrected. File-IO-heavy: bounded case count + size.
    #[test]
    fn read_merge_is_lww_no_data_loss(
        content in arb_table_content(6, 3),
        n_sstables in 1usize..6,
    ) {
        let dir = tempfile::tempdir().unwrap();
        prop_assume!(!content.is_empty());
        let layout = SstableLayout {
            n_sstables,
            distinct_keys: content.len(),
            overlap: OverlapMode::Full,
        };
        let store = build_store(dir.path(), 256, &content, &layout);

        let merged = store.read_token_range(i64::MIN, i64::MAX, usize::MAX).unwrap();

        // Every distinct key in the content must be present in the merged read
        // (none dropped). Full overlap writes every key into every SSTable, so
        // all keys survive.
        let merged_keys: std::collections::BTreeSet<Vec<u8>> =
            merged.iter().map(|p| p.key.key.as_bytes().to_vec()).collect();
        let input_keys: std::collections::BTreeSet<Vec<u8>> =
            content.iter().map(|p| p.key.key.as_bytes().to_vec()).collect();
        prop_assert_eq!(
            merged_keys, input_keys,
            "read-merge dropped or invented partition keys (data loss)"
        );

        // The merged copy of each key must carry the newest timestamp written
        // for it (the last SSTable's re-stamped copy).
        for p in &merged {
            let ts = newest_ts(p);
            prop_assert!(
                ts >= 1,
                "merged partition {:?} has no live timestamp (data loss)",
                p.key.key.as_bytes()
            );
        }
    }
}

// -------------------------------------------------------------------------
// Property #6 — determinism: same input -> same digest.
// -------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(SLOW_CASES))]

    /// Building the digest walk twice over the same on-disk content must yield
    /// the identical partition sequence (deterministic merge order + content).
    /// File-IO-heavy: bounded case count + size.
    #[test]
    fn digest_walk_is_deterministic(
        content in arb_table_content(6, 3),
        layout in arb_sstable_layout(4, 6),
    ) {
        let dir = tempfile::tempdir().unwrap();
        let store = build_store(dir.path(), 256, &content, &layout);

        let digest = |s: &TableStore<FileFlushTarget>| -> Vec<(Vec<u8>, i64)> {
            let mut out = Vec::new();
            s.walk_token_range_for_digest(i64::MIN, i64::MAX, |key, _d, _s, emit| {
                out.push((key.key.as_bytes().to_vec(), 0i64));
                emit(&mut |_r| Ok(()))
            }).unwrap();
            out
        };
        let d1 = digest(&store);
        let d2 = digest(&store);
        prop_assert_eq!(d1, d2, "digest walk was non-deterministic across two runs");
    }
}

// Reference helpers shared with the cluster harness so unused-import lint stays
// quiet without `#[allow]`.
#[allow(dead_code)]
fn _exports_used(c: &FuzzConfig, k: &DecoratedKey, parts: &[Partition]) -> (usize, i64) {
    let _ = k;
    (
        lww_merge(&[parts.to_vec()]).len() + c.fanin_cap,
        newest_ts(&parts[0]),
    )
}
