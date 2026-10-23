//! A flush that panics part-way must lose nothing and flush nothing twice.
//!
//! On 2026-10-02 node2's `storage-flush` thread panicked in the SSTable
//! encoder (`writer.rs:1965`) after the memtable swap (t_7681b32b). The binary
//! now supervises the flusher and retries it; these tests pin the storage
//! contract that retry depends on:
//!
//! - the rows of the memtable that was being flushed stay readable after the
//!   panic, and the next successful flush writes them exactly once;
//! - that holds across consecutive panicked flushes, not only one.
//!
//! A memtable a failed flush left behind stays in the view's list of sealed
//! memtables and is written to its OWN SSTable by the next flush; overlapping
//! SSTables are normal and compaction merges them. "Exactly once" is checked
//! by counting each key's partitions across every SSTable.

use super::*;

use crate::store::flush_fault_test_hook::{arm, FlushFault};
use ferrosa_common::cell::CellValue;
use ferrosa_common::key::PartitionKey;
use ferrosa_common::schema::ColumnDefinition;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo};

fn schema() -> TableSchema {
    TableSchema {
        keyspace: "flush_panic_ks".to_string(),
        table: "rows".to_string(),
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

fn tid() -> TableId {
    TableId::new("flush_panic_ks", "rows")
}

fn key(name: &str) -> DecoratedKey {
    DecoratedKey::new(PartitionKey::new(name.as_bytes().to_vec()))
}

fn row(value: &str, timestamp: i64) -> Row {
    Row {
        clustering: vec![0x00, 0x00, 0x00, 0x01],
        cells: vec![(0, CellValue::live(value.as_bytes().to_vec(), timestamp))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
    }
}

fn engine(dir: &std::path::Path) -> StorageEngine {
    let engine =
        StorageEngine::new(StorageEngineConfig::test_config(dir), None).expect("storage engine");
    engine.register_table(schema()).expect("register table");
    engine
}

fn write(engine: &StorageEngine, name: &str, timestamp: i64) {
    engine
        .write(&tid(), &key(name), row(name, timestamp), timestamp)
        .expect("write");
}

/// The value `name` reads back as, failing the test when the row is gone.
fn read_value(engine: &StorageEngine, name: &str) -> Vec<u8> {
    let partition = engine
        .read(&tid(), &key(name))
        .expect("read")
        .unwrap_or_else(|| panic!("row {name} is not readable"));
    let row = partition
        .rows
        .first()
        .unwrap_or_else(|| panic!("partition {name} has no rows"));
    row.cells
        .first()
        .and_then(|(_, cell)| cell.value.clone())
        .unwrap_or_else(|| panic!("row {name} has no value"))
}

/// How many SSTables hold a partition for `name`.
fn sstables_holding(engine: &StorageEngine, name: &str) -> usize {
    let state = engine.require_table(&tid()).expect("table registered");
    state
        .store
        .sstable_generation_ids()
        .iter()
        .filter(|gen| {
            state
                .store
                .read_from_generation_for_test(gen, &key(name))
                .expect("read one generation")
                .is_some()
        })
        .count()
}

/// Run one flush with a panic armed after the memtable swap, and require that
/// it panicked.
fn panicked_flush(engine: &StorageEngine) {
    arm(FlushFault::AfterSwap);
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| engine.flush(&tid())));
    assert!(outcome.is_err(), "the armed flush must panic");
}

#[test]
fn rows_of_a_panicked_flush_stay_readable_and_are_flushed_once() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine(dir.path());
    for name in ["a", "b", "c"] {
        write(&engine, name, 1_000);
    }

    panicked_flush(&engine);

    assert_eq!(
        engine.sstable_count(&tid()),
        0,
        "a flush that panicked before publishing must not leave an SSTable"
    );
    for name in ["a", "b", "c"] {
        assert_eq!(
            read_value(&engine, name),
            name.as_bytes(),
            "after the panic"
        );
    }

    write(&engine, "d", 2_000);
    engine.flush(&tid()).expect("the retried flush succeeds");

    assert_eq!(
        engine.sstable_count(&tid()),
        2,
        "the retry writes the panicked memtable and the new one, one SSTable each"
    );
    for name in ["a", "b", "c", "d"] {
        assert_eq!(
            sstables_holding(&engine, name),
            1,
            "{name} must be flushed exactly once"
        );
    }
    assert_eq!(engine.memtable_size(&tid()), 0, "nothing is left unflushed");
    for name in ["a", "b", "c", "d"] {
        assert_eq!(
            read_value(&engine, name),
            name.as_bytes(),
            "after the retry"
        );
    }
}

#[test]
fn rows_survive_consecutive_panicked_flushes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine(dir.path());
    write(&engine, "first", 1_000);
    panicked_flush(&engine);
    write(&engine, "second", 2_000);
    panicked_flush(&engine);

    for name in ["first", "second"] {
        assert_eq!(
            read_value(&engine, name),
            name.as_bytes(),
            "a row whose flush panicked twice must stay readable"
        );
    }

    engine.flush(&tid()).expect("the retried flush succeeds");
    assert_eq!(
        engine.sstable_count(&tid()),
        2,
        "one SSTable per failed flush's memtable"
    );
    for name in ["first", "second"] {
        assert_eq!(
            read_value(&engine, name),
            name.as_bytes(),
            "after the retry"
        );
    }
}

/// Two consecutive failed flushes, then a success: every row is written to
/// exactly one SSTable (no duplicate, none missing), nothing stays sealed, and
/// a row written to both failed memtables keeps its newest value.
#[test]
fn two_failed_flushes_then_a_success_write_every_row_exactly_once() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine(dir.path());
    write(&engine, "one", 1_000);
    write(&engine, "both", 1_000);
    panicked_flush(&engine);
    write(&engine, "two", 2_000);
    engine
        .write(&tid(), &key("both"), row("newer", 2_000), 2_000)
        .expect("write");
    panicked_flush(&engine);
    write(&engine, "three", 3_000);

    engine
        .flush(&tid())
        .expect("the flush after two failures succeeds");

    assert_eq!(engine.memtable_size(&tid()), 0, "nothing is left unflushed");
    let state = engine.require_table(&tid()).expect("table registered");
    assert!(
        state.store.sealed_memtable_count_for_test() == 0,
        "every sealed memtable was flushed and left the view"
    );
    for name in ["one", "two", "three"] {
        assert_eq!(read_value(&engine, name), name.as_bytes());
        assert_eq!(
            sstables_holding(&engine, name),
            1,
            "{name} must be flushed exactly once"
        );
    }
    assert_eq!(
        read_value(&engine, "both"),
        b"newer",
        "the newest write wins"
    );
    assert_eq!(
        sstables_holding(&engine, "both"),
        2,
        "\"both\" was in two memtables, so two SSTables hold it; reads merge them"
    );
}

/// The index side of a failed flush (residual t_38c52eee): the retry writes
/// the failed memtable's sidecars from that memtable's own postings, so an
/// index read after the retry, served from sidecars alone, finds its rows.
#[test]
fn an_indexed_row_whose_flush_failed_is_found_through_the_index_after_the_retry() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine =
        StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None).expect("engine");
    engine
        .register_table_with_indexes(schema(), vec![("val_idx".to_string(), 0_usize)])
        .expect("register table");
    write(&engine, "lost", 1_000);
    panicked_flush(&engine);
    write(&engine, "later", 2_000);
    engine.flush(&tid()).expect("the retried flush succeeds");
    assert_eq!(engine.memtable_size(&tid()), 0);

    let mut found = Vec::new();
    engine
        .read_by_index_each(
            &tid(),
            "val_idx",
            &ferrosa_index::IndexKey(b"lost".to_vec()),
            &mut |partition| {
                found.push(partition.key.key.as_bytes().to_vec());
                std::ops::ControlFlow::Continue(())
            },
        )
        .expect("index read");
    assert_eq!(
        found,
        vec![b"lost".to_vec()],
        "the failed memtable's posting was lost"
    );
}

// ── A crash between two segment generations ──────────────────────────────
//
// A segmented flush writes one generation per time window, one after another.
// A crash between two of them is the case retention depends on: every window
// that finished must be readable after restart, and no half-written window may
// be adopted as a generation.

/// The window used here, in microseconds: 17 minutes, deliberately not a
/// default.
const SEGMENT_WINDOW_MICROS: i64 = 17 * 60 * 1_000_000;

fn segmented_schema() -> TableSchema {
    TableSchema {
        keyspace: "rrd_panic_ks".to_string(),
        table: "tier".to_string(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_columns: vec![ColumnDefinition {
            name: "ts".to_string(),
            type_name: "org.apache.cassandra.db.marshal.LongType".to_string(),
        }],
        static_columns: vec![],
        regular_columns: vec![ColumnDefinition {
            name: "val".to_string(),
            type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        }],
        extensions: [
            ("rrd.segment_window".to_string(), "17m".to_string()),
            ("rrd.retain".to_string(), "4h".to_string()),
        ]
        .into_iter()
        .collect(),
    }
}

fn segmented_tid() -> TableId {
    TableId::new("rrd_panic_ks", "tier")
}

fn segmented_engine(dir: &std::path::Path) -> StorageEngine {
    let engine =
        StorageEngine::new(StorageEngineConfig::test_config(dir), None).expect("storage engine");
    engine
        .register_table(segmented_schema())
        .expect("register table");
    engine
}

/// A tier row whose clustering component is its data timestamp in micros.
fn tier_row(ts_micros: i64, value: &str) -> Row {
    Row {
        clustering: ts_micros.to_be_bytes().to_vec(),
        cells: vec![(0, CellValue::live(value.as_bytes().to_vec(), ts_micros))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(ts_micros),
    }
}

fn clustered_ts(row: &Row) -> i64 {
    let bytes: [u8; 8] = row
        .clustering
        .as_slice()
        .try_into()
        .expect("8-byte timestamp clustering component");
    i64::from_be_bytes(bytes)
}

fn window_start(ts_micros: i64) -> i64 {
    ts_micros - ts_micros.rem_euclid(SEGMENT_WINDOW_MICROS)
}

/// Every `{gen}-Data.db` on disk under `root` — the component the restart
/// loader keys a generation on.
fn data_files(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with("-Data.db"))
            {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

/// INVARIANT 5: a crash between generation k and k+1 leaves every completed
/// generation readable and no partial generation visible after restart.
#[test]
fn a_crash_between_segment_generations_leaves_the_finished_ones_readable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let a = key("sensor_a");
    let b = key("sensor_b");

    // Three windows in one memtable: window 0 (two partitions), window 1w,
    // window 2w.
    {
        let engine = segmented_engine(dir.path());
        for (key, ts, value) in [
            (&a, 10i64, "a0"),
            (&b, 20, "b0"),
            (&a, SEGMENT_WINDOW_MICROS + 10, "a1"),
            (&a, 2 * SEGMENT_WINDOW_MICROS + 10, "a2"),
        ] {
            engine
                .write(&segmented_tid(), key, tier_row(ts, value), ts)
                .expect("write");
        }

        // Crash after the SECOND window's generation is published, before the
        // third is started.
        arm(FlushFault::AfterSegmentGeneration(2));
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            engine.flush(&segmented_tid())
        }));
        assert!(outcome.is_err(), "the armed flush must panic");

        assert_eq!(
            engine.sstable_count(&segmented_tid()),
            2,
            "the two generations published before the crash must already be live"
        );
        assert_eq!(
            data_files(dir.path()).len(),
            2,
            "exactly the completed generations are on disk"
        );
    }
    // Engine dropped here — simulates the process dying mid-flush.

    // Restart: a second engine on the same directory, loading generations the
    // way the production startup loader does.
    {
        let engine = segmented_engine(dir.path());
        let state = engine.require_table(&segmented_tid()).expect("registered");
        let gens = state.store.sstable_generation_ids();
        assert_eq!(
            gens.len(),
            2,
            "the completed generations must load; nothing else may: {gens:?}"
        );

        // Every loaded generation holds exactly one window, complete.
        let mut windows = Vec::new();
        for gen in &gens {
            let mut windows_here = Vec::new();
            for key in [&a, &b] {
                if let Some(partition) = state
                    .store
                    .read_from_generation_for_test(gen, key)
                    .expect("read one generation")
                {
                    windows_here.extend(
                        partition
                            .rows
                            .iter()
                            .map(|row| window_start(clustered_ts(row))),
                    );
                }
            }
            windows_here.sort_unstable();
            windows_here.dedup();
            assert_eq!(
                windows_here.len(),
                1,
                "generation {gen} holds rows from windows {windows_here:?} after the restart"
            );
            windows.push(windows_here[0]);
        }
        windows.sort_unstable();
        assert_eq!(
            windows,
            vec![0, SEGMENT_WINDOW_MICROS],
            "the windows that finished before the crash are exactly the ones that loaded"
        );

        // The rows of those windows are readable through the normal read path.
        let partition = engine
            .read(&segmented_tid(), &a)
            .expect("read")
            .expect("partition");
        let mut timestamps: Vec<i64> = partition.rows.iter().map(clustered_ts).collect();
        timestamps.sort_unstable();
        assert!(
            timestamps.contains(&10) && timestamps.contains(&(SEGMENT_WINDOW_MICROS + 10)),
            "both completed windows' rows must survive the crash, got {timestamps:?}"
        );
        assert_eq!(
            engine
                .read(&segmented_tid(), &b)
                .expect("read")
                .expect("partition")
                .rows
                .len(),
            1,
            "the second partition's row of the completed window must survive too"
        );
    }

    // A generation torn by the crash — a `Data.db` promoted without the rest of
    // its component set — must not be adopted on the next restart, and must not
    // disturb the completed ones.
    {
        let written = data_files(dir.path());
        assert_eq!(written.len(), 2);
        let torn = written[1]
            .parent()
            .expect("table dir")
            .join("9000000000000000-Data.db");
        std::fs::copy(&written[1], &torn).expect("copy a torn generation's Data.db");
        assert_eq!(data_files(dir.path()).len(), 3);
    }
    {
        let engine = segmented_engine(dir.path());
        let state = engine.require_table(&segmented_tid()).expect("registered");
        let gens = state.store.sstable_generation_ids();
        assert_eq!(
            gens.len(),
            2,
            "a generation whose components are incomplete must not be visible after restart,              got {gens:?}"
        );
        assert!(
            !gens.iter().any(|gen| gen.starts_with("9000000000000000")),
            "the torn generation was adopted: {gens:?}"
        );
        let partition = engine
            .read(&segmented_tid(), &a)
            .expect("read")
            .expect("partition");
        assert!(
            partition.rows.iter().any(|row| clustered_ts(row) == 10),
            "the completed generations must still be readable beside a torn one"
        );
    }
}
