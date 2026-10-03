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
