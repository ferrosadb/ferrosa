//! A table with static columns AND collection columns, through the engine's
//! write path and memtable merge (write -> memtable merge -> read).
//!
//! These stop short of a flush on purpose: flushing ANY table that has static
//! columns currently panics in the SSTable writer, because the flush hands the
//! writer unified (static-first) cell ordinals while the writer indexes each
//! section's own column list. That is a separate, wider gap (board task
//! referenced in the T13 report), not something these tests mask.
//!
//! Row-cell ordinals are one index space defined by `TableSchema`: statics at
//! `0..static_columns.len()`, regulars after. The memtable merge normalizer
//! used to look a collection ordinal up in `regular_columns` alone, so on a
//! table with statics a legacy whole-collection write followed by an element
//! write was rejected (or expanded under the wrong column's type). These tests
//! pin the value landing on the right column after a write and read back.

use std::path::Path;
use std::time::Duration;

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_common::{CqlType, CqlValue};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
    TableId,
};

const UTF8: &str = "org.apache.cassandra.db.marshal.UTF8Type";
const LIST_OF_TEXT: &str =
    "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.UTF8Type)";
const SET_OF_TEXT: &str =
    "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type)";

const STATIC_S: u16 = 0;
const REGULAR_A: u16 = 1;
const REGULAR_L: u16 = 2;

fn column(name: &str, type_name: &str) -> ColumnDefinition {
    ColumnDefinition {
        name: name.to_string(),
        type_name: type_name.to_string(),
    }
}

fn engine_config(dir: &Path) -> StorageEngineConfig {
    StorageEngineConfig {
        commit_log: CommitLogConfig {
            segment_size: 256 * 1024,
            max_segment_age: Duration::from_secs(60),
            sync_strategy: SyncStrategyConfig::Batch,
            batch: Default::default(),
            log_dir: dir.join("commitlog"),
            checkpoint_dir: dir.join("commitlog"),
            archive: None,
        },
        compaction: CompactionConfig::from_env(dir.join("compaction")),
        object_store: None,
        local_cache_max_bytes: 1024 * 1024,
        local_disk_free_reserve_bytes: 0,
        flush_threshold_bytes: 4096,
        memtable_backpressure_bytes: u64::MAX,
        flush_max_age_secs: 5,
        data_dir: dir.to_path_buf(),
        index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
        auth_enabled: false,
        auth_warn: false,
        max_pending_replay_mutations_without_schema: 1024,
        memtable_num_shards: 64,
        cache_hot_window_secs: 900,
        write_verify: false,
    }
}

fn schema(
    table: &str,
    statics: Vec<ColumnDefinition>,
    regulars: Vec<ColumnDefinition>,
) -> TableSchema {
    TableSchema {
        keyspace: "ks".to_string(),
        table: table.to_string(),
        key_type: UTF8.to_string(),
        clustering_columns: vec![],
        static_columns: statics,
        regular_columns: regulars,
        extensions: Default::default(),
    }
}

fn key() -> DecoratedKey {
    DecoratedKey::new(PartitionKey::new(b"pk".to_vec()))
}

fn row(cells: Vec<(u16, CellValue)>, ts: i64) -> Row {
    Row {
        clustering: vec![],
        cells,
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(ts),
    }
}

/// Legacy whole-collection encoding: element count, then length-prefixed values.
fn blob(elements: &[&[u8]]) -> Vec<u8> {
    let mut out = (elements.len() as i32).to_be_bytes().to_vec();
    for element in elements {
        out.extend_from_slice(&(element.len() as i32).to_be_bytes());
        out.extend_from_slice(element);
    }
    out
}

fn text(s: &str) -> CqlValue {
    CqlValue::Text(s.into())
}

/// The cells of `ordinal` in the first row of the partition read back.
fn cells_at(row: &Row, ordinal: u16) -> Vec<&CellValue> {
    row.cells
        .iter()
        .filter(|(idx, _)| *idx == ordinal)
        .map(|(_, cell)| cell)
        .collect()
}

fn simple_value(row: &Row, ordinal: u16) -> Option<Vec<u8>> {
    let cells = cells_at(row, ordinal);
    assert_eq!(
        cells.len(),
        1,
        "column {ordinal} must hold exactly one cell"
    );
    cells[0].value.clone()
}

fn list_at(row: &Row, ordinal: u16) -> Option<CqlValue> {
    ferrosa_row_bridge::collection::assemble_column_cells(
        &CqlType::List(Box::new(CqlType::Varchar)),
        &cells_at(row, ordinal),
        0,
    )
    .unwrap()
}

fn engine_with(table: &str, schema: TableSchema) -> (tempfile::TempDir, StorageEngine, TableId) {
    let dir = tempfile::tempdir().unwrap();
    let engine = StorageEngine::new(engine_config(dir.path()), None).unwrap();
    engine.register_table(schema).unwrap();
    (dir, engine, TableId::new("ks", table))
}

fn static_and_list_schema(table: &str) -> TableSchema {
    schema(
        table,
        vec![column("s", UTF8)],
        vec![column("a", UTF8), column("l", LIST_OF_TEXT)],
    )
}

#[test]
fn whole_list_then_element_append_keeps_every_column_on_its_own_ordinal() {
    let (_dir, engine, tid) = engine_with(
        "whole_then_append",
        static_and_list_schema("whole_then_append"),
    );
    engine
        .write(
            &tid,
            &key(),
            row(
                vec![
                    (STATIC_S, CellValue::live(b"static".to_vec(), 1_000)),
                    (REGULAR_A, CellValue::live(b"plain".to_vec(), 1_000)),
                    (
                        REGULAR_L,
                        CellValue::live(blob(&[b"first", b"second"]), 1_000),
                    ),
                ],
                1_000,
            ),
            1_000,
        )
        .unwrap();
    engine
        .write(
            &tid,
            &key(),
            row(
                vec![(
                    REGULAR_L,
                    CellValue::live(b"third".to_vec(), 2_000)
                        .with_path(ferrosa_row_bridge::collection::list_cell_path(2_000, 0)),
                )],
                2_000,
            ),
            2_000,
        )
        .unwrap();

    let partition = engine.read(&tid, &key()).unwrap().unwrap();
    let row = &partition.rows[0];

    assert_eq!(
        simple_value(row, STATIC_S).as_deref(),
        Some(b"static".as_slice())
    );
    assert_eq!(
        simple_value(row, REGULAR_A).as_deref(),
        Some(b"plain".as_slice())
    );
    assert_eq!(
        list_at(row, REGULAR_L),
        Some(CqlValue::List(vec![
            text("first"),
            text("second"),
            text("third")
        ]))
    );
}

#[test]
fn collection_deletion_then_whole_overwrite_leaves_only_the_new_value() {
    let (_dir, engine, tid) = engine_with(
        "delete_then_overwrite",
        static_and_list_schema("delete_then_overwrite"),
    );
    // Element write first, so the row already holds path-bearing cells when
    // the legacy whole-collection overwrite arrives (that is what routes the
    // overwrite through the merge normalizer).
    engine
        .write(
            &tid,
            &key(),
            row(
                vec![
                    (STATIC_S, CellValue::live(b"static".to_vec(), 1_000)),
                    (REGULAR_A, CellValue::live(b"plain".to_vec(), 1_000)),
                    (
                        REGULAR_L,
                        CellValue::live(b"old".to_vec(), 1_000)
                            .with_path(ferrosa_row_bridge::collection::list_cell_path(1_000, 0)),
                    ),
                ],
                1_000,
            ),
            1_000,
        )
        .unwrap();
    // DELETE l: a pathless collection-level tombstone.
    engine
        .write(
            &tid,
            &key(),
            row(
                vec![(REGULAR_L, CellValue::tombstone(2_000, 1_700_000_000))],
                2_000,
            ),
            2_000,
        )
        .unwrap();
    engine
        .write(
            &tid,
            &key(),
            row(
                vec![(REGULAR_L, CellValue::live(blob(&[b"new"]), 3_000))],
                3_000,
            ),
            3_000,
        )
        .unwrap();

    let partition = engine.read(&tid, &key()).unwrap().unwrap();
    let row = &partition.rows[0];

    assert_eq!(
        simple_value(row, STATIC_S).as_deref(),
        Some(b"static".as_slice())
    );
    assert_eq!(
        simple_value(row, REGULAR_A).as_deref(),
        Some(b"plain".as_slice())
    );
    assert_eq!(
        list_at(row, REGULAR_L),
        Some(CqlValue::List(vec![text("new")]))
    );
}

#[test]
fn whole_set_in_a_static_column_then_element_append_reads_back() {
    let (_dir, engine, tid) = engine_with(
        "static_set",
        schema(
            "static_set",
            vec![column("ss", SET_OF_TEXT)],
            vec![column("a", UTF8)],
        ),
    );
    engine
        .write(
            &tid,
            &key(),
            row(
                vec![
                    (0, CellValue::live(blob(&[b"k1"]), 1_000)),
                    (1, CellValue::live(b"plain".to_vec(), 1_000)),
                ],
                1_000,
            ),
            1_000,
        )
        .unwrap();
    engine
        .write(
            &tid,
            &key(),
            row(
                vec![(
                    0,
                    CellValue::live(Vec::new(), 2_000).with_path(b"k2".to_vec()),
                )],
                2_000,
            ),
            2_000,
        )
        .unwrap();

    let partition = engine.read(&tid, &key()).unwrap().unwrap();
    let row = &partition.rows[0];

    assert_eq!(
        ferrosa_row_bridge::collection::assemble_column_cells(
            &CqlType::Set(Box::new(CqlType::Varchar)),
            &cells_at(row, 0),
            0,
        )
        .unwrap(),
        Some(CqlValue::Set(vec![text("k1"), text("k2")]))
    );
    assert_eq!(simple_value(row, 1).as_deref(), Some(b"plain".as_slice()));
}
