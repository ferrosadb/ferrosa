//! A CQL-shaped table WITH a static column through write -> flush ->
//! compaction -> read (t_65661473).
//!
//! Cells use the FLAT ordinal space everywhere above the SSTable boundary
//! (`TableSchema`: statics at `0..static_columns.len()`, regulars after), and
//! CQL writes a static cell inside the clustered row it was written with. An
//! SSTable numbers its static and regular columns separately, each from 0.
//! Before the boundary converted between the two, a flush handed flat
//! ordinals to the writer: regular `a` (flat 1) was written as regular column
//! 1 (`b`), and `b` (flat 2) was refused, which failed the flush.
//!
//! What a reader sees is the invariant: per clustered row, its own cells
//! overlaid on the partition's static cells, in flat ordinals. A flush or a
//! compaction may move a static cell between the clustered row and
//! `Partition::static_row`, but must never change that view.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};

use crate::{CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, TableId};

const UTF8: &str = "org.apache.cassandra.db.marshal.UTF8Type";
const INT32: &str = "org.apache.cassandra.db.marshal.Int32Type";

/// Flat ordinals of `(pk text, ck int, s text STATIC, a int, b text)`.
const S: u16 = 0;
const A: u16 = 1;
const B: u16 = 2;

fn column(name: &str, type_name: &str) -> ColumnDefinition {
    ColumnDefinition {
        name: name.to_string(),
        type_name: type_name.to_string(),
    }
}

fn schema(table: &str) -> TableSchema {
    TableSchema {
        keyspace: "ks".to_string(),
        table: table.to_string(),
        key_type: UTF8.to_string(),
        clustering_columns: vec![column("ck", INT32)],
        static_columns: vec![column("s", UTF8)],
        regular_columns: vec![column("a", INT32), column("b", UTF8)],
        extensions: Default::default(),
    }
}

fn engine(dir: &Path, table: &str) -> (Arc<StorageEngine>, TableId) {
    let config = StorageEngineConfig {
        commit_log: CommitLogConfig {
            log_dir: dir.join("commitlog"),
            checkpoint_dir: dir.join("commitlog"),
            archive: None,
            ..CommitLogConfig::default()
        },
        compaction: CompactionConfig::from_env(dir.join("compaction")),
        object_store: None,
        local_cache_max_bytes: 64 * 1024 * 1024,
        local_disk_free_reserve_bytes: 0,
        flush_threshold_bytes: 64 * 1024 * 1024,
        memtable_backpressure_bytes: u64::MAX,
        flush_max_age_secs: 3600,
        data_dir: dir.to_path_buf(),
        index_backend: crate::index::IndexBackendConfig::Local,
        auth_enabled: false,
        auth_warn: false,
        write_verify: false,
        max_pending_replay_mutations_without_schema: 1024,
        memtable_num_shards: 4,
        cache_hot_window_secs: 900,
    };
    let engine = Arc::new(StorageEngine::new(config, None).unwrap());
    engine.register_table(schema(table)).unwrap();
    (engine, TableId::new("ks", table))
}

fn key() -> DecoratedKey {
    DecoratedKey::new(PartitionKey::new(b"pk".to_vec()))
}

/// A CQL INSERT: the clustered row carries the static cell with it.
fn insert(ck: i32, cells: &[(u16, &[u8])], ts: i64) -> Row {
    Row {
        clustering: ck.to_be_bytes().to_vec(),
        cells: cells
            .iter()
            .map(|(idx, v)| (*idx, CellValue::live(v.to_vec(), ts)))
            .collect(),
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(ts),
    }
}

/// What a reader sees: clustering -> flat ordinal -> value, each row's cells
/// overlaid on the partition's static cells (newest timestamp wins).
fn visible(p: &Partition) -> BTreeMap<Vec<u8>, BTreeMap<u16, Option<Vec<u8>>>> {
    let statics: Vec<&(u16, CellValue)> =
        p.static_row.iter().flat_map(|r| r.cells.iter()).collect();
    p.rows
        .iter()
        .map(|row| {
            let mut cells: BTreeMap<u16, &CellValue> = BTreeMap::new();
            for (idx, cell) in statics
                .iter()
                .map(|(i, c)| (i, c))
                .chain(row.cells.iter().map(|(i, c)| (i, c)))
            {
                let keep = cells
                    .get(idx)
                    .is_none_or(|old| cell.timestamp >= old.timestamp);
                if keep {
                    cells.insert(*idx, cell);
                }
            }
            (
                row.clustering.clone(),
                cells
                    .into_iter()
                    .map(|(i, c)| (i, c.value.clone()))
                    .collect(),
            )
        })
        .collect()
}

fn read(engine: &StorageEngine, tid: &TableId) -> Partition {
    engine
        .read(tid, &key())
        .unwrap()
        .expect("partition must be readable")
}

fn value(
    view: &BTreeMap<Vec<u8>, BTreeMap<u16, Option<Vec<u8>>>>,
    ck: i32,
    idx: u16,
) -> Option<Vec<u8>> {
    view.get(&ck.to_be_bytes().to_vec())
        .and_then(|cells| cells.get(&idx))
        .cloned()
        .flatten()
}

/// RED before the fix: the flush fails with "cell col_idx 2 is out of range
/// (num_columns=2)" for `b`, after writing `a` as column `b`.
#[test]
fn static_and_regular_cells_survive_a_flush_on_their_own_columns() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, tid) = engine(dir.path(), "static_flush");
    let a = 7i32.to_be_bytes();
    engine
        .write(
            &tid,
            &key(),
            insert(1, &[(S, b"s1"), (A, &a), (B, b"b1")], 100),
            100,
        )
        .unwrap();
    let before = visible(&read(&engine, &tid));

    engine
        .flush(&tid)
        .expect("a table with a static column must flush");

    let after = visible(&read(&engine, &tid));
    assert_eq!(after, before, "flush changed what a reader sees");
    assert_eq!(value(&after, 1, S).as_deref(), Some(&b"s1"[..]));
    assert_eq!(
        value(&after, 1, A).as_deref(),
        Some(&a[..]),
        "a must stay on a"
    );
    assert_eq!(
        value(&after, 1, B).as_deref(),
        Some(&b"b1"[..]),
        "b must stay on b"
    );
}

/// A flushed SSTable and a newer memtable write merge on read, and a
/// compaction of two flushed SSTables keeps every cell on its column.
#[tokio::test(flavor = "multi_thread")]
async fn static_and_regular_cells_survive_compaction_and_a_memtable_overlay() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, tid) = engine(dir.path(), "static_compact");
    let a1 = 1i32.to_be_bytes();
    let a2 = 2i32.to_be_bytes();
    engine
        .write(
            &tid,
            &key(),
            insert(1, &[(S, b"s-old"), (A, &a1), (B, b"b1")], 100),
            100,
        )
        .unwrap();
    engine.flush(&tid).expect("first flush");
    engine
        .write(
            &tid,
            &key(),
            insert(2, &[(S, b"s-new"), (A, &a2)], 200),
            200,
        )
        .unwrap();

    // SSTable + memtable merge: the newer static wins for every row.
    let merged = visible(&read(&engine, &tid));
    assert_eq!(value(&merged, 1, B).as_deref(), Some(&b"b1"[..]));
    assert_eq!(value(&merged, 2, A).as_deref(), Some(&a2[..]));

    engine.flush(&tid).expect("second flush");
    engine.force_compact_all();
    engine
        .drive_compactions_until_idle(&tid, std::time::Duration::from_secs(120))
        .await;

    let compacted = visible(&read(&engine, &tid));
    assert_eq!(value(&compacted, 1, A).as_deref(), Some(&a1[..]), "row 1 a");
    assert_eq!(
        value(&compacted, 1, B).as_deref(),
        Some(&b"b1"[..]),
        "row 1 b"
    );
    assert_eq!(value(&compacted, 2, A).as_deref(), Some(&a2[..]), "row 2 a");
    assert_eq!(
        value(&compacted, 2, S).as_deref(),
        Some(&b"s-new"[..]),
        "row 2 s"
    );
    assert_eq!(
        value(&compacted, 1, S).as_deref(),
        Some(&b"s-new"[..]),
        "a static column has one value per partition: the newest"
    );
}
