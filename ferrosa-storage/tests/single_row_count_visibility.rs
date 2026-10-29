//! Cell-sensitive counts must use original values for LWW ties across sources.
use ferrosa_common::cell::CellValue;
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_common::{DecoratedKey, PartitionKey};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::{StorageEngine, StorageEngineConfig, TableId};

fn schema(names: &[&str]) -> TableSchema {
    TableSchema {
        keyspace: "count_visibility".into(),
        table: "rows".into(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
        clustering_columns: vec![],
        static_columns: vec![],
        regular_columns: names
            .iter()
            .map(|name| ColumnDefinition {
                name: (*name).into(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            })
            .collect(),
        extensions: Default::default(),
    }
}
fn row(column: u16, cell: CellValue) -> Row {
    Row {
        clustering: vec![],
        cells: vec![(column, cell)],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_ttl(1000, 1, 1),
    }
}

#[test]
fn single_row_count_retains_value_tie_winner_in_either_source_order() {
    for expired_winner in [false, true] {
        for reverse in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let engine =
                StorageEngine::new(StorageEngineConfig::test_config(directory.path()), None)
                    .unwrap();
            engine.register_table(schema(&["v"])).unwrap();
            let table = TableId::new("count_visibility", "rows");
            let key = DecoratedKey::new(PartitionKey::new(b"key".to_vec()));
            let loser = if expired_winner {
                CellValue::live(b"a".to_vec(), 2000)
            } else {
                CellValue::expiring(b"a".to_vec(), 2000, 1, 1)
            };
            let winner = if expired_winner {
                CellValue::expiring(b"z".to_vec(), 2000, 1, 1)
            } else {
                CellValue::live(b"z".to_vec(), 2000)
            };
            let cells = if reverse {
                [winner, loser]
            } else {
                [loser, winner]
            };
            for cell in cells {
                engine.write(&table, &key, row(0, cell), 2000).unwrap();
                engine.flush_all().unwrap();
            }
            let stored = engine.read(&table, &key).unwrap().unwrap();
            assert_eq!(
                stored.rows[0].cells[0].1.value.as_ref().unwrap().as_ref(),
                b"z"
            );
            let expected = u64::from(!expired_winner);
            assert_eq!(
                u64::from(stored.rows[0].is_visible_at(2_000_000_000)),
                expected
            );
            assert_eq!(
                engine
                    .count_live_single_row_range_matching(&table, None, None, &|_| true)
                    .unwrap(),
                expected,
                "reverse={reverse}, expired_winner={expired_winner}"
            );
        }
    }
}

#[test]
fn single_row_count_remaps_old_column_ordinals_before_cell_ties() {
    let directory = tempfile::tempdir().unwrap();
    let engine =
        StorageEngine::new(StorageEngineConfig::test_config(directory.path()), None).unwrap();
    engine.register_table(schema(&["v"])).unwrap();
    let table = TableId::new("count_visibility", "rows");
    let key = DecoratedKey::new(PartitionKey::new(b"key".to_vec()));
    engine
        .write(
            &table,
            &key,
            row(0, CellValue::live(b"a".to_vec(), 2000)),
            2000,
        )
        .unwrap();
    engine.flush_all().unwrap();
    engine
        .update_table_schema(&table, schema(&["a", "v"]))
        .unwrap();
    engine
        .write(
            &table,
            &key,
            row(1, CellValue::expiring(b"z".to_vec(), 2000, 1, 1)),
            2000,
        )
        .unwrap();
    engine.flush_all().unwrap();
    let stored = engine.read(&table, &key).unwrap().unwrap();
    assert_eq!(stored.rows[0].cells.len(), 1);
    assert_eq!(stored.rows[0].cells[0].0, 1);
    assert!(!stored.rows[0].is_visible_at(2_000_000_000));
    assert_eq!(
        engine
            .count_live_single_row_range_matching(&table, None, None, &|_| true)
            .unwrap(),
        0
    );
}

#[test]
fn single_row_count_rereads_before_disjoint_run_rollover() {
    let directory = tempfile::tempdir().unwrap();
    let engine =
        StorageEngine::new(StorageEngineConfig::test_config(directory.path()), None).unwrap();
    engine.register_table(schema(&["v"])).unwrap();
    let table = TableId::new("count_visibility", "rows");
    let mut keys: Vec<_> = (0..3)
        .map(|n| DecoratedKey::new(PartitionKey::new(format!("key{n}").into_bytes())))
        .collect();
    keys.sort();
    // The exception is the last partition in the first file. Refilling it moves
    // a disjoint run to the next file, which must not replace the pinned reader.
    for key in &keys[..2] {
        engine
            .write(
                &table,
                key,
                row(0, CellValue::live(b"live".to_vec(), 2000)),
                2000,
            )
            .unwrap();
    }
    engine.flush_all().unwrap();
    engine
        .write(
            &table,
            &keys[2],
            row(0, CellValue::expiring(b"dead".to_vec(), 2000, 1, 1)),
            2000,
        )
        .unwrap();
    engine.flush_all().unwrap();
    let visited = std::sync::Mutex::new(Vec::new());
    let count = engine
        .count_live_single_row_range_matching(&table, None, None, &|key| {
            visited.lock().unwrap().push(key.clone());
            true
        })
        .unwrap();
    assert_eq!(count, 2);
    assert_eq!(*visited.lock().unwrap(), keys);
}

#[test]
fn single_row_count_applies_table_delete_and_later_insert() {
    use ferrosa_storage::table_tombstone::{table_tombstone_key, table_tombstone_row};
    let directory = tempfile::tempdir().unwrap();
    let engine =
        StorageEngine::new(StorageEngineConfig::test_config(directory.path()), None).unwrap();
    engine.register_table(schema(&["v"])).unwrap();
    let table = TableId::new("count_visibility", "rows");
    let key = DecoratedKey::new(PartitionKey::new(b"key".to_vec()));
    let mut original = row(0, CellValue::live(b"old".to_vec(), 1000));
    original.primary_key_liveness = LivenessInfo::with_timestamp(1000);
    engine.write(&table, &key, original, 1000).unwrap();
    engine.flush_all().unwrap();
    engine
        .write(
            &table,
            &table_tombstone_key(),
            table_tombstone_row(3000, 1),
            3000,
        )
        .unwrap();
    for resurrected in [false, true] {
        if resurrected {
            let mut newer = row(0, CellValue::live(b"new".to_vec(), 4000));
            newer.primary_key_liveness = LivenessInfo::with_timestamp(4000);
            engine.write(&table, &key, newer, 4000).unwrap();
        }
        for flushed in [false, true] {
            if flushed {
                engine.flush_all().unwrap();
            }
            assert_eq!(
                engine
                    .count_live_single_row_range_matching(&table, None, None, &|_| true)
                    .unwrap(),
                u64::from(resurrected),
                "resurrected={resurrected} flushed={flushed}"
            );
        }
    }
}
