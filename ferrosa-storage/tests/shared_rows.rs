//! Rows are immutable once built, so the storage engine shares them instead
//! of copying them: the memtable stores the writer's `Arc<Row>` and a range
//! read hands that same allocation back (p0-oom-audit clone-on-row-data,
//! design A: `Partition.rows` and `Mutation.rows` are `Vec<Arc<Row>>`).
//!
//! Pointer identity is the observable: if any layer between the write and the
//! read deep-copied the row, `Arc::ptr_eq` fails.

use std::sync::Arc;

use ferrosa_common::cell::CellValue;
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_common::{DecoratedKey, PartitionKey};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::{StorageEngine, StorageEngineConfig, TableId};
use futures::StreamExt as _;

fn schema() -> TableSchema {
    TableSchema {
        keyspace: "shared_rows".into(),
        table: "t".into(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
        clustering_columns: vec![ColumnDefinition {
            name: "ck".into(),
            type_name: "org.apache.cassandra.db.marshal.Int32Type".into(),
        }],
        static_columns: vec![],
        regular_columns: vec![ColumnDefinition {
            name: "v".into(),
            type_name: "org.apache.cassandra.db.marshal.UTF8Type".into(),
        }],
        extensions: Default::default(),
    }
}

fn row(ck: i32) -> Arc<Row> {
    Arc::new(Row {
        clustering: ck.to_be_bytes().to_vec(),
        cells: vec![(0, CellValue::live(format!("v{ck}").into_bytes(), 5))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(5),
    })
}

/// Write two rows into one partition, then read the partition back through
/// both memtable range-read paths. Every row the read yields must be the
/// allocation the writer handed in.
#[tokio::test]
async fn a_memtable_range_read_hands_back_the_stored_rows_without_copying() {
    let dir = tempfile::tempdir().unwrap();
    let engine = StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None).unwrap();
    engine.register_table(schema()).unwrap();
    let table = TableId::new("shared_rows", "t");
    let key = DecoratedKey::new(PartitionKey::new(b"p".to_vec()));
    let written = [row(1), row(2)];
    for r in &written {
        engine.write(&table, &key, Arc::clone(r), 5).unwrap();
    }

    let mut whole = engine.range_iter(&table, None, None);
    let partition = whole.next().await.expect("one partition").unwrap();
    assert_eq!(partition.rows.len(), 2);
    for (read, wrote) in partition.rows.iter().zip(&written) {
        assert!(
            Arc::ptr_eq(read, wrote),
            "range_iter must hand back the stored row, not a copy"
        );
    }

    let mut fragments = engine.range_iter_fragmented(&table, None, None);
    let mut seen = 0;
    while let Some(fragment) = fragments.next().await {
        for read in &fragment.unwrap().rows {
            assert!(
                Arc::ptr_eq(read, &written[seen]),
                "range_iter_fragmented must hand back the stored row, not a copy"
            );
            seen += 1;
        }
    }
    assert_eq!(seen, 2, "both rows streamed");
}

/// Sharing rows must not change a byte on disk or on the wire: the commit-log
/// entry (and the internode `Mutation` frame, which is the same encoding) is
/// serialized from `&Row`. This pins the documented layout byte for byte for a
/// mutation whose row is still shared with its writer, and decodes it back
/// into a fresh, unshared `Arc`.
#[test]
fn a_shared_row_encodes_byte_for_byte_as_the_documented_commit_log_layout() {
    use ferrosa_storage::Mutation;

    let row = Arc::new(Row {
        clustering: vec![0xAA, 0xBB],
        cells: vec![(3, CellValue::live(b"v".to_vec(), 42))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(42),
    });
    let key = DecoratedKey::new(PartitionKey::new(b"k".to_vec()));
    let mut mutation = Mutation::new(
        "ks".into(),
        "t".into(),
        key.clone(),
        vec![Arc::clone(&row)],
        42,
    );
    mutation.mutation_id = [7u8; 16];

    let mut expected = Vec::new();
    expected.extend_from_slice(&[7u8; 16]);
    for s in [&b"ks"[..], b"t", b"k"] {
        expected.extend_from_slice(&(s.len() as u16).to_be_bytes());
        expected.extend_from_slice(s);
    }
    expected.extend_from_slice(&key.token.0.to_be_bytes());
    expected.extend_from_slice(&42i64.to_be_bytes());
    expected.extend_from_slice(&1u16.to_be_bytes());
    expected.extend_from_slice(&2u16.to_be_bytes());
    expected.extend_from_slice(&[0xAA, 0xBB]);
    expected.extend_from_slice(&DeletionTime::LIVE.marked_for_delete_at.to_be_bytes());
    expected.extend_from_slice(&DeletionTime::LIVE.local_deletion_time.to_be_bytes());
    let liveness = LivenessInfo::with_timestamp(42);
    expected.extend_from_slice(&liveness.timestamp.to_be_bytes());
    expected.extend_from_slice(&liveness.ttl.to_be_bytes());
    expected.extend_from_slice(&liveness.local_deletion_time.to_be_bytes());
    expected.extend_from_slice(&1u16.to_be_bytes());
    let cell = CellValue::live(b"v".to_vec(), 42);
    expected.extend_from_slice(&3u16.to_be_bytes());
    expected.extend_from_slice(&cell.timestamp.to_be_bytes());
    expected.extend_from_slice(&cell.ttl.to_be_bytes());
    expected.extend_from_slice(&cell.local_deletion_time.to_be_bytes());
    expected.extend_from_slice(&1i32.to_be_bytes());
    expected.push(b'v');

    let mut encoded = vec![0u8; mutation.serialized_size()];
    mutation.serialize_into(&mut encoded);
    assert_eq!(encoded, expected, "the encoding of a shared row changed");
    assert_eq!(Arc::strong_count(&row), 2, "encoding borrowed the row");

    let decoded = Mutation::deserialize_from(&encoded).unwrap();
    assert_eq!(decoded.rows, mutation.rows);
    assert_eq!(
        Arc::strong_count(&decoded.rows[0]),
        1,
        "a decoded row is a fresh allocation"
    );
}
