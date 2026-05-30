//! Drive a real compaction and read its output back, to diff against the oracle.
//!
//! This exercises the full pipeline — SSTable write, the executor's streaming
//! k-way merge, and SSTable read-back — so the differential check covers
//! serialization round-trips, not just the in-memory merge.

use std::collections::BTreeMap;
use std::path::Path;

use ferrosa_common::key::DecoratedKey;
use ferrosa_common::schema::TableSchema;
use ferrosa_sstable::types::Partition;

use crate::compaction::executor::CompactionExecutor;
use crate::compaction::metadata::{CompactionTask, SSTableMetadata};
use crate::TableId;

/// A logical projection keyed by `(partition key, clustering, column)` mapping
/// to the winning `(value, timestamp)`.
pub type LogicalProjection = BTreeMap<(DecoratedKey, Vec<u8>, u16), (Option<Vec<u8>>, i64)>;

/// Logical projection of a partition set: the winning `(value, timestamp)` per
/// `(key, clustering, column)`. Robust to serialization-level field
/// normalization, so it compares merge *results* rather than encodings.
pub fn logical_projection(partitions: &[Partition]) -> LogicalProjection {
    let mut map = BTreeMap::new();
    for p in partitions {
        for row in &p.rows {
            for (col, cell) in &row.cells {
                map.insert(
                    (p.key.clone(), row.clustering.clone(), *col),
                    (cell.value.clone(), cell.timestamp),
                );
            }
        }
    }
    map
}

/// Write one SSTable from `partitions` into `dir`, returning its metadata.
pub fn write_sstable(
    dir: &Path,
    partitions: &[Partition],
    schema: &TableSchema,
) -> SSTableMetadata {
    use crate::flush::{build_serialization_header, FileFlushTarget, FlushTarget};
    use ferrosa_sstable::writer::SSTableWriter;
    use ferrosa_sstable::WriteOptions;

    let mut sorted = partitions.to_vec();
    sorted.sort_by(|a, b| a.key.cmp(&b.key));

    let header = build_serialization_header(schema, &sorted);
    let options = WriteOptions {
        compression: None,
        ..WriteOptions::default()
    };
    let mut writer = SSTableWriter::new(options, header);
    for partition in &sorted {
        writer.add_partition(partition).expect("write partition");
    }
    let output = writer.finish().expect("finish sstable");

    let flush_target = FileFlushTarget::new(dir.to_path_buf()).expect("flush target");
    let _reader = flush_target.flush(output).expect("flush sstable");
    let generation = flush_target.generation();

    let size_bytes: u64 = std::fs::read_dir(dir)
        .expect("read sstable dir")
        .filter_map(|e| e.ok())
        .filter_map(|e| e.metadata().ok().map(|m| m.len()))
        .sum();

    SSTableMetadata {
        id: format!("{generation}"),
        path: dir.to_path_buf(),
        size_bytes,
        min_token: sorted.first().map(|p| p.key.token.0).unwrap_or(0),
        max_token: sorted.last().map(|p| p.key.token.0).unwrap_or(0),
        min_timestamp: 1,
        max_timestamp: 1_000_000,
        partition_count: sorted.len() as u64,
    }
}

/// Read every partition from a compaction output SSTable `generation` in `dir`.
pub fn read_sstable(dir: &Path, generation: &str) -> Vec<Partition> {
    use ferrosa_sstable::io::FileReadAt;
    use ferrosa_sstable::reader::{SSTableComponents, SSTableReader};

    let open = |suffix: &str| {
        FileReadAt::open(dir.join(format!("{generation}-{suffix}"))).expect("open component")
    };
    let read = |suffix: &str| std::fs::read(dir.join(format!("{generation}-{suffix}")));

    let reader = SSTableReader::open(SSTableComponents {
        data: open("Data.db"),
        partitions: open("Partitions.db"),
        rows: open("Rows.db"),
        filter: read("Filter.db").expect("read filter"),
        compression_info: read("CompressionInfo.db").ok(),
        statistics: read("Statistics.db").expect("read statistics"),
    })
    .expect("open output reader");

    reader
        .read_all_partitions()
        .expect("read output partitions")
}

/// Write each group as its own input SSTable under `base_dir`, run a single
/// all-inputs compaction, and return the merged output partitions.
pub fn compact_all(
    base_dir: &Path,
    groups: &[Vec<Partition>],
    schema: &TableSchema,
    table_id: TableId,
) -> Vec<Partition> {
    let inputs: Vec<SSTableMetadata> = groups
        .iter()
        .enumerate()
        .map(|(i, partitions)| {
            let dir = base_dir.join(format!("in_{i}"));
            std::fs::create_dir_all(&dir).expect("create input dir");
            write_sstable(&dir, partitions, schema)
        })
        .collect();

    let output_dir = base_dir.join("out");
    std::fs::create_dir_all(&output_dir).expect("create output dir");

    let task = CompactionTask {
        inputs,
        output_dir: output_dir.clone(),
        schema: schema.clone(),
        table_id,
    };
    let meta = CompactionExecutor::execute_task(&task).expect("compaction must succeed");
    read_sstable(&output_dir, &meta.id)
}

#[cfg(test)]
mod tests {
    use super::super::oracle::oracle_merge_all;
    use super::*;
    use ferrosa_common::key::{DecoratedKey, PartitionKey};
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
    use ferrosa_common::CellValue;
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

    fn test_schema() -> TableSchema {
        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
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

    fn part(key: &str, clustering: &[u8], cell: CellValue) -> Partition {
        Partition {
            key: DecoratedKey::new(PartitionKey::new(key.as_bytes().to_vec())),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![Row {
                clustering: clustering.to_vec(),
                cells: vec![(0, cell)],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(1),
            }],
        }
    }

    #[test]
    fn real_compaction_matches_oracle() {
        let tmp = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let ck = b"\x00\x00\x00\x01".to_vec();

        // Three overlapping SSTables: a write, a newer write, and a tombstone in
        // between, for the same cell — plus an unrelated key that must survive.
        let groups = vec![
            vec![
                part("k0001", &ck, CellValue::live(b"old".to_vec(), 1)),
                part("k0002", &ck, CellValue::live(b"x".to_vec(), 1)),
            ],
            vec![part("k0001", &ck, CellValue::live(b"new".to_vec(), 3))],
            vec![part("k0001", &ck, CellValue::tombstone(2, 100))],
        ];
        let all: Vec<Partition> = groups.iter().flatten().cloned().collect();

        let expected = oracle_merge_all(&all);
        let actual = compact_all(
            tmp.path(),
            &groups,
            &schema,
            crate::TableId::new("test_ks", "test_table"),
        );

        assert_eq!(
            logical_projection(&actual),
            logical_projection(&expected),
            "real compaction output must match the oracle merge"
        );
        // The newest write wins the cell.
        let key = DecoratedKey::new(PartitionKey::new(b"k0001".to_vec()));
        assert_eq!(
            logical_projection(&actual).get(&(key, ck.clone(), 0)),
            Some(&(Some(b"new".to_vec()), 3)),
        );
    }
}
