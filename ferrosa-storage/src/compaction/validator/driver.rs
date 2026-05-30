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

    use crate::compaction::{
        CompactionConfig, CompactionStrategy, SizeTieredStrategy, UcsConfig,
        UnifiedCompactionStrategy,
    };
    use crate::flush::{build_serialization_header, FileFlushTarget, FlushTarget};
    use ferrosa_sstable::writer::SSTableWriter;
    use ferrosa_sstable::WriteOptions;
    use std::collections::HashSet;
    use std::path::{Path, PathBuf};

    /// Write each group as its own SSTable into one shared `dir` (so generations
    /// are unique across inputs), returning their metadata.
    fn write_inputs(
        dir: &Path,
        groups: &[Vec<Partition>],
        schema: &TableSchema,
    ) -> Vec<SSTableMetadata> {
        std::fs::create_dir_all(dir).unwrap();
        let flush_target = FileFlushTarget::new(dir.to_path_buf()).unwrap();
        let mut metas = Vec::new();
        for group in groups {
            let mut sorted = group.clone();
            sorted.sort_by(|a, b| a.key.cmp(&b.key));
            let header = build_serialization_header(schema, &sorted);
            let mut writer = SSTableWriter::new(
                WriteOptions {
                    compression: None,
                    ..WriteOptions::default()
                },
                header,
            );
            for p in &sorted {
                writer.add_partition(p).unwrap();
            }
            flush_target.flush(writer.finish().unwrap()).unwrap();
            let gen = flush_target.generation();
            let size_bytes: u64 = std::fs::read_dir(dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.file_name()
                        .to_string_lossy()
                        .starts_with(&format!("{gen}-"))
                })
                .filter_map(|e| e.metadata().ok().map(|m| m.len()))
                .sum();
            metas.push(SSTableMetadata {
                id: gen.to_string(),
                path: dir.to_path_buf(),
                size_bytes,
                min_token: sorted.first().map(|p| p.key.token.0).unwrap_or(0),
                max_token: sorted.last().map(|p| p.key.token.0).unwrap_or(0),
                min_timestamp: 1,
                max_timestamp: 1_000_000,
                partition_count: sorted.len() as u64,
            });
        }
        metas
    }

    /// Execute whatever `strategy` selects (one round), then read every surviving
    /// SSTable and return the oracle-merged canonical view. Robust to partial
    /// selection: it tests that the strategy's compaction preserves the data.
    fn strategy_view(
        strategy: &dyn CompactionStrategy,
        metas: &[SSTableMetadata],
        schema: &TableSchema,
        table_id: &TableId,
        out_base: &Path,
    ) -> super::LogicalProjection {
        let mut surviving: Vec<(PathBuf, String)> = metas
            .iter()
            .map(|m| (m.path.clone(), m.id.clone()))
            .collect();
        for (i, task) in strategy
            .select(metas, schema, table_id)
            .into_iter()
            .enumerate()
        {
            let out = out_base.join(format!("o{i}"));
            std::fs::create_dir_all(&out).unwrap();
            let consumed: HashSet<String> = task.inputs.iter().map(|m| m.id.clone()).collect();
            let result = CompactionExecutor::execute_task(&CompactionTask {
                inputs: task.inputs.clone(),
                output_dir: out.clone(),
                schema: schema.clone(),
                table_id: table_id.clone(),
            })
            .expect("compaction must succeed");
            surviving.retain(|(_, id)| !consumed.contains(id));
            surviving.push((out, result.id));
        }
        let mut partitions = Vec::new();
        for (dir, id) in &surviving {
            partitions.extend(read_sstable(dir, id));
        }
        logical_projection(&oracle_merge_all(&partitions))
    }

    /// A/B differential: SizeTiered and Unified compaction differ only in which
    /// SSTables they group — never in the merge result — so each must preserve
    /// the same data, equal to the oracle.
    #[test]
    fn stcs_and_ucs_preserve_identical_data() {
        let tmp = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let ck = b"\x00\x00\x00\x01".to_vec();
        let table_id = crate::TableId::new("ab", "t");

        // Four SSTables over the same keys with ascending timestamps.
        let groups: Vec<Vec<Partition>> = (0..4u32)
            .map(|g| {
                (0..4)
                    .map(|k| {
                        part(
                            &format!("k{k:02}"),
                            &ck,
                            CellValue::live(format!("v{g}").into_bytes(), (g + 1) as i64),
                        )
                    })
                    .collect()
            })
            .collect();
        let all: Vec<Partition> = groups.iter().flatten().cloned().collect();
        let metas = write_inputs(&tmp.path().join("inputs"), &groups, &schema);

        let oracle = logical_projection(&oracle_merge_all(&all));
        let placeholder = PathBuf::from(tmp.path());
        let stcs = SizeTieredStrategy::new(CompactionConfig {
            min_threshold: 2,
            max_threshold: 32,
            max_compaction_bytes: 1 << 30,
            bucket_low: 0.5,
            bucket_high: 1.5,
            output_dir: placeholder.clone(),
        });
        let ucs = UnifiedCompactionStrategy::new(UcsConfig {
            fan_factor: 2,
            min_sstable_size: 1,
            max_levels: 32,
            output_dir: placeholder,
        });

        let stcs_view = strategy_view(&stcs, &metas, &schema, &table_id, &tmp.path().join("stcs"));
        let ucs_view = strategy_view(&ucs, &metas, &schema, &table_id, &tmp.path().join("ucs"));

        assert_eq!(stcs_view, oracle, "STCS must preserve the oracle's data");
        assert_eq!(ucs_view, oracle, "UCS must preserve the oracle's data");
        assert_eq!(
            stcs_view, ucs_view,
            "STCS and UCS must preserve identical data"
        );
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
