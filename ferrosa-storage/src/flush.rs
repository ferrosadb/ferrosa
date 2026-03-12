//! FlushTarget abstraction and serialization header construction.
//!
//! This module provides the [`FlushTarget`] trait, which decouples memtable
//! flush logic from the destination: in-memory buffers ([`InMemoryFlushTarget`])
//! for testing, or real files on disk ([`FileFlushTarget`]) for production.
//!
//! [`build_serialization_header`] scans a set of partitions to compute the
//! minimum timestamp, local deletion time, and TTL across all cells, then
//! builds a [`SerializationHeader`] compatible with the SSTable writer.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use ferrosa_common::schema::TableSchema;
use ferrosa_common::{Result, NO_DELETION_TIME, NO_TIMESTAMP, NO_TTL};
use ferrosa_sstable::io::{FileReadAt, ReadAt};
use ferrosa_sstable::reader::{SSTableComponents, SSTableReader};
use ferrosa_sstable::statistics::SerializationHeader;
use ferrosa_sstable::types::Partition;
use ferrosa_sstable::writer::SSTableOutput;

/// Build a [`SerializationHeader`] by scanning partitions for minimum values.
///
/// The header captures the minimum timestamp, local deletion time, and TTL
/// across all cells in the provided partitions. These minimums enable
/// delta-encoding in the SSTable data file.
///
/// If no cells are present, sentinel values from `ferrosa_common` are used
/// as defaults (NO_TIMESTAMP, NO_DELETION_TIME, NO_TTL).
pub fn build_serialization_header(
    schema: &TableSchema,
    partitions: &[Partition],
) -> SerializationHeader {
    let mut min_timestamp = NO_TIMESTAMP;
    let mut min_local_deletion_time = NO_DELETION_TIME;
    let mut min_ttl = NO_TTL;

    for partition in partitions {
        // Scan static row cells if present
        if let Some(ref static_row) = partition.static_row {
            for (_, cell) in &static_row.cells {
                if cell.timestamp != NO_TIMESTAMP
                    && (min_timestamp == NO_TIMESTAMP || cell.timestamp < min_timestamp)
                {
                    min_timestamp = cell.timestamp;
                }
                if cell.local_deletion_time != NO_DELETION_TIME
                    && (min_local_deletion_time == NO_DELETION_TIME
                        || cell.local_deletion_time < min_local_deletion_time)
                {
                    min_local_deletion_time = cell.local_deletion_time;
                }
                if cell.ttl != NO_TTL && (min_ttl == NO_TTL || cell.ttl < min_ttl) {
                    min_ttl = cell.ttl;
                }
            }
        }

        // Scan clustered row cells
        for row in &partition.rows {
            for (_, cell) in &row.cells {
                if cell.timestamp != NO_TIMESTAMP
                    && (min_timestamp == NO_TIMESTAMP || cell.timestamp < min_timestamp)
                {
                    min_timestamp = cell.timestamp;
                }
                if cell.local_deletion_time != NO_DELETION_TIME
                    && (min_local_deletion_time == NO_DELETION_TIME
                        || cell.local_deletion_time < min_local_deletion_time)
                {
                    min_local_deletion_time = cell.local_deletion_time;
                }
                if cell.ttl != NO_TTL && (min_ttl == NO_TTL || cell.ttl < min_ttl) {
                    min_ttl = cell.ttl;
                }
            }
        }
    }

    SerializationHeader {
        min_timestamp,
        min_local_deletion_time,
        min_ttl,
        key_type: schema.key_type.clone(),
        clustering_types: schema.clustering_types(),
        static_columns: schema
            .static_columns
            .iter()
            .map(|c| (c.name.as_bytes().to_vec(), c.type_name.clone()))
            .collect(),
        regular_columns: schema
            .regular_columns
            .iter()
            .map(|c| (c.name.as_bytes().to_vec(), c.type_name.clone()))
            .collect(),
    }
}

/// Trait abstracting where flushed SSTable component bytes are stored.
///
/// Implementers decide whether the output goes to in-memory buffers or
/// to the filesystem. After writing, the trait returns an `SSTableReader`
/// so the flushed data is immediately queryable.
pub trait FlushTarget {
    /// The reader type used to access component data after flushing.
    type Reader: ReadAt + Send + Sync + 'static;

    /// Write SSTable component bytes to the target and open a reader.
    fn flush(&self, output: SSTableOutput) -> Result<SSTableReader<Self::Reader>>;
}

/// In-memory flush target for testing — wraps output as `SSTableComponents<Vec<u8>>`.
///
/// No filesystem interaction. The flushed data lives entirely in memory.
pub struct InMemoryFlushTarget;

impl FlushTarget for InMemoryFlushTarget {
    type Reader = Vec<u8>;

    fn flush(&self, output: SSTableOutput) -> Result<SSTableReader<Vec<u8>>> {
        SSTableReader::open(SSTableComponents {
            data: output.data,
            partitions: output.partitions,
            rows: output.rows,
            filter: output.filter,
            compression_info: output.compression_info,
            statistics: output.statistics,
        })
    }
}

/// File-based flush target — writes components to numbered files on disk.
///
/// Each flush creates files named `{generation}-{Component}.db` under the
/// configured base directory. Component files are written in parallel using
/// `std::thread::scope`. An [`AtomicU64`] counter tracks the generation
/// number across flushes.
pub struct FileFlushTarget {
    /// Directory where SSTable component files are written.
    base_dir: PathBuf,
    /// Monotonically increasing generation counter.
    generation: AtomicU64,
}

impl FileFlushTarget {
    /// Create a new file flush target writing to the given directory.
    ///
    /// The directory is created if it does not exist. The generation
    /// counter starts at 0; the first flush produces generation 1.
    pub fn new(base_dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&base_dir)?;
        Ok(Self {
            base_dir,
            generation: AtomicU64::new(0),
        })
    }

    /// Returns the current generation counter value (the last generation written).
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }
}

impl FlushTarget for FileFlushTarget {
    type Reader = FileReadAt;

    fn flush(&self, output: SSTableOutput) -> Result<SSTableReader<FileReadAt>> {
        let gen = self.generation.fetch_add(1, Ordering::Relaxed) + 1;
        let base = &self.base_dir;

        let data_path = base.join(format!("{gen}-Data.db"));
        let partitions_path = base.join(format!("{gen}-Partitions.db"));
        let rows_path = base.join(format!("{gen}-Rows.db"));
        let filter_path = base.join(format!("{gen}-Filter.db"));
        let statistics_path = base.join(format!("{gen}-Statistics.db"));
        let toc_path = base.join(format!("{gen}-TOC.txt"));
        let compression_info_path = base.join(format!("{gen}-CompressionInfo.db"));

        let has_compression_info = output.compression_info.is_some();

        // Write compression info outside the thread scope to avoid borrow issues
        if let Some(ref ci) = output.compression_info {
            std::fs::write(&compression_info_path, ci)?;
        }

        std::thread::scope(|s| {
            let handles: Vec<_> = [
                s.spawn(|| std::fs::write(&data_path, &output.data)),
                s.spawn(|| std::fs::write(&partitions_path, &output.partitions)),
                s.spawn(|| std::fs::write(&rows_path, &output.rows)),
                s.spawn(|| std::fs::write(&filter_path, &output.filter)),
                s.spawn(|| std::fs::write(&statistics_path, &output.statistics)),
                s.spawn(|| std::fs::write(&toc_path, &output.toc)),
            ]
            .into_iter()
            .collect();

            for h in handles {
                h.join().unwrap()?;
            }

            Ok::<(), ferrosa_common::Error>(())
        })?;

        // FileReadAt::open returns ferrosa_common::Result — use ? directly
        let data = FileReadAt::open(&data_path)?;
        let partitions = FileReadAt::open(&partitions_path)?;
        let rows = FileReadAt::open(&rows_path)?;
        let filter = std::fs::read(&filter_path)?;
        let statistics = std::fs::read(&statistics_path)?;
        let compression_info = if has_compression_info {
            Some(std::fs::read(&compression_info_path)?)
        } else {
            None
        };

        SSTableReader::open(SSTableComponents {
            data,
            partitions,
            rows,
            filter,
            compression_info,
            statistics,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ferrosa_common::cell::CellValue;
    use ferrosa_common::key::{DecoratedKey, PartitionKey};
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Partition, Row};
    use ferrosa_sstable::{SSTableWriter, WriteOptions};

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
        }
    }

    fn make_key(s: &str) -> DecoratedKey {
        DecoratedKey::new(PartitionKey::new(s.as_bytes().to_vec()))
    }

    fn make_partition(key: &str, value: &[u8], ts: i64) -> Partition {
        Partition {
            key: make_key(key),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![Row {
                clustering: vec![0x00, 0x00, 0x00, 0x01], // Int32Type = 4 bytes
                cells: vec![(0, CellValue::live(value.to_vec(), ts))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(ts),
            }],
        }
    }

    #[test]
    fn build_serialization_header_computes_min_timestamp() {
        let schema = test_schema();
        let partitions = vec![
            make_partition("k1", b"v1", 5000),
            make_partition("k2", b"v2", 3000),
            make_partition("k3", b"v3", 7000),
        ];

        let header = build_serialization_header(&schema, &partitions);

        assert_eq!(header.min_timestamp, 3000);
        assert_eq!(header.min_local_deletion_time, NO_DELETION_TIME);
        assert_eq!(header.min_ttl, NO_TTL);
        assert_eq!(header.key_type, "org.apache.cassandra.db.marshal.UTF8Type");
        assert_eq!(header.regular_columns.len(), 1);
        assert_eq!(header.regular_columns[0].0, b"val");
    }

    #[test]
    fn build_serialization_header_with_static_columns() {
        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![ColumnDefinition {
                name: "s1".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            regular_columns: vec![ColumnDefinition {
                name: "val".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
        };

        let partition = Partition {
            key: make_key("k1"),
            deletion: DeletionTime::LIVE,
            static_row: Some(Row {
                clustering: vec![],
                cells: vec![(0, CellValue::live(b"static_val".to_vec(), 2000))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::NONE,
            }),
            rows: vec![Row {
                clustering: vec![],
                cells: vec![(0, CellValue::live(b"regular_val".to_vec(), 4000))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(4000),
            }],
        };

        let header = build_serialization_header(&schema, &[partition]);

        // min_timestamp should be 2000 (from the static row cell)
        assert_eq!(header.min_timestamp, 2000);
        assert_eq!(header.static_columns.len(), 1);
        assert_eq!(header.static_columns[0].0, b"s1");
        assert_eq!(header.regular_columns.len(), 1);
    }

    #[test]
    fn in_memory_flush_target_round_trip() {
        let schema = test_schema();
        let mut partitions = vec![
            make_partition("k1", b"v1", 5000),
            make_partition("k2", b"v2", 3000),
        ];
        partitions.sort_by(|a, b| a.key.cmp(&b.key));

        let header = build_serialization_header(&schema, &partitions);
        let options = WriteOptions {
            compression: None,
            ..WriteOptions::default()
        };

        let mut writer = SSTableWriter::new(options, header);
        for p in &partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer.finish().unwrap();

        let target = InMemoryFlushTarget;
        let reader = target.flush(output).unwrap();

        // Verify we can read back both partitions
        for p in &partitions {
            let got = reader.get_partition(&p.key).unwrap().expect("partition");
            assert_eq!(got.key.key.as_bytes(), p.key.key.as_bytes());
            assert_eq!(got.rows.len(), 1);
        }
    }

    #[test]
    fn file_flush_target_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let mut partitions = vec![
            make_partition("k1", b"v1", 5000),
            make_partition("k2", b"v2", 3000),
        ];
        partitions.sort_by(|a, b| a.key.cmp(&b.key));

        let header = build_serialization_header(&schema, &partitions);
        let options = WriteOptions {
            compression: None,
            ..WriteOptions::default()
        };

        let mut writer = SSTableWriter::new(options, header);
        for p in &partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer.finish().unwrap();

        let target = FileFlushTarget::new(dir.path().to_path_buf()).unwrap();
        let reader = target.flush(output).unwrap();

        // Verify we can read back both partitions
        for p in &partitions {
            let got = reader.get_partition(&p.key).unwrap().expect("partition");
            assert_eq!(got.key.key.as_bytes(), p.key.key.as_bytes());
            assert_eq!(got.rows.len(), 1);
        }
    }

    #[test]
    fn file_flush_target_creates_component_files() {
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();
        let mut partitions = vec![make_partition("k1", b"v1", 5000)];
        partitions.sort_by(|a, b| a.key.cmp(&b.key));

        let header = build_serialization_header(&schema, &partitions);
        let options = WriteOptions {
            compression: None,
            ..WriteOptions::default()
        };

        let mut writer = SSTableWriter::new(options, header);
        for p in &partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer.finish().unwrap();

        let target = FileFlushTarget::new(dir.path().to_path_buf()).unwrap();
        let _reader = target.flush(output).unwrap();

        // Verify component files were created
        assert!(dir.path().join("1-Data.db").exists());
        assert!(dir.path().join("1-Partitions.db").exists());
        assert!(dir.path().join("1-Rows.db").exists());
        assert!(dir.path().join("1-Filter.db").exists());
        assert!(dir.path().join("1-Statistics.db").exists());
        assert!(dir.path().join("1-TOC.txt").exists());
        // No compression, so CompressionInfo.db should not exist
        assert!(!dir.path().join("1-CompressionInfo.db").exists());
    }

    #[test]
    fn file_flush_target_increments_generation() {
        let dir = tempfile::tempdir().unwrap();
        let schema = test_schema();

        let target = FileFlushTarget::new(dir.path().to_path_buf()).unwrap();
        assert_eq!(target.generation(), 0);

        // First flush
        let mut partitions = vec![make_partition("k1", b"v1", 5000)];
        partitions.sort_by(|a, b| a.key.cmp(&b.key));
        let header = build_serialization_header(&schema, &partitions);
        let options = WriteOptions {
            compression: None,
            ..WriteOptions::default()
        };
        let mut writer = SSTableWriter::new(options.clone(), header);
        for p in &partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer.finish().unwrap();
        let _reader1 = target.flush(output).unwrap();
        assert_eq!(target.generation(), 1);

        // Second flush
        let mut partitions2 = vec![make_partition("k2", b"v2", 6000)];
        partitions2.sort_by(|a, b| a.key.cmp(&b.key));
        let header2 = build_serialization_header(&schema, &partitions2);
        let mut writer2 = SSTableWriter::new(options, header2);
        for p in &partitions2 {
            writer2.add_partition(p).unwrap();
        }
        let output2 = writer2.finish().unwrap();
        let _reader2 = target.flush(output2).unwrap();
        assert_eq!(target.generation(), 2);

        // Verify both generations have files
        assert!(dir.path().join("1-Data.db").exists());
        assert!(dir.path().join("2-Data.db").exists());
    }
}
