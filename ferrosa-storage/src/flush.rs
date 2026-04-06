//! FlushTarget abstraction and serialization header construction.
//!
//! This module provides the [`FlushTarget`] trait, which decouples memtable
//! flush logic from the destination: in-memory buffers ([`InMemoryFlushTarget`])
//! for testing, or real files on disk ([`FileFlushTarget`]) for production.
//!
//! [`build_serialization_header`] scans a set of partitions to compute the
//! minimum timestamp, local deletion time, and TTL across all cells, then
//! builds a [`SerializationHeader`] compatible with the SSTable writer.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use ferrosa_index::{IndexKey, RowPosition};

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
    let mut max_timestamp = i64::MIN;
    let mut min_local_deletion_time = NO_DELETION_TIME;
    let mut min_ttl = NO_TTL;

    /// Update `min_timestamp` if `ts` is a real timestamp (not sentinel).
    #[inline]
    fn update_min_ts(min_ts: &mut i64, ts: i64) {
        if ts != NO_TIMESTAMP && (*min_ts == NO_TIMESTAMP || ts < *min_ts) {
            *min_ts = ts;
        }
    }

    /// Update `min_local_deletion_time` if `ldt` is a real value (not sentinel).
    #[inline]
    fn update_min_ldt(min_ldt: &mut i32, ldt: i32) {
        if ldt != NO_DELETION_TIME && (*min_ldt == NO_DELETION_TIME || ldt < *min_ldt) {
            *min_ldt = ldt;
        }
    }

    /// Update `min_ttl` if `ttl` is a real value (not sentinel).
    #[inline]
    fn update_min_ttl(min_ttl_val: &mut i32, ttl: i32) {
        if ttl != NO_TTL && (*min_ttl_val == NO_TTL || ttl < *min_ttl_val) {
            *min_ttl_val = ttl;
        }
    }

    /// Update `max_timestamp` if `ts` is a real timestamp (not sentinel).
    #[inline]
    fn update_max_ts(max_ts: &mut i64, ts: i64) {
        if ts != NO_TIMESTAMP && ts > *max_ts {
            *max_ts = ts;
        }
    }

    /// Scan a row's liveness info and deletion time for min/max values.
    /// The SSTable writer delta-encodes these against the header minimums,
    /// so we must account for them to prevent subtraction overflow.
    #[inline]
    fn scan_row_metadata(
        row: &ferrosa_sstable::types::Row,
        min_ts: &mut i64,
        max_ts: &mut i64,
        min_ldt: &mut i32,
        min_ttl_val: &mut i32,
    ) {
        // Primary key liveness: timestamp, ttl, and local_deletion_time
        // are delta-encoded in the writer.
        if row.primary_key_liveness.has_timestamp() {
            update_min_ts(min_ts, row.primary_key_liveness.timestamp);
            update_max_ts(max_ts, row.primary_key_liveness.timestamp);
        }
        if row.primary_key_liveness.has_ttl() {
            update_min_ttl(min_ttl_val, row.primary_key_liveness.ttl);
            update_min_ldt(min_ldt, row.primary_key_liveness.local_deletion_time);
        }

        // Row-level deletion: marked_for_delete_at and local_deletion_time
        // are delta-encoded in the writer.
        if !row.deletion.is_live() {
            update_min_ts(min_ts, row.deletion.marked_for_delete_at);
            update_max_ts(max_ts, row.deletion.marked_for_delete_at);
            // DeletionTime.local_deletion_time is u32; cast to i32 for comparison
            // with the header field (i32). Values > i32::MAX are sentinel-like and
            // should not lower the minimum.
            let ldt = row.deletion.local_deletion_time;
            if ldt != u32::MAX {
                let ldt_i32 = ldt as i32;
                update_min_ldt(min_ldt, ldt_i32);
            }
        }
    }

    for partition in partitions {
        // Scan static row cells if present
        if let Some(ref static_row) = partition.static_row {
            scan_row_metadata(
                static_row,
                &mut min_timestamp,
                &mut max_timestamp,
                &mut min_local_deletion_time,
                &mut min_ttl,
            );
            for (_, cell) in &static_row.cells {
                update_min_ts(&mut min_timestamp, cell.timestamp);
                update_max_ts(&mut max_timestamp, cell.timestamp);
                update_min_ldt(&mut min_local_deletion_time, cell.local_deletion_time);
                update_min_ttl(&mut min_ttl, cell.ttl);
            }
        }

        // Scan clustered rows: metadata and cells
        for row in &partition.rows {
            scan_row_metadata(
                row,
                &mut min_timestamp,
                &mut max_timestamp,
                &mut min_local_deletion_time,
                &mut min_ttl,
            );
            for (_, cell) in &row.cells {
                update_min_ts(&mut min_timestamp, cell.timestamp);
                update_max_ts(&mut max_timestamp, cell.timestamp);
                update_min_ldt(&mut min_local_deletion_time, cell.local_deletion_time);
                update_min_ttl(&mut min_ttl, cell.ttl);
            }
        }
    }

    // If no real timestamps were found, use safe defaults.
    // Both must be reset symmetrically — a stale NO_TIMESTAMP min with a
    // real max would cause delta-encoding underflow in the SSTable writer.
    if max_timestamp == i64::MIN {
        max_timestamp = i64::MAX;
    }
    if min_timestamp == NO_TIMESTAMP {
        min_timestamp = 0;
    }

    SerializationHeader {
        min_timestamp,
        min_local_deletion_time,
        min_ttl,
        max_timestamp,
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

    /// Returns the generation number of the most recently flushed SSTable.
    ///
    /// Used by `TableStore` to determine which generation number to use
    /// when writing per-SSTable sidecar index files alongside the SSTable.
    /// Returns 0 for in-memory targets where no generation tracking occurs.
    fn last_generation(&self) -> u64 {
        0
    }

    /// Advance the generation counter to at least `min_gen + 1`.
    /// Prevents future flush file names from colliding with compaction output.
    fn advance_generation(&self, _min_gen: u64) {}

    /// Write per-index sidecar files alongside the flushed SSTable.
    ///
    /// Called after [`FlushTarget::flush`] with the same generation number. For each
    /// `(index_name, entries)` pair, writes a `{gen}-{index_name}.sidecar`
    /// file so that sidecar indexes survive process restarts.
    ///
    /// The default implementation is a no-op (in-memory targets do not
    /// persist sidecar files).
    fn write_sidecars(
        &self,
        _generation: u64,
        _sidecars: &HashMap<String, Vec<(IndexKey, RowPosition)>>,
    ) -> Result<()> {
        Ok(())
    }

    /// Write a full-text index (FTI) sidecar file alongside the SSTable.
    ///
    /// Writes `{gen}-FTI-{index_name}.db` to the SSTable directory.
    /// The default implementation is a no-op (in-memory targets do not
    /// persist FTI sidecar files).
    fn write_fti_sidecar(
        &self,
        _generation: u64,
        _index_name: &str,
        _fti_bytes: &[u8],
    ) -> Result<()> {
        Ok(())
    }
}

/// In-memory flush target for testing — wraps output as `SSTableComponents<Vec<u8>>`.
///
/// No filesystem interaction. The flushed data lives entirely in memory.
/// Tracks a monotonic generation counter so that each flush produces a
/// unique ID, matching the behavior of [`FileFlushTarget`].
pub struct InMemoryFlushTarget {
    generation: std::sync::atomic::AtomicU64,
}

impl InMemoryFlushTarget {
    /// Create a new in-memory flush target with the generation counter at 0.
    pub fn new() -> Self {
        Self {
            generation: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

impl Default for InMemoryFlushTarget {
    fn default() -> Self {
        Self::new()
    }
}

impl FlushTarget for InMemoryFlushTarget {
    type Reader = Vec<u8>;

    fn flush(&self, output: SSTableOutput) -> Result<SSTableReader<Vec<u8>>> {
        self.generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        SSTableReader::open(SSTableComponents {
            data: output.data,
            partitions: output.partitions,
            rows: output.rows,
            filter: output.filter,
            compression_info: output.compression_info,
            statistics: output.statistics,
        })
    }

    fn last_generation(&self) -> u64 {
        self.generation.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn advance_generation(&self, min_gen: u64) {
        self.generation
            .fetch_max(min_gen + 1, std::sync::atomic::Ordering::SeqCst);
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
        Self::cleanup_stale_tmp_files(&base_dir);
        Ok(Self {
            base_dir,
            generation: AtomicU64::new(0),
        })
    }

    /// Remove any stale `.tmp` files left behind by a crash during flush.
    fn cleanup_stale_tmp_files(dir: &Path) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().is_some_and(|ext| ext == "tmp") {
                    let _ = std::fs::remove_file(&path);
                }
            }
        }
    }

    /// Returns the current generation counter value (the last generation written).
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }

    /// Create a file flush target that starts after the highest existing generation.
    ///
    /// Scans the directory for existing SSTable files (`{gen}-Data.db`) and
    /// starts the generation counter at `max_gen + 1`. Used by the compaction
    /// executor to avoid overwriting SSTables from prior flushes.
    pub fn new_starting_at(base_dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&base_dir)?;
        Self::cleanup_stale_tmp_files(&base_dir);
        let max_gen = Self::scan_max_generation(&base_dir);
        Ok(Self {
            base_dir,
            generation: AtomicU64::new(max_gen),
        })
    }

    /// Scan a directory for the highest SSTable generation number.
    fn scan_max_generation(dir: &std::path::Path) -> u64 {
        std::fs::read_dir(dir)
            .into_iter()
            .flatten()
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name().to_str()?.to_string();
                if name.ends_with("-Data.db") {
                    name.split('-').next()?.parse::<u64>().ok()
                } else {
                    None
                }
            })
            .max()
            .unwrap_or(0)
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

        // Write to .tmp files first, then atomically rename.
        // If the process crashes mid-flush, only .tmp files exist — no corrupt
        // final SSTables. Stale .tmp files are cleaned up on next startup.
        let tmp = |p: &Path| p.with_extension("db.tmp");
        let toc_tmp = toc_path.with_extension("txt.tmp");

        if let Some(ref ci) = output.compression_info {
            std::fs::write(tmp(&compression_info_path), ci)?;
        }

        std::thread::scope(|s| {
            let handles: Vec<_> = [
                s.spawn(|| std::fs::write(tmp(&data_path), &output.data)),
                s.spawn(|| std::fs::write(tmp(&partitions_path), &output.partitions)),
                s.spawn(|| std::fs::write(tmp(&rows_path), &output.rows)),
                s.spawn(|| std::fs::write(tmp(&filter_path), &output.filter)),
                s.spawn(|| std::fs::write(tmp(&statistics_path), &output.statistics)),
                s.spawn(|| std::fs::write(&toc_tmp, &output.toc)),
            ]
            .into_iter()
            .collect();

            for h in handles {
                h.join().unwrap()?;
            }

            Ok::<(), ferrosa_common::Error>(())
        })?;

        // All tmp files written successfully — atomically rename to final names.
        // rename() is atomic on POSIX (same filesystem).
        std::fs::rename(tmp(&data_path), &data_path)?;
        std::fs::rename(tmp(&partitions_path), &partitions_path)?;
        std::fs::rename(tmp(&rows_path), &rows_path)?;
        std::fs::rename(tmp(&filter_path), &filter_path)?;
        std::fs::rename(tmp(&statistics_path), &statistics_path)?;
        std::fs::rename(&toc_tmp, &toc_path)?;
        if has_compression_info {
            std::fs::rename(tmp(&compression_info_path), &compression_info_path)?;
        }

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

    fn last_generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }

    fn advance_generation(&self, min_gen: u64) {
        self.generation.fetch_max(min_gen + 1, Ordering::SeqCst);
    }

    /// Write per-index sidecar files as `{gen}-{index_name}.sidecar`.
    ///
    /// Skips empty entry lists (no-ops for indexes with no data).
    /// Files that fail to write are logged but do not abort the flush —
    /// a missing sidecar degrades to a full-scan on that index, which is
    /// recoverable. This matches the `load_existing_sstables` "skip corrupt"
    /// policy.
    fn write_sidecars(
        &self,
        generation: u64,
        sidecars: &HashMap<String, Vec<(IndexKey, RowPosition)>>,
    ) -> Result<()> {
        use crate::index::sidecar::SidecarWriter;

        for (index_name, entries) in sidecars {
            if entries.is_empty() {
                continue;
            }
            let path = self
                .base_dir
                .join(format!("{generation}-{index_name}.sidecar"));
            if let Err(e) = SidecarWriter::write(&path, entries) {
                eprintln!("[flush] failed to write sidecar {}: {e}", path.display());
            }
        }
        Ok(())
    }

    fn write_fti_sidecar(&self, generation: u64, index_name: &str, fti_bytes: &[u8]) -> Result<()> {
        let path = self
            .base_dir
            .join(format!("{generation}-FTI-{index_name}.db"));
        std::fs::write(&path, fti_bytes)?;
        Ok(())
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
            extensions: Default::default(),
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
            extensions: Default::default(),
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

        let target = InMemoryFlushTarget::new();
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

    #[test]
    fn flush_does_not_leave_final_files_if_interrupted() {
        // Simulate a crash: write a .tmp file for Data.db but no final files.
        // On next load, the .tmp should be ignored and cleaned up.
        let dir = tempfile::tempdir().unwrap();

        // Create a stale .tmp file as if flush crashed mid-write
        std::fs::write(dir.path().join("1-Data.db.tmp"), b"partial data").unwrap();
        std::fs::write(dir.path().join("1-Partitions.db.tmp"), b"partial").unwrap();

        // These .tmp files must NOT be treated as valid SSTables
        assert!(
            !dir.path().join("1-Data.db").exists(),
            "final Data.db must not exist — flush was interrupted"
        );

        // Creating a new FileFlushTarget should clean up stale .tmp files
        let _target = FileFlushTarget::new(dir.path().to_path_buf()).unwrap();
        assert!(
            !dir.path().join("1-Data.db.tmp").exists(),
            "stale .tmp files must be cleaned up on startup"
        );
        assert!(
            !dir.path().join("1-Partitions.db.tmp").exists(),
            "stale .tmp files must be cleaned up on startup"
        );
    }

    #[test]
    fn flush_uses_atomic_rename() {
        // After a successful flush, no .tmp files should remain
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

        // Final files exist
        assert!(dir.path().join("1-Data.db").exists());
        assert!(dir.path().join("1-Partitions.db").exists());

        // No .tmp files remain
        let tmp_files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "tmp"))
            .collect();
        assert!(
            tmp_files.is_empty(),
            "no .tmp files should remain after successful flush, found: {tmp_files:?}"
        );
    }
}
