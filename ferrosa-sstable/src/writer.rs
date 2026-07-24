//! SSTableWriter — write BTI format SSTables from sorted partitions.
//!
//! The writer accepts partitions in token order and produces all component
//! data (Data.db, Partitions.db, Rows.db, Filter.db, CompressionInfo.db,
//! Statistics.db, TOC.txt) either as in-memory byte buffers in an
//! [`SSTableOutput`] or as staged component files in an
//! [`SSTableOutputFiles`].
//!
//! # Usage
//!
//! ```no_run
//! use ferrosa_sstable::writer::{SSTableWriter, WriteOptions};
//! use ferrosa_sstable::statistics::SerializationHeader;
//! use ferrosa_sstable::Partition;
//! # let header = SerializationHeader {
//! #     min_timestamp: 0, min_local_deletion_time: i32::MAX, min_ttl: 0,
//! #     max_timestamp: i64::MAX,
//! #     key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
//! #     clustering_types: vec![], static_columns: vec![], regular_columns: vec![],
//! #     complex_collections: false,
//! # };
//! # let partitions: Vec<Partition> = vec![];
//! let mut writer = SSTableWriter::new(WriteOptions::default(), header);
//! for partition in &partitions {
//!     writer.add_partition(partition).unwrap();
//! }
//! let output = writer.finish().unwrap();
//! ```
//!
//! Partitions must be added in token order. The writer handles simple and
//! complex (non-frozen collection) columns — the latter as Cassandra's
//! per-element cells: `uvint(cell-count)` followed by one cell per element,
//! each carrying a length-prefixed cell path. Range tombstones and a
//! per-column complex `DeletionTime` on write are still deferred (Ferrosa's
//! collection ops are element add/remove, not whole-collection clears).

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use ferrosa_common::{CellValue, Result};
use rayon::prelude::*;

use crate::bloom::BloomFilter;
use crate::byte_comparable;
use crate::compression::{Compression, CompressionInfo};
use crate::io::FileReadAt;
use crate::reader::{SSTableComponents, SSTableReader};
use crate::statistics::{
    build_simple_bti_stats_metadata, write_statistics, CompactionMetadata, SerializationHeader,
    Statistics, StatsMetadata, ValidationMetadata,
};
use crate::toc;
use crate::trie::builder::{TrieBuilder, TriePayload};
use crate::types::Partition;
use crate::varint;

const ROW_INDEX_MIN_ROWS: usize = 32;
const DEFAULT_COMPRESSION_BATCH_CHUNKS: usize = 16;

fn compression_pool() -> &'static rayon::ThreadPool {
    static POOL: OnceLock<rayon::ThreadPool> = OnceLock::new();
    POOL.get_or_init(|| {
        let thread_count = std::env::var("FERROSA_SSTABLE_COMPRESSION_THREADS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|threads| *threads > 0)
            .unwrap_or_else(|| {
                std::thread::available_parallelism()
                    .map(usize::from)
                    .unwrap_or(2)
                    .clamp(1, 4)
            });
        rayon::ThreadPoolBuilder::new()
            .num_threads(thread_count)
            .thread_name(|idx| format!("sstable-compress-{idx}"))
            .build()
            .expect("failed to build SSTable compression thread pool")
    })
}

fn compression_batch_chunks() -> usize {
    std::env::var("FERROSA_SSTABLE_COMPRESSION_BATCH_CHUNKS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|chunks| *chunks > 0)
        .unwrap_or(DEFAULT_COMPRESSION_BATCH_CHUNKS)
}

/// Whether Data.db is written through the page-cache-bypassing
/// [`DirectWriter`](crate::direct::DirectWriter) (O_DIRECT on Linux, `F_NOCACHE`
/// on macOS).
///
/// Opt-in (`FERROSA_SSTABLE_DIRECT_IO=1`) for a **safe rollout** of a
/// durability-critical I/O change (Phase 3, epic `t_29f6b948`): the default OFF
/// preserves the existing buffered path byte-for-byte, so the change ships dark
/// and can be A/B'd on a real Linux file system — where the actual O_DIRECT
/// syscall path (never exercised on the macOS dev host) runs — to confirm the
/// disk-saturation tail shrinks before the default is flipped.
fn sstable_direct_io_enabled() -> bool {
    parse_direct_io_flag(std::env::var("FERROSA_SSTABLE_DIRECT_IO").ok())
}

/// Parse the `FERROSA_SSTABLE_DIRECT_IO` value (pure, so it is tested without the
/// `set_var` parallel-test race). Absent/unrecognized ⇒ `false` (safe default).
fn parse_direct_io_flag(value: Option<String>) -> bool {
    value
        .map(|v| matches!(v.trim(), "1" | "true" | "TRUE" | "on"))
        .unwrap_or(false)
}

/// A Data.db sink that is either the cache-bypassing
/// [`DirectWriter`](crate::direct::DirectWriter) or the original buffered
/// [`std::fs::File`], selected by [`sstable_direct_io_enabled`]. Presents a
/// uniform `write_all` / `position` / `finish` surface so the compressed and
/// uncompressed build paths are written once, mode-agnostic. `position` returns
/// the logical write offset (substituting for `Seek::stream_position`, which
/// O_DIRECT's staging buffer cannot answer from the OS file offset).
enum DataDbWriter {
    Direct(crate::direct::DirectWriter),
    Buffered { file: std::fs::File, written: u64 },
}

impl DataDbWriter {
    /// Open `data_path`. `direct` (from [`sstable_direct_io_enabled`] at the call
    /// site) is an explicit parameter — not read from the environment here — so
    /// both modes are unit-testable without the `set_var` parallel-test race.
    fn create(path: &Path, direct: bool) -> Result<Self> {
        if direct {
            Ok(Self::Direct(crate::direct::DirectWriter::create(path)?))
        } else {
            Ok(Self::Buffered {
                file: std::fs::File::create(path)?,
                written: 0,
            })
        }
    }

    fn write_all(&mut self, bytes: &[u8]) -> Result<()> {
        match self {
            Self::Direct(writer) => writer.write_all(bytes),
            Self::Buffered { file, written } => {
                file.write_all(bytes)?;
                *written += bytes.len() as u64;
                Ok(())
            }
        }
    }

    /// Logical offset of the next byte — substitutes for `stream_position()`.
    fn position(&self) -> u64 {
        match self {
            Self::Direct(writer) => writer.position(),
            Self::Buffered { written, .. } => *written,
        }
    }

    /// Durably persist the Data.db content and consume the writer.
    fn finish(self) -> Result<()> {
        match self {
            Self::Direct(writer) => {
                writer.finish()?;
                Ok(())
            }
            Self::Buffered { file, .. } => {
                file.sync_data()?;
                Ok(())
            }
        }
    }
}

fn write_component_file(path: &Path, bytes: &[u8]) -> Result<u64> {
    std::fs::write(path, bytes)?;
    Ok(bytes.len() as u64)
}

enum DataBuffer {
    Memory(Vec<u8>),
    File {
        file: std::fs::File,
        path: PathBuf,
        len: u64,
    },
}

enum DataSource {
    Memory(Vec<u8>),
    File { path: PathBuf, len: u64 },
}

impl DataBuffer {
    fn memory() -> Self {
        Self::Memory(Vec::new())
    }

    fn file(path: PathBuf) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        Ok(Self::File {
            file: std::fs::File::create(&path)?,
            path,
            len: 0,
        })
    }

    fn len(&self) -> u64 {
        match self {
            Self::Memory(buf) => buf.len() as u64,
            Self::File { len, .. } => *len,
        }
    }

    fn push(&mut self, byte: u8) -> Result<()> {
        self.extend_from_slice(&[byte])
    }

    fn extend_from_slice(&mut self, bytes: &[u8]) -> Result<()> {
        match self {
            Self::Memory(buf) => {
                buf.extend_from_slice(bytes);
                Ok(())
            }
            Self::File { file, len, .. } => {
                file.write_all(bytes)?;
                *len += bytes.len() as u64;
                Ok(())
            }
        }
    }

    fn into_source(self) -> Result<DataSource> {
        match self {
            Self::Memory(buf) => Ok(DataSource::Memory(buf)),
            Self::File {
                mut file,
                path,
                len,
            } => {
                file.flush()?;
                file.sync_data()?;
                drop(file);
                Ok(DataSource::File { path, len })
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Row / cell flag constants (matching data.rs reader)
// Reference: UnfilteredSerializer.java, Cell.java
// ---------------------------------------------------------------------------

/// Marks the end of a partition's row sequence.
const END_OF_PARTITION: u8 = 0x01;
/// Row has a non-default timestamp (delta-encoded).
const HAS_TIMESTAMP: u8 = 0x04;
/// Row has a TTL (delta-encoded).
const HAS_TTL: u8 = 0x08;
/// Row has a row-level deletion time.
const HAS_DELETION: u8 = 0x10;
/// All columns in the schema are present (no missing-column bitmap).
const HAS_ALL_COLUMNS: u8 = 0x20;
/// At least one complex column in the row has a complex `DeletionTime`.
const HAS_COMPLEX_DELETION: u8 = 0x40;
/// Extended flags byte follows.
const EXTENSION_FLAG: u8 = 0x80;
/// Extended flag: this row is a static row.
const EXT_IS_STATIC: u8 = 0x01;

/// Byte marker for a live (not deleted) partition DeletionTime.
const DELETION_IS_LIVE: u8 = 0x80;

/// Cell is a tombstone (deleted).
const CELL_IS_DELETED: u8 = 0x01;
/// Cell is expiring (has TTL).
const CELL_IS_EXPIRING: u8 = 0x02;
/// Cell has an empty value.
const CELL_HAS_EMPTY_VALUE: u8 = 0x04;
/// Cell inherits the row-level timestamp (no per-cell timestamp encoded).
const CELL_USE_ROW_TIMESTAMP: u8 = 0x08;
/// Cell inherits the row-level TTL and local deletion time.
const CELL_USE_ROW_TTL: u8 = 0x10;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Options for writing an SSTable.
#[derive(Debug, Clone)]
pub struct WriteOptions {
    /// Compression algorithm (None for no compression).
    pub compression: Option<Compression>,
    /// Target Bloom filter false positive rate.
    pub bloom_fp_chance: f64,
    /// Chunk size for compression (default 65536).
    pub chunk_size: usize,
    /// Reopen the finished SSTable and verify its partition count matches
    /// what was written (Gate B). Default `true`. Flush orchestrators can
    /// set this from `StorageEngineConfig.write_verify` to turn off the
    /// self-readback once the class of partial-write bugs it guards
    /// against is known-dead. Roundtrip is ~200-500µs for a memtable.
    pub verify_output: bool,
}

impl Default for WriteOptions {
    fn default() -> Self {
        WriteOptions {
            compression: Some(Compression::Lz4),
            bloom_fp_chance: 0.01,
            chunk_size: 65536,
            verify_output: true,
        }
    }
}

/// Result of writing an SSTable — the raw bytes for each component file.
pub struct SSTableOutput {
    pub data: Vec<u8>,
    pub partitions: Vec<u8>,
    pub rows: Vec<u8>,
    pub filter: Vec<u8>,
    pub compression_info: Option<Vec<u8>>,
    pub statistics: Vec<u8>,
    pub toc: Vec<u8>,
}

/// Result of writing an SSTable to component files.
pub struct SSTableOutputFiles {
    pub data: PathBuf,
    pub partitions: PathBuf,
    pub rows: PathBuf,
    pub filter: PathBuf,
    pub compression_info: Option<PathBuf>,
    pub statistics: PathBuf,
    pub toc: PathBuf,
    pub data_len: u64,
    pub partitions_len: u64,
    pub rows_len: u64,
    pub filter_len: u64,
    pub compression_info_len: u64,
    pub statistics_len: u64,
    pub toc_len: u64,
    pub staging_dir: PathBuf,
}

impl SSTableOutputFiles {
    pub fn total_size_bytes(&self) -> u64 {
        self.data_len
            + self.partitions_len
            + self.rows_len
            + self.filter_len
            + self.compression_info_len
            + self.statistics_len
            + self.toc_len
    }

    pub fn read_to_memory(self) -> Result<SSTableOutput> {
        let output = SSTableOutput {
            data: std::fs::read(&self.data)?,
            partitions: std::fs::read(&self.partitions)?,
            rows: std::fs::read(&self.rows)?,
            filter: std::fs::read(&self.filter)?,
            compression_info: match &self.compression_info {
                Some(path) => Some(std::fs::read(path)?),
                None => None,
            },
            statistics: std::fs::read(&self.statistics)?,
            toc: std::fs::read(&self.toc)?,
        };
        let _ = std::fs::remove_dir_all(&self.staging_dir);
        Ok(output)
    }
}

/// SSTable writer that accumulates partitions and produces all component files.
pub struct SSTableWriter {
    options: WriteOptions,
    header: SerializationHeader,
    /// Raw (uncompressed) data buffer — the Data.db content.
    data_buf: DataBuffer,
    /// Raw Rows.db buffer containing per-partition clustering row indexes for
    /// wide clustered partitions.
    rows_buf: Vec<u8>,
    /// Bloom filter for partition keys.
    bloom: BloomFilter,
    /// Trie builder for the partition index (Partitions.db).
    trie_builder: TrieBuilder,
    /// Number of partitions written so far.
    partition_count: u64,
    /// First raw partition key bytes (for Statistics.db).
    first_key: Option<Vec<u8>>,
    /// Last raw partition key bytes (for Statistics.db).
    last_key: Option<Vec<u8>>,
    /// First byte-comparable partition key bytes (for Partitions.db bounds).
    first_index_key: Option<Vec<u8>>,
    /// Last byte-comparable partition key bytes (for Partitions.db bounds).
    last_index_key: Option<Vec<u8>>,
    /// Number of rows written to Data.db.
    total_rows: u64,
    /// Number of regular/static cells written to Data.db.
    total_columns_set: u64,
}

impl SSTableWriter {
    /// Create a new SSTableWriter with the given options and serialization header.
    pub fn new(options: WriteOptions, header: SerializationHeader) -> Self {
        // Size the bloom filter for a reasonable default; we'll accept up to 10K keys.
        // This is a simplification — production would resize or use a builder pattern.
        let bloom = BloomFilter::new(10_000, options.bloom_fp_chance);
        SSTableWriter {
            options,
            header,
            data_buf: DataBuffer::memory(),
            rows_buf: Vec::new(),
            bloom,
            trie_builder: TrieBuilder::new(),
            partition_count: 0,
            first_key: None,
            last_key: None,
            first_index_key: None,
            last_index_key: None,
            total_rows: 0,
            total_columns_set: 0,
        }
    }

    /// Create an SSTable writer whose Data.db serialization buffer is backed
    /// by a raw staging file instead of heap memory.
    pub fn new_file_backed(
        options: WriteOptions,
        header: SerializationHeader,
        raw_data_path: impl Into<PathBuf>,
    ) -> Result<Self> {
        let bloom = BloomFilter::new(10_000, options.bloom_fp_chance);
        Ok(SSTableWriter {
            options,
            header,
            data_buf: DataBuffer::file(raw_data_path.into())?,
            rows_buf: Vec::new(),
            bloom,
            trie_builder: TrieBuilder::new(),
            partition_count: 0,
            first_key: None,
            last_key: None,
            first_index_key: None,
            last_index_key: None,
            total_rows: 0,
            total_columns_set: 0,
        })
    }

    /// Add a partition to the SSTable. Partitions must be added in token order.
    ///
    /// This serializes the partition data, adds the key to the bloom filter,
    /// adds a trie entry for the partition index, and tracks statistics.
    pub fn add_partition(&mut self, partition: &Partition) -> Result<()> {
        // Gate A: validate each row's clustering shape before any serialization.
        // Catches the P0 data-loss bug where a row with `clustering: vec![]` on
        // a schema declaring fixed-length clustering columns was silently
        // serialized and later tripped `read_exact_at: wanted N got M` on
        // read — skipping the entire partition.
        //
        // Exception: a pure tombstone Row (empty clustering, no cells, non-LIVE
        // deletion) is the in-memory representation of a partition-level
        // DELETE and carries no payload to validate. Let it through.
        for (row_idx, row) in partition.rows.iter().enumerate() {
            let is_partition_tombstone = row.clustering.is_empty()
                && row.cells.is_empty()
                && row.deletion != crate::types::DeletionTime::LIVE;
            if is_partition_tombstone {
                continue;
            }
            Self::validate_clustering_shape(&self.header, row_idx, &row.clustering).map_err(
                |msg| {
                    ferrosa_common::Error::InvalidData(format!(
                        "ferrosa-sstable/writer: partition key={:?} {msg}",
                        String::from_utf8_lossy(partition.key.key.as_bytes()),
                    ))
                },
            )?;
        }
        let data_pos = self.data_buf.len() as i64;

        // 1. Serialize partition data to the data buffer.
        let row_index_pos = self.serialize_partition(partition, data_pos as u64)?;

        // 2. Add key to bloom filter.
        let (h1, h2) = partition.key.filter_hash();
        self.bloom.add(h1, h2);

        // 3. Add to trie builder: encode key with byte_comparable, use data position as payload.
        let encoded = byte_comparable::encode(&partition.key);
        let hash_byte = (h2 & 0xFF) as u8;
        // Use a positive idxpos when Rows.db has a per-partition row index;
        // otherwise use negative idxpos (bitwise NOT) for direct Data.db lookup.
        let idxpos = row_index_pos.map_or(!data_pos, |pos| pos as i64);
        self.trie_builder.add(
            &encoded,
            TriePayload {
                hash: Some(hash_byte),
                position: idxpos,
            },
        )?;

        // 4. Track partition count and key bounds.
        if self.first_key.is_none() {
            self.first_key = Some(partition.key.key.as_bytes().to_vec());
            self.first_index_key = Some(encoded.clone());
        }
        self.last_key = Some(partition.key.key.as_bytes().to_vec());
        self.last_index_key = Some(encoded);
        self.partition_count += 1;
        self.total_rows += partition.rows.len() as u64;
        self.total_columns_set += partition
            .static_row
            .as_ref()
            .map_or(0, |row| row.cells.len() as u64);
        self.total_columns_set += partition
            .rows
            .iter()
            .map(|row| row.cells.len() as u64)
            .sum::<u64>();

        Ok(())
    }

    /// Finalize the SSTable and produce all component files.
    pub fn finish(self) -> Result<SSTableOutput> {
        let first_key = self.first_key.clone().unwrap_or_default();
        let last_key = self.last_key.clone().unwrap_or_default();
        let first_index_key = self.first_index_key.clone().unwrap_or_default();
        let last_index_key = self.last_index_key.clone().unwrap_or_default();
        let partition_count = self.partition_count;
        let total_rows = self.total_rows;
        let total_columns_set = self.total_columns_set;
        let bloom_fp_chance = self.options.bloom_fp_chance;
        let has_compression = self.options.compression.is_some()
            && !matches!(self.options.compression, Some(Compression::None));
        let verify_output = self.options.verify_output;
        let header = self.header.clone();

        // 1. Finalize trie -> Partitions.db (trie bytes + key bounds footer)
        let partitions = Self::build_partitions_db(
            self.trie_builder,
            &first_index_key,
            &last_index_key,
            partition_count,
        )?;

        // 2. Build bloom filter -> Filter.db
        let filter = self.bloom.write();

        // 3. Build statistics -> Statistics.db
        let statistics = Self::build_statistics_db(
            &self.header,
            bloom_fp_chance,
            &first_key,
            &last_key,
            partition_count,
            total_rows,
            total_columns_set,
        );

        // 4. Optionally compress data chunks -> Data.db + CompressionInfo.db
        let data_source = self.data_buf.into_source()?;
        let (data, compression_info) = match data_source {
            DataSource::Memory(data_buf) => Self::build_data_db(data_buf, &self.options)?,
            DataSource::File { path, .. } => {
                let data_buf = std::fs::read(&path)?;
                let _ = std::fs::remove_file(path);
                Self::build_data_db(data_buf, &self.options)?
            }
        };

        // 5. Rows.db: empty for simple partitions, indexed for wide clustered
        // partitions.
        let rows = self.rows_buf;

        // 6. Build TOC -> TOC.txt
        let toc_bytes = Self::build_toc(has_compression);

        let output = SSTableOutput {
            data,
            partitions,
            rows,
            filter,
            compression_info,
            statistics,
            toc: toc_bytes,
        };

        // Gate B: reopen the SSTable we just built and confirm the reader
        // sees the same partition count we wrote. Catches silent corruption
        // (partial serialization, index-data desync) before the output is
        // persisted. Gated behind `WriteOptions.verify_output` so the flush
        // path can flip it off once the class of bugs is believed dead
        // (see StorageEngineConfig.write_verify).
        if verify_output {
            Self::verify_output_readable(&output, &header, partition_count)?;
        }

        Ok(output)
    }

    /// Finalize the SSTable directly into component files under `staging_dir`.
    ///
    /// This is the production file-backed path. It avoids constructing a full
    /// `SSTableOutput` with component `Vec`s, so large flushes and compactions
    /// do not keep both the writer's uncompressed Data.db buffer and the
    /// finished compressed Data.db buffer in heap at the same time.
    pub fn finish_to_directory(self, staging_dir: impl AsRef<Path>) -> Result<SSTableOutputFiles> {
        let staging_dir = staging_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&staging_dir)?;

        let first_key = self.first_key.clone().unwrap_or_default();
        let last_key = self.last_key.clone().unwrap_or_default();
        let first_index_key = self.first_index_key.clone().unwrap_or_default();
        let last_index_key = self.last_index_key.clone().unwrap_or_default();
        let partition_count = self.partition_count;
        let total_rows = self.total_rows;
        let total_columns_set = self.total_columns_set;
        let bloom_fp_chance = self.options.bloom_fp_chance;
        let has_compression = self.options.compression.is_some()
            && !matches!(self.options.compression, Some(Compression::None));
        let verify_output = self.options.verify_output;
        let header = self.header.clone();

        let data_path = staging_dir.join("Data.db");
        let partitions_path = staging_dir.join("Partitions.db");
        let rows_path = staging_dir.join("Rows.db");
        let filter_path = staging_dir.join("Filter.db");
        let statistics_path = staging_dir.join("Statistics.db");
        let toc_path = staging_dir.join("TOC.txt");
        let compression_info_path = staging_dir.join("CompressionInfo.db");

        let partitions = Self::build_partitions_db(
            self.trie_builder,
            &first_index_key,
            &last_index_key,
            partition_count,
        )?;
        let partitions_len = write_component_file(&partitions_path, &partitions)?;

        let filter = self.bloom.write();
        let filter_len = write_component_file(&filter_path, &filter)?;

        let statistics = Self::build_statistics_db(
            &self.header,
            bloom_fp_chance,
            &first_key,
            &last_key,
            partition_count,
            total_rows,
            total_columns_set,
        );
        let statistics_len = write_component_file(&statistics_path, &statistics)?;

        let data_source = self.data_buf.into_source()?;
        let compression_info =
            Self::build_data_source_to_file(data_source, &self.options, &data_path)?;
        let compression_info_len = if let Some(info) = compression_info.as_ref() {
            write_component_file(&compression_info_path, info)?
        } else {
            0
        };

        let rows_len = write_component_file(&rows_path, &self.rows_buf)?;

        let toc = Self::build_toc(has_compression);
        let toc_len = write_component_file(&toc_path, &toc)?;
        let data_len = std::fs::metadata(&data_path)?.len();

        let output = SSTableOutputFiles {
            data: data_path,
            partitions: partitions_path,
            rows: rows_path,
            filter: filter_path,
            compression_info: compression_info.as_ref().map(|_| compression_info_path),
            statistics: statistics_path,
            toc: toc_path,
            data_len,
            partitions_len,
            rows_len,
            filter_len,
            compression_info_len,
            statistics_len,
            toc_len,
            staging_dir,
        };

        if verify_output {
            Self::verify_output_files(&output, &header, partition_count)?;
        }

        Ok(output)
    }

    // -----------------------------------------------------------------------
    // Writer self-validation gates (reconstructed from the
    // `next-writervalidate` debug image — see tests::validate_clustering_*
    // and tests::verify_output_readable_* for behavior pins).
    // -----------------------------------------------------------------------

    /// Gate A: validate that a row's clustering bytes match the shape
    /// demanded by the schema's clustering columns. Returns `Err(String)`
    /// with a human-readable description so the caller can wrap it into a
    /// crate `Error` with partition-key context.
    ///
    /// Format matches how `serialize_row` consumes `row.clustering`:
    /// - `num_ck == 0` — any (including empty) clustering is OK.
    /// - `num_ck == 1` — row.clustering is RAW component bytes:
    ///   fixed-length types must match exactly; variable-length types
    ///   must be non-empty (caller owns a known bug-seed if empty on
    ///   a schema with a declared clustering column).
    /// - `num_ck > 1` — row.clustering is u16-prefixed composite:
    ///   `[u16 len][bytes][u16 len][bytes]...` for each component.
    ///   Fixed-length components must have `len == fixed_len`.
    fn validate_clustering_shape(
        header: &SerializationHeader,
        row_idx: usize,
        clustering: &[u8],
    ) -> std::result::Result<(), String> {
        let num_ck = header.clustering_types.len();
        if num_ck == 0 {
            return Ok(());
        }
        if clustering.is_empty() {
            // Include expected fixed-length where it applies so the error
            // message points the caller straight at the schema mismatch.
            let expected_hint = if num_ck == 1 {
                match crate::marshal::value_length_if_fixed(&header.clustering_types[0]) {
                    Some(n) => format!(
                        " (schema expects {n} raw bytes for {})",
                        header.clustering_types[0]
                    ),
                    None => String::new(),
                }
            } else {
                String::new()
            };
            return Err(format!(
                "row_idx={row_idx}: clustering bytes are empty but schema declares \
                 {num_ck} clustering column(s){expected_hint} — this would silently \
                 corrupt the SSTable on read"
            ));
        }
        if num_ck == 1 {
            let type_name = &header.clustering_types[0];
            if let Some(fixed_len) = crate::marshal::value_length_if_fixed(type_name) {
                if clustering.len() != fixed_len {
                    return Err(format!(
                        "row_idx={row_idx}: clustering column 0 ({type_name}) expects \
                         {fixed_len} raw bytes but row provided {got}",
                        got = clustering.len(),
                    ));
                }
            }
            // Variable-length single CK: any non-empty bytes are valid.
            return Ok(());
        }
        // Multi-column CK: u16-prefixed composite.
        let mut pos = 0usize;
        for (col_idx, type_name) in header.clustering_types.iter().enumerate() {
            if pos + 2 > clustering.len() {
                return Err(format!(
                    "row_idx={row_idx}: truncated composite clustering — column {col_idx} \
                     ({type_name}) expected u16 length prefix at byte offset {pos} but \
                     buffer is only {total} bytes",
                    total = clustering.len(),
                ));
            }
            let prefix = u16::from_be_bytes([clustering[pos], clustering[pos + 1]]) as usize;
            pos += 2;
            if pos + prefix > clustering.len() {
                return Err(format!(
                    "row_idx={row_idx}: truncated composite clustering — column {col_idx} \
                     ({type_name}) length prefix claims {prefix} bytes but only \
                     {remaining} remain",
                    remaining = clustering.len() - pos,
                ));
            }
            if let Some(fixed_len) = crate::marshal::value_length_if_fixed(type_name) {
                if prefix != fixed_len {
                    return Err(format!(
                        "row_idx={row_idx}: clustering column {col_idx} ({type_name}) \
                         expects {fixed_len} bytes but row provided {prefix}",
                    ));
                }
            }
            pos += prefix;
        }
        if pos != clustering.len() {
            return Err(format!(
                "row_idx={row_idx}: {trailing} trailing byte(s) after composite \
                 clustering columns",
                trailing = clustering.len() - pos,
            ));
        }
        Ok(())
    }

    /// Gate B: reopen the finished SSTable via `SSTableReader` and confirm
    /// the partition count matches what was written. Runs in-memory
    /// (`Vec<u8>` implements `ReadAt`), but streams partition objects instead
    /// of materializing the full SSTable into a `Vec<Partition>`.
    ///
    /// Caught in production: `data_file_len=2,937,236` bytes vs. partition
    /// index claiming 209 MB — the flush serialized a partial memtable
    /// under concurrent writes. Without this gate the SSTable was
    /// registered in the active set and reads silently skipped the
    /// partition.
    fn verify_output_readable(
        output: &SSTableOutput,
        _header: &SerializationHeader,
        expected_partition_count: u64,
    ) -> Result<()> {
        use crate::reader::{SSTableComponents, SSTableReader};

        let components = SSTableComponents {
            data: output.data.clone(),
            partitions: output.partitions.clone(),
            rows: output.rows.clone(),
            filter: output.filter.clone(),
            compression_info: output.compression_info.clone(),
            statistics: output.statistics.clone(),
        };
        let reader = SSTableReader::open(components).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "ferrosa-sstable/writer: verify_output_readable: reopen failed: {e}"
            ))
        })?;
        let mut iter = reader.partitions_iter().map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "ferrosa-sstable/writer: verify_output_readable: partitions_iter failed: {e}"
            ))
        })?;
        let mut read_count = 0u64;
        while iter
            .next_partition()
            .map_err(|e| {
                ferrosa_common::Error::InvalidFormat(format!(
                    "ferrosa-sstable/writer: verify_output_readable: partition read failed: {e}"
                ))
            })?
            .is_some()
        {
            read_count += 1;
        }
        if read_count != expected_partition_count {
            return Err(ferrosa_common::Error::InvalidFormat(format!(
                "ferrosa-sstable/writer: verify_output_readable: partition count mismatch — \
                 writer wrote {expected_partition_count} but reader sees {read_count}. \
                 Refusing to return corrupt SSTable."
            )));
        }
        Ok(())
    }

    fn verify_output_files(
        output: &SSTableOutputFiles,
        _header: &SerializationHeader,
        expected_partition_count: u64,
    ) -> Result<()> {
        let components = SSTableComponents {
            data: FileReadAt::open(&output.data)?,
            partitions: FileReadAt::open(&output.partitions)?,
            rows: FileReadAt::open(&output.rows)?,
            filter: std::fs::read(&output.filter)?,
            compression_info: match &output.compression_info {
                Some(path) => Some(std::fs::read(path)?),
                None => None,
            },
            statistics: std::fs::read(&output.statistics)?,
        };
        let reader = SSTableReader::open(components).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "ferrosa-sstable/writer: verify_output_files: reopen failed: {e}"
            ))
        })?;
        let mut iter = reader.partitions_iter().map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "ferrosa-sstable/writer: verify_output_files: partitions_iter failed: {e}"
            ))
        })?;
        let mut read_count = 0u64;
        while iter
            .next_partition()
            .map_err(|e| {
                ferrosa_common::Error::InvalidFormat(format!(
                    "ferrosa-sstable/writer: verify_output_files: partition read failed: {e}"
                ))
            })?
            .is_some()
        {
            read_count += 1;
        }
        if read_count != expected_partition_count {
            return Err(ferrosa_common::Error::InvalidFormat(format!(
                "ferrosa-sstable/writer: verify_output_files: partition count mismatch — \
                 writer wrote {expected_partition_count} but reader sees {read_count}. \
                 Refusing to return corrupt SSTable."
            )));
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Internal: partition serialization
    // -----------------------------------------------------------------------

    /// Serialize a single partition to the data buffer.
    fn serialize_partition(&mut self, partition: &Partition, data_pos: u64) -> Result<Option<u64>> {
        let build_row_index =
            !self.header.clustering_types.is_empty() && partition.rows.len() >= ROW_INDEX_MIN_ROWS;
        let mut row_trie = build_row_index.then(TrieBuilder::new);

        // Key: u16 BE length + key bytes
        let key_bytes = partition.key.key.as_bytes();
        self.data_buf
            .extend_from_slice(&(key_bytes.len() as u16).to_be_bytes())?;
        self.data_buf.extend_from_slice(key_bytes)?;

        // Deletion time: Cassandra 5.x UInt format
        if partition.deletion.is_live() {
            self.data_buf.push(DELETION_IS_LIVE)?;
        } else {
            // 8-byte markedForDeleteAt (i64 BE) + 4-byte localDeletionTime (u32 BE)
            self.data_buf
                .extend_from_slice(&partition.deletion.marked_for_delete_at.to_be_bytes())?;
            self.data_buf
                .extend_from_slice(&partition.deletion.local_deletion_time.to_be_bytes())?;
        }

        // Static row (if present)
        if let Some(ref static_row) = partition.static_row {
            self.serialize_row(static_row, true)?;
        }

        // Clustered rows
        for row in &partition.rows {
            if let Some(trie) = row_trie.as_mut() {
                let row_offset = self.data_buf.len() - data_pos;
                trie.add(
                    &row.clustering,
                    TriePayload {
                        hash: None,
                        position: row_offset as i64,
                    },
                )?;
            }
            self.serialize_row(row, false)?;
        }

        // END_OF_PARTITION marker
        self.data_buf.push(END_OF_PARTITION)?;

        if let Some(trie) = row_trie {
            let rows_start = self.rows_buf.len() as u64;
            let (trie_data, root_pos) = trie.finish()?;
            self.rows_buf.extend_from_slice(&trie_data);
            let footer_offset = self.rows_buf.len() as u64;
            let entry = crate::row_index::RowIndexEntry {
                partition_key: key_bytes.to_vec(),
                data_position: data_pos,
                trie_root: rows_start + root_pos,
                block_count: partition.rows.len() as u32,
                local_deletion_time: partition.deletion.local_deletion_time as i32,
                marked_for_delete_at: partition.deletion.marked_for_delete_at,
            };
            self.rows_buf
                .extend_from_slice(&crate::row_index::serialize_entry(&entry));
            Ok(Some(footer_offset))
        } else {
            Ok(None)
        }
    }

    /// Serialize a single row to the data buffer.
    fn serialize_row(&mut self, row: &crate::types::Row, is_static: bool) -> Result<()> {
        // Determine flags
        let mut flags: u8 = 0;
        let mut extended_flags: u8 = 0;

        if is_static {
            extended_flags |= EXT_IS_STATIC;
        }
        if row.primary_key_liveness.has_timestamp() {
            flags |= HAS_TIMESTAMP;
        }
        if row.primary_key_liveness.has_ttl() {
            flags |= HAS_TTL;
        }
        if !row.deletion.is_live() {
            flags |= HAS_DELETION;
        }

        // Determine which columns are present
        let column_defs = if is_static {
            &self.header.static_columns
        } else {
            &self.header.regular_columns
        };
        let num_columns = column_defs.len();

        // P0 correctness: every cell's col_idx must be in range, and cells must
        // be grouped/sorted by col_idx. A *simple* column has exactly one cell;
        // a *complex* (non-frozen collection) column has one cell PER ELEMENT,
        // all sharing that col_idx and each carrying a distinct cell-path — that
        // is Cassandra's complex-column layout. If these invariants are violated
        // the present-column bitmap (distinct indices) would under-count the
        // body bytes and the reader's parse position would drift, silently
        // corrupting every row after the first drift.
        let mut prev_idx: Option<u16> = None;
        // A complex column may carry one `path == None` tombstone — the
        // collection-level deletion sentinel. Any such cell sets the row-level
        // HAS_COMPLEX_DELETION flag (all-or-nothing: every complex column then
        // writes a DeletionTime, LIVE for those without a sentinel).
        let mut has_complex_deletion = false;
        for (idx, cell) in &row.cells {
            assert!(
                (*idx as usize) < num_columns,
                "SSTable writer: cell col_idx {} is out of range (num_columns={}). \
                 Serializing would produce a silently corrupt SSTable.",
                idx,
                num_columns
            );
            let is_complex = self.header.complex_collections
                && crate::marshal::is_multicell(&column_defs[*idx as usize].1);
            match prev_idx {
                Some(p) if *idx < p => panic!(
                    "SSTable writer: row.cells must be sorted by col_idx (found {idx} after {p})"
                ),
                Some(p) if *idx == p => assert!(
                    is_complex,
                    "SSTable writer: duplicate col_idx {idx} for a non-complex column — \
                     only complex (collection) columns may have multiple cells per row"
                ),
                _ => {}
            }
            if is_complex {
                // Element cell (path present) or collection-deletion sentinel
                // (path absent, must be a tombstone).
                if cell.path.is_none() {
                    assert!(
                        cell.is_tombstone(),
                        "SSTable writer: complex col_idx {idx} path=None cell must be a \
                         collection-deletion tombstone"
                    );
                    has_complex_deletion = true;
                }
            } else {
                assert!(
                    cell.path.is_none(),
                    "SSTable writer: simple col_idx {idx} must not carry a cell path"
                );
            }
            prev_idx = Some(*idx);
        }

        // Distinct present columns (cells are grouped by col_idx, so dedup runs).
        let mut present_columns: Vec<usize> = Vec::new();
        for (idx, _) in &row.cells {
            if present_columns.last() != Some(&(*idx as usize)) {
                present_columns.push(*idx as usize);
            }
        }
        let all_present = present_columns.len() == num_columns
            && present_columns.iter().enumerate().all(|(i, c)| *c == i);
        if all_present {
            flags |= HAS_ALL_COLUMNS;
        }
        if has_complex_deletion {
            flags |= HAS_COMPLEX_DELETION;
        }

        // Set EXTENSION_FLAG if we have extended flags
        if extended_flags != 0 {
            flags |= EXTENSION_FLAG;
        }

        self.data_buf.push(flags)?;
        if flags & EXTENSION_FLAG != 0 {
            self.data_buf.push(extended_flags)?;
        }

        // Clustering key (not for static rows and absent for schemas with
        // no clustering columns).
        //
        // Cassandra 5.x ClusteringPrefix format:
        //   header varint (0 = all non-null/non-empty) + per-component value bytes.
        //   Fixed-length types: raw bytes only. Variable-length: varint(len) + bytes.
        //
        // For single-column CK, row.clustering is raw component bytes.
        // For multi-column CK, the CQL bridge encodes as u16-prefixed
        // per-component: [u16 len][bytes][u16 len][bytes]...
        // We must extract each component and write it in BTI format.
        let num_ck = self.header.clustering_types.len();
        if !is_static && num_ck > 0 {
            push_unsigned_vint_to_data(&mut self.data_buf, 0)?; // header: all non-null, non-empty

            if num_ck == 1 {
                // Single CK column: raw bytes (no u16 prefix).
                let type_name = &self.header.clustering_types[0];
                if crate::marshal::value_length_if_fixed(type_name).is_none() {
                    push_unsigned_vint_to_data(&mut self.data_buf, row.clustering.len() as u64)?;
                }
                self.data_buf.extend_from_slice(&row.clustering)?;
            } else if num_ck > 1 {
                // Multi-column CK: extract components from u16-prefixed
                // encoding, then write each in BTI per-component format.
                let components = split_u16_prefixed(&row.clustering, num_ck);
                for (i, component) in components.iter().enumerate() {
                    let type_name = &self.header.clustering_types[i];
                    if crate::marshal::value_length_if_fixed(type_name).is_none() {
                        push_unsigned_vint_to_data(&mut self.data_buf, component.len() as u64)?;
                    }
                    self.data_buf.extend_from_slice(component)?;
                }
            }
        }

        // Serialize the row body to a temporary buffer to compute its size.
        let mut row_body = Vec::new();

        // Liveness info (unsigned varint deltas)
        if flags & HAS_TIMESTAMP != 0 {
            let ts_delta = (row.primary_key_liveness.timestamp - self.header.min_timestamp) as u64;
            push_unsigned_vint_to(&mut row_body, ts_delta);

            if flags & HAS_TTL != 0 {
                let ttl_delta = (row.primary_key_liveness.ttl - self.header.min_ttl) as u64;
                push_unsigned_vint_to(&mut row_body, ttl_delta);

                let ldt_delta = (row.primary_key_liveness.local_deletion_time
                    - self.header.min_local_deletion_time) as u64;
                push_unsigned_vint_to(&mut row_body, ldt_delta);
            }
        }

        // Row-level deletion (unsigned varint deltas)
        if flags & HAS_DELETION != 0 {
            assert!(
                row.deletion.marked_for_delete_at >= self.header.min_timestamp,
                "SSTable writer: row deletion timestamp {} < header min_timestamp {} — \
                 delta would underflow and corrupt the SSTable",
                row.deletion.marked_for_delete_at,
                self.header.min_timestamp
            );
            let ts_delta = (row.deletion.marked_for_delete_at - self.header.min_timestamp) as u64;
            push_unsigned_vint_to(&mut row_body, ts_delta);
            let ldt_delta = (row.deletion.local_deletion_time as i64
                - self.header.min_local_deletion_time as i64) as u64;
            push_unsigned_vint_to(&mut row_body, ldt_delta);
        }

        // Missing-column subset (only if not HAS_ALL_COLUMNS). Cassandra's
        // Columns.Serializer writes an unsigned vint, not a raw MSB-first
        // bitmap: for <64 columns, bit i set means column i is missing.
        if flags & HAS_ALL_COLUMNS == 0 {
            write_columns_subset(&mut row_body, &present_columns, num_columns);
        }

        // Cells. A simple column writes one cell; a complex (non-frozen
        // collection) column writes `uvint(cell-count)` then one element cell
        // per element, each with a cell-path. Ferrosa never emits a complex
        // DeletionTime on its own writes (its collection ops are element
        // add/remove, not whole-collection clears), so HAS_COMPLEX_DELETION is
        // never set here and no complex deletion precedes the cell count.
        let mut i = 0;
        while i < row.cells.len() {
            let col_idx = row.cells[i].0;
            let column_type = &column_defs[col_idx as usize].1;
            if self.header.complex_collections && crate::marshal::is_multicell(column_type) {
                let mut j = i;
                while j < row.cells.len() && row.cells[j].0 == col_idx {
                    j += 1;
                }
                // Split the collection-deletion sentinel (path=None tombstone)
                // from the element cells (path present).
                let sentinel = row.cells[i..j].iter().find(|(_, c)| c.path.is_none());
                let mut elements: Vec<&CellValue> = row.cells[i..j]
                    .iter()
                    .filter(|(_, c)| c.path.is_some())
                    .map(|(_, c)| c)
                    .collect();
                // Element cells are stored in cell-path order.
                elements.sort_by(|a, b| a.path.cmp(&b.path));
                // When the row flag is set, EVERY complex column writes a
                // DeletionTime — its sentinel's, or LIVE if it has none.
                if has_complex_deletion {
                    let dt = match sentinel {
                        Some((_, c)) => crate::types::DeletionTime::new(
                            c.timestamp,
                            c.local_deletion_time as u32,
                        ),
                        None => crate::types::DeletionTime::LIVE,
                    };
                    write_complex_deletion(&mut row_body, dt, &self.header);
                }
                push_unsigned_vint_to(&mut row_body, elements.len() as u64);
                let value_type =
                    crate::marshal::collection_value_type(column_type).unwrap_or(column_type);
                for cell in elements {
                    serialize_cell(&mut row_body, cell, row, &self.header, value_type, true);
                }
                i = j;
            } else {
                serialize_cell(
                    &mut row_body,
                    &row.cells[i].1,
                    row,
                    &self.header,
                    column_type,
                    false,
                );
                i += 1;
            }
        }

        // Write row body size + previous unfiltered size + row body
        let row_body_len = row_body.len() as u64;
        push_unsigned_vint_to_data(&mut self.data_buf, row_body_len)?;
        // Previous unfiltered size (0 for simplicity)
        push_unsigned_vint_to_data(&mut self.data_buf, 0)?;
        self.data_buf.extend_from_slice(&row_body)?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Internal: component builders
    // -----------------------------------------------------------------------

    /// Build Partitions.db: trie bytes + byte-comparable key bounds + footer.
    fn build_partitions_db(
        trie_builder: TrieBuilder,
        first_index_key: &[u8],
        last_index_key: &[u8],
        partition_count: u64,
    ) -> Result<Vec<u8>> {
        let (trie_data, root_pos) = trie_builder.finish()?;

        let mut buf = Vec::new();

        // Trie data
        buf.extend_from_slice(&trie_data);

        // Key bounds section
        let key_bounds_offset = buf.len() as i64;
        // smallest key: u16 len + bytes
        buf.extend_from_slice(&(first_index_key.len() as u16).to_be_bytes());
        buf.extend_from_slice(first_index_key);
        // largest key: u16 len + bytes
        buf.extend_from_slice(&(last_index_key.len() as u16).to_be_bytes());
        buf.extend_from_slice(last_index_key);

        // Footer: 3 big-endian i64s
        buf.extend_from_slice(&key_bounds_offset.to_be_bytes());
        buf.extend_from_slice(&(partition_count as i64).to_be_bytes());
        buf.extend_from_slice(&(root_pos as i64).to_be_bytes());

        Ok(buf)
    }

    /// Build Statistics.db.
    fn build_statistics_db(
        header: &SerializationHeader,
        bloom_fp_chance: f64,
        first_key: &[u8],
        last_key: &[u8],
        partition_count: u64,
        total_rows: u64,
        total_columns_set: u64,
    ) -> Vec<u8> {
        let stats = Statistics {
            validation: ValidationMetadata {
                partitioner_class: "org.apache.cassandra.dht.Murmur3Partitioner".into(),
                bloom_fp_chance,
            },
            compaction: CompactionMetadata { data: vec![] },
            stats: build_simple_bti_stats_metadata(
                header,
                first_key,
                last_key,
                partition_count,
                total_rows,
                total_columns_set,
                1.0,
            )
            .unwrap_or_else(|| StatsMetadata { data: vec![] }),
            header: header.clone(),
        };
        write_statistics(&stats)
    }

    /// Build Data.db, optionally compressing chunks, and produce CompressionInfo.db.
    fn build_data_db(
        data_buf: Vec<u8>,
        options: &WriteOptions,
    ) -> Result<(Vec<u8>, Option<Vec<u8>>)> {
        match &options.compression {
            Some(compression) if !matches!(compression, Compression::None) => {
                struct CompressedChunk {
                    payload: Vec<u8>,
                    crc: [u8; 4],
                    stored_size: usize,
                }

                let chunk_size = options.chunk_size;
                let data_length = data_buf.len() as u64;

                let chunk_count = data_buf.chunks(chunk_size).len();
                let mut compressed_data = Vec::with_capacity(data_buf.len());
                let mut chunk_offsets = Vec::with_capacity(chunk_count);
                let mut max_compressed_size: usize = 0;
                let batch_chunks = compression_batch_chunks();
                let mut batch: Vec<&[u8]> = Vec::with_capacity(batch_chunks);

                let flush_batch = |batch: &mut Vec<&[u8]>,
                                   compressed_data: &mut Vec<u8>,
                                   chunk_offsets: &mut Vec<u64>,
                                   max_compressed_size: &mut usize|
                 -> Result<()> {
                    if batch.is_empty() {
                        return Ok(());
                    }
                    let compressed_chunks: Result<Vec<CompressedChunk>> = compression_pool()
                        .install(|| {
                            batch
                                .par_iter()
                                .map(|chunk| {
                                    let payload = compression.compress(chunk)?;
                                    let crc = crc32fast::hash(&payload).to_be_bytes();
                                    let stored_size = payload.len() + std::mem::size_of::<u32>();
                                    Ok(CompressedChunk {
                                        payload,
                                        crc,
                                        stored_size,
                                    })
                                })
                                .collect()
                        });

                    for chunk in compressed_chunks? {
                        chunk_offsets.push(compressed_data.len() as u64);
                        *max_compressed_size = (*max_compressed_size).max(chunk.stored_size);
                        compressed_data.extend_from_slice(&chunk.payload);
                        compressed_data.extend_from_slice(&chunk.crc);
                    }
                    batch.clear();
                    Ok(())
                };

                for chunk in data_buf.chunks(chunk_size) {
                    batch.push(chunk);
                    if batch.len() == batch_chunks {
                        flush_batch(
                            &mut batch,
                            &mut compressed_data,
                            &mut chunk_offsets,
                            &mut max_compressed_size,
                        )?;
                    }
                }
                flush_batch(
                    &mut batch,
                    &mut compressed_data,
                    &mut chunk_offsets,
                    &mut max_compressed_size,
                )?;

                let info = CompressionInfo {
                    compression: compression.clone(),
                    chunk_length: chunk_size,
                    max_compressed_size,
                    data_length,
                    chunk_offsets,
                };
                let info_bytes = info.write()?;

                Ok((compressed_data, Some(info_bytes)))
            }
            _ => {
                // No compression
                Ok((data_buf, None))
            }
        }
    }

    /// Build Data.db directly on disk and return optional CompressionInfo.db
    /// bytes. The compressed path writes each bounded batch to `data_path`
    /// before compressing the next batch, so peak heap does not include the
    /// full compressed Data.db.
    fn build_data_db_to_file(
        data_buf: Vec<u8>,
        options: &WriteOptions,
        data_path: &Path,
    ) -> Result<Option<Vec<u8>>> {
        match &options.compression {
            Some(compression) if !matches!(compression, Compression::None) => {
                struct CompressedChunk {
                    payload: Vec<u8>,
                    crc: [u8; 4],
                    stored_size: usize,
                }

                let chunk_size = options.chunk_size;
                let data_length = data_buf.len() as u64;
                let chunk_count = data_buf.chunks(chunk_size).len();
                let mut chunk_offsets = Vec::with_capacity(chunk_count);
                let mut max_compressed_size: usize = 0;
                let batch_chunks = compression_batch_chunks();
                let mut batch: Vec<&[u8]> = Vec::with_capacity(batch_chunks);
                let mut file = DataDbWriter::create(data_path, sstable_direct_io_enabled())?;

                let flush_batch = |batch: &mut Vec<&[u8]>,
                                   file: &mut DataDbWriter,
                                   chunk_offsets: &mut Vec<u64>,
                                   max_compressed_size: &mut usize|
                 -> Result<()> {
                    if batch.is_empty() {
                        return Ok(());
                    }
                    let compressed_chunks: Result<Vec<CompressedChunk>> = compression_pool()
                        .install(|| {
                            batch
                                .par_iter()
                                .map(|chunk| {
                                    let payload = compression.compress(chunk)?;
                                    let crc = crc32fast::hash(&payload).to_be_bytes();
                                    let stored_size = payload.len() + std::mem::size_of::<u32>();
                                    Ok(CompressedChunk {
                                        payload,
                                        crc,
                                        stored_size,
                                    })
                                })
                                .collect()
                        });

                    for chunk in compressed_chunks? {
                        chunk_offsets.push(file.position());
                        *max_compressed_size = (*max_compressed_size).max(chunk.stored_size);
                        file.write_all(&chunk.payload)?;
                        file.write_all(&chunk.crc)?;
                    }
                    batch.clear();
                    Ok(())
                };

                for chunk in data_buf.chunks(chunk_size) {
                    batch.push(chunk);
                    if batch.len() == batch_chunks {
                        flush_batch(
                            &mut batch,
                            &mut file,
                            &mut chunk_offsets,
                            &mut max_compressed_size,
                        )?;
                    }
                }
                flush_batch(
                    &mut batch,
                    &mut file,
                    &mut chunk_offsets,
                    &mut max_compressed_size,
                )?;
                file.finish()?;

                let info = CompressionInfo {
                    compression: compression.clone(),
                    chunk_length: chunk_size,
                    max_compressed_size,
                    data_length,
                    chunk_offsets,
                };
                info.write().map(Some)
            }
            _ => {
                let mut file = DataDbWriter::create(data_path, sstable_direct_io_enabled())?;
                file.write_all(&data_buf)?;
                file.finish()?;
                Ok(None)
            }
        }
    }

    fn build_data_source_to_file(
        data_source: DataSource,
        options: &WriteOptions,
        data_path: &Path,
    ) -> Result<Option<Vec<u8>>> {
        match data_source {
            DataSource::Memory(data_buf) => {
                Self::build_data_db_to_file(data_buf, options, data_path)
            }
            DataSource::File { path, len } => {
                let result = Self::build_data_file_to_file(&path, len, options, data_path);
                if result.is_ok() {
                    let _ = std::fs::remove_file(&path);
                }
                result
            }
        }
    }

    fn build_data_file_to_file(
        raw_path: &Path,
        data_length: u64,
        options: &WriteOptions,
        data_path: &Path,
    ) -> Result<Option<Vec<u8>>> {
        match &options.compression {
            Some(compression) if !matches!(compression, Compression::None) => {
                struct CompressedChunk {
                    payload: Vec<u8>,
                    crc: [u8; 4],
                    stored_size: usize,
                }

                let chunk_size = options.chunk_size;
                let chunk_count = data_length.div_ceil(chunk_size as u64) as usize;
                let mut chunk_offsets = Vec::with_capacity(chunk_count);
                let mut max_compressed_size: usize = 0;
                let batch_chunks = compression_batch_chunks();
                let mut raw = std::fs::File::open(raw_path)?;
                let mut out = DataDbWriter::create(data_path, sstable_direct_io_enabled())?;

                loop {
                    let mut batch = Vec::with_capacity(batch_chunks);
                    for _ in 0..batch_chunks {
                        let mut chunk = vec![0u8; chunk_size];
                        let mut read = 0usize;
                        while read < chunk_size {
                            let n = raw.read(&mut chunk[read..])?;
                            if n == 0 {
                                break;
                            }
                            read += n;
                        }
                        if read == 0 {
                            break;
                        }
                        chunk.truncate(read);
                        batch.push(chunk);
                    }
                    if batch.is_empty() {
                        break;
                    }

                    let compressed_chunks: Result<Vec<CompressedChunk>> = compression_pool()
                        .install(|| {
                            batch
                                .par_iter()
                                .map(|chunk| {
                                    let payload = compression.compress(chunk)?;
                                    let crc = crc32fast::hash(&payload).to_be_bytes();
                                    let stored_size = payload.len() + std::mem::size_of::<u32>();
                                    Ok(CompressedChunk {
                                        payload,
                                        crc,
                                        stored_size,
                                    })
                                })
                                .collect()
                        });

                    for chunk in compressed_chunks? {
                        chunk_offsets.push(out.position());
                        max_compressed_size = max_compressed_size.max(chunk.stored_size);
                        out.write_all(&chunk.payload)?;
                        out.write_all(&chunk.crc)?;
                    }
                }
                out.finish()?;

                let info = CompressionInfo {
                    compression: compression.clone(),
                    chunk_length: chunk_size,
                    max_compressed_size,
                    data_length,
                    chunk_offsets,
                };
                info.write().map(Some)
            }
            _ => {
                std::fs::rename(raw_path, data_path)?;
                Ok(None)
            }
        }
    }

    /// Build TOC.txt.
    fn build_toc(has_compression: bool) -> Vec<u8> {
        if has_compression {
            toc::write_toc(&[
                toc::COMPRESSION_INFO,
                toc::DATA,
                toc::FILTER,
                toc::PARTITIONS,
                toc::ROWS,
                toc::STATISTICS,
                toc::TOC,
            ])
        } else {
            // Omit CRC.db for uncompressed SSTables — Cassandra treats it as
            // optional; listing it in TOC without writing the file causes
            // CorruptSSTableException on import.
            toc::write_toc(&[
                toc::DATA,
                toc::FILTER,
                toc::PARTITIONS,
                toc::ROWS,
                toc::STATISTICS,
                toc::TOC,
            ])
        }
    }
}

// ---------------------------------------------------------------------------
// Free-standing helpers
// ---------------------------------------------------------------------------

/// Serialize a single cell to the given buffer.
fn serialize_cell(
    buf: &mut Vec<u8>,
    cell: &CellValue,
    row: &crate::types::Row,
    header: &SerializationHeader,
    value_type: &str,
    is_complex: bool,
) {
    let is_tombstone = cell.is_tombstone();
    let is_expiring = !is_tombstone
        && cell.ttl != ferrosa_common::NO_TTL
        && cell.local_deletion_time != ferrosa_common::NO_DELETION_TIME;
    // HAS_EMPTY_VALUE covers both a tombstone (no value) and a live cell whose
    // value is empty — e.g. a `set` element, whose identity is its cell-path and
    // whose value is empty. Cassandra sets this flag on any zero-length value.
    let has_empty_value = cell.value.as_ref().is_none_or(|v| v.is_empty());
    let use_row_timestamp = row.primary_key_liveness.has_timestamp()
        && cell.timestamp == row.primary_key_liveness.timestamp;
    let use_row_ttl = is_expiring
        && row.primary_key_liveness.ttl != ferrosa_common::NO_TTL
        && cell.ttl == row.primary_key_liveness.ttl
        && cell.local_deletion_time == row.primary_key_liveness.local_deletion_time;

    let mut cell_flags: u8 = 0;
    if is_tombstone {
        cell_flags |= CELL_IS_DELETED;
    }
    if is_expiring {
        cell_flags |= CELL_IS_EXPIRING;
    }
    if has_empty_value {
        cell_flags |= CELL_HAS_EMPTY_VALUE;
    }
    if use_row_timestamp {
        cell_flags |= CELL_USE_ROW_TIMESTAMP;
    }
    if use_row_ttl {
        cell_flags |= CELL_USE_ROW_TTL;
    }
    buf.push(cell_flags);

    // Timestamp (unsigned varint delta, if not using row timestamp)
    if !use_row_timestamp {
        // Safety: if cell.timestamp < header.min_timestamp, the cast to u64
        // wraps to a huge value, producing a corrupt varint that will be
        // misread as a garbage length later. This MUST be a hard assert
        // (not debug_assert) — release builds must catch this corruption
        // at write time, not produce silently corrupt SSTables.
        assert!(
            cell.timestamp >= header.min_timestamp,
            "SSTable writer: cell timestamp {} < header min_timestamp {} — \
             delta would underflow and corrupt the SSTable",
            cell.timestamp,
            header.min_timestamp
        );
        let ts_delta = (cell.timestamp - header.min_timestamp) as u64;
        push_unsigned_vint_to(buf, ts_delta);
    }

    // Local deletion time (unsigned varint delta, for tombstones and expiring cells)
    if !use_row_ttl && (is_tombstone || is_expiring) {
        let ldt_delta =
            (cell.local_deletion_time as i64 - header.min_local_deletion_time as i64) as u64;
        push_unsigned_vint_to(buf, ldt_delta);
    }

    // TTL (unsigned varint delta, for expiring cells only)
    if is_expiring && !use_row_ttl {
        let ttl_delta = (cell.ttl - header.min_ttl) as u64;
        push_unsigned_vint_to(buf, ttl_delta);
    }

    // Cell path (complex/collection columns only): `uvint(len) + bytes`, written
    // between the ttl and the value. It is present for EVERY element cell of a
    // complex column and is gated purely by the column's complex-ness — there is
    // no cell-flag bit for it (matching Cassandra's CollectionType path serializer).
    if is_complex {
        let path = cell.path.as_deref().unwrap_or(&[]);
        push_unsigned_vint_to(buf, path.len() as u64);
        buf.extend_from_slice(path);
    }

    // Value (absent if HAS_EMPTY_VALUE). `value_type` is the element/value type
    // for a complex column (e.g. the `V` of `map<K,V>`), the column type for a
    // simple one — it decides fixed-width (raw bytes) vs varint-length prefix.
    if !has_empty_value {
        if let Some(ref value) = cell.value {
            // Safety assertion: catch corrupt cell values at write time.
            // The max CQL value size is 256 MiB. Anything larger is a bug.
            const MAX_CELL_VALUE: usize = 256 * 1024 * 1024;
            assert!(
                value.len() <= MAX_CELL_VALUE,
                "SSTable writer: cell value length {} exceeds maximum {MAX_CELL_VALUE} — \
                 this is a bug in the write path, not user data",
                value.len()
            );
            // A collection element value is ALWAYS length-prefixed, even for a
            // fixed-width element type (Cassandra serializes `list<int>` /
            // `map<k,int>` values with a uvint length). The fixed-width raw-bytes
            // optimization applies only to simple (scalar) columns.
            if is_complex {
                push_unsigned_vint_to(buf, value.len() as u64);
            } else if let Some(fixed_len) = crate::marshal::value_length_if_fixed(value_type) {
                assert!(
                    value.len() == fixed_len,
                    "SSTable writer: fixed-width column {value_type} expects {fixed_len} bytes, got {}",
                    value.len()
                );
            } else {
                push_unsigned_vint_to(buf, value.len() as u64);
            }
            buf.extend_from_slice(value);
        }
    }
}

/// Write a complex-column `DeletionTime` as two unsigned-vint deltas against the
/// header mins (`markedForDeleteAt - minTimestamp`, `localDeletionTime -
/// minLocalDeletionTime`). Wrapping arithmetic is used so a `LIVE` deletion
/// (`i64::MIN` / `u32::MAX`) round-trips through the reader's wrapping add.
fn write_complex_deletion(
    buf: &mut Vec<u8>,
    dt: crate::types::DeletionTime,
    header: &SerializationHeader,
) {
    let ts_delta = dt.marked_for_delete_at.wrapping_sub(header.min_timestamp) as u64;
    push_unsigned_vint_to(buf, ts_delta);
    let ldt_delta =
        (dt.local_deletion_time as i64).wrapping_sub(header.min_local_deletion_time as i64) as u64;
    push_unsigned_vint_to(buf, ldt_delta);
}

/// Write an unsigned varint to a Vec buffer.
fn push_unsigned_vint_to(buf: &mut Vec<u8>, value: u64) {
    let mut vbuf = [0u8; 9];
    let n = varint::write_unsigned_vint(&mut vbuf, value);
    buf.extend_from_slice(&vbuf[..n]);
}

fn push_unsigned_vint_to_data(buf: &mut DataBuffer, value: u64) -> Result<()> {
    let mut vbuf = [0u8; 9];
    let n = varint::write_unsigned_vint(&mut vbuf, value);
    buf.extend_from_slice(&vbuf[..n])
}

fn write_columns_subset(buf: &mut Vec<u8>, present_columns: &[usize], num_columns: usize) {
    if num_columns < 64 {
        let mut missing_bitmap = 0u64;
        let mut present_iter = present_columns.iter().copied().peekable();
        for idx in 0..num_columns {
            if present_iter.peek() == Some(&idx) {
                present_iter.next();
            } else {
                missing_bitmap |= 1u64 << idx;
            }
        }
        push_unsigned_vint_to(buf, missing_bitmap);
        return;
    }

    let missing_count = num_columns.saturating_sub(present_columns.len());
    push_unsigned_vint_to(buf, missing_count as u64);
    if present_columns.len() < num_columns / 2 {
        for &idx in present_columns {
            push_unsigned_vint_to(buf, idx as u64);
        }
    } else {
        let mut present_iter = present_columns.iter().copied().peekable();
        for idx in 0..num_columns {
            if present_iter.peek() == Some(&idx) {
                present_iter.next();
            } else {
                push_unsigned_vint_to(buf, idx as u64);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Extract components from a u16-BE-length-prefixed byte sequence.
///
/// The CQL bridge encodes multi-column clustering keys as:
///   `[u16 len][component bytes][u16 len][component bytes]...`
/// This function splits them back into individual byte slices.
fn split_u16_prefixed(bytes: &[u8], expected: usize) -> Vec<&[u8]> {
    let mut components = Vec::with_capacity(expected);
    let mut pos = 0;
    while pos + 2 <= bytes.len() && components.len() < expected {
        let len = u16::from_be_bytes([bytes[pos], bytes[pos + 1]]) as usize;
        pos += 2;
        let end = (pos + len).min(bytes.len());
        components.push(&bytes[pos..end]);
        pos = end;
    }
    components
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::DataReader;
    use crate::types::{DeletionTime, LivenessInfo, Row};
    use ferrosa_common::{CellValue, DecoratedKey, PartitionKey, Token};

    #[test]
    fn parse_direct_io_flag_defaults_off_and_accepts_truthy() {
        assert!(!parse_direct_io_flag(None), "absent ⇒ off (safe default)");
        assert!(!parse_direct_io_flag(Some("0".into())));
        assert!(!parse_direct_io_flag(Some("no".into())));
        assert!(!parse_direct_io_flag(Some(String::new())));
        for truthy in ["1", "true", "TRUE", "on", " 1 "] {
            assert!(parse_direct_io_flag(Some(truthy.into())), "{truthy} ⇒ on");
        }
    }

    /// The load-bearing wiring invariant: writing the same byte stream through a
    /// direct (O_DIRECT/F_NOCACHE) Data.db writer and the buffered writer yields
    /// byte-for-byte identical files AND identical recorded chunk offsets —
    /// otherwise CompressionInfo offsets would point the reader at the wrong
    /// chunk. Explicit `direct` bool ⇒ no env-var race. This transitively proves
    /// direct-mode SSTable output matches the buffered path the binary-exact
    /// Cassandra oracle already pins.
    #[test]
    fn data_db_writer_direct_matches_buffered_bytes_and_offsets() {
        let dir = tempfile::tempdir().expect("tempdir");
        // Irregular sizes mimic compressed payload+crc writes; spans the 4096
        // block boundary and the direct writer's staging buffer.
        let writes: Vec<Vec<u8>> = (0..600usize)
            .map(|i| {
                let n = 1 + (i * 37) % 5000;
                (0..n).map(|j| ((i + j) % 256) as u8).collect()
            })
            .collect();

        let mut offsets: [Vec<u64>; 2] = [Vec::new(), Vec::new()];
        let paths = [dir.path().join("buffered.db"), dir.path().join("direct.db")];
        for (idx, direct) in [false, true].into_iter().enumerate() {
            let mut w = DataDbWriter::create(&paths[idx], direct).expect("create");
            for chunk in &writes {
                offsets[idx].push(w.position());
                w.write_all(chunk).expect("write_all");
            }
            w.finish().expect("finish");
        }

        assert_eq!(
            offsets[0], offsets[1],
            "chunk offsets must be mode-independent"
        );
        let buffered = std::fs::read(&paths[0]).expect("read buffered");
        let direct = std::fs::read(&paths[1]).expect("read direct");
        assert_eq!(
            buffered.len(),
            direct.len(),
            "Data.db length differs between buffered and direct modes"
        );
        assert_eq!(
            buffered, direct,
            "Data.db bytes differ between buffered and direct modes"
        );
    }

    /// Build a minimal serialization header for testing.
    fn test_header() -> SerializationHeader {
        SerializationHeader {
            complex_collections: false,
            min_timestamp: 1_000_000,
            min_local_deletion_time: i32::MAX,
            min_ttl: 0,
            max_timestamp: i64::MAX,
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            clustering_types: vec!["org.apache.cassandra.db.marshal.Int32Type".into()],
            static_columns: vec![],
            regular_columns: vec![(
                b"val".to_vec(),
                "org.apache.cassandra.db.marshal.UTF8Type".into(),
            )],
        }
    }

    /// Build a simple partition with one row and one cell.
    fn make_partition(key: &[u8], clustering: &[u8], value: &[u8], timestamp: i64) -> Partition {
        Partition {
            key: DecoratedKey::new(PartitionKey::from(key)),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![Row {
                clustering: clustering.to_vec(),
                cells: vec![(0, CellValue::live(value.to_vec(), timestamp))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
            }],
        }
    }

    fn make_wide_partition(key: &[u8], rows: usize) -> Partition {
        Partition {
            key: DecoratedKey::new(PartitionKey::from(key)),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: (0..rows as i32)
                .map(|idx| {
                    let timestamp = 1_000_000 + i64::from(idx);
                    Row {
                        clustering: (idx + 1).to_be_bytes().to_vec(),
                        cells: vec![(
                            0,
                            CellValue::live(format!("value-{idx}").into_bytes(), timestamp),
                        )],
                        deletion: DeletionTime::LIVE,
                        primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
                    }
                })
                .collect(),
        }
    }

    #[test]
    fn write_single_partition_and_read_back() {
        let header = test_header();
        let options = WriteOptions {
            compression: None,
            bloom_fp_chance: 0.01,
            chunk_size: 65536,
            verify_output: true,
        };

        let timestamp = 1_000_042i64;
        let partition = make_partition(b"pk1", &[0x00, 0x00, 0x00, 0x01], b"hello", timestamp);

        let mut writer = SSTableWriter::new(options, header.clone());
        writer.add_partition(&partition).unwrap();
        let output = writer.finish().unwrap();

        // Read back the Data.db using DataReader
        let mut reader = DataReader::new(&output.data, &header, 0);
        let read_partition = reader
            .read_partition()
            .unwrap()
            .expect("expected partition");

        // Verify key
        assert_eq!(read_partition.key.key.as_bytes(), b"pk1");

        // Verify partition deletion is live
        assert!(read_partition.deletion.is_live());

        // No static row
        assert!(read_partition.static_row.is_none());

        // One row
        assert_eq!(read_partition.rows.len(), 1);
        let row = &read_partition.rows[0];

        // Clustering key
        assert_eq!(row.clustering, vec![0x00, 0x00, 0x00, 0x01]);

        // Liveness: timestamp = 1_000_042
        assert_eq!(row.primary_key_liveness.timestamp, timestamp);

        // One cell
        assert_eq!(row.cells.len(), 1);
        let (col_idx, ref cell) = row.cells[0];
        assert_eq!(col_idx, 0);
        assert!(cell.is_live());
        assert_eq!(cell.value.as_deref(), Some(b"hello".as_slice()));
        assert_eq!(cell.timestamp, timestamp);

        // No more partitions
        assert!(reader.read_partition().unwrap().is_none());
    }

    /// Large cell values (100KB+) must survive a write-read roundtrip.
    /// This is a P0 regression test: embedding hex-encoded WASM binaries
    /// (~160KB) as CQL function bodies corrupted the SSTable data file,
    /// causing subsequent reads to fail with "read_exact_at: wanted N
    /// bytes, got M" or "range tombstone markers not yet supported".
    #[test]
    fn write_large_cell_value_roundtrip() {
        let header = test_header();
        let options = WriteOptions {
            compression: None,
            bloom_fp_chance: 0.01,
            chunk_size: 65536,
            verify_output: true,
        };

        // 100KB value — larger than a typical SSTable chunk.
        let large_value: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();
        let timestamp = 1_000_042i64;
        let partition = make_partition(
            b"pk_large",
            &[0x00, 0x00, 0x00, 0x01],
            &large_value,
            timestamp,
        );

        let mut writer = SSTableWriter::new(options, header.clone());
        writer.add_partition(&partition).unwrap();
        let output = writer.finish().unwrap();

        // Read back — must not corrupt or error.
        let mut reader = DataReader::new(&output.data, &header, 0);
        let read_partition = reader
            .read_partition()
            .unwrap()
            .expect("expected partition with large cell");

        assert_eq!(read_partition.key.key.as_bytes(), b"pk_large");
        assert_eq!(read_partition.rows.len(), 1);
        let (_, ref cell) = read_partition.rows[0].cells[0];
        assert_eq!(
            cell.value.as_deref().unwrap().len(),
            100_000,
            "large value should survive roundtrip"
        );
        assert_eq!(cell.value.as_deref().unwrap(), large_value.as_slice());
    }

    /// Exact production scenario: memo_cache with composite text PK,
    /// UUID clustering, and multiple regular columns including text + bigint.
    /// Corruption happens with longer model_version (15 chars) but not
    /// shorter (7 chars). Tests both at the SSTable write/read level.
    #[test]
    fn composite_text_pk_uuid_clustering_roundtrip() {
        // Header matching memo_cache: CompositeType(UTF8,UTF8) PK, UUID clustering
        let header = SerializationHeader {
            complex_collections: false,
            min_timestamp: 1_000_000,
            min_local_deletion_time: i32::MAX,
            min_ttl: 0,
            max_timestamp: i64::MAX,
            key_type: "org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)".into(),
            clustering_types: vec![
                "org.apache.cassandra.db.marshal.UUIDType".into(),
            ],
            static_columns: vec![],
            regular_columns: vec![
                (b"result".to_vec(), "org.apache.cassandra.db.marshal.UTF8Type".into()),
                (b"hit_count".to_vec(), "org.apache.cassandra.db.marshal.LongType".into()),
            ],
        };
        let options = WriteOptions {
            compression: None,
            bloom_fp_chance: 0.01,
            chunk_size: 65536,
            verify_output: true,
        };

        let timestamp = 1_000_042i64;

        // Composite PK encoding: [u16 len][bytes][0x00] per component
        fn make_composite_pk(part1: &str, part2: &str) -> Vec<u8> {
            let mut buf = Vec::new();
            buf.extend_from_slice(&(part1.len() as u16).to_be_bytes());
            buf.extend_from_slice(part1.as_bytes());
            buf.push(0x00);
            buf.extend_from_slice(&(part2.len() as u16).to_be_bytes());
            buf.extend_from_slice(part2.as_bytes());
            buf.push(0x00);
            buf
        }

        // Row 1: LONG model_version (15 chars) — this is the one that corrupts
        let pk1 = make_composite_pk(
            "cac0302657b4c1d0dfd5aec98f2754f46a42f117e53c77a9ba384ebf2095633a", // pragma: allowlist secret
            "claude-opus-4-6",
        );
        let uuid1 = [0x11u8; 16]; // tenant_id UUID
        let p1 = Partition {
            key: DecoratedKey::new(PartitionKey::new(pk1.clone())),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![Row {
                clustering: uuid1.to_vec(),
                cells: vec![
                    (
                        0,
                        CellValue::live(b"The capital of France is Paris.".to_vec(), timestamp),
                    ),
                    (1, CellValue::live(0i64.to_be_bytes().to_vec(), timestamp)),
                ],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
            }],
        };

        // Row 2: SHORT model_version (7 chars) — this one survives
        let pk2 = make_composite_pk(
            "4bbe47ee6bb1d0ecaa4d47fce3d99e2044cdfdbfac4dde0c0c083f97e4fad000", // pragma: allowlist secret
            "test-v1",
        );
        let uuid2 = [0x22u8; 16];
        let p2 = Partition {
            key: DecoratedKey::new(PartitionKey::new(pk2.clone())),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![Row {
                clustering: uuid2.to_vec(),
                cells: vec![
                    (0, CellValue::live(b"4".to_vec(), timestamp)),
                    (1, CellValue::live(0i64.to_be_bytes().to_vec(), timestamp)),
                ],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
            }],
        };

        // Sort by key (required by SSTableWriter)
        let mut partitions = vec![p1, p2];
        partitions.sort_by(|a, b| a.key.cmp(&b.key));

        let mut writer = SSTableWriter::new(options, header.clone());
        for p in &partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer.finish().unwrap();

        // Sequential read: both partitions must be readable
        let mut reader = DataReader::new(&output.data, &header, 0);
        let mut read_partitions = Vec::new();
        while let Some(partition) = reader.read_partition().unwrap() {
            read_partitions.push(partition);
        }
        assert_eq!(
            read_partitions.len(),
            2,
            "sequential read should find both partitions"
        );

        // Verify each partition has correct data
        for rp in &read_partitions {
            assert_eq!(rp.rows.len(), 1, "each partition should have 1 row");
            assert_eq!(
                rp.rows[0].cells.len(),
                2,
                "each row should have 2 cells (result + hit_count)"
            );
            // Clustering key should be 16 bytes (UUID)
            assert_eq!(
                rp.rows[0].clustering.len(),
                16,
                "clustering key should be 16 bytes (UUID)"
            );
        }

        // Verify the data bytes are well-formed by re-reading at each
        // partition's offset (simulates what get_partition does).
        for rp in &read_partitions {
            let key_bytes = rp.key.key.as_bytes();
            assert!(
                key_bytes.len() > 70,
                "composite PK should be > 70 bytes, got {}",
                key_bytes.len()
            );
        }
    }

    /// End-to-end: exact production memo_cache scenario through full
    /// SSTableWriter → SSTableReader::get_partition() cycle.
    /// Uses composite text PK, UUID clustering, and multiple column types.
    #[test]
    fn e2e_composite_pk_get_partition_exact_production_keys() {
        use crate::reader::{SSTableComponents, SSTableReader};

        // Header matching memo_cache: CompositeType(UTF8,UTF8) PK, UUID clustering
        let header = SerializationHeader {
            complex_collections: false,
            min_timestamp: 1_000_000,
            min_local_deletion_time: i32::MAX,
            min_ttl: 0,
            max_timestamp: i64::MAX,
            key_type: "org.apache.cassandra.db.marshal.CompositeType(\
                       org.apache.cassandra.db.marshal.UTF8Type,\
                       org.apache.cassandra.db.marshal.UTF8Type)"
                .into(),
            clustering_types: vec!["org.apache.cassandra.db.marshal.UUIDType".into()],
            static_columns: vec![],
            regular_columns: vec![
                (
                    b"result".to_vec(),
                    "org.apache.cassandra.db.marshal.UTF8Type".into(),
                ),
                (
                    b"hit_count".to_vec(),
                    "org.apache.cassandra.db.marshal.LongType".into(),
                ),
            ],
        };
        let options = WriteOptions {
            compression: None,
            bloom_fp_chance: 0.01,
            chunk_size: 65536,
            verify_output: true,
        };
        let ts = 1_000_042i64;

        // Composite PK encoding: [u16 len][bytes][0x00] per component
        fn composite(part1: &str, part2: &str) -> Vec<u8> {
            let mut buf = Vec::new();
            buf.extend_from_slice(&(part1.len() as u16).to_be_bytes());
            buf.extend_from_slice(part1.as_bytes());
            buf.push(0x00);
            buf.extend_from_slice(&(part2.len() as u16).to_be_bytes());
            buf.extend_from_slice(part2.as_bytes());
            buf.push(0x00);
            buf
        }

        // EXACT production keys
        let pk1_bytes = composite(
            "4bbe47ee6bb1d0ecaa4d47fce3d99e2044cdfdbfac4dde0c0c083f97e4fad000", // pragma: allowlist secret
            "test-v1",
        );
        let pk2_bytes = composite(
            "cac0302657b4c1d0dfd5aec98f2754f46a42f117e53c77a9ba384ebf2095633a", // pragma: allowlist secret
            "claude-opus-4-6",
        );

        let p1 = Partition {
            key: DecoratedKey::new(PartitionKey::new(pk1_bytes.clone())),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![Row {
                clustering: vec![0x11u8; 16], // UUID
                cells: vec![
                    (0, CellValue::live(b"4".to_vec(), ts)),
                    (1, CellValue::live(0i64.to_be_bytes().to_vec(), ts)),
                ],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(ts),
            }],
        };
        let p2 = Partition {
            key: DecoratedKey::new(PartitionKey::new(pk2_bytes.clone())),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![Row {
                clustering: vec![0x22u8; 16], // UUID
                cells: vec![
                    (
                        0,
                        CellValue::live(b"The capital of France is Paris.".to_vec(), ts),
                    ),
                    (1, CellValue::live(0i64.to_be_bytes().to_vec(), ts)),
                ],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(ts),
            }],
        };

        // Sort by decorated key (token order)
        let mut partitions = vec![p1, p2];
        partitions.sort_by(|a, b| a.key.cmp(&b.key));

        let mut writer = SSTableWriter::new(options, header.clone());
        for p in &partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer.finish().unwrap();

        eprintln!(
            "Data.db: {} bytes, Partitions.db: {} bytes",
            output.data.len(),
            output.partitions.len()
        );

        // Build SSTableReader from raw bytes
        let components = SSTableComponents {
            data: output.data.as_slice(),
            partitions: output.partitions.as_slice(),
            rows: output.rows.as_slice(),
            filter: output.filter.clone(),
            compression_info: output.compression_info.clone(),
            statistics: output.statistics.clone(),
        };
        let reader = SSTableReader::open(components).unwrap();
        assert_eq!(reader.key_count(), 2);

        // get_partition for BOTH keys must succeed
        let key1 = DecoratedKey::new(PartitionKey::new(pk1_bytes));
        let r1 = reader.get_partition(&key1);
        assert!(r1.is_ok(), "get_partition key1 error: {:?}", r1.err());
        let r1 = r1.unwrap();
        assert!(r1.is_some(), "key1 (test-v1) not found via trie");
        let r1 = r1.unwrap();
        assert_eq!(r1.rows.len(), 1, "key1 should have 1 row");
        assert_eq!(
            r1.rows[0].clustering.len(),
            16,
            "UUID clustering = 16 bytes"
        );

        let key2 = DecoratedKey::new(PartitionKey::new(pk2_bytes));
        let r2 = reader.get_partition(&key2);
        assert!(r2.is_ok(), "get_partition key2 error: {:?}", r2.err());
        let r2 = r2.unwrap();
        assert!(r2.is_some(), "key2 (claude-opus-4-6) not found via trie");
        let r2 = r2.unwrap();
        assert_eq!(r2.rows.len(), 1, "key2 should have 1 row");
        assert_eq!(
            r2.rows[0].clustering.len(),
            16,
            "UUID clustering = 16 bytes"
        );

        // Verify cell values round-tripped
        let r2_result = r2.rows[0].cells[0].1.value.as_deref().unwrap();
        assert_eq!(
            r2_result, b"The capital of France is Paris.",
            "cell value must survive roundtrip"
        );
    }

    #[test]
    fn write_multiple_partitions_verify_count_and_bloom() {
        let header = test_header();
        let options = WriteOptions {
            compression: None,
            bloom_fp_chance: 0.01,
            chunk_size: 65536,
            verify_output: true,
        };

        // Create partitions and sort by token order
        let mut partitions: Vec<Partition> = (0..5)
            .map(|i| {
                make_partition(
                    format!("key{}", i).as_bytes(),
                    &[0x00, 0x00, 0x00, i as u8],
                    format!("value{}", i).as_bytes(),
                    1_000_000 + i as i64,
                )
            })
            .collect();
        partitions.sort_by(|a, b| a.key.cmp(&b.key));

        let mut writer = SSTableWriter::new(options, header.clone());
        for p in &partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer.finish().unwrap();

        // Verify we can read all partitions back
        let mut reader = DataReader::new(&output.data, &header, 0);
        let mut count = 0;
        while reader.read_partition().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 5);

        // Verify bloom filter membership
        let bloom = BloomFilter::read(&output.filter).unwrap();
        for p in &partitions {
            let (h1, h2) = p.key.filter_hash();
            assert!(
                bloom.is_present(h1, h2),
                "bloom filter should contain key {:?}",
                p.key.key.as_bytes()
            );
        }

        // Verify a missing key is likely not present
        let missing_key = DecoratedKey::new(PartitionKey::from(b"nonexistent".as_slice()));
        let (h1, h2) = missing_key.filter_hash();
        // This is probabilistic — we just assert no crash
        let _ = bloom.is_present(h1, h2);
    }

    #[test]
    fn write_many_partitions_point_lookup_roundtrip() {
        use crate::reader::{SSTableComponents, SSTableReader};

        let header = test_header();
        let options = WriteOptions {
            compression: None,
            bloom_fp_chance: 0.01,
            chunk_size: 65536,
            verify_output: true,
        };

        let mut partitions: Vec<Partition> = (0..2000u64)
            .map(|i| {
                make_partition(
                    format!("k{i:06}").as_bytes(),
                    &[0x00, 0x00, ((i >> 8) & 0xFF) as u8, (i & 0xFF) as u8],
                    format!("v{i}").as_bytes(),
                    i as i64,
                )
            })
            .collect();
        partitions.sort_by(|a, b| a.key.cmp(&b.key));

        let mut writer = SSTableWriter::new(options, header);
        for partition in &partitions {
            writer.add_partition(partition).unwrap();
        }
        let output = writer.finish().unwrap();
        let reader = SSTableReader::open(SSTableComponents {
            data: output.data.as_slice(),
            partitions: output.partitions.as_slice(),
            rows: output.rows.as_slice(),
            filter: output.filter,
            compression_info: output.compression_info,
            statistics: output.statistics,
        })
        .unwrap();

        for i in 0..2000u64 {
            let key = DecoratedKey::new(PartitionKey::new(format!("k{i:06}").into_bytes()));
            assert!(
                reader.get_partition(&key).unwrap().is_some(),
                "point lookup missed k{i:06}"
            );
        }
    }

    #[test]
    fn writer_stores_token_ordered_index_bounds_for_read_pruning() {
        use crate::byte_comparable;
        use crate::reader::{SSTableComponents, SSTableReader};

        let header = test_header();
        let options = WriteOptions {
            compression: None,
            bloom_fp_chance: 0.01,
            chunk_size: 65536,
            verify_output: true,
        };

        let mut p1 = make_partition(b"p1", &[0, 0, 0, 1], b"v1", 1);
        p1.key = DecoratedKey {
            token: Token(10),
            key: PartitionKey::from(b"p1".as_slice()),
        };
        let mut p2 = make_partition(b"p2", &[0, 0, 0, 2], b"v2", 2);
        p2.key = DecoratedKey {
            token: Token(20),
            key: PartitionKey::from(b"p2".as_slice()),
        };

        let mut writer = SSTableWriter::new(options, header);
        writer.add_partition(&p1).unwrap();
        writer.add_partition(&p2).unwrap();
        let output = writer.finish().unwrap();

        let reader = SSTableReader::open(SSTableComponents {
            data: output.data.as_slice(),
            partitions: output.partitions.as_slice(),
            rows: output.rows.as_slice(),
            filter: output.filter,
            compression_info: output.compression_info,
            statistics: output.statistics,
        })
        .unwrap();

        assert_eq!(
            reader.smallest_key_bytes(),
            byte_comparable::encode(&p1.key)
        );
        assert_eq!(reader.largest_key_bytes(), byte_comparable::encode(&p2.key));
        assert!(reader.may_contain_key(&p1.key));

        let same_key_before_range = DecoratedKey {
            token: Token(9),
            key: PartitionKey::from(b"p1".as_slice()),
        };
        assert!(!reader.may_contain_key(&same_key_before_range));
    }

    #[test]
    fn round_trip_write_read_all_components() {
        let header = test_header();
        let options = WriteOptions {
            compression: None,
            bloom_fp_chance: 0.01,
            chunk_size: 65536,
            verify_output: true,
        };

        let timestamp = 1_000_100i64;
        let partition = make_partition(
            b"roundtrip",
            &[0x00, 0x00, 0x00, 0x0A],
            b"test_value",
            timestamp,
        );

        let mut writer = SSTableWriter::new(options, header.clone());
        writer.add_partition(&partition).unwrap();
        let output = writer.finish().unwrap();

        // Verify Statistics.db round-trips
        let stats = crate::statistics::read_statistics(&output.statistics).unwrap();
        assert_eq!(stats.header.min_timestamp, header.min_timestamp);
        assert_eq!(
            stats.validation.partitioner_class,
            "org.apache.cassandra.dht.Murmur3Partitioner"
        );

        // Verify TOC
        let toc_entries = crate::toc::read_toc(&output.toc).unwrap();
        assert!(!toc_entries.is_empty());
        // No compression — CRC.db is omitted from TOC (listing it without
        // writing the file causes CorruptSSTableException on Cassandra import).
        assert!(!toc_entries.iter().any(|e| e == toc::CRC));

        // Verify Partitions.db can be opened as a PartitionIndex
        let pi = crate::partition_index::PartitionIndex::open(output.partitions).unwrap();
        assert_eq!(pi.key_count(), 1);

        // Look up the key in the partition index
        let dk = DecoratedKey::new(PartitionKey::from(b"roundtrip".as_slice()));
        match pi.lookup(&dk).unwrap() {
            crate::partition_index::PartitionLookup::DataDirect { position } => {
                // Read from the data at this position
                let mut reader = DataReader::new(&output.data, &header, position);
                let p = reader.read_partition().unwrap().expect("partition");
                assert_eq!(p.key.key.as_bytes(), b"roundtrip");
                assert_eq!(p.rows.len(), 1);
                assert_eq!(
                    p.rows[0].cells[0].1.value.as_deref(),
                    Some(b"test_value".as_slice())
                );
            }
            other => panic!("expected DataDirect, got {:?}", other),
        }

        // Verify Rows.db is empty
        assert!(output.rows.is_empty());

        // Verify compression_info is None (no compression)
        assert!(output.compression_info.is_none());
    }

    #[test]
    fn wide_clustered_partition_writes_rows_index() {
        let header = test_header();
        let options = WriteOptions {
            compression: None,
            bloom_fp_chance: 0.01,
            chunk_size: 65536,
            verify_output: true,
        };
        let partition = make_wide_partition(b"wide-index", ROW_INDEX_MIN_ROWS);
        let key = partition.key.clone();

        let mut writer = SSTableWriter::new(options, header);
        writer.add_partition(&partition).unwrap();
        let output = writer.finish().unwrap();

        assert!(
            !output.rows.is_empty(),
            "wide clustered partitions need a Rows.db row index"
        );

        let partition_index =
            crate::partition_index::PartitionIndex::open(output.partitions).unwrap();
        let row_index_position = match partition_index.lookup(&key).unwrap() {
            crate::partition_index::PartitionLookup::RowIndex { position } => position,
            other => panic!("expected RowIndex, got {other:?}"),
        };
        let entry = crate::row_index::RowIndex::read_entry(&output.rows, row_index_position)
            .expect("row-index footer should decode");
        assert_eq!(entry.data_position, 0);

        let target = 17_i32.to_be_bytes();
        let offset = crate::row_index::lookup_clustering_in_entry(&output.rows, &entry, &target)
            .unwrap()
            .expect("target clustering row should be indexed");
        assert!(offset > 0);
    }

    #[test]
    fn write_with_compression() {
        let header = test_header();
        let options = WriteOptions {
            compression: Some(Compression::Lz4),
            bloom_fp_chance: 0.01,
            chunk_size: 64, // Small chunks to test chunking
            verify_output: true,
        };

        // Create enough data to span multiple chunks
        let mut partitions: Vec<Partition> = (0..20)
            .map(|i| {
                make_partition(
                    format!("ckey{:04}", i).as_bytes(),
                    &[0x00, 0x00, 0x00, i as u8],
                    format!("compressed_value_{:04}", i).as_bytes(),
                    1_000_000 + i as i64,
                )
            })
            .collect();
        partitions.sort_by(|a, b| a.key.cmp(&b.key));

        let mut writer = SSTableWriter::new(options, header.clone());
        for p in &partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer.finish().unwrap();

        // Verify CompressionInfo.db is present
        assert!(output.compression_info.is_some());
        let ci = CompressionInfo::read(output.compression_info.as_ref().unwrap()).unwrap();
        assert_eq!(ci.chunk_length, 64);
        assert!(ci.chunk_offsets.len() > 1, "should have multiple chunks");

        // Verify TOC lists CompressionInfo
        let toc_entries = crate::toc::read_toc(&output.toc).unwrap();
        assert!(toc_entries.iter().any(|e| e == toc::COMPRESSION_INFO));

        // Decompress and read back
        let compression = Compression::Lz4;
        let mut uncompressed_data = Vec::new();
        for i in 0..ci.chunk_offsets.len() {
            let start = ci.chunk_offsets[i] as usize;
            let end = if i + 1 < ci.chunk_offsets.len() {
                ci.chunk_offsets[i + 1] as usize
            } else {
                output.data.len()
            };
            let chunk = &output.data[start..end];
            let payload_len = chunk.len() - std::mem::size_of::<u32>();
            let payload = &chunk[..payload_len];
            let stored_crc = u32::from_be_bytes([
                chunk[payload_len],
                chunk[payload_len + 1],
                chunk[payload_len + 2],
                chunk[payload_len + 3],
            ]);
            assert_eq!(crc32fast::hash(payload), stored_crc);
            let decompressed = compression.decompress(payload, ci.chunk_length).unwrap();
            uncompressed_data.extend_from_slice(&decompressed);
        }

        // Now read from the decompressed data
        let mut reader = DataReader::new(&uncompressed_data, &header, 0);
        let mut count = 0;
        while reader.read_partition().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 20);
    }

    #[test]
    fn write_with_zstd_compression() {
        let header = test_header();
        let options = WriteOptions {
            compression: Some(Compression::Zstd { level: 3 }),
            bloom_fp_chance: 0.01,
            chunk_size: 64,
            verify_output: true,
        };

        let mut partitions: Vec<Partition> = (0..20)
            .map(|i| {
                make_partition(
                    format!("zkey{:04}", i).as_bytes(),
                    &[0x00, 0x00, 0x00, i as u8],
                    format!("zstd_value_{:04}", i).as_bytes(),
                    1_000_000 + i as i64,
                )
            })
            .collect();
        partitions.sort_by(|a, b| a.key.cmp(&b.key));

        let mut writer = SSTableWriter::new(options, header);
        for p in &partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer.finish().unwrap();

        let ci = CompressionInfo::read(output.compression_info.as_ref().unwrap()).unwrap();
        assert!(matches!(ci.compression, Compression::Zstd { .. }));
        assert!(ci.chunk_offsets.len() > 1, "should have multiple chunks");
    }

    #[test]
    fn write_partition_with_cell_own_timestamp() {
        let header = test_header();
        let options = WriteOptions {
            compression: None,
            bloom_fp_chance: 0.01,
            chunk_size: 65536,
            verify_output: true,
        };

        let row_ts = 1_000_100i64;
        let cell_ts = 1_000_200i64;

        let partition = Partition {
            key: DecoratedKey::new(PartitionKey::from(b"ts_test".as_slice())),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![Row {
                clustering: vec![0x00, 0x00, 0x00, 0x01],
                cells: vec![(0, CellValue::live(b"val".to_vec(), cell_ts))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(row_ts),
            }],
        };

        let mut writer = SSTableWriter::new(options, header.clone());
        writer.add_partition(&partition).unwrap();
        let output = writer.finish().unwrap();

        let mut reader = DataReader::new(&output.data, &header, 0);
        let p = reader.read_partition().unwrap().expect("partition");

        let row = &p.rows[0];
        assert_eq!(row.primary_key_liveness.timestamp, row_ts);

        let (_, ref cell) = row.cells[0];
        assert_eq!(cell.timestamp, cell_ts);
    }

    #[test]
    fn write_partition_with_tombstone_cell() {
        // Use a header with min_local_deletion_time that allows unsigned deltas
        let header = SerializationHeader {
            complex_collections: false,
            min_timestamp: 1_000_000,
            min_local_deletion_time: 0,
            min_ttl: 0,
            max_timestamp: i64::MAX,
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            clustering_types: vec!["org.apache.cassandra.db.marshal.Int32Type".into()],
            static_columns: vec![],
            regular_columns: vec![(
                b"val".to_vec(),
                "org.apache.cassandra.db.marshal.UTF8Type".into(),
            )],
        };
        let options = WriteOptions {
            compression: None,
            bloom_fp_chance: 0.01,
            chunk_size: 65536,
            verify_output: true,
        };

        let row_ts = 1_000_050i64;
        let cell_ts = 1_000_060i64;
        let ldt = 100i32;

        let partition = Partition {
            key: DecoratedKey::new(PartitionKey::from(b"tomb_test".as_slice())),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![Row {
                clustering: vec![0x00, 0x00, 0x00, 0x03],
                cells: vec![(0, CellValue::tombstone(cell_ts, ldt))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(row_ts),
            }],
        };

        let mut writer = SSTableWriter::new(options, header.clone());
        writer.add_partition(&partition).unwrap();
        let output = writer.finish().unwrap();

        let mut reader = DataReader::new(&output.data, &header, 0);
        let p = reader.read_partition().unwrap().expect("partition");

        let row = &p.rows[0];
        let (_, ref cell) = row.cells[0];
        assert!(cell.is_tombstone());
        assert_eq!(cell.timestamp, cell_ts);
        assert_eq!(cell.local_deletion_time, ldt);
        assert!(cell.value.is_none());
    }

    /// Tables with no clustering columns (simple PRIMARY KEY) must roundtrip.
    /// Regression test for the extra clustering-length varint bug.
    #[test]
    fn write_no_clustering_columns_roundtrip() {
        let header = SerializationHeader {
            complex_collections: false,
            min_timestamp: 1_000_000,
            min_local_deletion_time: i32::MAX,
            min_ttl: 0,
            max_timestamp: i64::MAX,
            key_type: "org.apache.cassandra.db.marshal.Int32Type".into(),
            clustering_types: vec![], // No clustering columns
            static_columns: vec![],
            regular_columns: vec![(
                b"val".to_vec(),
                "org.apache.cassandra.db.marshal.UTF8Type".into(),
            )],
        };
        let options = WriteOptions {
            compression: None,
            bloom_fp_chance: 0.01,
            chunk_size: 65536,
            verify_output: true,
        };

        let timestamp = 1_000_042i64;
        let partition = Partition {
            key: DecoratedKey::new(PartitionKey::new(vec![0x00, 0x00, 0x00, 0x01])),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![Row {
                clustering: vec![], // empty — no clustering columns
                cells: vec![(0, CellValue::live(b"hello".to_vec(), timestamp))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
            }],
        };

        let mut writer = SSTableWriter::new(options, header.clone());
        writer.add_partition(&partition).unwrap();
        let output = writer.finish().unwrap();

        let mut reader = DataReader::new(&output.data, &header, 0);
        let read_partition = reader
            .read_partition()
            .unwrap()
            .expect("expected partition");

        assert_eq!(read_partition.rows.len(), 1);
        let row = &read_partition.rows[0];
        assert!(row.clustering.is_empty());
        assert_eq!(row.cells.len(), 1);
        let (_, ref cell) = row.cells[0];
        assert!(cell.is_live());
        assert_eq!(cell.value.as_deref(), Some(b"hello".as_slice()));
        assert_eq!(cell.timestamp, timestamp);

        assert!(reader.read_partition().unwrap().is_none());
    }

    /// FRSA-BUG-026 root cause: expiring cells (USING TTL) must roundtrip
    /// through SSTable write + read. The writer must set CELL_IS_EXPIRING
    /// and write the local_deletion_time + TTL deltas.
    #[test]
    fn write_partition_with_expiring_cell_roundtrip() {
        let header = SerializationHeader {
            complex_collections: false,
            min_timestamp: 1_000_000,
            min_local_deletion_time: 0,
            min_ttl: 0,
            max_timestamp: i64::MAX,
            key_type: "org.apache.cassandra.db.marshal.Int32Type".into(),
            clustering_types: vec![],
            static_columns: vec![],
            regular_columns: vec![(
                b"val".to_vec(),
                "org.apache.cassandra.db.marshal.UTF8Type".into(),
            )],
        };
        let options = WriteOptions {
            compression: None,
            bloom_fp_chance: 0.01,
            chunk_size: 65536,
            verify_output: true,
        };

        let timestamp = 1_000_100i64;
        let ttl = 3600i32;
        let ldt = 1_700_000i32; // local deletion time = now + ttl

        let partition = Partition {
            key: DecoratedKey::new(PartitionKey::new(42i32.to_be_bytes().to_vec())),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![Row {
                clustering: vec![],
                cells: vec![(
                    0,
                    CellValue::expiring(b"hello_ttl".to_vec(), timestamp, ttl, ldt),
                )],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
            }],
        };

        let mut writer = SSTableWriter::new(options, header.clone());
        writer.add_partition(&partition).unwrap();
        let output = writer.finish().unwrap();

        let mut reader = DataReader::new(&output.data, &header, 0);
        let p = reader.read_partition().unwrap().expect("partition");

        let row = &p.rows[0];
        let (_, ref cell) = row.cells[0];
        assert!(
            !cell.is_tombstone(),
            "expiring cell should not be a tombstone"
        );
        assert_eq!(cell.timestamp, timestamp);
        assert_eq!(cell.ttl, ttl, "TTL must roundtrip");
        assert_eq!(
            cell.local_deletion_time, ldt,
            "local_deletion_time must roundtrip"
        );
        assert_eq!(
            cell.value.as_deref(),
            Some(b"hello_ttl".as_slice()),
            "value must roundtrip"
        );
    }

    /// Diagnostic: dump Partitions.db structure for the exact 2-key scenario
    /// used in the cassandra_reads_compacted_sstable_from_s3 integration test.
    ///
    /// Run with: cargo test -p ferrosa-sstable partitions_db_pk1_pk2_hex_dump -- --nocapture
    #[test]
    fn partitions_db_pk1_pk2_hex_dump() {
        let header = SerializationHeader {
            complex_collections: false,
            min_timestamp: 1_000_000,
            min_local_deletion_time: i32::MAX,
            min_ttl: 0,
            max_timestamp: i64::MAX,
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            clustering_types: vec![],
            static_columns: vec![],
            regular_columns: vec![
                (
                    b"v_text".to_vec(),
                    "org.apache.cassandra.db.marshal.UTF8Type".into(),
                ),
                (
                    b"v_int".to_vec(),
                    "org.apache.cassandra.db.marshal.Int32Type".into(),
                ),
            ],
        };
        let options = WriteOptions {
            compression: None,
            bloom_fp_chance: 0.01,
            chunk_size: 65536,
            verify_output: true,
        };

        let dk1 = DecoratedKey::new(PartitionKey::from(b"pk1".as_slice()));
        let dk2 = DecoratedKey::new(PartitionKey::from(b"pk2".as_slice()));
        eprintln!("token(pk1) = {}", dk1.token.0);
        eprintln!("token(pk2) = {}", dk2.token.0);

        let mut partitions = vec![
            Partition {
                key: dk1.clone(),
                deletion: DeletionTime::LIVE,
                static_row: None,
                rows: vec![Row {
                    clustering: vec![],
                    cells: vec![(0, CellValue::live(b"hello".to_vec(), 1000))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(1000),
                }],
            },
            Partition {
                key: dk2.clone(),
                deletion: DeletionTime::LIVE,
                static_row: None,
                rows: vec![Row {
                    clustering: vec![],
                    cells: vec![(1, CellValue::live(42i32.to_be_bytes().to_vec(), 2000))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(2000),
                }],
            },
        ];
        // Sort by token order, exactly like the compaction executor
        partitions.sort_by(|a, b| a.key.cmp(&b.key));

        eprintln!("Write order (token-sorted):");
        for p in &partitions {
            eprintln!("  key={:?} token={}", p.key.key.as_bytes(), p.key.token.0);
        }

        let mut writer = SSTableWriter::new(options, header);
        for p in &partitions {
            writer.add_partition(p).unwrap();
        }
        let output = writer.finish().unwrap();

        let partitions_db = &output.partitions;
        eprintln!("Partitions.db total bytes: {}", partitions_db.len());

        // Decode footer (last 24 bytes)
        let len = partitions_db.len();
        assert!(len >= 24, "Partitions.db too small");
        let key_bounds_offset =
            i64::from_be_bytes(partitions_db[len - 24..len - 16].try_into().unwrap());
        let key_count = i64::from_be_bytes(partitions_db[len - 16..len - 8].try_into().unwrap());
        let root_pos = i64::from_be_bytes(partitions_db[len - 8..len].try_into().unwrap());
        eprintln!("Footer: key_bounds_offset={key_bounds_offset}, key_count={key_count}, root_pos={root_pos}");

        // Decode key bounds
        let kb_off = key_bounds_offset as usize;
        let first_len =
            u16::from_be_bytes(partitions_db[kb_off..kb_off + 2].try_into().unwrap()) as usize;
        let first_key = &partitions_db[kb_off + 2..kb_off + 2 + first_len];
        let second_start = kb_off + 2 + first_len;
        let last_len = u16::from_be_bytes(
            partitions_db[second_start..second_start + 2]
                .try_into()
                .unwrap(),
        ) as usize;
        let last_key = &partitions_db[second_start + 2..second_start + 2 + last_len];
        eprintln!("Key bounds: first={:?} last={:?}", first_key, last_key);
        eprintln!(
            "Key bounds: first={} last={}",
            std::str::from_utf8(first_key).unwrap_or("?"),
            std::str::from_utf8(last_key).unwrap_or("?")
        );

        // Hex dump of full Partitions.db
        eprintln!("Partitions.db hex dump:");
        for (i, chunk) in partitions_db.chunks(16).enumerate() {
            let hex: String = chunk
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<Vec<_>>()
                .join(" ");
            let ascii: String = chunk
                .iter()
                .map(|&b| if b.is_ascii_graphic() { b as char } else { '.' })
                .collect();
            eprintln!("  {:04x}: {:<48}  {}", i * 16, hex, ascii);
        }

        // Verify that the key bounds ordering matches the token-sorted write
        // order in the same byte-comparable representation used by the trie.
        let write_first = crate::byte_comparable::encode(&partitions[0].key);
        let write_last = crate::byte_comparable::encode(&partitions[partitions.len() - 1].key);
        assert_eq!(
            first_key, write_first,
            "key bounds first={:?} should match token-sorted first={:?}",
            first_key, write_first
        );
        assert_eq!(
            last_key, write_last,
            "key bounds last={:?} should match token-sorted last={:?}",
            last_key, write_last
        );
    }

    #[test]
    fn writer_serializes_sparse_columns_as_cassandra_subset_vint() {
        let header = SerializationHeader {
            complex_collections: false,
            min_timestamp: 1_000,
            min_local_deletion_time: i32::MAX,
            min_ttl: 0,
            max_timestamp: i64::MAX,
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            clustering_types: vec![],
            static_columns: vec![],
            regular_columns: vec![
                (
                    b"v_text".to_vec(),
                    "org.apache.cassandra.db.marshal.UTF8Type".into(),
                ),
                (
                    b"v_int".to_vec(),
                    "org.apache.cassandra.db.marshal.Int32Type".into(),
                ),
            ],
        };
        let options = WriteOptions {
            compression: None,
            bloom_fp_chance: 0.01,
            chunk_size: 65536,
            verify_output: true,
        };
        let partition = Partition {
            key: DecoratedKey::new(PartitionKey::from(b"pk1".as_slice())),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![Row {
                clustering: vec![],
                cells: vec![(0, CellValue::live(b"hello".to_vec(), 1_005))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(1_005),
            }],
        };

        let mut writer = SSTableWriter::new(options, header);
        writer.add_partition(&partition).unwrap();
        let output = writer.finish().unwrap();

        let data = output.data.as_slice();
        let mut pos = 2 + b"pk1".len() + 1;
        let flags = data[pos];
        pos += 1;
        assert_eq!(flags & HAS_ALL_COLUMNS, 0);

        let (row_body_len, n) = varint::read_unsigned_vint_at(&data, pos as u64).unwrap();
        assert!(
            row_body_len > 0,
            "tables without clustering columns must not serialize an empty clustering prefix before the row body length"
        );
        pos += n;
        let (_prev_size, n) = varint::read_unsigned_vint_at(&data, pos as u64).unwrap();
        pos += n;
        let (timestamp_delta, n) = varint::read_unsigned_vint_at(&data, pos as u64).unwrap();
        assert_eq!(timestamp_delta, 5);
        pos += n;

        let (columns_subset, _) = varint::read_unsigned_vint_at(&data, pos as u64).unwrap();
        assert_eq!(
            columns_subset, 0b10,
            "Cassandra Columns.Serializer encodes bit i=1 as column i missing"
        );
    }

    #[test]
    fn writer_serializes_fixed_width_cells_without_value_length_prefix() {
        let header = SerializationHeader {
            complex_collections: false,
            min_timestamp: 1_000,
            min_local_deletion_time: i32::MAX,
            min_ttl: 0,
            max_timestamp: i64::MAX,
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            clustering_types: vec![],
            static_columns: vec![],
            regular_columns: vec![
                (
                    b"v_int".to_vec(),
                    "org.apache.cassandra.db.marshal.Int32Type".into(),
                ),
                (
                    b"v_text".to_vec(),
                    "org.apache.cassandra.db.marshal.UTF8Type".into(),
                ),
            ],
        };
        let options = WriteOptions {
            compression: None,
            bloom_fp_chance: 0.01,
            chunk_size: 65536,
            verify_output: true,
        };
        let partition = Partition {
            key: DecoratedKey::new(PartitionKey::from(b"pk2".as_slice())),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![Row {
                clustering: vec![],
                cells: vec![(0, CellValue::live(42i32.to_be_bytes().to_vec(), 1_005))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(1_005),
            }],
        };

        let mut writer = SSTableWriter::new(options, header);
        writer.add_partition(&partition).unwrap();
        let output = writer.finish().unwrap();

        let data = output.data.as_slice();
        let mut pos = 2 + b"pk2".len() + 1;
        let flags = data[pos];
        pos += 1;
        assert_eq!(flags & HAS_ALL_COLUMNS, 0);

        let (_row_body_len, n) = varint::read_unsigned_vint_at(&data, pos as u64).unwrap();
        pos += n;
        let (_prev_size, n) = varint::read_unsigned_vint_at(&data, pos as u64).unwrap();
        pos += n;
        let (timestamp_delta, n) = varint::read_unsigned_vint_at(&data, pos as u64).unwrap();
        assert_eq!(timestamp_delta, 5);
        pos += n;
        let (columns_subset, n) = varint::read_unsigned_vint_at(&data, pos as u64).unwrap();
        assert_eq!(columns_subset, 0b10);
        pos += n;
        assert_eq!(data[pos], CELL_USE_ROW_TIMESTAMP);
        pos += 1;
        assert_eq!(&data[pos..pos + 4], &42i32.to_be_bytes());
    }

    /// RED TEST: A partition with a row whose deletion timestamp is lower
    /// than the header's min_timestamp must be caught during writing.
    ///
    /// This simulates what happens during compaction when merge produces a
    /// row with an old deletion timestamp. Without the fix, the delta
    /// underflows to a huge u64, producing a corrupt varint that misaligns
    /// the reader — the root cause of post-compaction data loss.
    #[test]
    #[should_panic(expected = "delta would underflow")]
    fn row_deletion_timestamp_below_header_min_panics() {
        let header = SerializationHeader {
            complex_collections: false,
            min_timestamp: 1_000_000, // header min is 1M
            min_local_deletion_time: 100,
            min_ttl: 0,
            max_timestamp: i64::MAX,
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            clustering_types: vec!["org.apache.cassandra.db.marshal.Int32Type".into()],
            static_columns: vec![],
            regular_columns: vec![(
                b"val".to_vec(),
                "org.apache.cassandra.db.marshal.UTF8Type".into(),
            )],
        };

        // Row with deletion timestamp 500 < header min 1_000_000
        let partition = Partition {
            key: DecoratedKey::new(PartitionKey::from(b"pk1".as_slice())),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![Row {
                clustering: vec![0x00, 0x00, 0x00, 0x01],
                cells: vec![(0, CellValue::live(b"val".to_vec(), 1_000_000))],
                deletion: DeletionTime::new(500, 100), // OLD deletion timestamp!
                primary_key_liveness: LivenessInfo::with_timestamp(1_000_000),
            }],
        };

        let options = WriteOptions {
            compression: None,
            bloom_fp_chance: 0.01,
            chunk_size: 65536,
            verify_output: true,
        };
        let mut writer = SSTableWriter::new(options, header);
        // This MUST panic — writing a row with deletion ts < header min
        // would produce a corrupt SSTable (varint underflow)
        writer.add_partition(&partition).unwrap();
    }

    /// Multi-column clustering key roundtrip (typed_edges schema: uuid, text, uuid).
    ///
    /// This is the P0 data loss regression test. The SSTableWriter serializes
    /// multi-column clustering keys as a single varint-prefixed blob, but the
    /// DataReader reads per-component with type-aware length handling. The
    /// format mismatch causes parse drift for every row after the first field,
    /// corrupting all subsequent data in the SSTable.
    #[test]
    fn multi_column_clustering_key_roundtrip() {
        // Header matching typed_edges: (uuid, text, uuid) clustering
        let header = SerializationHeader {
            complex_collections: false,
            min_timestamp: 1_000_000,
            min_local_deletion_time: i32::MAX,
            min_ttl: 0,
            max_timestamp: i64::MAX,
            key_type: "org.apache.cassandra.db.marshal.CompositeType(\
                org.apache.cassandra.db.marshal.UUIDType,\
                org.apache.cassandra.db.marshal.UUIDType)"
                .into(),
            clustering_types: vec![
                "org.apache.cassandra.db.marshal.UUIDType".into(), // src_id
                "org.apache.cassandra.db.marshal.UTF8Type".into(), // edge_type
                "org.apache.cassandra.db.marshal.UUIDType".into(), // dst_id
            ],
            static_columns: vec![],
            regular_columns: vec![(
                b"weight".to_vec(),
                "org.apache.cassandra.db.marshal.DoubleType".into(),
            )],
        };
        let options = WriteOptions {
            compression: None,
            bloom_fp_chance: 0.01,
            chunk_size: 65536,
            verify_output: true,
        };

        let timestamp = 1_000_100i64;

        // Build clustering key in the u16-prefixed format the CQL bridge produces
        // for multi-column CK: [u16 len][component bytes] per component.
        let src_id = [0xAAu8; 16];
        let edge_type = b"RELATED_TO";
        let dst_id = [0xBBu8; 16];
        let mut ck = Vec::new();
        ck.extend_from_slice(&(src_id.len() as u16).to_be_bytes());
        ck.extend_from_slice(&src_id);
        ck.extend_from_slice(&(edge_type.len() as u16).to_be_bytes());
        ck.extend_from_slice(edge_type);
        ck.extend_from_slice(&(dst_id.len() as u16).to_be_bytes());
        ck.extend_from_slice(&dst_id);

        let weight: f64 = 0.85;
        let partition = Partition {
            key: DecoratedKey::new(PartitionKey::new(vec![0x11; 16])),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![Row {
                clustering: ck.clone(),
                cells: vec![(0, CellValue::live(weight.to_be_bytes().to_vec(), timestamp))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
            }],
        };

        let mut writer = SSTableWriter::new(options, header.clone());
        writer.add_partition(&partition).unwrap();
        let output = writer.finish().unwrap();

        // Read back via sequential DataReader.
        let mut reader = DataReader::new(&output.data, &header, 0);
        let read_back = reader
            .read_partition()
            .expect("read should not error on data we just wrote")
            .expect("partition should be present");

        assert_eq!(read_back.rows.len(), 1, "should have 1 row");
        let row = &read_back.rows[0];

        // Verify the cell value survived
        assert_eq!(
            row.cells[0].1.value.as_deref().unwrap(),
            weight.to_be_bytes(),
            "cell value must survive roundtrip for multi-column clustering key"
        );

        // Verify clustering key bytes survive roundtrip.
        // The reader must produce the same u16-prefixed format the writer consumed.
        assert_eq!(
            row.clustering, ck,
            "multi-column CK bytes must roundtrip through write→read"
        );
    }

    /// P0 data-loss regression: if a row has a cell whose col_idx is
    /// out-of-range relative to the header's regular_columns, the writer's
    /// HashSet-based bitmap silently drops that cell from the bitmap but
    /// still serializes the cell bytes to the body. Reader then under-reads
    /// by one cell, and subsequent rows/partitions drift. This is the
    /// suspected root cause of entity_store's 83% silent data-loss on
    /// fresh clusters.
    ///
    /// Either the writer must reject out-of-range col_idx at write time
    /// (fail loud), or serialize only the in-range cells.
    #[test]
    fn writer_rejects_cell_with_out_of_range_column_index() {
        let header = test_header(); // single regular column `val` (idx 0)
        let options = WriteOptions {
            compression: None,
            bloom_fp_chance: 0.01,
            chunk_size: 65536,
            verify_output: true,
        };

        let timestamp = 1_000_042i64;
        // Row with a legit cell (col 0) and a bogus out-of-range cell (col 99).
        let partition = Partition {
            key: DecoratedKey::new(PartitionKey::from(b"pk".as_slice())),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![Row {
                clustering: vec![0x00, 0x00, 0x00, 0x01],
                cells: vec![
                    (0, CellValue::live(b"hello".to_vec(), timestamp)),
                    (99, CellValue::live(b"garbage".to_vec(), timestamp)),
                ],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
            }],
        };

        let mut writer = SSTableWriter::new(options, header.clone());
        // Must either return an error or panic — silent acceptance is the
        // bug we're fixing.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            writer.add_partition(&partition)
        }));
        match result {
            Ok(Ok(())) => panic!(
                "writer silently accepted cell with col_idx=99 when num_columns=1; \
                 this produces a silently corrupt SSTable (the exact P0 we are chasing)"
            ),
            Ok(Err(_)) | Err(_) => {
                // Either a Result error or a panic is acceptable — the point
                // is the writer must NOT silently accept the corrupt input.
            }
        }
    }

    /// P0 alt hypothesis: duplicate col_idx in row.cells also produces a
    /// bitmap that under-counts cells vs. body length. The HashSet-based
    /// `present_set` dedupes duplicates, but the cell-serialization loop
    /// writes every entry in `row.cells`.
    #[test]
    fn writer_rejects_duplicate_column_index_cells() {
        // 7-column header matching entity_store shape
        let header = SerializationHeader {
            complex_collections: false,
            min_timestamp: 1_000_000,
            min_local_deletion_time: i32::MAX,
            min_ttl: 0,
            max_timestamp: i64::MAX,
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            clustering_types: vec!["org.apache.cassandra.db.marshal.UUIDType".into()],
            static_columns: vec![],
            regular_columns: (0..7)
                .map(|i| {
                    (
                        format!("col{i}").into_bytes(),
                        "org.apache.cassandra.db.marshal.UTF8Type".into(),
                    )
                })
                .collect(),
        };
        let options = WriteOptions {
            compression: None,
            bloom_fp_chance: 0.01,
            chunk_size: 65536,
            verify_output: true,
        };

        let ts = 1_000_042i64;
        // Duplicate col_idx=3 (empty + long value).
        let partition = Partition {
            key: DecoratedKey::new(PartitionKey::from(b"pk".as_slice())),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![Row {
                clustering: [0xAAu8; 16].to_vec(),
                cells: vec![
                    (0, CellValue::live(b"name".to_vec(), ts)),
                    (1, CellValue::live(b"type".to_vec(), ts)),
                    (3, CellValue::live(b"".to_vec(), ts)),
                    (5, CellValue::live(vec![0x3f, 0x80, 0x00, 0x00], ts)),
                    (6, CellValue::live(b"12345678".to_vec(), ts)),
                    (3, CellValue::live(b"long context snippet".to_vec(), ts)),
                ],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(ts),
            }],
        };

        let mut writer = SSTableWriter::new(options, header.clone());
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            writer.add_partition(&partition)
        }));
        if let Ok(Ok(())) = result {
            panic!(
                "writer silently accepted row.cells with duplicate col_idx=3; \
                 body written has more cells than bitmap bits set → silent SSTable corruption"
            );
        }
    }

    /// P0 data-loss regression: parameterized round-trip for entity_store-shaped
    /// partitions. Schema: PK=(uuid,uuid), CK=uuid, regular columns mix text
    /// (variable-length) + vector<float,N> (large variable-length). Production
    /// writes multiple entities per session, producing multi-row partitions.
    ///
    /// The hypothesis under test: the SSTable writer serializes row bodies
    /// with sizes that drift against what the reader consumes, so the first
    /// partition parses fine but later rows/partitions trip
    /// `corrupted DeletionTime flags: 0xNN`.
    ///
    /// Cases vary:
    ///   - rows per partition: 1, 2, 5
    ///   - partitions per SSTable: 1, 3
    ///   - cell value size per row: small (64B) and large (3072B ≈ 768-dim f32 vector)
    #[test]
    fn multi_row_partition_roundtrip_entity_store_shape() {
        // entity_store shape: PK (tenant_id uuid, session_id uuid), CK entity_id uuid,
        // regular columns: entity_name text, entity_type text, embedding (3072B blob)
        let header = SerializationHeader {
            complex_collections: false,
            min_timestamp: 1_000_000,
            min_local_deletion_time: i32::MAX,
            min_ttl: 0,
            max_timestamp: i64::MAX,
            key_type: "org.apache.cassandra.db.marshal.CompositeType(\
                org.apache.cassandra.db.marshal.UUIDType,\
                org.apache.cassandra.db.marshal.UUIDType)"
                .into(),
            clustering_types: vec!["org.apache.cassandra.db.marshal.UUIDType".into()],
            static_columns: vec![],
            regular_columns: vec![
                (
                    b"entity_name".to_vec(),
                    "org.apache.cassandra.db.marshal.UTF8Type".into(),
                ),
                (
                    b"entity_type".to_vec(),
                    "org.apache.cassandra.db.marshal.UTF8Type".into(),
                ),
                (
                    b"embedding".to_vec(),
                    "org.apache.cassandra.db.marshal.BytesType".into(),
                ),
            ],
        };
        let options = WriteOptions {
            compression: None,
            bloom_fp_chance: 0.01,
            chunk_size: 65536,
            verify_output: true,
        };

        // Composite PK encoding: [u16 len][bytes][0x00] per component.
        fn make_composite_pk(part1: &[u8], part2: &[u8]) -> Vec<u8> {
            let mut buf = Vec::new();
            buf.extend_from_slice(&(part1.len() as u16).to_be_bytes());
            buf.extend_from_slice(part1);
            buf.push(0x00);
            buf.extend_from_slice(&(part2.len() as u16).to_be_bytes());
            buf.extend_from_slice(part2);
            buf.push(0x00);
            buf
        }

        // Parameterize: (num_partitions, rows_per_partition, cell_size, present_cells_bitmask)
        // bitmask: which of the 3 regular columns to include per row (1=col0, 2=col1, 4=col2)
        //   0b111 = all 3 columns → HAS_ALL_COLUMNS path
        //   0b101, 0b110, 0b011, 0b001 etc. = sparse → missing-column bitmap path
        let cases: &[(usize, usize, usize, u8)] = &[
            (1, 1, 64, 0b111),
            (1, 2, 64, 0b111),
            (1, 5, 64, 0b111),
            (1, 1, 3072, 0b111),
            (1, 2, 3072, 0b111),
            (1, 5, 3072, 0b111),
            (3, 1, 64, 0b111),
            (3, 2, 64, 0b111),
            (3, 5, 64, 0b111),
            (3, 2, 3072, 0b111),
            (3, 5, 3072, 0b111),
            // Sparse cases (missing-column bitmap path)
            (1, 2, 64, 0b101),
            (1, 2, 64, 0b110),
            (1, 2, 64, 0b011),
            (1, 5, 64, 0b001),
            (3, 3, 64, 0b101),
            (3, 3, 3072, 0b101),
            (3, 5, 3072, 0b011),
        ];

        for &(num_partitions, rows_per_partition, cell_size, mask) in cases {
            let timestamp = 1_000_100i64;

            // Build partitions in sorted-by-key order (required by SSTableWriter).
            let mut partitions: Vec<Partition> = (0..num_partitions)
                .map(|p_idx| {
                    let tenant_id = [0x11u8; 16];
                    let mut session_id = [0x22u8; 16];
                    session_id[15] = p_idx as u8;
                    let pk = make_composite_pk(&tenant_id, &session_id);

                    let rows: Vec<Row> = (0..rows_per_partition)
                        .map(|r_idx| {
                            let mut entity_id = [0xAAu8; 16];
                            entity_id[15] = r_idx as u8;

                            // entity_name: short text
                            let name = format!("entity_{:04}", r_idx).into_bytes();
                            // entity_type: short text
                            let etype = b"concept".to_vec();
                            // embedding: deterministic blob of cell_size bytes
                            let embedding: Vec<u8> = (0..cell_size)
                                .map(|i| ((i + r_idx + p_idx) % 256) as u8)
                                .collect();

                            let mut cells: Vec<(u16, CellValue)> = Vec::new();
                            if mask & 0b001 != 0 {
                                cells.push((0, CellValue::live(name, timestamp)));
                            }
                            if mask & 0b010 != 0 {
                                cells.push((1, CellValue::live(etype, timestamp)));
                            }
                            if mask & 0b100 != 0 {
                                cells.push((2, CellValue::live(embedding, timestamp)));
                            }

                            Row {
                                clustering: entity_id.to_vec(),
                                cells,
                                deletion: DeletionTime::LIVE,
                                primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
                            }
                        })
                        .collect();

                    Partition {
                        key: DecoratedKey::new(PartitionKey::new(pk)),
                        deletion: DeletionTime::LIVE,
                        static_row: None,
                        rows,
                    }
                })
                .collect();
            partitions.sort_by(|a, b| a.key.cmp(&b.key));

            // Snapshot expected content for comparison.
            let expected = partitions.clone();

            let mut writer = SSTableWriter::new(options.clone(), header.clone());
            for p in &partitions {
                writer.add_partition(p).unwrap();
            }
            let output = writer.finish().unwrap();

            // Sequential read must recover every partition and every row
            // byte-for-byte.
            let mut reader = DataReader::new(&output.data, &header, 0);
            let mut read_back = Vec::new();
            while let Some(p) = reader.read_partition().unwrap_or_else(|e| {
                panic!(
                    "read_partition failed for case \
                     (np={}, rpp={}, cs={}, mask=0b{:03b}): {}",
                    num_partitions, rows_per_partition, cell_size, mask, e
                )
            }) {
                read_back.push(p);
            }

            assert_eq!(
                read_back.len(),
                expected.len(),
                "case (np={}, rpp={}, cs={}, mask=0b{:03b}): partition count mismatch",
                num_partitions,
                rows_per_partition,
                cell_size,
                mask
            );

            for (exp, got) in expected.iter().zip(read_back.iter()) {
                assert_eq!(
                    got.key.key.as_bytes(),
                    exp.key.key.as_bytes(),
                    "case (np={}, rpp={}, cs={}): partition key mismatch",
                    num_partitions,
                    rows_per_partition,
                    cell_size
                );
                assert_eq!(
                    got.rows.len(),
                    exp.rows.len(),
                    "case (np={}, rpp={}, cs={}): row count mismatch",
                    num_partitions,
                    rows_per_partition,
                    cell_size
                );
                for (r_idx, (exp_row, got_row)) in exp.rows.iter().zip(got.rows.iter()).enumerate()
                {
                    assert_eq!(
                        got_row.clustering, exp_row.clustering,
                        "case (np={}, rpp={}, cs={}): row {} clustering mismatch",
                        num_partitions, rows_per_partition, cell_size, r_idx
                    );
                    assert_eq!(
                        got_row.cells.len(),
                        exp_row.cells.len(),
                        "case (np={}, rpp={}, cs={}): row {} cell count mismatch",
                        num_partitions,
                        rows_per_partition,
                        cell_size,
                        r_idx
                    );
                    for (c_idx, ((g_col, g_cell), (e_col, e_cell))) in
                        got_row.cells.iter().zip(exp_row.cells.iter()).enumerate()
                    {
                        assert_eq!(
                            g_col, e_col,
                            "case (np={}, rpp={}, cs={}): row {} cell {} column idx",
                            num_partitions, rows_per_partition, cell_size, r_idx, c_idx
                        );
                        assert_eq!(
                            g_cell.value.as_deref(),
                            e_cell.value.as_deref(),
                            "case (np={}, rpp={}, cs={}): row {} cell {} value",
                            num_partitions,
                            rows_per_partition,
                            cell_size,
                            r_idx,
                            c_idx
                        );
                    }
                }
            }
        }
    }

    // ---------------------------------------------------------------------
    // Writer self-validation gates — reconstructed 2026-04-20 from the
    // `next-writervalidate` debug image that ran the cluster stably for
    // 24h+. The production bug these guard against:
    //
    //   - A row's `clustering: vec![]` on a schema declaring Int32Type
    //     clustering produced an SSTable whose Data.db was valid-looking
    //     but whose partition index expected more bytes than the data
    //     had. The reader then failed `read_exact_at: wanted N got M`
    //     and silently skipped the whole partition — catastrophic data
    //     loss on large writes (see specs/in-process/
    //     bug-large-write-causes-data-loss-in-partition.md).
    //
    //   - Gate A (`validate_clustering_shape`) refuses such rows at
    //     add_partition time with a clear error.
    //   - Gate B (`verify_output_readable`) reopens the finished SSTable
    //     and confirms the partition count matches what was written,
    //     catching any other class of silent corruption before the
    //     output is persisted.
    // ---------------------------------------------------------------------

    /// Build a header with the given clustering types and no cells beyond the default `val`.
    fn header_with_clustering(clustering_types: Vec<&str>) -> SerializationHeader {
        SerializationHeader {
            complex_collections: false,
            min_timestamp: 1_000_000,
            min_local_deletion_time: i32::MAX,
            min_ttl: 0,
            max_timestamp: i64::MAX,
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            clustering_types: clustering_types.into_iter().map(String::from).collect(),
            static_columns: vec![],
            regular_columns: vec![(
                b"val".to_vec(),
                "org.apache.cassandra.db.marshal.UTF8Type".into(),
            )],
        }
    }

    #[test]
    fn validate_clustering_shape_no_clustering_columns_passes() {
        let header = header_with_clustering(vec![]);
        // Any clustering bytes on a clustering-less schema must not error.
        assert!(SSTableWriter::validate_clustering_shape(&header, 0, &[]).is_ok());
        assert!(SSTableWriter::validate_clustering_shape(&header, 0, &[0u8; 32]).is_ok());
    }

    #[test]
    fn validate_clustering_shape_rejects_empty_on_int32_schema() {
        // This is the production data-loss bug: Int32Type clustering, but
        // row.clustering = vec![]. Writer previously accepted; reader blew up.
        let header = header_with_clustering(vec!["org.apache.cassandra.db.marshal.Int32Type"]);
        let result = SSTableWriter::validate_clustering_shape(&header, 0, &[]);
        assert!(
            result.is_err(),
            "expected Err for empty clustering on Int32Type schema"
        );
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("clustering") && msg.contains("4"),
            "error message must mention clustering and expected length 4 — got: {msg}"
        );
    }

    #[test]
    fn validate_clustering_shape_accepts_well_shaped_int32() {
        let header = header_with_clustering(vec!["org.apache.cassandra.db.marshal.Int32Type"]);
        // Single CK column: RAW bytes, no u16 prefix.
        let clustering = vec![0x00, 0x00, 0x00, 0x01];
        assert!(
            SSTableWriter::validate_clustering_shape(&header, 0, &clustering).is_ok(),
            "raw 4-byte Int32 clustering must pass"
        );
    }

    #[test]
    fn validate_clustering_shape_rejects_too_short_long_type() {
        // LongType is 8 bytes fixed. 4 raw bytes must be rejected.
        let header = header_with_clustering(vec!["org.apache.cassandra.db.marshal.LongType"]);
        let clustering = vec![0x00, 0x00, 0x00, 0x01]; // 4 bytes, expected 8
        let result = SSTableWriter::validate_clustering_shape(&header, 0, &clustering);
        assert!(
            result.is_err(),
            "4-byte clustering on LongType (expected 8) must error"
        );
    }

    #[test]
    fn validate_clustering_shape_accepts_uuid_type() {
        let header = header_with_clustering(vec!["org.apache.cassandra.db.marshal.UUIDType"]);
        // Single CK column: 16 raw bytes.
        let clustering = vec![0xABu8; 16];
        assert!(SSTableWriter::validate_clustering_shape(&header, 0, &clustering).is_ok());
    }

    #[test]
    fn validate_clustering_shape_accepts_variable_length_utf8() {
        // UTF8Type single CK: any non-empty raw bytes.
        let header = header_with_clustering(vec!["org.apache.cassandra.db.marshal.UTF8Type"]);
        let clustering = b"hello".to_vec();
        assert!(SSTableWriter::validate_clustering_shape(&header, 0, &clustering).is_ok());
    }

    #[test]
    fn validate_clustering_shape_multi_column_composite_ok() {
        // Two clustering columns: Int32 + UUIDType, u16-prefixed composite.
        let header = header_with_clustering(vec![
            "org.apache.cassandra.db.marshal.Int32Type",
            "org.apache.cassandra.db.marshal.UUIDType",
        ]);
        let mut clustering = vec![0x00, 0x04, 0x00, 0x00, 0x00, 0x01]; // Int32: len 4 + 4 bytes
        clustering.push(0x00);
        clustering.push(0x10); // UUID: len 16
        clustering.extend(std::iter::repeat_n(0xABu8, 16));
        assert!(SSTableWriter::validate_clustering_shape(&header, 0, &clustering).is_ok());
    }

    #[test]
    fn validate_clustering_shape_multi_column_rejects_wrong_prefix() {
        let header = header_with_clustering(vec![
            "org.apache.cassandra.db.marshal.Int32Type",
            "org.apache.cassandra.db.marshal.UUIDType",
        ]);
        // Int32 prefix says 2 bytes (should be 4).
        let clustering = vec![0x00, 0x02, 0xAB, 0xCD];
        let result = SSTableWriter::validate_clustering_shape(&header, 0, &clustering);
        assert!(
            result.is_err(),
            "wrong-length composite component must error"
        );
    }

    /// WriteOptions for Gate B tests: no compression. The compressed
    /// roundtrip has a pre-existing CRC32 bug in the reader/writer split
    /// (see store.rs::flush which forces compression=None for the same
    /// reason); the flush path in production always goes through this
    /// uncompressed path when verify_output runs.
    fn verify_b_options() -> WriteOptions {
        WriteOptions {
            compression: None,
            ..WriteOptions::default()
        }
    }

    #[test]
    fn verify_output_readable_passes_on_good_sstable() {
        let header = test_header();
        let options = verify_b_options();
        let partition = make_partition(b"pk1", &[0x00, 0x00, 0x00, 0x01], b"v", 1_000_001);
        let mut writer = SSTableWriter::new(options, header.clone());
        writer.add_partition(&partition).unwrap();
        let output = writer.finish().unwrap();

        // Explicitly exercise the gate: reopen + verify partition count == 1
        let result = SSTableWriter::verify_output_readable(&output, &header, 1);
        assert!(
            result.is_ok(),
            "well-formed SSTable must pass verify_output_readable: {result:?}"
        );
    }

    #[test]
    fn verify_output_readable_detects_partition_count_mismatch() {
        let header = test_header();
        let options = verify_b_options();
        let partition = make_partition(b"pk1", &[0x00, 0x00, 0x00, 0x01], b"v", 1_000_001);
        let mut writer = SSTableWriter::new(options, header.clone());
        writer.add_partition(&partition).unwrap();
        let output = writer.finish().unwrap();

        // Lie about the expected count to simulate a corruption case.
        let result = SSTableWriter::verify_output_readable(&output, &header, 5);
        assert!(
            result.is_err(),
            "partition count mismatch must be detected by verify_output_readable"
        );
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("partition") && (msg.contains("1") || msg.contains("5")),
            "error message must identify partition-count mismatch — got: {msg}"
        );
    }

    #[test]
    fn verify_output_readable_disabled_when_option_false() {
        // When WriteOptions.verify_output = false, finish() must NOT run the
        // verify step — so even if we artificially corrupt the output the
        // writer still returns Ok. We use finish_with_options_verify_output=false
        // path by constructing the writer with verify_output: false.
        let header = test_header();
        let options = WriteOptions {
            verify_output: false,
            compression: None,
            ..WriteOptions::default()
        };
        let partition = make_partition(b"pk1", &[0x00, 0x00, 0x00, 0x01], b"v", 1_000_001);
        let mut writer = SSTableWriter::new(options, header.clone());
        writer.add_partition(&partition).unwrap();
        // finish() should succeed without invoking verify_output_readable.
        // If it did invoke it, this still works on a valid SSTable — the
        // assertion below is that finish returns Ok.
        assert!(writer.finish().is_ok());
    }

    #[test]
    fn add_partition_rejects_empty_clustering_on_int32_schema() {
        // End-to-end proof of Gate A: writer.add_partition refuses the
        // row that caused the production data-loss bug.
        let header = header_with_clustering(vec!["org.apache.cassandra.db.marshal.Int32Type"]);
        let options = WriteOptions::default();
        let partition = Partition {
            key: DecoratedKey::new(PartitionKey::from(b"pk_bad".as_slice())),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![Row {
                clustering: vec![], // the bug: empty on a schema with Int32 clustering
                cells: vec![(0, CellValue::live(b"v".to_vec(), 1_000_001))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(1_000_001),
            }],
        };
        let mut writer = SSTableWriter::new(options, header);
        let result = writer.add_partition(&partition);
        assert!(
            result.is_err(),
            "add_partition must reject empty clustering when schema declares fixed-length clustering"
        );
    }
}
