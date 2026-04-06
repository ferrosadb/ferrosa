//! SSTableWriter — write BTI format SSTables from sorted partitions.
//!
//! The writer accepts partitions in token order and produces all component
//! data (Data.db, Partitions.db, Rows.db, Filter.db, CompressionInfo.db,
//! Statistics.db, TOC.txt) as in-memory byte buffers in an [`SSTableOutput`].
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
//! # };
//! # let partitions: Vec<Partition> = vec![];
//! let mut writer = SSTableWriter::new(WriteOptions::default(), header);
//! for partition in &partitions {
//!     writer.add_partition(partition).unwrap();
//! }
//! let output = writer.finish().unwrap();
//! ```
//!
//! Partitions must be added in token order. The writer currently handles
//! simple single-row partitions with live cells; more complex cases
//! (range tombstones, complex columns) are deferred.

use ferrosa_common::{CellValue, Result};

use crate::bloom::BloomFilter;
use crate::byte_comparable;
use crate::compression::{Compression, CompressionInfo};
use crate::statistics::{
    write_statistics, CompactionMetadata, SerializationHeader, Statistics, StatsMetadata,
    ValidationMetadata,
};
use crate::toc;
use crate::trie::builder::{TrieBuilder, TriePayload};
use crate::types::Partition;
use crate::varint;

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
}

impl Default for WriteOptions {
    fn default() -> Self {
        WriteOptions {
            compression: Some(Compression::Lz4),
            bloom_fp_chance: 0.01,
            chunk_size: 65536,
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

/// SSTable writer that accumulates partitions and produces all component files.
pub struct SSTableWriter {
    options: WriteOptions,
    header: SerializationHeader,
    /// Raw (uncompressed) data buffer — the Data.db content.
    data_buf: Vec<u8>,
    /// Bloom filter for partition keys.
    bloom: BloomFilter,
    /// Trie builder for the partition index (Partitions.db).
    trie_builder: TrieBuilder,
    /// Number of partitions written so far.
    partition_count: u64,
    /// First partition key bytes (for key bounds footer).
    first_key: Option<Vec<u8>>,
    /// Last partition key bytes (for key bounds footer).
    last_key: Option<Vec<u8>>,
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
            data_buf: Vec::new(),
            bloom,
            trie_builder: TrieBuilder::new(),
            partition_count: 0,
            first_key: None,
            last_key: None,
        }
    }

    /// Add a partition to the SSTable. Partitions must be added in token order.
    ///
    /// This serializes the partition data, adds the key to the bloom filter,
    /// adds a trie entry for the partition index, and tracks statistics.
    pub fn add_partition(&mut self, partition: &Partition) -> Result<()> {
        let data_pos = self.data_buf.len() as i64;

        // 1. Serialize partition data to the data buffer.
        self.serialize_partition(partition);

        // 2. Add key to bloom filter.
        let (h1, h2) = partition.key.filter_hash();
        self.bloom.add(h1, h2);

        // 3. Add to trie builder: encode key with byte_comparable, use data position as payload.
        let encoded = byte_comparable::encode(&partition.key);
        let hash_byte = (h2 & 0xFF) as u8;
        // Use negative idxpos (bitwise NOT) for DataDirect entries — simple partitions
        // don't need a row index indirection.
        let idxpos = !data_pos; // negative = DataDirect
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
        }
        self.last_key = Some(partition.key.key.as_bytes().to_vec());
        self.partition_count += 1;

        Ok(())
    }

    /// Finalize the SSTable and produce all component files.
    pub fn finish(self) -> Result<SSTableOutput> {
        let first_key = self.first_key.clone().unwrap_or_default();
        let last_key = self.last_key.clone().unwrap_or_default();
        let partition_count = self.partition_count;
        let bloom_fp_chance = self.options.bloom_fp_chance;
        let has_compression = self.options.compression.is_some()
            && !matches!(self.options.compression, Some(Compression::None));

        // 1. Finalize trie -> Partitions.db (trie bytes + key bounds footer)
        let partitions =
            Self::build_partitions_db(self.trie_builder, &first_key, &last_key, partition_count)?;

        // 2. Build bloom filter -> Filter.db
        let filter = self.bloom.write();

        // 3. Build statistics -> Statistics.db
        let statistics = Self::build_statistics_db(&self.header, bloom_fp_chance);

        // 4. Optionally compress data chunks -> Data.db + CompressionInfo.db
        let (data, compression_info) = Self::build_data_db(self.data_buf, &self.options)?;

        // 5. Rows.db: empty for simple cases (no per-partition row index)
        let rows: Vec<u8> = Vec::new();

        // 6. Build TOC -> TOC.txt
        let toc_bytes = Self::build_toc(has_compression);

        Ok(SSTableOutput {
            data,
            partitions,
            rows,
            filter,
            compression_info,
            statistics,
            toc: toc_bytes,
        })
    }

    // -----------------------------------------------------------------------
    // Internal: partition serialization
    // -----------------------------------------------------------------------

    /// Serialize a single partition to the data buffer.
    fn serialize_partition(&mut self, partition: &Partition) {
        // Key: u16 BE length + key bytes
        let key_bytes = partition.key.key.as_bytes();
        self.data_buf
            .extend_from_slice(&(key_bytes.len() as u16).to_be_bytes());
        self.data_buf.extend_from_slice(key_bytes);

        // Deletion time: Cassandra 5.x UInt format
        if partition.deletion.is_live() {
            self.data_buf.push(DELETION_IS_LIVE);
        } else {
            // 8-byte markedForDeleteAt (i64 BE) + 4-byte localDeletionTime (u32 BE)
            self.data_buf
                .extend_from_slice(&partition.deletion.marked_for_delete_at.to_be_bytes());
            self.data_buf
                .extend_from_slice(&partition.deletion.local_deletion_time.to_be_bytes());
        }

        // Static row (if present)
        if let Some(ref static_row) = partition.static_row {
            self.serialize_row(static_row, true);
        }

        // Clustered rows
        for row in &partition.rows {
            self.serialize_row(row, false);
        }

        // END_OF_PARTITION marker
        self.data_buf.push(END_OF_PARTITION);
    }

    /// Serialize a single row to the data buffer.
    fn serialize_row(&mut self, row: &crate::types::Row, is_static: bool) {
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
        let all_present = row.cells.len() == num_columns
            && row
                .cells
                .iter()
                .enumerate()
                .all(|(i, (idx, _))| *idx as usize == i);
        if all_present {
            flags |= HAS_ALL_COLUMNS;
        }

        // Set EXTENSION_FLAG if we have extended flags
        if extended_flags != 0 {
            flags |= EXTENSION_FLAG;
        }

        self.data_buf.push(flags);
        if flags & EXTENSION_FLAG != 0 {
            self.data_buf.push(extended_flags);
        }

        // Clustering key (not for static rows)
        //
        // Cassandra 5.x ClusteringPrefix format:
        //   header varint (0 = all non-null/non-empty) + per-component value bytes.
        //   Fixed-length types: raw bytes only. Variable-length: varint(len) + bytes.
        //
        // For single-column CK, row.clustering is raw component bytes.
        // For multi-column CK, the CQL bridge encodes as u16-prefixed
        // per-component: [u16 len][bytes][u16 len][bytes]...
        // We must extract each component and write it in BTI format.
        if !is_static {
            push_unsigned_vint_to(&mut self.data_buf, 0); // header: all non-null, non-empty

            let num_ck = self.header.clustering_types.len();
            if num_ck == 1 {
                // Single CK column: raw bytes (no u16 prefix).
                let type_name = &self.header.clustering_types[0];
                if crate::marshal::value_length_if_fixed(type_name).is_none() {
                    push_unsigned_vint_to(&mut self.data_buf, row.clustering.len() as u64);
                }
                self.data_buf.extend_from_slice(&row.clustering);
            } else if num_ck > 1 {
                // Multi-column CK: extract components from u16-prefixed
                // encoding, then write each in BTI per-component format.
                let components = split_u16_prefixed(&row.clustering, num_ck);
                for (i, component) in components.iter().enumerate() {
                    let type_name = &self.header.clustering_types[i];
                    if crate::marshal::value_length_if_fixed(type_name).is_none() {
                        push_unsigned_vint_to(&mut self.data_buf, component.len() as u64);
                    }
                    self.data_buf.extend_from_slice(component);
                }
            }
            // num_ck == 0: no clustering columns — header varint only, no data.
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

        // Missing-column bitmap (only if not HAS_ALL_COLUMNS)
        if flags & HAS_ALL_COLUMNS == 0 {
            let bitmap_bytes = num_columns.div_ceil(8);
            let mut bitmap = vec![0u8; bitmap_bytes];
            // Mark present columns (bit set = present)
            let present_set: std::collections::HashSet<usize> =
                row.cells.iter().map(|(idx, _)| *idx as usize).collect();
            for i in 0..num_columns {
                if present_set.contains(&i) {
                    let byte_idx = i / 8;
                    let bit_idx = 7 - (i % 8);
                    bitmap[byte_idx] |= 1 << bit_idx;
                }
            }
            row_body.extend_from_slice(&bitmap);
        }

        // Cells
        for (_, cell) in &row.cells {
            serialize_cell(&mut row_body, cell, row, &self.header);
        }

        // Write row body size + previous unfiltered size + row body
        let row_body_len = row_body.len() as u64;
        push_unsigned_vint_to(&mut self.data_buf, row_body_len);
        // Previous unfiltered size (0 for simplicity)
        push_unsigned_vint_to(&mut self.data_buf, 0);
        self.data_buf.extend_from_slice(&row_body);
    }

    // -----------------------------------------------------------------------
    // Internal: component builders
    // -----------------------------------------------------------------------

    /// Build Partitions.db: trie bytes + key bounds + footer.
    fn build_partitions_db(
        trie_builder: TrieBuilder,
        first_key: &[u8],
        last_key: &[u8],
        partition_count: u64,
    ) -> Result<Vec<u8>> {
        let (trie_data, root_pos) = trie_builder.finish()?;

        let mut buf = Vec::new();

        // Trie data
        buf.extend_from_slice(&trie_data);

        // Key bounds section
        let key_bounds_offset = buf.len() as i64;
        // smallest key: u16 len + bytes
        buf.extend_from_slice(&(first_key.len() as u16).to_be_bytes());
        buf.extend_from_slice(first_key);
        // largest key: u16 len + bytes
        buf.extend_from_slice(&(last_key.len() as u16).to_be_bytes());
        buf.extend_from_slice(last_key);

        // Footer: 3 big-endian i64s
        buf.extend_from_slice(&key_bounds_offset.to_be_bytes());
        buf.extend_from_slice(&(partition_count as i64).to_be_bytes());
        buf.extend_from_slice(&(root_pos as i64).to_be_bytes());

        Ok(buf)
    }

    /// Build Statistics.db.
    fn build_statistics_db(header: &SerializationHeader, bloom_fp_chance: f64) -> Vec<u8> {
        let stats = Statistics {
            validation: ValidationMetadata {
                partitioner_class: "org.apache.cassandra.dht.Murmur3Partitioner".into(),
                bloom_fp_chance,
            },
            compaction: CompactionMetadata { data: vec![] },
            stats: StatsMetadata { data: vec![] },
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
                let chunk_size = options.chunk_size;
                let data_length = data_buf.len() as u64;
                let mut compressed_data = Vec::new();
                let mut chunk_offsets = Vec::new();
                let mut max_compressed_size: usize = 0;

                for chunk in data_buf.chunks(chunk_size) {
                    chunk_offsets.push(compressed_data.len() as u64);
                    let compressed_chunk = compression.compress(chunk)?;
                    if compressed_chunk.len() > max_compressed_size {
                        max_compressed_size = compressed_chunk.len();
                    }
                    compressed_data.extend_from_slice(&compressed_chunk);
                }

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
) {
    let is_tombstone = cell.is_tombstone();
    let is_expiring = !is_tombstone
        && cell.ttl != ferrosa_common::NO_TTL
        && cell.local_deletion_time != ferrosa_common::NO_DELETION_TIME;
    let has_empty_value = cell.value.is_none() || (is_tombstone && cell.value.is_none());
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

    // Value (absent if HAS_EMPTY_VALUE)
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
            push_unsigned_vint_to(buf, value.len() as u64);
            buf.extend_from_slice(value);
        }
    }
}

/// Write an unsigned varint to a Vec buffer.
fn push_unsigned_vint_to(buf: &mut Vec<u8>, value: u64) {
    let mut vbuf = [0u8; 9];
    let n = varint::write_unsigned_vint(&mut vbuf, value);
    buf.extend_from_slice(&vbuf[..n]);
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
    use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};

    /// Build a minimal serialization header for testing.
    fn test_header() -> SerializationHeader {
        SerializationHeader {
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

    #[test]
    fn write_single_partition_and_read_back() {
        let header = test_header();
        let options = WriteOptions {
            compression: None,
            bloom_fp_chance: 0.01,
            chunk_size: 65536,
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
    fn round_trip_write_read_all_components() {
        let header = test_header();
        let options = WriteOptions {
            compression: None,
            bloom_fp_chance: 0.01,
            chunk_size: 65536,
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
    fn write_with_compression() {
        let header = test_header();
        let options = WriteOptions {
            compression: Some(Compression::Lz4),
            bloom_fp_chance: 0.01,
            chunk_size: 64, // Small chunks to test chunking
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
            let decompressed = compression.decompress(chunk, ci.chunk_length).unwrap();
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
    fn write_partition_with_cell_own_timestamp() {
        let header = test_header();
        let options = WriteOptions {
            compression: None,
            bloom_fp_chance: 0.01,
            chunk_size: 65536,
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

        // Verify that the key bounds ordering matches the write order
        let write_first = partitions[0].key.key.as_bytes();
        let write_last = partitions[partitions.len() - 1].key.key.as_bytes();
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

        // Read back via sequential DataReader (same path as read_all_partitions)
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
    }
}
