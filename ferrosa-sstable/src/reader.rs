//! SSTableReader -- compose all components into a single read interface.
//!
//! Opens a BTI SSTable from component file handles and provides:
//! - Partition lookup by DecoratedKey
//! - Full partition iteration in token order

use ferrosa_common::{DecoratedKey, Result};

use crate::bloom::BloomFilter;
use crate::compression::CompressionInfo;
use crate::data::DataReader;
use crate::io::ReadAt;
use crate::partition_index::{PartitionIndex, PartitionLookup};
use crate::statistics::{read_statistics, SerializationHeader};
use crate::types::Partition;

/// Decompress all chunks of Data.db into a contiguous buffer.
///
/// Each compressed chunk in Data.db has a trailing 4-byte CRC32 checksum
/// appended by Cassandra's `CompressedSequentialWriter`. The CRC covers
/// the compressed data (including the size prefix) and must be stripped
/// before passing to the decompressor.
fn decompress_data<R: ReadAt>(data: &R, ci: &CompressionInfo) -> Result<Vec<u8>> {
    let file_len = data.len()?;
    let mut decompressed = Vec::with_capacity(ci.data_length as usize);

    for (i, &chunk_offset) in ci.chunk_offsets.iter().enumerate() {
        // Determine compressed chunk size: from this offset to the next (or end of file)
        let next_offset = if i + 1 < ci.chunk_offsets.len() {
            ci.chunk_offsets[i + 1]
        } else {
            file_len
        };
        let chunk_size = (next_offset - chunk_offset) as usize;

        let mut compressed = vec![0u8; chunk_size];
        data.read_exact_at(&mut compressed, chunk_offset)?;

        // Strip trailing 4-byte CRC32 checksum
        let payload = &compressed[..chunk_size.saturating_sub(4)];
        let chunk = ci.compression.decompress(payload, ci.chunk_length)?;
        decompressed.extend_from_slice(&chunk);
    }

    Ok(decompressed)
}

/// Handles to all component files for an SSTable.
pub struct SSTableComponents<R> {
    /// Data.db file handle.
    pub data: R,
    /// Partitions.db file handle.
    pub partitions: R,
    /// Rows.db file handle.
    pub rows: R,
    /// Bloom filter bytes (Filter.db, read fully into memory).
    pub filter: Vec<u8>,
    /// CompressionInfo.db bytes (`None` if uncompressed).
    pub compression_info: Option<Vec<u8>>,
    /// Statistics.db bytes.
    pub statistics: Vec<u8>,
}

/// Composes all SSTable component readers into a single read interface.
pub struct SSTableReader<R: ReadAt> {
    partition_index: PartitionIndex<R>,
    bloom_filter: BloomFilter,
    compression_info: Option<CompressionInfo>,
    header: SerializationHeader,
    data: R,
    /// Previously cached decompressed Data.db contents. Removed to prevent
    /// unbounded memory growth — decompression now happens on demand in
    /// read_all_partitions() and get_partition().
    _decompressed_data: Option<Vec<u8>>,
    #[allow(dead_code)]
    rows: R,
}

impl<R: ReadAt> SSTableReader<R> {
    /// Open an SSTable from its component file handles.
    ///
    /// Parses the bloom filter, compression info, and statistics from their
    /// in-memory byte buffers, and opens the partition index from its reader.
    pub fn open(components: SSTableComponents<R>) -> Result<Self> {
        let bloom_filter = BloomFilter::read(&components.filter)?;

        let compression_info = match components.compression_info {
            Some(ref ci_bytes) => Some(CompressionInfo::read(ci_bytes)?),
            None => None,
        };

        let stats = read_statistics(&components.statistics)?;
        let header = stats.header;

        let partition_index = PartitionIndex::open(components.partitions)?;

        // Decompression moved to read_all_partitions() — eager decompression
        // caused unbounded memory growth (the entire Data.db decompressed as a
        // Vec<u8> was held for the lifetime of the SSTableReader, causing ~1.4GB
        // growth on a 28k-row scan). Now decompressed on demand and freed after.
        let decompressed_data = None;

        Ok(SSTableReader {
            partition_index,
            bloom_filter,
            compression_info,
            header,
            data: components.data,
            _decompressed_data: decompressed_data,
            rows: components.rows,
        })
    }

    /// Look up a partition by its decorated key.
    ///
    /// 1. Checks the bloom filter; returns `None` immediately if the key is
    ///    definitely absent.
    /// 2. Looks up the key in the partition index trie.
    /// 3. Reads the partition from Data.db at the resolved position.
    pub fn get_partition(&self, key: &DecoratedKey) -> Result<Option<Partition>> {
        // Step 1: bloom filter check
        let (h1, h2) = key.filter_hash();
        if !self.bloom_filter.is_present(h1, h2) {
            return Ok(None);
        }

        // Step 2: partition index lookup
        let lookup = self.partition_index.lookup(key)?;

        let data_position = match lookup {
            PartitionLookup::RowIndex { position } => {
                // For now, treat RowIndex the same as DataDirect: the position
                // refers to an offset where we can begin reading the partition.
                // A full implementation would consult Rows.db to find the
                // exact data offset, but for simple cases the row index entry
                // contains the data position in its footer.
                position
            }
            PartitionLookup::DataDirect { position } => position,
            PartitionLookup::NotFound => return Ok(None),
        };

        // Step 3: read partition from Data.db (decompressed if applicable)
        if let Some(ref ci) = self.compression_info {
            let decompressed = decompress_data(&self.data, ci)?;
            let mut data_reader = DataReader::new(&decompressed, &self.header, data_position);
            data_reader.read_partition()
        } else {
            let mut data_reader = DataReader::new(&self.data, &self.header, data_position);
            data_reader.read_partition()
        }
    }

    /// Returns the number of partitions in this SSTable.
    pub fn key_count(&self) -> u64 {
        self.partition_index.key_count()
    }

    /// Returns a reference to the bloom filter.
    pub fn bloom_filter(&self) -> &BloomFilter {
        &self.bloom_filter
    }

    /// Returns a reference to the serialization header.
    pub fn header(&self) -> &SerializationHeader {
        &self.header
    }

    /// Returns a reference to the compression info, if present.
    pub fn compression_info(&self) -> Option<&CompressionInfo> {
        self.compression_info.as_ref()
    }

    /// Returns the length of the Data.db file (or buffer) in bytes.
    pub fn data_file_length(&self) -> Result<u64> {
        self.data.len()
    }

    /// Returns the approximate total size of this SSTable in bytes.
    ///
    /// Sums the sizes of the data file and the partition index. For
    /// in-memory readers (tests), this is the byte count of the `Vec<u8>`
    /// buffers. For file-backed readers, it reflects the actual file sizes.
    pub fn total_size(&self) -> u64 {
        let data_len = self.data.len().unwrap_or(0);
        let partitions_len = self.partition_index.file_size();
        data_len + partitions_len
    }

    /// Returns the smallest key stored in the partition index as raw
    /// byte-comparable encoded bytes. Decode with `byte_comparable::decode`.
    pub fn smallest_key_bytes(&self) -> &[u8] {
        self.partition_index.smallest_key()
    }

    /// Returns the largest key stored in the partition index as raw
    /// byte-comparable encoded bytes. Decode with `byte_comparable::decode`.
    pub fn largest_key_bytes(&self) -> &[u8] {
        self.partition_index.largest_key()
    }

    /// Read all partitions from this SSTable in storage order.
    ///
    /// Scans the Data.db file sequentially from position 0, reading each
    /// partition until EOF. **Materializes the entire SSTable into memory.**
    ///
    /// Prefer [`Self::partitions_iter`] for compaction and other large-scan
    /// callers — full materialization here was OOM-ing the compaction
    /// executor on tombstone-heavy workloads (`cql_timeseries2`, IoT TTL
    /// patterns).  See `specs/in-process/streaming-compaction.md`.
    pub fn read_all_partitions(&self) -> Result<Vec<crate::types::Partition>> {
        self.read_partitions_limited(usize::MAX)
    }

    /// Stream partitions from this SSTable in storage (token) order, one at
    /// a time, without materializing the whole file into memory.
    ///
    /// The returned iterator borrows this `SSTableReader` for its lifetime
    /// and decompresses the Data.db once up-front (for compressed
    /// SSTables) or reads directly from the underlying [`ReadAt`] (for
    /// uncompressed). Each call to [`PartitionIter::next_partition`] yields
    /// at most one partition; the iterator returns `Ok(None)` at EOF.
    ///
    /// Memory: `O(decompressed_data_size)` for compressed SSTables (single
    /// pre-decompressed buffer held for the iterator's lifetime), `O(1)`
    /// for uncompressed.  Independent of partition count.
    pub fn partitions_iter(&self) -> Result<PartitionIter<'_, R>> {
        PartitionIter::new(self)
    }

    /// Scan partitions sequentially, stopping once `limit` partitions have
    /// been decoded. This bounds range-read materialization while preserving
    /// the existing all-partitions API for compaction callers.
    pub fn read_partitions_limited(&self, limit: usize) -> Result<Vec<crate::types::Partition>> {
        self.read_partitions_limited_rows(limit, 0)
    }

    /// Scan partitions sequentially, retaining at most `row_limit` rows per
    /// decoded partition when `row_limit > 0`.
    pub fn read_partitions_limited_rows(
        &self,
        limit: usize,
        row_limit: usize,
    ) -> Result<Vec<crate::types::Partition>> {
        let mut partitions = Vec::new();
        if limit == 0 {
            return Ok(partitions);
        }
        if let Some(ref ci) = self.compression_info {
            // Decompress on demand — the buffer is freed when this scope ends.
            // Previously the decompressed buffer was cached in the SSTableReader,
            // causing ~1.4GB growth on a 28k-row scan because every loaded
            // SSTable held its entire decompressed Data.db in memory.
            let decompressed = decompress_data(&self.data, ci)?;
            let mut reader = crate::data::DataReader::new(&decompressed, &self.header, 0);
            while partitions.len() < limit {
                let Some(partition) = reader.read_partition_limited_rows(row_limit)? else {
                    break;
                };
                partitions.push(partition);
            }
        } else {
            let mut reader = crate::data::DataReader::new(&self.data, &self.header, 0);
            while partitions.len() < limit {
                let Some(partition) = reader.read_partition_limited_rows(row_limit)? else {
                    break;
                };
                partitions.push(partition);
            }
        }
        Ok(partitions)
    }
}

/// Streaming partition iterator over an SSTable.
///
/// Returned by [`SSTableReader::partitions_iter`]. Yields partitions in
/// storage (token) order, one at a time. Decompression happens once at
/// construction time for compressed SSTables; the decompressed buffer is
/// held for the iterator's lifetime and freed on drop.
///
/// Memory cost is constant in the number of partitions — only the
/// currently-yielded `Partition` is materialized.
pub struct PartitionIter<'a, R: ReadAt> {
    sst: &'a SSTableReader<R>,
    pos: u64,
    /// Decompressed Data.db for compressed SSTables. `None` for
    /// uncompressed, where the iterator reads directly from `sst.data`.
    decompressed: Option<Vec<u8>>,
}

impl<'a, R: ReadAt> PartitionIter<'a, R> {
    fn new(sst: &'a SSTableReader<R>) -> Result<Self> {
        let decompressed = match &sst.compression_info {
            Some(ci) => Some(decompress_data(&sst.data, ci)?),
            None => None,
        };
        Ok(Self {
            sst,
            pos: 0,
            decompressed,
        })
    }

    /// Yield the next partition in storage order. Returns `Ok(None)` when
    /// the iterator has reached EOF.
    pub fn next_partition(&mut self) -> Result<Option<crate::types::Partition>> {
        let header = &self.sst.header;
        if let Some(ref buf) = self.decompressed {
            let slice: &[u8] = buf.as_slice();
            let mut reader = crate::data::DataReader::new(&slice, header, self.pos);
            let result = reader.read_partition()?;
            self.pos = reader.position();
            Ok(result)
        } else {
            let mut reader = crate::data::DataReader::new(&self.sst.data, header, self.pos);
            let result = reader.read_partition()?;
            self.pos = reader.position();
            Ok(result)
        }
    }

    /// Yield `(partition_key, row_count)` for the next partition without
    /// decoding any cell payloads. Cells are byte-skipped via
    /// `DataReader::read_partition_count`. Used by the COUNT(*) fast
    /// path so a full-table count never pays the per-cell decode cost.
    /// Returns `Ok(None)` at EOF.
    pub fn next_partition_count(
        &mut self,
    ) -> Result<Option<(ferrosa_common::key::DecoratedKey, u64)>> {
        let header = &self.sst.header;
        if let Some(ref buf) = self.decompressed {
            let slice: &[u8] = buf.as_slice();
            let mut reader = crate::data::DataReader::new(&slice, header, self.pos);
            let result = reader.read_partition_count()?;
            self.pos = reader.position();
            Ok(result)
        } else {
            let mut reader = crate::data::DataReader::new(&self.sst.data, header, self.pos);
            let result = reader.read_partition_count()?;
            self.pos = reader.position();
            Ok(result)
        }
    }

    /// Yield the next partition decoding only the cells whose
    /// ordinals are in `wanted`. Cells outside the projection are
    /// byte-skipped via `DataReader::read_cell_skip` — saves one
    /// syscall, one heap alloc, and the value-byte memcpy per
    /// skipped cell. Used by the CQL projection fast path so a
    /// `SELECT a, b FROM t` on a wide table (especially with
    /// embedding columns) doesn't pay the read+decode cost for
    /// columns the caller doesn't want.
    ///
    /// An empty `wanted` slice yields rows with empty `cells` —
    /// useful when only clustering keys / metadata are needed
    /// (similar to `next_partition_metadata` but going through
    /// the per-cell skip path).
    pub fn next_partition_projected(
        &mut self,
        wanted: &[u16],
    ) -> Result<Option<crate::types::Partition>> {
        let header = &self.sst.header;
        if let Some(ref buf) = self.decompressed {
            let slice: &[u8] = buf.as_slice();
            let mut reader = crate::data::DataReader::new(&slice, header, self.pos);
            let result = reader.read_partition_projected(wanted)?;
            self.pos = reader.position();
            Ok(result)
        } else {
            let mut reader = crate::data::DataReader::new(&self.sst.data, header, self.pos);
            let result = reader.read_partition_projected(wanted)?;
            self.pos = reader.position();
            Ok(result)
        }
    }

    /// Yield the next partition with full row metadata (clustering
    /// keys, row-level deletion, liveness) but **no cell payloads**
    /// — `Partition.rows[*].cells` is always empty. Used by the
    /// COUNT(*) fast path where the storage layer needs row-level
    /// dedup via `merge::merge_partitions` but doesn't need cell
    /// data. Returns `Ok(None)` at EOF.
    pub fn next_partition_metadata(
        &mut self,
    ) -> Result<Option<crate::types::Partition>> {
        let header = &self.sst.header;
        if let Some(ref buf) = self.decompressed {
            let slice: &[u8] = buf.as_slice();
            let mut reader = crate::data::DataReader::new(&slice, header, self.pos);
            let result = reader.read_partition_metadata()?;
            self.pos = reader.position();
            Ok(result)
        } else {
            let mut reader = crate::data::DataReader::new(&self.sst.data, header, self.pos);
            let result = reader.read_partition_metadata()?;
            self.pos = reader.position();
            Ok(result)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bloom::BloomFilter;
    use crate::statistics::{
        write_statistics, CompactionMetadata, SerializationHeader, Statistics, StatsMetadata,
        ValidationMetadata,
    };
    use crate::trie::builder::{TrieBuilder, TriePayload};
    use crate::{byte_comparable, varint};
    use ferrosa_common::PartitionKey;

    // -----------------------------------------------------------------------
    // Test helpers
    // -----------------------------------------------------------------------

    /// Write an unsigned varint to a buffer.
    fn push_unsigned_vint(out: &mut Vec<u8>, value: u64) {
        let mut buf = [0u8; 9];
        let n = varint::write_unsigned_vint(&mut buf, value);
        out.extend_from_slice(&buf[..n]);
    }

    /// Build a test SerializationHeader with one regular column.
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

    /// Build a full Statistics.db blob from the given header.
    fn build_statistics(header: SerializationHeader) -> Vec<u8> {
        let stats = Statistics {
            validation: ValidationMetadata {
                partitioner_class: "org.apache.cassandra.dht.Murmur3Partitioner".into(),
                bloom_fp_chance: 0.01,
            },
            compaction: CompactionMetadata { data: vec![0x00] },
            stats: StatsMetadata { data: vec![0x00] },
            header,
        };
        write_statistics(&stats)
    }

    /// Row flags constants (mirrored from data.rs for test use).
    const HAS_TIMESTAMP: u8 = 0x04;
    const HAS_ALL_COLUMNS: u8 = 0x20;
    const CELL_USE_ROW_TIMESTAMP: u8 = 0x08;
    const END_OF_PARTITION: u8 = 0x01;
    const DELETION_IS_LIVE: u8 = 0x80;

    /// Build a Data.db blob for a single partition.
    ///
    /// Key is the raw partition key bytes. Produces one row with clustering
    /// key `[0,0,0,1]`, timestamp delta 42, and cell value `b"hello-0"`.
    fn build_data_blob(key: &[u8]) -> Vec<u8> {
        build_data_blob_with_rows(key, 1)
    }

    fn build_data_blob_with_rows(key: &[u8], rows: usize) -> Vec<u8> {
        let mut data = Vec::new();

        // Partition header: u16 BE key len + key bytes
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key);

        // Live deletion time (Cassandra 5.x: single byte 0x80)
        data.push(DELETION_IS_LIVE);

        for row_idx in 0..rows {
            // Row flags: HAS_TIMESTAMP | HAS_ALL_COLUMNS
            data.push(HAS_TIMESTAMP | HAS_ALL_COLUMNS);

            // Clustering key (ClusteringPrefix format, Int32Type = fixed-length)
            let clustering = (row_idx as i32 + 1).to_be_bytes();
            push_unsigned_vint(&mut data, 0); // clustering header: all non-null, non-empty
            data.extend_from_slice(&clustering);

            let value = format!("hello-{row_idx}");
            let mut row_body = Vec::new();
            push_unsigned_vint(&mut row_body, 42 + row_idx as u64);
            row_body.push(CELL_USE_ROW_TIMESTAMP);
            push_unsigned_vint(&mut row_body, value.len() as u64);
            row_body.extend_from_slice(value.as_bytes());

            // Row body size + prev unfiltered size + body
            push_unsigned_vint(&mut data, row_body.len() as u64);
            push_unsigned_vint(&mut data, 0);
            data.extend_from_slice(&row_body);
        }

        // End of partition
        data.push(END_OF_PARTITION);

        data
    }

    /// Build a Partitions.db file from entries.
    ///
    /// Each entry is `(DecoratedKey, data_position)`. The position is encoded
    /// as a negative idxpos (DataDirect) via bitwise NOT.
    fn build_partition_index(entries: &[(&DecoratedKey, u64)]) -> Vec<u8> {
        let mut encoded_entries: Vec<(Vec<u8>, u8, i64)> = entries
            .iter()
            .map(|(dk, pos)| {
                let encoded = byte_comparable::encode(dk);
                let (_h1, h2) = dk.filter_hash();
                let hash = (h2 & 0xFF) as u8;
                // Negative idxpos -> DataDirect (bitwise NOT of position)
                let idxpos = !(*pos as i64);
                (encoded, hash, idxpos)
            })
            .collect();
        encoded_entries.sort_by(|a, b| a.0.cmp(&b.0));

        let mut builder = TrieBuilder::new();
        for (encoded, hash, idxpos) in &encoded_entries {
            builder
                .add(
                    encoded,
                    TriePayload {
                        hash: Some(*hash),
                        position: *idxpos,
                    },
                )
                .unwrap();
        }
        let (trie_data, root_pos) = builder.finish().unwrap();

        // Assemble: trie data + key bounds + footer
        let mut buf = Vec::new();
        buf.extend_from_slice(&trie_data);

        // Key bounds
        let key_bounds_offset = buf.len() as i64;
        // Use first and last partition keys as bounds
        let smallest = entries.first().map(|(dk, _)| dk.key.as_bytes()).unwrap();
        let largest = entries.last().map(|(dk, _)| dk.key.as_bytes()).unwrap();
        buf.extend_from_slice(&(smallest.len() as u16).to_be_bytes());
        buf.extend_from_slice(smallest);
        buf.extend_from_slice(&(largest.len() as u16).to_be_bytes());
        buf.extend_from_slice(largest);

        // Footer: key_bounds_offset, key_count, root_pos
        buf.extend_from_slice(&key_bounds_offset.to_be_bytes());
        buf.extend_from_slice(&(entries.len() as i64).to_be_bytes());
        buf.extend_from_slice(&(root_pos as i64).to_be_bytes());

        buf
    }

    /// Build a BloomFilter containing the given keys.
    fn build_bloom_filter(keys: &[&DecoratedKey]) -> Vec<u8> {
        let mut bf = BloomFilter::new(keys.len().max(10), 0.01);
        for dk in keys {
            let (h1, h2) = dk.filter_hash();
            bf.add(h1, h2);
        }
        bf.write()
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    #[test]
    fn open_and_read_single_partition() {
        let header = test_header();

        let dk = DecoratedKey::new(PartitionKey::from(b"pk1".as_slice()));

        // Build Data.db
        let data_bytes = build_data_blob(b"pk1");

        // Build Partitions.db pointing to position 0 in Data.db
        let partitions_bytes = build_partition_index(&[(&dk, 0)]);

        // Build Filter.db
        let filter_bytes = build_bloom_filter(&[&dk]);

        // Build Statistics.db
        let stats_bytes = build_statistics(header);

        let components = SSTableComponents {
            data: data_bytes,
            partitions: partitions_bytes,
            rows: Vec::new(),
            filter: filter_bytes,
            compression_info: None,
            statistics: stats_bytes,
        };

        let reader = SSTableReader::open(components).unwrap();

        // Verify key count
        assert_eq!(reader.key_count(), 1);

        // Read the partition
        let partition = reader
            .get_partition(&dk)
            .unwrap()
            .expect("expected partition");

        assert_eq!(partition.key.key.as_bytes(), b"pk1");
        assert!(partition.deletion.is_live());
        assert_eq!(partition.rows.len(), 1);

        let row = &partition.rows[0];
        assert_eq!(row.clustering, vec![0x00, 0x00, 0x00, 0x01]);
        assert_eq!(row.primary_key_liveness.timestamp, 1_000_042);
        assert_eq!(row.cells.len(), 1);
        assert_eq!(row.cells[0].1.value.as_deref(), Some(b"hello-0".as_slice()));
    }

    #[test]
    fn bloom_filter_rejects_absent_key() {
        let header = test_header();

        let dk = DecoratedKey::new(PartitionKey::from(b"pk1".as_slice()));
        let missing = DecoratedKey::new(PartitionKey::from(b"nonexistent".as_slice()));

        // Build Data.db with just dk
        let data_bytes = build_data_blob(b"pk1");

        // Build Partitions.db with just dk
        let partitions_bytes = build_partition_index(&[(&dk, 0)]);

        // Build Filter.db with ONLY dk (missing key not added)
        let filter_bytes = build_bloom_filter(&[&dk]);

        // Build Statistics.db
        let stats_bytes = build_statistics(header);

        let components = SSTableComponents {
            data: data_bytes,
            partitions: partitions_bytes,
            rows: Vec::new(),
            filter: filter_bytes,
            compression_info: None,
            statistics: stats_bytes,
        };

        let reader = SSTableReader::open(components).unwrap();

        // The missing key should not be present in the bloom filter.
        // Due to the probabilistic nature, we verify via the bloom_filter
        // accessor directly.
        let (h1, h2) = missing.filter_hash();
        if !reader.bloom_filter().is_present(h1, h2) {
            // Bloom filter correctly rejects -- get_partition should return None
            let result = reader.get_partition(&missing).unwrap();
            assert!(result.is_none(), "expected None for bloom-rejected key");
        }
        // If bloom filter has a false positive, the partition index lookup
        // will return NotFound, which is also correct behavior.
        let result = reader.get_partition(&missing).unwrap();
        assert!(result.is_none(), "expected None for absent key");
    }

    #[test]
    fn read_partitions_limited_rows_skips_unretained_rows_and_continues() {
        let header = test_header();

        let dk1 = DecoratedKey::new(PartitionKey::from(b"k1".as_slice()));
        let dk2 = DecoratedKey::new(PartitionKey::from(b"k2".as_slice()));

        let mut data_bytes = Vec::new();
        let pos1 = data_bytes.len() as u64;
        data_bytes.extend_from_slice(&build_data_blob_with_rows(b"k1", 3));
        let pos2 = data_bytes.len() as u64;
        data_bytes.extend_from_slice(&build_data_blob_with_rows(b"k2", 2));

        let partitions_bytes = build_partition_index(&[(&dk1, pos1), (&dk2, pos2)]);
        let filter_bytes = build_bloom_filter(&[&dk1, &dk2]);
        let stats_bytes = build_statistics(header);

        let components = SSTableComponents {
            data: data_bytes,
            partitions: partitions_bytes,
            rows: Vec::new(),
            filter: filter_bytes,
            compression_info: None,
            statistics: stats_bytes,
        };

        let reader = SSTableReader::open(components).unwrap();
        let partitions = reader.read_partitions_limited_rows(2, 1).unwrap();

        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions[0].key.key.as_bytes(), b"k1");
        assert_eq!(partitions[1].key.key.as_bytes(), b"k2");
        assert_eq!(partitions[0].rows.len(), 1);
        assert_eq!(partitions[1].rows.len(), 1);
        assert_eq!(partitions[0].rows[0].clustering, 1_i32.to_be_bytes());
        assert_eq!(partitions[1].rows[0].clustering, 1_i32.to_be_bytes());
    }

    #[test]
    fn key_count_is_correct() {
        let header = test_header();

        let dk1 = DecoratedKey::new(PartitionKey::from(b"k1".as_slice()));
        let dk2 = DecoratedKey::new(PartitionKey::from(b"k2".as_slice()));
        let dk3 = DecoratedKey::new(PartitionKey::from(b"k3".as_slice()));

        // Build Data.db with three partitions concatenated
        let mut data_bytes = Vec::new();
        let pos1 = data_bytes.len() as u64;
        data_bytes.extend_from_slice(&build_data_blob(b"k1"));
        let pos2 = data_bytes.len() as u64;
        data_bytes.extend_from_slice(&build_data_blob(b"k2"));
        let pos3 = data_bytes.len() as u64;
        data_bytes.extend_from_slice(&build_data_blob(b"k3"));

        // Build Partitions.db
        let partitions_bytes = build_partition_index(&[(&dk1, pos1), (&dk2, pos2), (&dk3, pos3)]);

        // Build Filter.db
        let filter_bytes = build_bloom_filter(&[&dk1, &dk2, &dk3]);

        // Build Statistics.db
        let stats_bytes = build_statistics(header);

        let components = SSTableComponents {
            data: data_bytes,
            partitions: partitions_bytes,
            rows: Vec::new(),
            filter: filter_bytes,
            compression_info: None,
            statistics: stats_bytes,
        };

        let reader = SSTableReader::open(components).unwrap();
        assert_eq!(reader.key_count(), 3);

        // Verify header accessor
        assert_eq!(
            reader.header().key_type,
            "org.apache.cassandra.db.marshal.UTF8Type"
        );

        // Verify each partition is readable
        for (dk, key_bytes) in [(&dk1, b"k1"), (&dk2, b"k2"), (&dk3, b"k3")] {
            let partition = reader
                .get_partition(dk)
                .unwrap()
                .expect("expected partition");
            assert_eq!(partition.key.key.as_bytes(), key_bytes.as_slice());
            assert_eq!(partition.rows.len(), 1);
        }

        // Verify compression_info is None
        assert!(reader.compression_info().is_none());
    }

    /// Parity: streaming `partitions_iter()` yields the same sequence as
    /// the materializing `read_all_partitions()`.  This is the regression
    /// guard for the streaming-compaction refactor.
    #[test]
    fn partitions_iter_matches_read_all_partitions() {
        let header = test_header();
        let dks: Vec<_> = (0..7u32)
            .map(|i| DecoratedKey::new(PartitionKey::from(format!("pk{i:02}").as_bytes())))
            .collect();
        let mut data_bytes = Vec::new();
        let mut positions = Vec::new();
        for dk in &dks {
            positions.push(data_bytes.len() as u64);
            data_bytes.extend_from_slice(&build_data_blob(dk.key.as_bytes()));
        }
        let dk_pos: Vec<_> = dks.iter().zip(positions.iter().copied()).collect();
        let partitions_bytes =
            build_partition_index(&dk_pos.iter().map(|(d, p)| (*d, *p)).collect::<Vec<_>>());
        let filter_bytes = build_bloom_filter(&dks.iter().collect::<Vec<_>>());
        let stats_bytes = build_statistics(header);

        let components = SSTableComponents {
            data: data_bytes,
            partitions: partitions_bytes,
            rows: Vec::new(),
            filter: filter_bytes,
            compression_info: None,
            statistics: stats_bytes,
        };
        let reader = SSTableReader::open(components).unwrap();

        let materialized = reader.read_all_partitions().expect("read_all");
        let mut streamed = Vec::new();
        let mut iter = reader.partitions_iter().expect("partitions_iter");
        while let Some(p) = iter.next_partition().expect("next") {
            streamed.push(p);
        }

        assert_eq!(materialized.len(), streamed.len(), "same partition count");
        for (m, s) in materialized.iter().zip(streamed.iter()) {
            assert_eq!(m.key, s.key);
            assert_eq!(m.rows.len(), s.rows.len());
        }
        // EOF stays at EOF
        assert!(iter.next_partition().unwrap().is_none());
        assert!(iter.next_partition().unwrap().is_none());
    }

    /// ADR-020 COUNT(*) fast path: next_partition_count yields the
    /// same partition keys as next_partition, with row_count
    /// matching `partition.rows.len()`. Crucially does NOT decode
    /// any cell payloads — `read_partition_count` advances by
    /// byte-skipping via `skip_row_body`.
    #[test]
    fn next_partition_count_matches_partition_rows_len() {
        let header = test_header();
        let dks: Vec<_> = (0..5u32)
            .map(|i| DecoratedKey::new(PartitionKey::from(format!("pk{i:02}").as_bytes())))
            .collect();
        let mut data_bytes = Vec::new();
        let mut positions = Vec::new();
        for dk in &dks {
            positions.push(data_bytes.len() as u64);
            data_bytes.extend_from_slice(&build_data_blob(dk.key.as_bytes()));
        }
        let dk_pos: Vec<_> = dks.iter().zip(positions.iter().copied()).collect();
        let partitions_bytes =
            build_partition_index(&dk_pos.iter().map(|(d, p)| (*d, *p)).collect::<Vec<_>>());
        let filter_bytes = build_bloom_filter(&dks.iter().collect::<Vec<_>>());
        let stats_bytes = build_statistics(header);

        let components = SSTableComponents {
            data: data_bytes,
            partitions: partitions_bytes,
            rows: Vec::new(),
            filter: filter_bytes,
            compression_info: None,
            statistics: stats_bytes,
        };
        let reader = SSTableReader::open(components).unwrap();

        // Reference: full partition iteration with row counts.
        let mut iter_full = reader.partitions_iter().expect("partitions_iter");
        let mut expected: Vec<(_, u64)> = Vec::new();
        while let Some(p) = iter_full.next_partition().expect("next") {
            expected.push((p.key, p.rows.len() as u64));
        }

        // Under test: counts-only iteration.
        let mut iter_counts = reader.partitions_iter().expect("partitions_iter (counts)");
        let mut got: Vec<(_, u64)> = Vec::new();
        while let Some(pc) = iter_counts.next_partition_count().expect("next_count") {
            got.push(pc);
        }

        assert_eq!(got, expected, "count iterator must match full iter");
        // EOF stable.
        assert!(iter_counts.next_partition_count().unwrap().is_none());
        assert!(iter_counts.next_partition_count().unwrap().is_none());
    }

    /// ADR-020 fast COUNT(*) metadata path: next_partition_metadata
    /// yields partitions with the same key + same row count + same
    /// per-row clustering keys as the full path, but with empty
    /// `cells`. Verifies the body-end skip arithmetic stays aligned
    /// across rows.
    #[test]
    fn next_partition_metadata_matches_keys_drops_cells() {
        let header = test_header();
        let dks: Vec<_> = (0..5u32)
            .map(|i| DecoratedKey::new(PartitionKey::from(format!("pk{i:02}").as_bytes())))
            .collect();
        let mut data_bytes = Vec::new();
        let mut positions = Vec::new();
        for dk in &dks {
            positions.push(data_bytes.len() as u64);
            data_bytes.extend_from_slice(&build_data_blob(dk.key.as_bytes()));
        }
        let dk_pos: Vec<_> = dks.iter().zip(positions.iter().copied()).collect();
        let partitions_bytes =
            build_partition_index(&dk_pos.iter().map(|(d, p)| (*d, *p)).collect::<Vec<_>>());
        let filter_bytes = build_bloom_filter(&dks.iter().collect::<Vec<_>>());
        let stats_bytes = build_statistics(header);

        let components = SSTableComponents {
            data: data_bytes,
            partitions: partitions_bytes,
            rows: Vec::new(),
            filter: filter_bytes,
            compression_info: None,
            statistics: stats_bytes,
        };
        let reader = SSTableReader::open(components).unwrap();

        let mut iter_full = reader.partitions_iter().expect("partitions_iter (full)");
        let mut full = Vec::new();
        while let Some(p) = iter_full.next_partition().expect("next full") {
            full.push(p);
        }

        let mut iter_meta = reader.partitions_iter().expect("partitions_iter (meta)");
        let mut meta = Vec::new();
        while let Some(p) = iter_meta.next_partition_metadata().expect("next meta") {
            meta.push(p);
        }

        assert_eq!(meta.len(), full.len(), "same partition count");
        for (f, m) in full.iter().zip(meta.iter()) {
            assert_eq!(m.key, f.key, "partition key matches");
            assert_eq!(m.rows.len(), f.rows.len(), "row count matches");
            for (fr, mr) in f.rows.iter().zip(m.rows.iter()) {
                assert_eq!(mr.clustering, fr.clustering, "clustering matches");
                assert!(
                    mr.cells.is_empty(),
                    "metadata path must NOT decode cells (got {} cells)",
                    mr.cells.len()
                );
            }
        }
        // EOF stable.
        assert!(iter_meta.next_partition_metadata().unwrap().is_none());
    }

    /// ADR-020 projection-aware decode: `next_partition_projected`
    /// returns the same partition keys + same row count + same
    /// clustering keys as the full path, but `cells` contains only
    /// the cells the caller named in `wanted`. Cells outside the
    /// projection are byte-skipped via `read_cell_skip`.
    #[test]
    fn next_partition_projected_filters_cells_to_wanted_set() {
        let header = test_header();
        let dks: Vec<_> = (0..3u32)
            .map(|i| DecoratedKey::new(PartitionKey::from(format!("pk{i:02}").as_bytes())))
            .collect();
        let mut data_bytes = Vec::new();
        let mut positions = Vec::new();
        for dk in &dks {
            positions.push(data_bytes.len() as u64);
            data_bytes.extend_from_slice(&build_data_blob(dk.key.as_bytes()));
        }
        let dk_pos: Vec<_> = dks.iter().zip(positions.iter().copied()).collect();
        let partitions_bytes =
            build_partition_index(&dk_pos.iter().map(|(d, p)| (*d, *p)).collect::<Vec<_>>());
        let filter_bytes = build_bloom_filter(&dks.iter().collect::<Vec<_>>());
        let stats_bytes = build_statistics(header);

        let components = SSTableComponents {
            data: data_bytes,
            partitions: partitions_bytes,
            rows: Vec::new(),
            filter: filter_bytes,
            compression_info: None,
            statistics: stats_bytes,
        };
        let reader = SSTableReader::open(components).unwrap();

        // Reference: full partition iteration.
        let mut iter_full = reader.partitions_iter().expect("partitions_iter (full)");
        let mut full = Vec::new();
        while let Some(p) = iter_full.next_partition().expect("next full") {
            full.push(p);
        }

        // Projection: wanted = {0} → only column 0's cell per row.
        let wanted: Vec<u16> = vec![0];
        let mut iter_proj = reader.partitions_iter().expect("partitions_iter (proj)");
        let mut proj = Vec::new();
        while let Some(p) = iter_proj
            .next_partition_projected(&wanted)
            .expect("next projected")
        {
            proj.push(p);
        }

        assert_eq!(proj.len(), full.len(), "same partition count");
        for (f, p) in full.iter().zip(proj.iter()) {
            assert_eq!(p.key, f.key, "key matches");
            assert_eq!(p.rows.len(), f.rows.len(), "row count matches");
            for (fr, pr) in f.rows.iter().zip(p.rows.iter()) {
                assert_eq!(pr.clustering, fr.clustering, "clustering matches");
                // Projected: only column 0 cells should remain.
                let proj_col_ids: Vec<u16> = pr.cells.iter().map(|(c, _)| *c).collect();
                assert!(
                    proj_col_ids.iter().all(|c| wanted.contains(c)),
                    "projected row only has wanted cells: {proj_col_ids:?}"
                );
                // Full had column 0; ensure projection didn't drop it.
                let full_has_col0 = fr.cells.iter().any(|(c, _)| *c == 0);
                let proj_has_col0 = pr.cells.iter().any(|(c, _)| *c == 0);
                assert_eq!(
                    proj_has_col0, full_has_col0,
                    "column 0 presence preserved"
                );
            }
        }

        // Empty projection = no cells.
        let mut iter_empty = reader.partitions_iter().expect("partitions_iter (empty)");
        if let Some(p) = iter_empty
            .next_partition_projected(&[])
            .expect("next empty projection")
        {
            for r in &p.rows {
                assert!(r.cells.is_empty(), "empty projection leaves cells empty");
            }
        }
    }
}
