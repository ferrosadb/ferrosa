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
    /// Decompressed Data.db contents (when compression is used).
    decompressed_data: Option<Vec<u8>>,
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

        // Decompress Data.db if compression is configured
        let decompressed_data = if let Some(ref ci) = compression_info {
            Some(decompress_data(&components.data, ci)?)
        } else {
            None
        };

        Ok(SSTableReader {
            partition_index,
            bloom_filter,
            compression_info,
            header,
            data: components.data,
            decompressed_data,
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
        if let Some(ref decompressed) = self.decompressed_data {
            let mut data_reader = DataReader::new(decompressed, &self.header, data_position);
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
    /// partition until EOF. Used by the compaction executor to merge
    /// multiple SSTables.
    pub fn read_all_partitions(&self) -> Result<Vec<crate::types::Partition>> {
        let mut partitions = Vec::new();
        if let Some(ref dec) = self.decompressed_data {
            let mut reader = crate::data::DataReader::new(dec, &self.header, 0);
            while let Some(partition) = reader.read_partition()? {
                partitions.push(partition);
            }
        } else {
            let mut reader = crate::data::DataReader::new(&self.data, &self.header, 0);
            while let Some(partition) = reader.read_partition()? {
                partitions.push(partition);
            }
        }
        Ok(partitions)
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
    /// key `[0,0,0,1]`, timestamp delta 42, and cell value `b"hello"`.
    fn build_data_blob(key: &[u8]) -> Vec<u8> {
        let mut data = Vec::new();

        // Partition header: u16 BE key len + key bytes
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key);

        // Live deletion time (Cassandra 5.x: single byte 0x80)
        data.push(DELETION_IS_LIVE);

        // Row flags: HAS_TIMESTAMP | HAS_ALL_COLUMNS
        data.push(HAS_TIMESTAMP | HAS_ALL_COLUMNS);

        // Clustering key (ClusteringPrefix format, Int32Type = fixed-length)
        let clustering = [0x00u8, 0x00, 0x00, 0x01];
        push_unsigned_vint(&mut data, 0); // clustering header: all non-null, non-empty
        data.extend_from_slice(&clustering);

        // Row body size + prev unfiltered size (both skipped by reader)
        push_unsigned_vint(&mut data, 20);
        push_unsigned_vint(&mut data, 0);

        // Liveness timestamp delta = 42 (unsigned varint)
        push_unsigned_vint(&mut data, 42);

        // Cell: use row timestamp, value = b"hello"
        data.push(CELL_USE_ROW_TIMESTAMP);
        let value = b"hello";
        push_unsigned_vint(&mut data, value.len() as u64);
        data.extend_from_slice(value);

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
        assert_eq!(row.cells[0].1.value.as_deref(), Some(b"hello".as_slice()));
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
}
