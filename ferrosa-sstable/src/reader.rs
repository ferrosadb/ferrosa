//! SSTableReader — compose all components into a single read interface.
//!
//! Opens a BTI SSTable from component file handles and provides:
//! - Partition lookup by key
//! - Full partition iteration in token order

use ferrosa_common::Result;

use crate::bloom::BloomFilter;
use crate::compression::CompressionInfo;
use crate::data::DataReader;
use crate::io::ReadAt;
use crate::partition_index::{PartitionIndex, PartitionLookup};
use crate::row_index::read_row_index_entry;
use crate::statistics::{read_statistics, SerializationHeader};
use crate::types::Partition;

/// Handles to all component files/buffers for an SSTable.
pub struct SSTableComponents<R> {
    /// Data.db — partition data.
    pub data: R,
    /// Partitions.db — partition index trie.
    pub partitions: R,
    /// Rows.db — row index (optional; needed for RowIndex lookups).
    pub rows: Option<R>,
    /// Filter.db — Bloom filter (read into memory).
    pub filter: Vec<u8>,
    /// CompressionInfo.db (optional, read into memory).
    pub compression_info: Option<Vec<u8>>,
    /// Statistics.db (read into memory).
    pub statistics: Vec<u8>,
}

/// Unified SSTable reader composing all components.
pub struct SSTableReader<R: ReadAt> {
    partition_index: PartitionIndex<R>,
    bloom_filter: BloomFilter,
    #[allow(dead_code)]
    compression_info: Option<CompressionInfo>,
    header: SerializationHeader,
    data_reader: DataReader<R>,
    rows_reader: Option<R>,
}

impl<R: ReadAt> SSTableReader<R> {
    /// Open an SSTable from its components.
    pub fn open(components: SSTableComponents<R>) -> Result<Self> {
        // 1. Parse statistics -> get SerializationHeader.
        let stats = read_statistics(&components.statistics)?;
        let header = stats.header;

        // 2. Parse bloom filter.
        let bloom_filter = BloomFilter::read(&components.filter)?;

        // 3. Parse compression info (if present).
        let compression_info = match components.compression_info {
            Some(ref data) => Some(CompressionInfo::read(data)?),
            None => None,
        };

        // 4. Open partition index.
        let partition_index = PartitionIndex::open(components.partitions)?;

        // 5. Create data reader with header.
        let data_reader = DataReader::new(components.data, header.clone());

        Ok(SSTableReader {
            partition_index,
            bloom_filter,
            compression_info,
            header,
            data_reader,
            rows_reader: components.rows,
        })
    }

    /// Look up a partition by its byte-comparable encoded key.
    ///
    /// `encoded_key` is the byte-comparable encoding of the partition key
    /// (produced by [`crate::byte_comparable::encode`]).
    ///
    /// `filter_hash` is an optional hash byte for trie-level bloom filter
    /// rejection. If the partition index stores hash bytes in its payloads,
    /// a mismatch will return `None` without reading data.
    ///
    /// Returns `None` if the partition is not found.
    pub fn get_partition(
        &self,
        encoded_key: &[u8],
        filter_hash: Option<u8>,
    ) -> Result<Option<Partition>> {
        // 1. Look up in partition index.
        let lookup = self.partition_index.lookup_raw(encoded_key, filter_hash)?;

        let data_position = match lookup {
            PartitionLookup::NotFound => return Ok(None),
            PartitionLookup::DataDirect { position } => position,
            PartitionLookup::RowIndex { position } => {
                // Read row index entry to get the data position.
                let rows = self.rows_reader.as_ref().ok_or_else(|| {
                    ferrosa_common::Error::InvalidData(
                        "RowIndex lookup requires Rows.db reader".into(),
                    )
                })?;
                let (entry, _) = read_row_index_entry(rows, position)?;
                entry.partition_position
            }
        };

        // 2. Read partition from data file.
        let (partition, _next_offset) = self.data_reader.read_partition(data_position)?;
        Ok(Some(partition))
    }

    /// Key count from the partition index.
    pub fn key_count(&self) -> u64 {
        self.partition_index.key_count()
    }

    /// Access the serialization header.
    pub fn header(&self) -> &SerializationHeader {
        &self.header
    }

    /// Access the bloom filter.
    pub fn bloom_filter(&self) -> &BloomFilter {
        &self.bloom_filter
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::statistics::{write_statistics, Statistics, ValidationMetadata};
    use crate::trie::builder::{TrieBuilder, TriePayload};
    use crate::varint;

    /// Build a serialization header for testing.
    fn test_header() -> SerializationHeader {
        SerializationHeader {
            min_timestamp: 1_000_000,
            min_local_deletion_time: 0,
            min_ttl: 0,
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_types: vec!["org.apache.cassandra.db.marshal.UTF8Type".to_string()],
            static_columns: vec![],
            regular_columns: vec![(
                b"value".to_vec(),
                "org.apache.cassandra.db.marshal.BytesType".to_string(),
            )],
        }
    }

    /// Build a Statistics.db buffer from the test header.
    fn build_statistics(header: &SerializationHeader) -> Vec<u8> {
        let stats = Statistics {
            validation: ValidationMetadata {
                partitioner: "org.apache.cassandra.dht.Murmur3Partitioner".to_string(),
                bloom_fp_chance: 0.01,
            },
            compaction: vec![0x00],
            stats: vec![0x00],
            header: header.clone(),
        };
        write_statistics(&stats)
    }

    /// Helper: write an unsigned varint to a buffer.
    fn write_uvarint(buf: &mut Vec<u8>, val: u64) {
        let mut tmp = [0u8; 9];
        let n = varint::write_unsigned_vint(&mut tmp, val);
        buf.extend_from_slice(&tmp[..n]);
    }

    /// Build a single partition in Data.db format.
    ///
    /// Returns the serialized partition bytes.
    fn build_data_partition(
        key_bytes: &[u8],
        clustering: &[u8],
        cell_value: &[u8],
        timestamp_delta: u64,
    ) -> Vec<u8> {
        let mut data = Vec::new();

        // Partition key: u16 BE length + bytes.
        data.extend_from_slice(&(key_bytes.len() as u16).to_be_bytes());
        data.extend_from_slice(key_bytes);

        // Deletion time: LIVE (i32::MAX, i64::MIN).
        data.extend_from_slice(&i32::MAX.to_be_bytes());
        data.extend_from_slice(&i64::MIN.to_be_bytes());

        // Row: flags = HAS_CLUSTERING | HAS_TIMESTAMP | HAS_ALL_COLUMNS
        let flags: u8 = 0x02 | 0x04 | 0x20;
        data.push(flags);

        // Clustering key: varint length + bytes.
        write_uvarint(&mut data, clustering.len() as u64);
        data.extend_from_slice(clustering);

        // Timestamp delta.
        write_uvarint(&mut data, timestamp_delta);

        // Cell: flags = HAS_VALUE | USE_ROW_TIMESTAMP
        data.push(0x01 | 0x20);

        // Value: varint length + bytes.
        write_uvarint(&mut data, cell_value.len() as u64);
        data.extend_from_slice(cell_value);

        // END_OF_PARTITION.
        data.push(0x01);

        data
    }

    /// Build a partition index file (Partitions.db) from entries.
    ///
    /// Each entry is (encoded_key, idxpos). Use negative idxpos for DataDirect
    /// positions: idxpos = !(data_position).
    fn build_partition_index(
        entries: &[(&[u8], i64)],
        smallest_key: &[u8],
        largest_key: &[u8],
    ) -> Vec<u8> {
        let mut builder = TrieBuilder::new();
        for &(key, position) in entries {
            builder
                .add(
                    key,
                    TriePayload {
                        hash: None,
                        position,
                    },
                )
                .unwrap();
        }
        let (trie_data, root_pos) = builder.finish().unwrap();

        let mut file = trie_data;
        let key_bounds_offset = file.len() as i64;

        // Write smallest key: u16 length + bytes.
        file.extend_from_slice(&(smallest_key.len() as u16).to_be_bytes());
        file.extend_from_slice(smallest_key);

        // Write largest key: u16 length + bytes.
        file.extend_from_slice(&(largest_key.len() as u16).to_be_bytes());
        file.extend_from_slice(largest_key);

        // Footer: key_bounds_offset, key_count, root_pos.
        let key_count = entries.len() as i64;
        file.extend_from_slice(&key_bounds_offset.to_be_bytes());
        file.extend_from_slice(&key_count.to_be_bytes());
        file.extend_from_slice(&(root_pos as i64).to_be_bytes());

        file
    }

    #[test]
    fn open_and_get_single_partition() {
        let header = test_header();
        let key = b"hello";
        let clustering = b"ck1";
        let cell_value = b"world";
        let timestamp_delta = 42u64;

        // Build Data.db with one partition.
        let data_buf = build_data_partition(key, clustering, cell_value, timestamp_delta);

        // Build byte-comparable encoded key for the partition index.
        let dk =
            ferrosa_common::DecoratedKey::new(ferrosa_common::PartitionKey::from(key.as_slice()));
        let encoded_key = crate::byte_comparable::encode(&dk);

        // Build partition index: DataDirect at position 0.
        // Negative idxpos means DataDirect: position = !data_pos.
        let idxpos: i64 = !0i64; // !0 = -1, DataDirect position = !(-1) = 0
        let partitions_buf = build_partition_index(&[(&encoded_key, idxpos)], key, key);

        // Build Statistics.db.
        let statistics_buf = build_statistics(&header);

        // Build Filter.db (bloom filter for 1 key).
        let bloom = BloomFilter::new(1, 0.01);
        let filter_buf = bloom.write();

        // Open the SSTableReader.
        let components = SSTableComponents {
            data: data_buf,
            partitions: partitions_buf,
            rows: None,
            filter: filter_buf,
            compression_info: None,
            statistics: statistics_buf,
        };

        let reader = SSTableReader::open(components).unwrap();

        assert_eq!(reader.key_count(), 1);

        // Look up the partition.
        let partition = reader.get_partition(&encoded_key, None).unwrap();
        assert!(partition.is_some());
        let partition = partition.unwrap();

        assert_eq!(partition.key.key.as_bytes(), b"hello");
        assert!(partition.deletion.is_live());
        assert_eq!(partition.rows.len(), 1);
        assert_eq!(partition.rows[0].clustering, b"ck1");
        assert_eq!(
            partition.rows[0].primary_key_liveness.timestamp,
            1_000_000 + 42
        );
        assert_eq!(
            partition.rows[0].cells[0].1.value.as_deref(),
            Some(b"world".as_slice())
        );
    }

    #[test]
    fn get_missing_partition_returns_none() {
        let header = test_header();
        let key = b"exists";

        // Build Data.db with one partition.
        let data_buf = build_data_partition(key, b"ck", b"val", 0);

        // Build partition index.
        let dk =
            ferrosa_common::DecoratedKey::new(ferrosa_common::PartitionKey::from(key.as_slice()));
        let encoded_key = crate::byte_comparable::encode(&dk);
        let idxpos: i64 = !0i64;
        let partitions_buf = build_partition_index(&[(&encoded_key, idxpos)], key, key);

        let statistics_buf = build_statistics(&header);
        let bloom = BloomFilter::new(1, 0.01);
        let filter_buf = bloom.write();

        let components = SSTableComponents {
            data: data_buf,
            partitions: partitions_buf,
            rows: None,
            filter: filter_buf,
            compression_info: None,
            statistics: statistics_buf,
        };

        let reader = SSTableReader::open(components).unwrap();

        // Look up a key that doesn't exist.
        let missing_dk = ferrosa_common::DecoratedKey::new(ferrosa_common::PartitionKey::from(
            b"missing".as_slice(),
        ));
        let missing_encoded = crate::byte_comparable::encode(&missing_dk);
        let result = reader.get_partition(&missing_encoded, None).unwrap();
        assert!(result.is_none());
    }
}
