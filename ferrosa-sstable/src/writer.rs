//! SSTableWriter — write BTI SSTables from sorted partition input.
//!
//! Accepts partitions in token order and produces all component files.
//!
//! # Data.db Partition Format
//!
//! ```text
//! [key_length: u16 BE] [key_bytes]
//! [deletion_time: i32 local_deletion_time BE + i64 marked_for_delete_at BE]
//! [rows...]
//! [END_OF_PARTITION marker: flags = 0x01]
//! ```

use ferrosa_common::Result;

use crate::bloom::BloomFilter;
use crate::compression::Compression;
use crate::statistics::{write_statistics, SerializationHeader, Statistics, ValidationMetadata};
use crate::toc::write_toc;
use crate::trie::builder::{TrieBuilder, TriePayload};
use crate::types::Partition;
use crate::varint;

// Row flags bits (matching data.rs).
const END_OF_PARTITION: u8 = 0x01;
const HAS_CLUSTERING: u8 = 0x02;
const HAS_TIMESTAMP: u8 = 0x04;
const HAS_ALL_COLUMNS: u8 = 0x20;

// Cell flags bits (matching data.rs).
const CELL_HAS_VALUE: u8 = 0x01;
const CELL_USE_ROW_TIMESTAMP: u8 = 0x20;

/// Options for writing an SSTable.
#[derive(Debug, Clone)]
pub struct WriteOptions {
    /// Compression algorithm for Data.db chunks. `None` means uncompressed.
    pub compression: Option<Compression>,
    /// Target false-positive rate for the Bloom filter.
    pub bloom_fp_chance: f64,
    /// Partitioner class name for Statistics.db validation metadata.
    pub partitioner: String,
}

impl Default for WriteOptions {
    fn default() -> Self {
        WriteOptions {
            compression: Some(Compression::Lz4),
            bloom_fp_chance: 0.01,
            partitioner: "org.apache.cassandra.dht.Murmur3Partitioner".to_string(),
        }
    }
}

/// SSTable writer producing all component buffers.
pub struct SSTableWriter {
    options: WriteOptions,
    header: SerializationHeader,
    data_buf: Vec<u8>,
    trie_builder: TrieBuilder,
    bloom_filter: BloomFilter,
    partition_count: u64,
    smallest_key: Option<Vec<u8>>,
    largest_key: Option<Vec<u8>>,
}

/// All written component buffers.
pub struct WrittenSSTable {
    /// Data.db — serialized partition data.
    pub data: Vec<u8>,
    /// Partitions.db — partition index trie + key bounds + footer.
    pub partitions: Vec<u8>,
    /// Filter.db — Bloom filter.
    pub filter: Vec<u8>,
    /// Statistics.db — metadata and serialization header.
    pub statistics: Vec<u8>,
    /// TOC.txt — table of contents listing component names.
    pub toc: Vec<u8>,
}

impl SSTableWriter {
    /// Create a new writer with the given serialization header and options.
    ///
    /// The `expected_partitions` hint is used to size the Bloom filter.
    pub fn new(
        header: SerializationHeader,
        options: WriteOptions,
        expected_partitions: usize,
    ) -> Self {
        let bloom_filter = BloomFilter::new(expected_partitions.max(1), options.bloom_fp_chance);
        SSTableWriter {
            options,
            header,
            data_buf: Vec::new(),
            trie_builder: TrieBuilder::new(),
            bloom_filter,
            partition_count: 0,
            smallest_key: None,
            largest_key: None,
        }
    }

    /// Add a partition. Partitions must be added in sorted key order.
    ///
    /// - `encoded_key`: byte-comparable encoded key (for the partition index trie)
    /// - `key_bytes`: raw partition key bytes (stored in Data.db)
    /// - `partition`: the partition data to write
    /// - `filter_h1`, `filter_h2`: Murmur3 hash values for the Bloom filter
    pub fn add_partition(
        &mut self,
        encoded_key: &[u8],
        key_bytes: &[u8],
        partition: &Partition,
        filter_h1: i64,
        filter_h2: i64,
    ) -> Result<()> {
        // 1. Track smallest/largest key.
        if self.smallest_key.is_none() {
            self.smallest_key = Some(key_bytes.to_vec());
        }
        self.largest_key = Some(key_bytes.to_vec());

        // 2. Add to bloom filter.
        self.bloom_filter.add(filter_h1, filter_h2);

        // 3. Record current data position and write partition to data buffer.
        let data_position = self.data_buf.len() as u64;
        self.write_partition_data(key_bytes, partition);

        // 4. Add to trie builder with DataDirect position.
        // DataDirect uses negative idxpos: idxpos = !(data_position).
        let idxpos = !(data_position as i64);
        self.trie_builder.add(
            encoded_key,
            TriePayload {
                hash: None,
                position: idxpos,
            },
        )?;

        // 5. Increment partition count.
        self.partition_count += 1;

        Ok(())
    }

    /// Finalize and return all component buffers.
    pub fn finish(self) -> Result<WrittenSSTable> {
        let SSTableWriter {
            options,
            header,
            data_buf,
            trie_builder,
            bloom_filter,
            partition_count,
            smallest_key,
            largest_key,
        } = self;

        // 1. Finish trie builder -> partitions data + root pos.
        let (trie_data, root_pos) = trie_builder.finish()?;

        // 2. Build Partitions.db: trie data + key bounds + footer.
        let partitions = build_partitions_file(
            trie_data,
            root_pos,
            partition_count,
            smallest_key.as_deref(),
            largest_key.as_deref(),
        );

        // 3. Build Statistics.db.
        let statistics = build_statistics_buf(&options, &header);

        // 4. Build Filter.db.
        let filter = bloom_filter.write();

        // 5. Build TOC.
        let toc_components = if options.compression.is_some() {
            vec![
                "Data.db",
                "Partitions.db",
                "Rows.db",
                "Filter.db",
                "CompressionInfo.db",
                "Statistics.db",
                "TOC.txt",
            ]
        } else {
            vec![
                "Data.db",
                "Partitions.db",
                "Rows.db",
                "Filter.db",
                "CRC.db",
                "Statistics.db",
                "TOC.txt",
            ]
        };
        let toc = write_toc(&toc_components);

        Ok(WrittenSSTable {
            data: data_buf,
            partitions,
            filter,
            statistics,
            toc,
        })
    }

    /// Serialize a partition to the data buffer.
    fn write_partition_data(&mut self, key_bytes: &[u8], partition: &Partition) {
        // Partition key: u16 BE length + bytes.
        self.data_buf
            .extend_from_slice(&(key_bytes.len() as u16).to_be_bytes());
        self.data_buf.extend_from_slice(key_bytes);

        // Deletion time: i32 local_deletion_time BE + i64 marked_for_delete_at BE.
        let ldt = if partition.deletion.is_live() {
            i32::MAX
        } else {
            partition.deletion.local_deletion_time as i32
        };
        self.data_buf.extend_from_slice(&ldt.to_be_bytes());
        self.data_buf
            .extend_from_slice(&partition.deletion.marked_for_delete_at.to_be_bytes());

        // Write rows.
        for row in &partition.rows {
            self.write_row(row);
        }

        // END_OF_PARTITION marker.
        self.data_buf.push(END_OF_PARTITION);
    }

    /// Serialize a row to the data buffer.
    fn write_row(&mut self, row: &crate::types::Row) {
        // Compute flags.
        let mut flags: u8 = 0;
        if !row.clustering.is_empty() {
            flags |= HAS_CLUSTERING;
        }
        if row.primary_key_liveness.has_timestamp() {
            flags |= HAS_TIMESTAMP;
        }
        flags |= HAS_ALL_COLUMNS;

        self.data_buf.push(flags);

        // Clustering key.
        if flags & HAS_CLUSTERING != 0 {
            self.write_uvarint(row.clustering.len() as u64);
            self.data_buf.extend_from_slice(&row.clustering);
        }

        // Timestamp delta.
        if flags & HAS_TIMESTAMP != 0 {
            let delta = (row.primary_key_liveness.timestamp - self.header.min_timestamp) as u64;
            self.write_uvarint(delta);
        }

        // Write cells.
        for (_col_idx, cell) in &row.cells {
            self.write_cell(cell);
        }
    }

    /// Serialize a cell to the data buffer.
    fn write_cell(&mut self, cell: &ferrosa_common::CellValue) {
        // Simplified cell serialization: HAS_VALUE | USE_ROW_TIMESTAMP.
        let cell_flags: u8 = if cell.value.is_some() {
            CELL_HAS_VALUE | CELL_USE_ROW_TIMESTAMP
        } else {
            // Tombstone: no value, just the flags byte.
            CELL_USE_ROW_TIMESTAMP
        };
        self.data_buf.push(cell_flags);

        if let Some(ref value) = cell.value {
            self.write_uvarint(value.len() as u64);
            self.data_buf.extend_from_slice(value);
        }
    }

    /// Write an unsigned varint to the data buffer.
    fn write_uvarint(&mut self, val: u64) {
        let mut tmp = [0u8; 9];
        let n = varint::write_unsigned_vint(&mut tmp, val);
        self.data_buf.extend_from_slice(&tmp[..n]);
    }
}

/// Build the Partitions.db file from trie data and metadata.
fn build_partitions_file(
    trie_data: Vec<u8>,
    root_pos: u64,
    partition_count: u64,
    smallest_key: Option<&[u8]>,
    largest_key: Option<&[u8]>,
) -> Vec<u8> {
    let mut file = trie_data;
    let key_bounds_offset = file.len() as i64;

    let smallest = smallest_key.unwrap_or(b"");
    let largest = largest_key.unwrap_or(b"");

    // Key bounds: u16-length-prefixed smallest + largest keys.
    file.extend_from_slice(&(smallest.len() as u16).to_be_bytes());
    file.extend_from_slice(smallest);
    file.extend_from_slice(&(largest.len() as u16).to_be_bytes());
    file.extend_from_slice(largest);

    // Footer: key_bounds_offset, key_count, root_pos.
    file.extend_from_slice(&key_bounds_offset.to_be_bytes());
    file.extend_from_slice(&(partition_count as i64).to_be_bytes());
    file.extend_from_slice(&(root_pos as i64).to_be_bytes());

    file
}

/// Build Statistics.db from the header and options.
fn build_statistics_buf(options: &WriteOptions, header: &SerializationHeader) -> Vec<u8> {
    let stats = Statistics {
        validation: ValidationMetadata {
            partitioner: options.partitioner.clone(),
            bloom_fp_chance: options.bloom_fp_chance,
        },
        compaction: vec![0x00], // Minimal opaque compaction metadata.
        stats: vec![0x00],      // Minimal opaque stats metadata.
        header: header.clone(),
    };
    write_statistics(&stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::{SSTableComponents, SSTableReader};
    use crate::types::DeletionTime;
    use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};

    /// Build a test serialization header with one regular column.
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

    /// Helper: build a simple partition with one row.
    fn make_partition(key: &[u8], clustering: &[u8], value: &[u8], ts: i64) -> Partition {
        Partition {
            key: DecoratedKey::new(PartitionKey::from(key)),
            deletion: DeletionTime::LIVE,
            static_row: None,
            rows: vec![crate::types::Row {
                clustering: clustering.to_vec(),
                cells: vec![(0, CellValue::live(value.to_vec(), ts))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: crate::types::LivenessInfo::with_timestamp(ts),
            }],
        }
    }

    #[test]
    fn write_then_read_round_trip() {
        let header = test_header();
        let options = WriteOptions {
            compression: None,
            bloom_fp_chance: 0.01,
            ..WriteOptions::default()
        };

        let mut writer = SSTableWriter::new(header.clone(), options, 1);

        let key = b"hello";
        let partition = make_partition(key, b"ck1", b"world", 1_000_042);

        let dk = DecoratedKey::new(PartitionKey::from(key.as_slice()));
        let encoded_key = crate::byte_comparable::encode(&dk);
        let (h1, h2) = dk.filter_hash();

        writer
            .add_partition(&encoded_key, key, &partition, h1, h2)
            .unwrap();

        let written = writer.finish().unwrap();

        // Open with SSTableReader.
        let components = SSTableComponents {
            data: written.data,
            partitions: written.partitions,
            rows: None,
            filter: written.filter,
            compression_info: None,
            statistics: written.statistics,
        };

        let reader = SSTableReader::open(components).unwrap();
        assert_eq!(reader.key_count(), 1);

        // Read back the partition.
        let read_partition = reader.get_partition(&encoded_key, None).unwrap().unwrap();

        assert_eq!(read_partition.key.key.as_bytes(), b"hello");
        assert!(read_partition.deletion.is_live());
        assert_eq!(read_partition.rows.len(), 1);
        assert_eq!(read_partition.rows[0].clustering, b"ck1");
        assert_eq!(
            read_partition.rows[0].primary_key_liveness.timestamp,
            1_000_042
        );
        assert_eq!(
            read_partition.rows[0].cells[0].1.value.as_deref(),
            Some(b"world".as_slice())
        );
    }

    #[test]
    fn empty_sstable() {
        let header = test_header();
        let options = WriteOptions::default();

        let writer = SSTableWriter::new(header, options, 0);
        let written = writer.finish().unwrap();

        // All buffers should be non-empty (at minimum they contain headers/metadata).
        assert!(!written.statistics.is_empty());
        assert!(!written.filter.is_empty());
        assert!(!written.toc.is_empty());
        // Data.db should be empty (no partitions written).
        assert!(written.data.is_empty());
    }

    #[test]
    fn multiple_partitions_round_trip() {
        let header = test_header();
        let options = WriteOptions {
            compression: None,
            bloom_fp_chance: 0.01,
            ..WriteOptions::default()
        };

        // Create 3 partitions and sort them by their encoded keys
        // (byte-comparable encoding sorts by token order).
        let keys: Vec<&[u8]> = vec![b"alpha", b"beta", b"gamma"];

        // Build partitions with their decorated keys.
        let mut entries: Vec<(DecoratedKey, Vec<u8>, Partition)> = keys
            .iter()
            .enumerate()
            .map(|(i, &key)| {
                let dk = DecoratedKey::new(PartitionKey::from(key));
                let encoded = crate::byte_comparable::encode(&dk);
                let value = format!("value_{i}");
                let ts = 1_000_000 + i as i64;
                let partition = make_partition(key, b"ck", value.as_bytes(), ts);
                (dk, encoded, partition)
            })
            .collect();

        // Sort by encoded key (token order) — required by trie builder.
        entries.sort_by(|a, b| a.1.cmp(&b.1));

        let mut writer = SSTableWriter::new(header.clone(), options, entries.len());

        for (dk, encoded_key, partition) in &entries {
            let (h1, h2) = dk.filter_hash();
            writer
                .add_partition(encoded_key, dk.key.as_bytes(), partition, h1, h2)
                .unwrap();
        }

        let written = writer.finish().unwrap();

        // Open with SSTableReader.
        let components = SSTableComponents {
            data: written.data,
            partitions: written.partitions,
            rows: None,
            filter: written.filter,
            compression_info: None,
            statistics: written.statistics,
        };

        let reader = SSTableReader::open(components).unwrap();
        assert_eq!(reader.key_count(), 3);

        // Look up each partition.
        for (dk, encoded_key, original) in &entries {
            let read_partition = reader
                .get_partition(encoded_key, None)
                .unwrap()
                .expect("partition should exist");

            assert_eq!(read_partition.key.key.as_bytes(), dk.key.as_bytes());
            assert!(read_partition.deletion.is_live());
            assert_eq!(read_partition.rows.len(), 1);
            assert_eq!(
                read_partition.rows[0].cells[0].1.value,
                original.rows[0].cells[0].1.value
            );
        }

        // Verify a missing key returns None.
        let missing_dk = DecoratedKey::new(PartitionKey::from(b"missing".as_slice()));
        let missing_encoded = crate::byte_comparable::encode(&missing_dk);
        assert!(reader
            .get_partition(&missing_encoded, None)
            .unwrap()
            .is_none());
    }
}
