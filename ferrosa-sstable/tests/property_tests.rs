//! Property-based tests for ferrosa-sstable leaf components.

use ferrosa_common::{DecoratedKey, PartitionKey, Token};
use ferrosa_sstable::{bloom::BloomFilter, byte_comparable, compression::Compression, varint};
use proptest::prelude::*;

proptest! {
    /// Unsigned varint round-trip: decode(encode(n)) == n.
    #[test]
    fn varint_unsigned_round_trip(value: u64) {
        // VInt only supports up to i64::MAX
        let value = value % (i64::MAX as u64 + 1);
        let mut buf = [0u8; 9];
        let n = varint::write_unsigned_vint(&mut buf, value);
        let (decoded, consumed) = varint::read_unsigned_vint(&buf[..n]).unwrap();
        prop_assert_eq!(decoded, value);
        prop_assert_eq!(consumed, n);
    }

    /// Signed varint round-trip: decode(encode(n)) == n.
    #[test]
    fn varint_signed_round_trip(value: i64) {
        let mut buf = [0u8; 9];
        let n = varint::write_signed_vint(&mut buf, value);
        let (decoded, consumed) = varint::read_signed_vint(&buf[..n]).unwrap();
        prop_assert_eq!(decoded, value);
        prop_assert_eq!(consumed, n);
    }

    /// LZ4 compression round-trip: decompress(compress(data)) == data.
    #[test]
    fn lz4_round_trip(data: Vec<u8>) {
        let comp = Compression::Lz4;
        let compressed = comp.compress(&data).unwrap();
        let decompressed = comp.decompress(&compressed, data.len()).unwrap();
        prop_assert_eq!(decompressed, data);
    }

    /// Zstd compression round-trip: decompress(compress(data)) == data.
    #[test]
    fn zstd_round_trip(data: Vec<u8>) {
        let comp = Compression::Zstd { level: 1 };
        let compressed = comp.compress(&data).unwrap();
        let decompressed = comp.decompress(&compressed, data.len()).unwrap();
        prop_assert_eq!(decompressed, data);
    }

    /// Bloom filter: no false negatives. All inserted keys are found.
    #[test]
    fn bloom_no_false_negatives(keys in prop::collection::vec(any::<u64>(), 1..100)) {
        let mut bf = BloomFilter::new(keys.len(), 0.01);
        let hashes: Vec<(i64, i64)> = keys
            .iter()
            .map(|k| ferrosa_common::murmur3::hash3_x64_128(&k.to_le_bytes(), 0))
            .collect();

        for &(h1, h2) in &hashes {
            bf.add(h1, h2);
        }

        for &(h1, h2) in &hashes {
            prop_assert!(bf.is_present(h1, h2), "false negative in bloom filter");
        }
    }

    /// Byte-comparable round-trip: decode(encode(key)) == key.
    #[test]
    fn byte_comparable_round_trip(
        token: i64,
        key_bytes in prop::collection::vec(any::<u8>(), 0..256),
    ) {
        let dk = DecoratedKey {
            token: Token(token),
            key: PartitionKey::new(key_bytes.clone()),
        };
        let encoded = byte_comparable::encode(&dk);
        let decoded = byte_comparable::decode(&encoded).unwrap();
        prop_assert_eq!(decoded.token, dk.token);
        prop_assert_eq!(decoded.key.as_bytes(), key_bytes.as_slice());
    }

    /// Byte-comparable encoding preserves token ordering.
    #[test]
    fn byte_comparable_ordering(
        t1: i64,
        t2: i64,
    ) {
        let key = b"k";
        let e1 = byte_comparable::encode(&DecoratedKey {
            token: Token(t1),
            key: PartitionKey::from(key.as_slice()),
        });
        let e2 = byte_comparable::encode(&DecoratedKey {
            token: Token(t2),
            key: PartitionKey::from(key.as_slice()),
        });

        // Same key, so ordering should follow token ordering
        prop_assert_eq!(e1.cmp(&e2), t1.cmp(&t2));
    }
}

// ── SSTable cell write-read roundtrip fuzzing ───────────────────────────

mod cell_roundtrip {
    use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
    use ferrosa_sstable::data::DataReader;
    use ferrosa_sstable::statistics::SerializationHeader;
    use ferrosa_sstable::types::*;
    use ferrosa_sstable::writer::{SSTableWriter, WriteOptions};
    use proptest::prelude::*;

    fn make_header(min_ts: i64, min_ldt: i32, min_ttl: i32) -> SerializationHeader {
        SerializationHeader {
            min_timestamp: min_ts,
            min_local_deletion_time: min_ldt,
            min_ttl,
            max_timestamp: i64::MAX,
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            clustering_types: vec![],
            static_columns: vec![],
            regular_columns: vec![(
                b"v".to_vec(),
                "org.apache.cassandra.db.marshal.UTF8Type".into(),
            )],
        }
    }

    fn write_options() -> WriteOptions {
        WriteOptions {
            compression: None,
            bloom_fp_chance: 0.01,
            chunk_size: 65536,
            verify_output: true,
        }
    }

    /// Strategy: generate a live cell with random value and timestamp.
    #[allow(dead_code)]
    fn arb_live_cell() -> impl Strategy<Value = (Vec<u8>, i64)> {
        (
            prop::collection::vec(any::<u8>(), 0..256),
            1_000_000i64..2_000_000i64,
        )
    }

    /// Strategy: generate an expiring cell with random value, timestamp, TTL, and LDT.
    #[allow(dead_code)]
    fn arb_expiring_cell() -> impl Strategy<Value = (Vec<u8>, i64, i32, i32)> {
        (
            prop::collection::vec(any::<u8>(), 0..256),
            1_000_000i64..2_000_000i64,
            1i32..86400i32,             // TTL: 1 second to 1 day
            1_700_000i32..1_800_000i32, // LDT
        )
    }

    proptest! {
        /// Live cell roundtrip: write any value + timestamp, read back exact match.
        #[test]
        fn live_cell_roundtrip(value in prop::collection::vec(any::<u8>(), 0..512), ts in 1_000_000i64..2_000_000i64) {
            let header = make_header(1_000_000, i32::MAX, 0);
            let partition = Partition {
                key: DecoratedKey::new(PartitionKey::from(b"k".as_slice())),
                deletion: DeletionTime::LIVE,
                static_row: None,
                rows: vec![Row {
                    clustering: vec![],
                    cells: vec![(0, CellValue::live(value.clone(), ts))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(ts),
                }],
            };

            let mut writer = SSTableWriter::new(write_options(), header.clone());
            writer.add_partition(&partition).unwrap();
            let output = writer.finish().unwrap();

            let mut reader = DataReader::new(&output.data, &header, 0);
            let p = reader.read_partition().unwrap().expect("partition");
            let cell = &p.rows[0].cells[0].1;

            prop_assert!(!cell.is_tombstone());
            prop_assert_eq!(cell.timestamp, ts);
            prop_assert_eq!(cell.value.as_deref(), Some(value.as_slice()));
            prop_assert_eq!(cell.ttl, ferrosa_common::NO_TTL);
        }

        /// Tombstone cell roundtrip: write tombstone with any timestamp + LDT.
        #[test]
        fn tombstone_cell_roundtrip(ts in 1_000_000i64..2_000_000i64, ldt in 0i32..2_000_000i32) {
            let header = make_header(1_000_000, 0, 0);
            let partition = Partition {
                key: DecoratedKey::new(PartitionKey::from(b"k".as_slice())),
                deletion: DeletionTime::LIVE,
                static_row: None,
                rows: vec![Row {
                    clustering: vec![],
                    cells: vec![(0, CellValue::tombstone(ts, ldt))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(ts),
                }],
            };

            let mut writer = SSTableWriter::new(write_options(), header.clone());
            writer.add_partition(&partition).unwrap();
            let output = writer.finish().unwrap();

            let mut reader = DataReader::new(&output.data, &header, 0);
            let p = reader.read_partition().unwrap().expect("partition");
            let cell = &p.rows[0].cells[0].1;

            prop_assert!(cell.is_tombstone());
            prop_assert_eq!(cell.timestamp, ts);
            prop_assert_eq!(cell.local_deletion_time, ldt);
            prop_assert!(cell.value.is_none());
        }

        /// Expiring cell roundtrip: write cell with TTL, verify TTL + LDT + value survive.
        #[test]
        fn expiring_cell_roundtrip(
            value in prop::collection::vec(any::<u8>(), 0..512),
            ts in 1_000_000i64..2_000_000i64,
            ttl in 1i32..86400i32,
            ldt in 1_000_000i32..2_000_000i32,
        ) {
            let header = make_header(1_000_000, 0, 0);
            let partition = Partition {
                key: DecoratedKey::new(PartitionKey::from(b"k".as_slice())),
                deletion: DeletionTime::LIVE,
                static_row: None,
                rows: vec![Row {
                    clustering: vec![],
                    cells: vec![(0, CellValue::expiring(value.clone(), ts, ttl, ldt))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(ts),
                }],
            };

            let mut writer = SSTableWriter::new(write_options(), header.clone());
            writer.add_partition(&partition).unwrap();
            let output = writer.finish().unwrap();

            let mut reader = DataReader::new(&output.data, &header, 0);
            let p = reader.read_partition().unwrap().expect("partition");
            let cell = &p.rows[0].cells[0].1;

            prop_assert!(!cell.is_tombstone());
            prop_assert_eq!(cell.timestamp, ts);
            prop_assert_eq!(cell.ttl, ttl);
            prop_assert_eq!(cell.local_deletion_time, ldt);
            prop_assert_eq!(cell.value.as_deref(), Some(value.as_slice()));
        }

        /// Mixed partition: multiple rows with different cell types in same SSTable.
        #[test]
        fn mixed_cell_types_in_one_partition(
            live_val in prop::collection::vec(any::<u8>(), 1..64),
            ttl_val in prop::collection::vec(any::<u8>(), 1..64),
            ts in 1_000_000i64..1_500_000i64,
            ttl in 1i32..3600i32,
            ldt in 1_700_000i32..1_800_000i32,
        ) {
            let header = make_header(1_000_000, 0, 0);
            let header_with_ck = SerializationHeader {
                clustering_types: vec!["org.apache.cassandra.db.marshal.Int32Type".into()],
                ..header
            };

            let partition = Partition {
                key: DecoratedKey::new(PartitionKey::from(b"mixed".as_slice())),
                deletion: DeletionTime::LIVE,
                static_row: None,
                rows: vec![
                    Row {
                        clustering: 1i32.to_be_bytes().to_vec(),
                        cells: vec![(0, CellValue::live(live_val.clone(), ts))],
                        deletion: DeletionTime::LIVE,
                        primary_key_liveness: LivenessInfo::with_timestamp(ts),
                    },
                    Row {
                        clustering: 2i32.to_be_bytes().to_vec(),
                        cells: vec![(0, CellValue::tombstone(ts + 1, ldt))],
                        deletion: DeletionTime::LIVE,
                        primary_key_liveness: LivenessInfo::with_timestamp(ts + 1),
                    },
                    Row {
                        clustering: 3i32.to_be_bytes().to_vec(),
                        cells: vec![(0, CellValue::expiring(ttl_val.clone(), ts + 2, ttl, ldt))],
                        deletion: DeletionTime::LIVE,
                        primary_key_liveness: LivenessInfo::with_timestamp(ts + 2),
                    },
                ],
            };

            let mut writer = SSTableWriter::new(write_options(), header_with_ck.clone());
            writer.add_partition(&partition).unwrap();
            let output = writer.finish().unwrap();

            let mut reader = DataReader::new(&output.data, &header_with_ck, 0);
            let p = reader.read_partition().unwrap().expect("partition");

            prop_assert_eq!(p.rows.len(), 3);

            // Row 1: live
            let c1 = &p.rows[0].cells[0].1;
            prop_assert!(!c1.is_tombstone());
            prop_assert_eq!(c1.value.as_deref(), Some(live_val.as_slice()));
            prop_assert_eq!(c1.ttl, ferrosa_common::NO_TTL);

            // Row 2: tombstone
            let c2 = &p.rows[1].cells[0].1;
            prop_assert!(c2.is_tombstone());

            // Row 3: expiring
            let c3 = &p.rows[2].cells[0].1;
            prop_assert!(!c3.is_tombstone());
            prop_assert_eq!(c3.ttl, ttl);
            prop_assert_eq!(c3.local_deletion_time, ldt);
            prop_assert_eq!(c3.value.as_deref(), Some(ttl_val.as_slice()));
        }
    }
}

// ── SSTable reader fuzz: arbitrary bytes must never panic ────────────────
//
// The reader must handle any byte sequence without panicking. It should
// return Err for malformed data, never crash. This catches:
// - Capacity overflow from garbage cell lengths
// - Index out of bounds from truncated headers
// - Integer overflow from varint decoding
// - Infinite loops from circular structures

mod reader_fuzz {
    use ferrosa_sstable::data::DataReader;
    use ferrosa_sstable::statistics::SerializationHeader;
    use proptest::prelude::*;

    fn default_header() -> SerializationHeader {
        SerializationHeader {
            min_timestamp: 0,
            min_local_deletion_time: 0,
            min_ttl: 0,
            max_timestamp: i64::MAX,
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
            clustering_types: vec![],
            static_columns: vec![],
            regular_columns: vec![(
                b"v".to_vec(),
                "org.apache.cassandra.db.marshal.UTF8Type".into(),
            )],
        }
    }

    fn header_with_clustering() -> SerializationHeader {
        SerializationHeader {
            clustering_types: vec!["org.apache.cassandra.db.marshal.Int32Type".into()],
            ..default_header()
        }
    }

    proptest! {
        /// Completely random bytes: the reader must return Ok or Err, never panic.
        #[test]
        fn random_bytes_never_panic(data in prop::collection::vec(any::<u8>(), 0..1024)) {
            let header = default_header();
            let mut reader = DataReader::new(&data, &header, 0);
            // Read until exhausted — every call must return Ok or Err, not panic
            loop {
                match reader.read_partition() {
                    Ok(Some(_)) => continue,
                    Ok(None) => break,
                    Err(_) => break, // errors are fine, panics are not
                }
            }
        }

        /// Random bytes with clustering columns in the header.
        #[test]
        fn random_bytes_with_clustering_never_panic(data in prop::collection::vec(any::<u8>(), 0..1024)) {
            let header = header_with_clustering();
            let mut reader = DataReader::new(&data, &header, 0);
            loop {
                match reader.read_partition() {
                    Ok(Some(_)) => continue,
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
        }

        /// Valid SSTable prefix followed by garbage: must not panic.
        /// This simulates truncated writes (crash during flush).
        #[test]
        fn valid_prefix_then_garbage_never_panic(
            prefix_len in 0usize..50,
            garbage in prop::collection::vec(any::<u8>(), 0..200),
        ) {
            // Start with a valid partition header-like prefix
            let mut data = Vec::new();
            // Partition key length (2 bytes) + key bytes
            let pk = b"test_key";
            data.extend_from_slice(&(pk.len() as u16).to_be_bytes());
            data.extend_from_slice(pk);
            // Truncate to prefix_len then append garbage
            data.truncate(prefix_len);
            data.extend_from_slice(&garbage);

            let header = default_header();
            let mut reader = DataReader::new(&data, &header, 0);
            loop {
                match reader.read_partition() {
                    Ok(Some(_)) => continue,
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
        }

        /// Write valid SSTable, corrupt random byte, read back: must not panic.
        /// Errors are expected, panics are bugs.
        #[test]
        fn corrupt_single_byte_never_panic(
            value in prop::collection::vec(any::<u8>(), 1..128),
            ts in 1_000_000i64..2_000_000i64,
            corrupt_pos in any::<prop::sample::Index>(),
            corrupt_val in any::<u8>(),
        ) {
            use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
            use ferrosa_sstable::types::*;
            use ferrosa_sstable::writer::{SSTableWriter, WriteOptions};

            let header = default_header();
            let options = WriteOptions {
                compression: None,
                bloom_fp_chance: 0.01,
                chunk_size: 65536,
                verify_output: true,
            };

            let partition = Partition {
                key: DecoratedKey::new(PartitionKey::from(b"k".as_slice())),
                deletion: DeletionTime::LIVE,
                static_row: None,
                rows: vec![Row {
                    clustering: vec![],
                    cells: vec![(0, CellValue::live(value, ts))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(ts),
                }],
            };

            let mut writer = SSTableWriter::new(options, header.clone());
            writer.add_partition(&partition).unwrap();
            let output = writer.finish().unwrap();

            // Corrupt one byte
            let mut corrupted = output.data.clone();
            if !corrupted.is_empty() {
                let idx = corrupt_pos.index(corrupted.len());
                corrupted[idx] = corrupt_val;
            }

            // Read the corrupted data — must not panic
            let mut reader = DataReader::new(&corrupted, &header, 0);
            loop {
                match reader.read_partition() {
                    Ok(Some(_)) => continue,
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
        }

        /// Write valid SSTable with expiring cell, corrupt random byte: must not panic.
        #[test]
        fn corrupt_expiring_cell_never_panic(
            value in prop::collection::vec(any::<u8>(), 1..64),
            ts in 1_000_000i64..2_000_000i64,
            ttl in 1i32..86400i32,
            ldt in 1_000_000i32..2_000_000i32,
            corrupt_pos in any::<prop::sample::Index>(),
            corrupt_val in any::<u8>(),
        ) {
            use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
            use ferrosa_sstable::types::*;
            use ferrosa_sstable::writer::{SSTableWriter, WriteOptions};

            let header = SerializationHeader {
                min_timestamp: 1_000_000,
                min_local_deletion_time: 0,
                min_ttl: 0,
                ..default_header()
            };
            let options = WriteOptions {
                compression: None,
                bloom_fp_chance: 0.01,
                chunk_size: 65536,
                verify_output: true,
            };

            let partition = Partition {
                key: DecoratedKey::new(PartitionKey::from(b"k".as_slice())),
                deletion: DeletionTime::LIVE,
                static_row: None,
                rows: vec![Row {
                    clustering: vec![],
                    cells: vec![(0, CellValue::expiring(value, ts, ttl, ldt))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(ts),
                }],
            };

            let mut writer = SSTableWriter::new(options, header.clone());
            writer.add_partition(&partition).unwrap();
            let output = writer.finish().unwrap();

            let mut corrupted = output.data.clone();
            if !corrupted.is_empty() {
                let idx = corrupt_pos.index(corrupted.len());
                corrupted[idx] = corrupt_val;
            }

            let mut reader = DataReader::new(&corrupted, &header, 0);
            loop {
                match reader.read_partition() {
                    Ok(Some(_)) => continue,
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
        }
    }
}
