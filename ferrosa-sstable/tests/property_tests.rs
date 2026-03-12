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
