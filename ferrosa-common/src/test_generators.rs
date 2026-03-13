//! Shared proptest generators for ferrosa types.
//!
//! Enabled by the `test-generators` feature. These produce arbitrary
//! [`CellValue`], [`DecoratedKey`], and [`PartitionKey`] values for
//! property-based testing across crates.
//!
//! Generators for [`Row`], [`Partition`], etc. live in consuming crates
//! (e.g., `ferrosa-storage`) because they depend on `ferrosa-sstable` types.

use proptest::prelude::*;

use crate::cell::CellValue;
use crate::key::{DecoratedKey, PartitionKey};

/// Arbitrary cell value: live, tombstone, or expiring (with TTL).
pub fn arb_cell_value() -> impl Strategy<Value = CellValue> {
    prop_oneof![
        // Live cell with arbitrary bytes
        (prop::collection::vec(any::<u8>(), 0..1024), 1i64..1_000_000)
            .prop_map(|(v, ts)| CellValue::live(v, ts)),
        // Tombstone
        (1i64..1_000_000, 1_700_000_000i32..1_700_100_000)
            .prop_map(|(ts, ldt)| CellValue::tombstone(ts, ldt)),
        // Expiring cell with TTL
        (
            prop::collection::vec(any::<u8>(), 0..256),
            1i64..1_000_000,
            1i32..86400,
            1_700_000_000i32..1_700_100_000,
        )
            .prop_map(|(v, ts, ttl, ldt)| CellValue::expiring(v, ts, ttl, ldt)),
    ]
}

/// Arbitrary cell: (column_index, CellValue) pair.
pub fn arb_cell() -> impl Strategy<Value = (u16, CellValue)> {
    (0u16..64, arb_cell_value())
}

/// Arbitrary partition key (1-128 random bytes).
pub fn arb_partition_key() -> impl Strategy<Value = PartitionKey> {
    prop::collection::vec(any::<u8>(), 1..128).prop_map(PartitionKey::new)
}

/// Arbitrary decorated key (partition key + auto-computed Murmur3 token).
pub fn arb_decorated_key() -> impl Strategy<Value = DecoratedKey> {
    arb_partition_key().prop_map(DecoratedKey::new)
}
