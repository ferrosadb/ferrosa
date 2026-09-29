//! Shared proptest generators for ferrosa types.
//!
//! Enabled by the `test-generators` feature. These produce arbitrary
//! [`CellValue`], [`DecoratedKey`], and [`PartitionKey`] values for
//! property-based testing across crates.
//!
//! Generators for `Row`, `Partition`, etc. live in consuming crates
//! (e.g., `ferrosa-storage`) because they depend on `ferrosa-sstable` types.

use proptest::prelude::*;

use crate::cell::CellValue;
use crate::key::{DecoratedKey, PartitionKey};

/// Shrink-friendly table name for generated DDL/snapshot histories.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GeneratedTableName {
    pub keyspace: String,
    pub table: String,
}

/// Shrink-friendly table identity for generated DDL/snapshot histories.
///
/// `generation` stands in for the table UUID/generation the snapshot-drop ADR
/// requires before destructive cleanup may run. Name alone is not enough:
/// `DROP TABLE k.t; CREATE TABLE k.t` must not let an old drop marker delete the
/// recreated table's artifacts.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GeneratedTableIdentity {
    pub name: GeneratedTableName,
    pub generation: u64,
}

/// Explicit destructive marker carried by generated snapshot histories.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GeneratedDropMarker {
    pub identity: GeneratedTableIdentity,
    pub drop_log_index: u64,
}

/// Generated secondary-index declaration tied to a table identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GeneratedIndexDeclaration {
    pub table: GeneratedTableIdentity,
    pub index: String,
}

/// Small, readable keyspace names for generated DDL/snapshot tests.
///
/// The set intentionally includes non-`system` application keyspaces so tests
/// can reach the incident class, plus system-prefixed names to keep the current
/// name-filter behavior visible.
pub fn arb_ddl_keyspace() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("agent_memory".to_string()),
        Just("agent_memory_test".to_string()),
        Just("app".to_string()),
        Just("system_schema".to_string()),
        Just("system_auth".to_string()),
    ]
}

/// Small, readable table names for generated DDL/snapshot tests.
pub fn arb_ddl_table_name() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("entity_store".to_string()),
        Just("document_chunks".to_string()),
        Just("co_occurs_with".to_string()),
        Just("t".to_string()),
    ]
}

/// Generated table names, biased toward realistic exposed application tables.
pub fn arb_generated_table_name() -> impl Strategy<Value = GeneratedTableName> {
    (arb_ddl_keyspace(), arb_ddl_table_name())
        .prop_map(|(keyspace, table)| GeneratedTableName { keyspace, table })
}

/// Generated table identities, with a small generation range for good shrinking.
pub fn arb_generated_table_identity() -> impl Strategy<Value = GeneratedTableIdentity> {
    (arb_generated_table_name(), 0u64..8)
        .prop_map(|(name, generation)| GeneratedTableIdentity { name, generation })
}

/// Generated explicit drop markers, including the drop log index used for
/// snapshot log-order checks.
pub fn arb_generated_drop_marker() -> impl Strategy<Value = GeneratedDropMarker> {
    (arb_generated_table_identity(), 0u64..16).prop_map(|(identity, drop_log_index)| {
        GeneratedDropMarker {
            identity,
            drop_log_index,
        }
    })
}

/// Generated index declarations tied to table identities.
pub fn arb_generated_index_declaration() -> impl Strategy<Value = GeneratedIndexDeclaration> {
    (
        arb_generated_table_identity(),
        prop_oneof![
            Just("idx_by_id".to_string()),
            Just("idx_by_text".to_string()),
            Just("idx_by_target".to_string()),
        ],
    )
        .prop_map(|(table, index)| GeneratedIndexDeclaration { table, index })
}

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
