//! Neutral storage-row bridge shared by `ferrosa-cql` and `ferrosa-postgres`.
//!
//! This crate holds the storage-row decode/codec and `Partition` -> row
//! decomposition logic that *both* front-ends must agree on byte-for-byte.
//! Originally these functions lived in `ferrosa-cql` (`bridge.rs` / `types.rs`).
//! The Postgres front-end reuses the *exact same* decomposition so that its
//! column ordering matches the CQL read path — duplicating the logic would risk
//! silently-divergent row ordering (the top FMEA risk for the SQL front-end).
//!
//! To let `ferrosa-postgres` reuse it **without** depending on the ~54k-LOC
//! `ferrosa-cql` crate (decision D10), the closure was extracted here. This
//! crate depends only on `ferrosa-common`, `ferrosa-sstable`, `ferrosa-schema`,
//! `num-bigint`, `uuid`, and `tracing` — never on `ferrosa-cql`.
//!
//! `ferrosa-cql` re-exports these functions at their original public paths
//! (`ferrosa_cql::types::{encode_value, decode_value}`,
//! `ferrosa_cql::bridge::{partition_to_rows_with_storage_mapping, ...}`), so its
//! internal callers are unaffected.
//!
//! ## Modules
//! - [`codec`] — CQL wire-format `encode_value` / `decode_value` plus the CQL
//!   type-name parser (`parse_cql_type` / `parse_cql_type_in_keyspace`).
//! - [`row`] — `Partition` -> row decomposition
//!   (`partition_to_rows_with_storage_mapping` and friends) and the partition /
//!   clustering key decoders.

pub mod codec;
pub mod collection;
pub mod row;

/// Error returned by the fallible row-bridge functions (`decode_value`,
/// `parse_cql_type`, `parse_cql_type_in_keyspace`).
///
/// The original code returned `ferrosa_cql::error::CqlError::Invalid(String)`;
/// every failure path in the moved closure used exactly that single variant.
/// This crate carries a minimal stand-in so it does not depend on `ferrosa-cql`.
/// `ferrosa-cql` provides `impl From<RowBridgeError> for CqlError` at its
/// re-export boundary, mapping it back to `CqlError::Invalid` so callers see the
/// identical error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowBridgeError(pub String);

impl RowBridgeError {
    /// Construct an invalid-input error with the given message. Mirrors the
    /// `CqlError::Invalid(msg)` construction in the original code.
    pub fn invalid(msg: impl Into<String>) -> Self {
        RowBridgeError(msg.into())
    }

    /// The error message (the inner `String`).
    pub fn message(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for RowBridgeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for RowBridgeError {}

pub use codec::{decode_value, encode_value, parse_cql_type, parse_cql_type_in_keyspace};
pub use row::{
    build_decorated_key, build_delete_row, build_row, consume_partition_rows_with_clustering,
    decode_clustering, decode_pk, encode_clustering, partition_to_rows,
    partition_to_rows_with_clustering, partition_to_rows_with_storage_mapping,
    visit_partition_rows_with_clustering, write_partition_raw_rows_with_storage_mapping,
};

// Liveness helpers are re-exported for `ferrosa-cql`'s remaining metadata
// decomposition variants, which still live in `ferrosa-cql` but reuse these.
pub use row::{cell_is_live, ldt_is_expired};

// Collection (CRDT per-element) cell encoding/assembly. Lives here (not in
// `ferrosa-cql`) because both the write builder and the read assembly use this
// crate's `encode_value`/`decode_value` codecs and `ferrosa_common::reconcile`,
// and the primary SELECT read path (`row::decode_output_row`) must call the
// assembly directly. `ferrosa-cql::collection_cells` re-exports these.
pub use collection::{
    assemble_collection, assemble_column_cells, build_collection_cells, list_cell_path,
    timeuuid_time, AssembleError, CollectionOp, UnsupportedCollectionOp,
};
