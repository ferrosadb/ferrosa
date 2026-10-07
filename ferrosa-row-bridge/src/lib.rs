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
pub struct RowBridgeError(pub String, Option<JsonbFault>);

/// Why a stored jsonb cell was refused on read (T-151, FM-07, FM-90). The two
/// variants are distinct on purpose: an unknown envelope byte is most likely a
/// newer codec version than this node understands (mixed-version cluster), not
/// bit rot, and an operator responds to it differently. Both fail the read;
/// neither is ever presented as NULL, text or a blob.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonbFault {
    /// The cell is malformed or not canonical. `reason` is the validator's
    /// message and `len` the cell length in bytes.
    CorruptJsonb { reason: String, len: usize },
    /// The first byte is not a known envelope version.
    UnknownEnvelope { byte: u8, len: usize },
}

impl std::fmt::Display for JsonbFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JsonbFault::CorruptJsonb { reason, len } => {
                write!(f, "corrupt jsonb cell ({len} bytes): {reason}")
            }
            JsonbFault::UnknownEnvelope { byte, len } => write!(
                f,
                "jsonb cell ({len} bytes) has unknown envelope byte {byte:#04x}: \
                 written by a newer codec version?"
            ),
        }
    }
}

/// An error that may carry a typed jsonb fault, so a caller that wraps it in a
/// [`RowDecodeError`] can keep the fault instead of flattening it to text.
pub trait HasJsonbFault {
    /// The typed fault, if a jsonb cell caused the error.
    fn fault(&self) -> Option<&JsonbFault>;
}

impl HasJsonbFault for RowBridgeError {
    fn fault(&self) -> Option<&JsonbFault> {
        self.jsonb_fault()
    }
}

impl HasJsonbFault for collection::AssembleError {
    fn fault(&self) -> Option<&JsonbFault> {
        self.jsonb.as_ref()
    }
}

static CORRUPT_JSONB_TOTAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Total jsonb cells refused on read since process start (M10). Monotonic;
/// intended for a metrics exporter.
pub fn corrupt_jsonb_count() -> u64 {
    CORRUPT_JSONB_TOTAL.load(std::sync::atomic::Ordering::Relaxed)
}

impl RowBridgeError {
    /// Construct an invalid-input error with the given message. Mirrors the
    /// `CqlError::Invalid(msg)` construction in the original code.
    pub fn invalid(msg: impl Into<String>) -> Self {
        RowBridgeError(msg.into(), None)
    }

    /// A refused jsonb cell. Counts the refusal (M10) and logs it, since the
    /// read that hits it fails.
    pub fn jsonb(fault: JsonbFault) -> Self {
        CORRUPT_JSONB_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::error!(%fault, "refusing jsonb cell");
        RowBridgeError(fault.to_string(), Some(fault))
    }

    /// The typed jsonb fault, when this error came from a jsonb cell.
    pub fn jsonb_fault(&self) -> Option<&JsonbFault> {
        self.1.as_ref()
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

/// A stored cell (partition key, clustering key, or column cell) that cannot be
/// decoded. Reads fail with this error rather than presenting the value as NULL
/// (FM `RB-Tcf7ca2cc`): a corrupt value handed to a client as "missing" is
/// silent data loss.
///
/// The bridge knows the column and partition key; the caller knows the table
/// and attaches it with [`RowDecodeError::in_table`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowDecodeError {
    table: Option<String>,
    column: String,
    partition_key: Vec<u8>,
    reason: String,
    /// Boxed to keep `Result<_, RowDecodeError>` small (clippy `result_large_err`).
    jsonb: Option<Box<JsonbFault>>,
}

impl RowDecodeError {
    /// Build an error for `column` in the partition identified by `partition_key`.
    pub fn new(column: impl Into<String>, partition_key: &[u8], reason: impl Into<String>) -> Self {
        Self {
            table: None,
            column: column.into(),
            partition_key: partition_key.to_vec(),
            reason: reason.into(),
            jsonb: None,
        }
    }

    /// Attach the typed jsonb fault behind this error (`None` leaves it unset).
    #[must_use]
    pub fn with_jsonb_fault(mut self, fault: Option<JsonbFault>) -> Self {
        self.jsonb = fault.map(Box::new);
        self
    }

    /// The typed jsonb fault, when the failing cell was jsonb (T-151).
    pub fn jsonb_fault(&self) -> Option<&JsonbFault> {
        self.jsonb.as_deref()
    }

    /// Attach the table name (`keyspace.table`) if none is set yet. The
    /// innermost caller that knows the table wins.
    #[must_use]
    pub fn in_table(mut self, table: impl Into<String>) -> Self {
        if self.table.is_none() {
            self.table = Some(table.into());
        }
        self
    }

    /// The table this error was raised in, if a caller attached it.
    pub fn table(&self) -> Option<&str> {
        self.table.as_deref()
    }

    /// The column whose stored value could not be decoded.
    pub fn column(&self) -> &str {
        &self.column
    }

    /// Raw partition key bytes of the affected partition.
    pub fn partition_key(&self) -> &[u8] {
        &self.partition_key
    }

    /// Why decoding failed.
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

impl std::fmt::Display for RowDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "corrupt cell")?;
        if let Some(table) = &self.table {
            write!(f, " in table {table}")?;
        }
        write!(f, ", column {}, partition key 0x", self.column)?;
        for b in &self.partition_key {
            write!(f, "{b:02x}")?;
        }
        write!(f, ": {}", self.reason)
    }
}

impl std::error::Error for RowDecodeError {}

pub use codec::{decode_value, encode_value, parse_cql_type, parse_cql_type_in_keyspace};
pub use row::{
    build_decorated_key, build_delete_row, build_row, column_projection,
    consume_partition_rows_with_clustering, decode_clustering, decode_pk, encode_clustering,
    partition_to_rows, partition_to_rows_with_clustering, partition_to_rows_with_storage_mapping,
    projected_column_raw_encodable, visit_partition_rows_with_clustering,
    write_partition_raw_rows_projected, write_partition_raw_rows_with_storage_mapping,
};

// Liveness helpers are re-exported for `ferrosa-cql`'s remaining metadata
// decomposition variants, which still live in `ferrosa-cql` but reuse these.
pub use row::{cell_is_live, ldt_is_expired, overlay_static_cells};

// Collection (CRDT per-element) cell encoding/assembly. Lives here (not in
// `ferrosa-cql`) because both the write builder and the read assembly use this
// crate's `encode_value`/`decode_value` codecs and `ferrosa_common::reconcile`,
// and the primary SELECT read path (`row::decode_output_row`) must call the
// assembly directly. `ferrosa-cql::collection_cells` re-exports these.
pub use collection::{
    assemble_collection, assemble_column_cells, build_collection_cells, corrupt_element_count,
    list_cell_path, timeuuid_time, AssembleError, CollectionOp, UnsupportedCollectionOp,
};
