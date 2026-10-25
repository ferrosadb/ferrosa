//! Correctness: point reads preserve SQL equality, snapshot visibility and cluster consistency.
//! Last revised: 2026-10-24
//! Last changed: Route safe scalar key equalities through exact partition reads and MVCC lookup.
//!
//! Storage-backed table loading: bridge real ferrosa storage to the bespoke
//! relational engine's row model (`ferrosa-sql`).
//!
//! ## Why this exists (the sync/async impedance mismatch)
//!
//! The relational engine is fully **synchronous**: [`ferrosa_sql::TableProvider::scan`]
//! returns a plain iterator and [`ferrosa_sql::Catalog::resolve`] is a sync call.
//! Real ferrosa storage, by contrast, exposes its range scan as an **async**
//! `Stream<Item = Result<Partition>>` ([`ferrosa_storage::StorageEngine::range_iter`]).
//!
//! We resolve this with a **bounded hand-off**, not a materialization.
//! [`load_table`] resolves the table's metadata and decode context, then returns
//! a [`StreamingTable`] that has read no rows at all. Each call to its `scan()`
//! spawns an async producer that drains `range_iter` and pushes decoded rows
//! into an [`mpsc`] channel of capacity [`SCAN_BUFFER_ROWS`]; the sync iterator
//! on the other end pulls with `blocking_recv`. The channel is what supplies
//! backpressure: the producer may run at most `SCAN_BUFFER_ROWS` rows ahead of
//! the consumer, so the source-side peak is **one partition plus the channel**,
//! never the table.
//!
//! ### Why this is safe to block on
//!
//! `blocking_recv` would panic on an async worker, and blocking one would be the
//! deadlock this module used to avoid by materializing. It is legal here because
//! the synchronous executor no longer runs on an async worker: the `result_stream`
//! module moved `ferrosa_sql::execute` onto `spawn_blocking` (t_d3b2dec1). That
//! change is the prerequisite for this one — the sync consumer is now *allowed* to
//! block, which is exactly what a bounded channel needs.
//!
//! ### What is bounded, and what is not
//!
//! [`SCAN_BUFFER_ROWS`] is a **work bound** — how many rows may be in flight —
//! and never a result bound. Every row of the table is still delivered; the
//! channel only decides how far ahead the producer may run. A cap on rows
//! *returned* would be a different thing entirely and is not what this is.
//!
//! This bounds the **source** side. The result side is bounded too: the
//! executor's rows reach the client through `result_stream`, which delivers
//! them in bounded batches with backpressure from the socket, so an end-to-end
//! `SELECT *` holds O(batch) rows (t_f348ba0b).
//!
//! ### Re-scannable, because `scan()` is
//!
//! `TableProvider::scan(&self)` may be called more than once on the same
//! provider — a self-join (`FROM t JOIN t`) resolves both sides to one
//! `Arc`. A single-shot channel would hand the second scan an empty relation,
//! which is a wrong answer rather than a loud one. So each `scan()` spawns its
//! own producer and re-reads from storage. That trades a second pass over
//! storage for a memory bound, which is the trade this module exists to make.
//!
//! ### A storage error must not become a short result
//!
//! `scan()` returns `Iterator<Item = Row>`, which has no way to report a
//! mid-stream failure: a producer that died on a storage error would simply
//! close the channel and the query would return *fewer rows, successfully*.
//! That is the silent-truncation failure this codebase forbids. Instead the
//! producer records the error in a [`ScanFailure`] slot shared by every table in
//! the catalog, and the query layer checks that slot after `execute` returns and
//! fails the whole query loud. The rows already produced are discarded.
//!
//! ## R15 guard: missing table is NOT an empty table
//!
//! `StorageEngine::range_iter` returns an *empty stream* for an unregistered
//! table (`None => stream::empty()`). That is indistinguishable from a registered
//! but empty table at the stream level. To avoid silently scanning "nothing" for
//! a name that does not exist, [`load_table`] checks the **schema metadata** first
//! and fails loud with [`LoadError::NoSuchTable`] when the table is absent — a
//! distinct outcome from `Ok` with zero rows for an existing empty table.
//!
//! ## Column ordering (mirrors the CQL SELECT read path)
//!
//! Partition decomposition is delegated to
//! [`ferrosa_row_bridge::partition_to_rows_with_storage_mapping`] — the *same*
//! helper the CQL `route_select` path uses (ferrosa-cql re-exports it as
//! `ferrosa_cql::bridge::partition_to_rows_with_storage_mapping`; see
//! `ferrosa-cql/src/router.rs`, `decode_agreed_row_to_map` /
//! `route_select_user_table`). Output rows follow the table's **declared column
//! order** (`TableMetadata.columns`, an `IndexMap`): partition-key,
//! clustering-key, and regular/static columns in their DDL order. We do not
//! invent an ordering — reusing the shared bridge guarantees parity with CQL
//! while keeping ferrosa-postgres free of any ferrosa-cql dependency (D10).
//!
//! ## [`CqlValue`] -> [`ferrosa_sql::Value`] conversion
//!
//! The [`ferrosa_sql::Value`] model covers `Null | Int(i64) | Text | Bool |
//! Float(f64) | Uuid | Bytea | Timestamp(i64 micros) | Date(i32 days) |
//! Time(i64 micros) | Inet(IpAddr) | Numeric{unscaled,scale}`. [`cql_to_value`]
//! maps the integral / textual / boolean / floating-point CQL scalars onto it
//! losslessly (f32 widens to f64); `uuid`/`timeuuid` → `Value::Uuid`, `blob` →
//! `Value::Bytea`; the temporal (`timestamp`/`date`/`time`), network (`inet`),
//! and arbitrary-precision (`decimal`/`varint`) scalars now map through to their
//! exact-Postgres-text engine variants. Unsupported composites and durations
//! return a conversion error; scan callers propagate it as a query error rather
//! than showing a non-NULL stored value as SQL `NULL`.
//!
//! - Temporal: `Duration` (no clean Postgres-scalar mapping)
//! - Collections / composites: `List`, `Set`, `Map`, `Tuple`, `Udt`, `Vector`

use std::fmt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};

use ferrosa_common::{CqlType, CqlValue};
use ferrosa_schema::{ColumnKind, Schema, TableMetadata};
use ferrosa_sql::{Column, RelSchema, Row, TableProvider, Value};
use ferrosa_storage::{StorageEngine, TableId};
use futures::{FutureExt, StreamExt};
use tokio::runtime::Handle;
use tokio::sync::mpsc;

use crate::cluster_read::{read_consistency, PartitionStream, RangeRead};

/// Convert a ferrosa [`CqlValue`] to [`ferrosa_sql::Value`]. Unsupported values
/// return an error so callers cannot turn real data into fabricated SQL NULLs.
pub fn cql_to_value(v: &CqlValue) -> Result<Value, String> {
    Ok(match v {
        CqlValue::Null => Value::Null,
        // Integral types widen into i64 losslessly.
        CqlValue::Int(i) => Value::Int(i64::from(*i)),
        CqlValue::Bigint(i) | CqlValue::Counter(i) => Value::Int(*i),
        CqlValue::Smallint(i) => Value::Int(i64::from(*i)),
        CqlValue::Tinyint(i) => Value::Int(i64::from(*i)),
        // Textual types.
        CqlValue::Text(s) | CqlValue::Ascii(s) => Value::Text(s.clone()),
        // Boolean.
        CqlValue::Boolean(b) => Value::Bool(*b),
        // Floating point: `Float`/`Double` carry IEEE-754 bit patterns (so the
        // CQL value type can be Eq/Ord). Reconstruct the float and widen f32→f64.
        CqlValue::Float(bits) => Value::float(f32::from_bits(*bits) as f64),
        CqlValue::Double(bits) => Value::float(f64::from_bits(*bits)),
        // Identifiers / binary: `uuid` and `timeuuid` carry a `uuid::Uuid`
        // (Postgres uuid, OID 2950); `blob` carries raw bytes (Postgres bytea,
        // OID 17). Both have exact, unambiguous Postgres text representations.
        CqlValue::Uuid(u) | CqlValue::Timeuuid(u) => Value::Uuid(*u),
        CqlValue::Blob(bytes) => Value::Bytea(bytes.clone()),
        // ── Temporal / network / arbitrary-precision (exact Postgres text) ──
        // `Timestamp` carries i64 MILLIS since the Unix epoch; the engine's
        // `Value::Timestamp` is MICROS, so widen by 1000.
        CqlValue::Timestamp(ms) => Value::Timestamp(
            ms.checked_mul(1000)
                .ok_or_else(|| "CQL timestamp is outside PostgreSQL timestamp range".to_string())?,
        ),
        // `Date` carries u32 days centered at 2^31 (CQL epoch encoding); the
        // engine's `Value::Date` is signed days since the Unix epoch.
        CqlValue::Date(d) => Value::Date((i64::from(*d) - 2_147_483_648) as i32),
        // `Time` carries i64 NANOS since midnight; `Value::Time` is MICROS.
        CqlValue::Time(nanos) => Value::Time(nanos / 1000),
        // `Inet` carries an `IpAddr` directly.
        CqlValue::Inet(ip) => Value::Inet(*ip),
        // `Decimal` ⇒ normalized `Value::Numeric`. `Varint` is an integer ⇒ a
        // numeric with scale 0.
        CqlValue::Decimal { scale, unscaled } => Value::numeric(unscaled.clone(), *scale),
        CqlValue::Varint(b) => Value::numeric(b.clone(), 0),
        // These values have no lossless SQL scalar representation here. Fail
        // the query instead of returning NULL for a non-NULL stored value.
        CqlValue::Duration { .. }
        | CqlValue::List(_)
        | CqlValue::Set(_)
        | CqlValue::Map(_)
        | CqlValue::Tuple(_)
        | CqlValue::Udt(_)
        | CqlValue::Vector(_) => {
            return Err(format!("unsupported CQL value for PostgreSQL: {v:?}"))
        }
        // The validated cell moves across unchanged (T-160): no re-parse, no
        // text detour, so a scale variant like `1.0` keeps its bytes.
        CqlValue::Jsonb(doc) => Value::Jsonb(doc.clone()),
    })
}

/// Convert one owned decoded storage row to the SQL executor's positional values.
fn decoded_row_values(row: Vec<Option<CqlValue>>) -> Result<Vec<Value>, String> {
    row.into_iter()
        .map(|cell| match cell {
            Some(CqlValue::Text(text) | CqlValue::Ascii(text)) => Ok(Value::Text(text)),
            Some(CqlValue::Blob(bytes)) => Ok(Value::Bytea(bytes)),
            Some(CqlValue::Decimal { scale, unscaled }) => Ok(Value::numeric(unscaled, scale)),
            Some(CqlValue::Varint(unscaled)) => Ok(Value::numeric(unscaled, 0)),
            Some(CqlValue::Jsonb(doc)) => Ok(Value::Jsonb(doc)),
            Some(value) => cql_to_value(&value),
            None => Ok(Value::Null),
        })
        .collect()
}

/// Failure modes of [`load_table`].
///
/// [`LoadError::NoSuchTable`] is deliberately distinct from a successful load of
/// an existing-but-empty table (the R15 guard): the former is an error, the
/// latter is `Ok` with zero rows.
#[derive(Debug)]
pub enum LoadError {
    /// The `keyspace.table` is not present in the schema snapshot. The loader
    /// refuses to substitute an empty relation for a missing table.
    NoSuchTable { keyspace: String, table: String },
    /// A storage / decode error surfaced while materializing the scan.
    Storage(String),
}

impl fmt::Display for LoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LoadError::NoSuchTable { keyspace, table } => {
                write!(f, "no such table: {keyspace}.{table}")
            }
            LoadError::Storage(msg) => write!(f, "storage error loading table: {msg}"),
        }
    }
}

impl std::error::Error for LoadError {}

/// Build the engine [`RelSchema`] from a table's column names and resolved types,
/// in declared order.
///
/// Each column's engine type comes from [`crate::pg_types::pg_type_of`], the same
/// map the catalog and RowDescription use, so the three cannot drift.
fn rel_schema_for(col_names: &[String], col_types: &[CqlType]) -> RelSchema {
    debug_assert_eq!(col_names.len(), col_types.len());
    let columns = col_names
        .iter()
        .zip(col_types)
        .map(|(name, ty)| Column::new(name.clone(), crate::pg_types::pg_type_of(ty).column_type))
        .collect();
    RelSchema::new(columns)
}

/// Column indices (into the declared-order column list) that form the partition
/// key, in partition-key order. Mirrors `router::decode_agreed_row_to_map`.
fn pk_indices(meta: &TableMetadata) -> Vec<usize> {
    meta.partition_key
        .iter()
        .filter_map(|name| meta.columns.get_index_of(name))
        .collect()
}

/// Column indices that form the clustering key, in clustering order. Mirrors
/// `router::decode_agreed_row_to_map`.
fn ck_indices(meta: &TableMetadata) -> Vec<usize> {
    meta.clustering_key
        .iter()
        .filter_map(|(name, _)| meta.columns.get_index_of(name))
        .collect()
}

/// Storage-cell-ordinal -> declared-column-index map for regular/static columns.
///
/// A re-implementation of the (private) `router::storage_to_table_indices`: a
/// storage `Row`'s cells carry a `u16` ordinal in the SSTable's static+regular
/// column space; this turns that ordinal into the column's position in the
/// table's declared order so the bridge can place each cell correctly.
fn storage_to_table_indices(meta: &TableMetadata) -> Vec<usize> {
    let mut pairs: Vec<(u16, usize)> = meta
        .columns
        .iter()
        .filter(|(_, col)| matches!(col.kind, ColumnKind::Regular | ColumnKind::Static))
        .filter_map(|(name, _)| {
            let storage_idx = meta.storage_column_index(name)?;
            let table_idx = meta.columns.get_index_of(name)?;
            Some((storage_idx, table_idx))
        })
        .collect();
    pairs.sort_by_key(|(storage_idx, _)| *storage_idx);
    pairs.into_iter().map(|(_, table_idx)| table_idx).collect()
}

/// The per-table decode projections a row image needs — computed **once** per
/// table for a commit, not once per row.
///
/// `read_row_image` used to rebuild every one of these on each call: the
/// declared column names, the per-column CQL types (each a keyspace-scoped
/// parse), the partition/clustering key indices, and the storage-ordinal ->
/// declared-index map. A transactional `COPY` calls it once per row, so a
/// 1.1M-row load rebuilt data that cannot change within one COMMIT ~1.1M times —
/// an O(rows x columns) cost where O(columns) suffices. The projections are a
/// pure function of the registered table schema, so hoisting them is
/// behaviour-preserving. This is the "the allocations themselves are the
/// defect" rule applied to the read path: remove the repeated allocation rather
/// than shrink the schema.
pub(crate) struct TableCodec {
    /// Declared column names, in declared order.
    names: Vec<String>,
    /// Parsed CQL type per declared column, in declared order.
    types: Vec<CqlType>,
    /// Declared-index positions of the partition key, in key order.
    pk: Vec<usize>,
    /// Declared-index positions of the clustering key, in clustering order.
    ck: Vec<usize>,
    /// Storage ordinal -> declared-index map for regular/static columns.
    storage_to_table: Vec<usize>,
}

impl TableCodec {
    /// Build the projections for `keyspace.table`. Fails loud if the table is
    /// not registered or a column type cannot be parsed in its keyspace.
    pub(crate) fn build(keyspace: &str, table: &str, schema: &Schema) -> Result<Self, String> {
        let snapshot = schema.snapshot();
        let meta = snapshot
            .tables
            .get(&(keyspace.to_string(), table.to_string()))
            .ok_or_else(|| format!("table {keyspace}.{table} is not registered"))?;
        let names: Vec<String> = meta.columns.keys().cloned().collect();
        let types = meta
            .columns
            .values()
            .map(|column| {
                ferrosa_row_bridge::parse_cql_type_in_keyspace(
                    &column.column_type,
                    keyspace,
                    schema,
                )
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())?;
        Ok(Self {
            names,
            types,
            pk: pk_indices(meta),
            ck: ck_indices(meta),
            storage_to_table: storage_to_table_indices(meta),
        })
    }

    /// Declared column names, in declared order.
    pub(crate) fn names(&self) -> &[String] {
        &self.names
    }

    /// Parsed CQL type per declared column, in declared order.
    pub(crate) fn types(&self) -> &[CqlType] {
        &self.types
    }

    /// Declared-index positions of the partition key, in key order.
    pub(crate) fn pk(&self) -> &[usize] {
        &self.pk
    }

    /// Declared-index positions of the clustering key, in clustering order.
    pub(crate) fn ck(&self) -> &[usize] {
        &self.ck
    }

    /// Storage ordinal -> declared-index map for regular/static columns.
    pub(crate) fn storage_to_table(&self) -> &[usize] {
        &self.storage_to_table
    }
}

/// How many decoded rows may sit between the async producer and the sync
/// consumer at once.
///
/// This is a **work bound**, not a result bound: it caps how far the storage
/// scan may run ahead of the executor, and every row of the table is still
/// delivered. Sized so a scan of wide rows keeps its in-flight window in the
/// low megabytes while still leaving the producer enough slack to stay busy
/// across a partition boundary.
pub const SCAN_BUFFER_ROWS: usize = crate::mvcc::DEFAULT_SCAN_BUFFER_ROWS;

/// The first storage error hit by any scan in one query, shared by every table
/// in that query's catalog.
///
/// A scan producer cannot return an error through `Iterator<Item = Row>`, so it
/// records it here and stops. The query layer takes this slot after `execute`
/// returns; a recorded failure turns the whole query into an `ErrorResponse`
/// instead of a short, successful-looking result. First failure wins — later
/// ones are consequences of the same collapse and would only obscure it.
#[derive(Clone, Default)]
pub struct ScanFailure(
    Arc<Mutex<Option<ScanCause>>>,
    #[cfg(test)] Arc<ScanKeyMetrics>,
);

#[cfg(test)]
#[derive(Default)]
struct ScanKeyMetrics {
    live: std::sync::atomic::AtomicUsize,
    peak: std::sync::atomic::AtomicUsize,
    capacity: std::sync::atomic::AtomicUsize,
}

/// Exact membership is needed to restore a row first deleted AFTER a physical
/// row was emitted without emitting it twice. Own decoded keys, never rows.
struct ScanSeen {
    keys: std::collections::HashSet<Vec<Value>>,
    #[cfg(test)]
    metrics: Arc<ScanKeyMetrics>,
}
impl ScanSeen {
    fn new(_failure: &ScanFailure) -> Self {
        Self {
            keys: Default::default(),
            #[cfg(test)]
            metrics: _failure.1.clone(),
        }
    }
    fn insert(&mut self, key: Vec<Value>) {
        let inserted = self.keys.insert(key);
        #[cfg(test)]
        if inserted {
            use std::sync::atomic::Ordering::Relaxed;
            let live = self.metrics.live.fetch_add(1, Relaxed) + 1;
            self.metrics.peak.fetch_max(live, Relaxed);
            self.metrics
                .capacity
                .fetch_max(self.keys.capacity(), Relaxed);
        }
        #[cfg(not(test))]
        let _ = inserted;
    }
    fn contains(&self, key: &Vec<Value>) -> bool {
        self.keys.contains(key)
    }
}
#[cfg(test)]
impl Drop for ScanSeen {
    fn drop(&mut self) {
        self.metrics
            .live
            .fetch_sub(self.keys.len(), std::sync::atomic::Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct ScanCause {
    code: &'static str,
    message: String,
}

impl ScanFailure {
    /// Record `message` unless a failure is already recorded.
    ///
    /// Poisoning is impossible in practice (the guarded value is a `String` and
    /// nothing panics while held), but recovering rather than unwrapping means a
    /// poisoned lock still reports the error instead of masking it with a panic
    /// on the way out.
    pub fn record(&self, message: String) {
        self.record_cause("58000", message);
    }

    fn record_cause(&self, code: &'static str, message: String) {
        let mut slot = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if slot.is_none() {
            *slot = Some(ScanCause { code, message });
        }
    }

    fn expired(&self) {
        self.record_cause("40001", "PostgreSQL transaction snapshot expired".into());
    }

    pub(crate) fn take_with_code(&self) -> Option<(&'static str, String)> {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .map(|cause| (cause.code, cause.message))
    }

    /// Take the recorded failure, if any, leaving the slot empty.
    pub fn take(&self) -> Option<String> {
        self.take_with_code().map(|(_, message)| message)
    }
}

impl fmt::Debug for ScanFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let slot = self.0.lock().unwrap_or_else(|e| e.into_inner());
        f.debug_tuple("ScanFailure").field(&*slot).finish()
    }
}

/// Everything a scan producer needs, resolved once at load time so that
/// re-scanning costs a storage pass and not a metadata pass.
struct ScanContext {
    engine: Arc<StorageEngine>,
    table_id: TableId,
    col_names: Vec<String>,
    col_types: Vec<CqlType>,
    pk_idx: Vec<usize>,
    ck_idx: Vec<usize>,
    storage_to_table: Vec<usize>,
    /// Sparse PostgreSQL row replacements/deletions for the query snapshot.
    overlay: Arc<std::collections::HashMap<Vec<Value>, Option<Row>>>,
    /// The cluster range-read path, when this deployment has one. `Some` routes
    /// the scan through the ring at [`read_consistency`]; `None` (a standalone
    /// node or a unit test) scans this node's local storage, which IS the
    /// complete answer there.
    cluster_read: Option<Arc<dyn RangeRead>>,
    /// The keyspace's replication strategy, used only by the cluster read. It
    /// is resolved once here rather than per scan.
    replication: ferrosa_cluster::ring::strategy::ReplicationStrategy,
    /// Complete key for a single-row table. The relational executor still
    /// evaluates the predicate; this only narrows physical I/O and MVCC tails.
    point: Option<PointLookup>,
}

pub(crate) struct PointLookup {
    pub(crate) storage_key: ferrosa_common::DecoratedKey,
    pub(crate) row_key: Vec<Value>,
}

/// Open the partition stream for a scan or an exact key lookup.
///
/// The **cluster** range read is used when this deployment has one: it fans out
/// across the ring at the level [`read_consistency`] resolves
/// (`FERROSA_PG_READ_CONSISTENCY`, `QUORUM` by default) and refuses (loudly) to
/// serve a read it cannot complete, including one whose configured level is not
/// a level at all. Otherwise this node's **local** storage is read — complete
/// for a standalone node, and the local path unit tests exercise.
///
/// A failure here is an `Err` that the caller records into [`ScanFailure`]; it is
/// **never** downgraded to a local-only scan, because the whole point is that an
/// under-return must not look like a small table.
async fn open_partition_stream(ctx: &Arc<ScanContext>) -> Result<PartitionStream, String> {
    match &ctx.cluster_read {
        Some(reader) => {
            let cl = read_consistency().map_err(|error| {
                format!(
                    "cluster range read of {}.{} refused: {error}",
                    ctx.table_id.keyspace, ctx.table_id.table
                )
            })?;
            let stream = match &ctx.point {
                Some(point) => {
                    reader
                        .stream_key(&ctx.table_id, &point.storage_key, cl, &ctx.replication)
                        .await
                }
                None => reader.stream_all(&ctx.table_id, cl, &ctx.replication).await,
            };
            stream.map_err(|error| {
                format!(
                    "cluster range read of {}.{} failed: {error}",
                    ctx.table_id.keyspace, ctx.table_id.table
                )
            })
        }
        None => {
            if let Some(point) = &ctx.point {
                let partition = ctx
                    .engine
                    .read(&ctx.table_id, &point.storage_key)
                    .map_err(|error| error.to_string())?;
                return Ok(Box::pin(futures::stream::iter(partition.map(Ok))));
            }
            let stream = ctx
                .engine
                .range_iter(&ctx.table_id, None, None)
                .map(|item| item.map_err(|e| e.to_string()).map(Arc::unwrap_or_clone));
            Ok(Box::pin(stream))
        }
    }
}

/// Check SQL representability without retaining a converted row.
fn validate_decoded_row(row: &[Option<CqlValue>]) -> Result<(), String> {
    for value in row.iter().flatten() {
        match value {
            // Already validated by the shared physical decoder. These SQL
            // representations are infallible, so no owned payload is needed.
            CqlValue::Text(_)
            | CqlValue::Ascii(_)
            | CqlValue::Blob(_)
            | CqlValue::Decimal { .. }
            | CqlValue::Varint(_)
            | CqlValue::Jsonb(_) => {}
            // Keep checked timestamp widening and unsupported-type errors in
            // the existing converter, including its error text.
            _ => {
                cql_to_value(value)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod validation_tests {
    use super::*;

    #[test]
    fn physical_validation_does_not_materialize_payloads() {
        let row = vec![
            Some(CqlValue::Text("x".repeat(4096))),
            Some(CqlValue::Ascii("y".repeat(4096))),
            Some(CqlValue::Blob(vec![7; 4096])),
            Some(CqlValue::Int(12)),
            None,
        ];
        let pointers: Vec<_> = row
            .iter()
            .flatten()
            .filter_map(|v| match v {
                CqlValue::Text(s) | CqlValue::Ascii(s) => Some(s.as_ptr()),
                CqlValue::Blob(b) => Some(b.as_ptr()),
                _ => None,
            })
            .collect();
        let allocations = crate::mvcc::MvccManager::count_allocations_for_test(|| {
            validate_decoded_row(&row).unwrap()
        });
        assert_eq!(allocations, 0);
        let after: Vec<_> = row
            .iter()
            .flatten()
            .filter_map(|v| match v {
                CqlValue::Text(s) | CqlValue::Ascii(s) => Some(s.as_ptr()),
                CqlValue::Blob(b) => Some(b.as_ptr()),
                _ => None,
            })
            .collect();
        assert_eq!(pointers, after);
    }

    #[test]
    fn physical_validation_matches_conversion_errors() {
        let cases = vec![
            CqlValue::Timestamp(i64::MAX),
            CqlValue::Timestamp(1),
            CqlValue::Duration {
                months: 1,
                days: 0,
                nanos: 0,
            },
            CqlValue::List(vec![CqlValue::Int(1)]),
            CqlValue::Text("valid".into()),
            CqlValue::Null,
        ];
        for value in cases {
            let expected = cql_to_value(&value).map(|_| ());
            assert_eq!(validate_decoded_row(&[Some(value)]), expected);
        }
    }
}

/// Drain `range_iter` into `tx`, decoding one partition at a time.
///
/// Returns — releasing the storage stream and the engine handle — on any of the
/// three exits: the stream ends, storage errors (recorded in `failure` first),
/// or the consumer drops its receiver. Nothing is left running behind a
/// cancelled or short-circuited query.
async fn produce_scan(
    ctx: Arc<ScanContext>,
    tx: mpsc::Sender<Row>,
    failure: ScanFailure,
    view: Option<crate::mvcc::MvccTableView>,
) {
    let mut stream = match open_partition_stream(&ctx).await {
        Ok(stream) => stream,
        Err(message) => {
            // Fail loud: a cluster read that cannot be served at the consistency
            // level MUST NOT fall back to a local-only scan, which would return a
            // short result set that looks like the whole answer.
            failure.record(message);
            return;
        }
    };
    let mut overlay_seen = ScanSeen::new(&failure);
    while let Some(item) = stream.next().await {
        let partition = match item {
            Ok(p) => p,
            Err(e) => {
                // Fail loud. The consumer only sees the channel close, so this
                // slot is the ONLY thing standing between a storage error and a
                // silently-truncated result set.
                failure.record(format!(
                    "scan of {}.{} failed: {e}",
                    ctx.table_id.keyspace, ctx.table_id.table
                ));
                return;
            }
        };
        // Mirror the CQL SELECT path: one engine row per logical CQL row, with
        // values in the table's declared column order. The decomposition is
        // per-partition, so this holds one partition's rows, not the table's.
        let cql_rows = match ferrosa_row_bridge::partition_to_rows_with_storage_mapping(
            &partition,
            &ctx.col_names,
            &ctx.col_types,
            &ctx.pk_idx,
            &ctx.ck_idx,
            &ctx.storage_to_table,
        ) {
            Ok(rows) => rows,
            Err(error) => {
                // A corrupt cell fails the query. It must never surface as a
                // NULL or a truncated result (PG-Tcf7ca2cc).
                let error =
                    error.in_table(format!("{}.{}", ctx.table_id.keyspace, ctx.table_id.table));
                failure.record(format!("scan failed: {error}"));
                return;
            }
        };
        for cql_row in cql_rows {
            if let Err(error) = validate_decoded_row(&cql_row) {
                failure.record(format!(
                    "scan of {}.{} cannot represent a stored value: {error}",
                    ctx.table_id.keyspace, ctx.table_id.table
                ));
                return;
            }
            let key = match ctx
                .pk_idx
                .iter()
                .chain(ctx.ck_idx.iter())
                .map(|index| {
                    cql_row[*index]
                        .as_ref()
                        .map_or(Ok(Value::Null), cql_to_value)
                })
                .collect::<Result<Vec<Value>, String>>()
            {
                Ok(key) => key,
                Err(error) => {
                    failure.record(error);
                    return;
                }
            };
            // Resolve AFTER the physical read. A key can first acquire a
            // before-image while this scan is in progress. Staged own writes
            // have precedence over the snapshot root.
            let historical = match view.as_ref().map(|view| view.row(&key)).transpose() {
                Ok(row) => row.flatten(),
                Err(_) => {
                    failure.expired();
                    return;
                }
            };
            let replacement = ctx.overlay.get(&key).or(historical.as_deref());
            let row = match replacement {
                Some(Some(row)) => Some(row.clone()),
                Some(None) => None,
                None => match decoded_row_values(cql_row) {
                    Ok(values) => Some(Row::new(values)),
                    Err(error) => {
                        failure.record(error);
                        return;
                    }
                },
            };
            if view.is_some() || ctx.overlay.contains_key(&key) {
                overlay_seen.insert(key);
            }
            let Some(row) = row else {
                continue;
            };
            if tx.send(row).await.is_err() {
                // The consumer is gone: the executor short-circuited, errored,
                // or the connection dropped. Not a failure — stop producing.
                return;
            }
        }
    }
    // A missing physical key can still have a visible before-image or staged
    // insert. Resolve only that key: enumerating every historical key would
    // turn a physical point lookup back into an O(table size) read.
    if let Some(point) = &ctx.point {
        if !overlay_seen.contains(&point.row_key) {
            let historical = match view
                .as_ref()
                .map(|view| view.row(&point.row_key))
                .transpose()
            {
                Ok(row) => row.flatten(),
                Err(_) => {
                    failure.expired();
                    return;
                }
            };
            if let Some(Some(row)) = ctx.overlay.get(&point.row_key).or(historical.as_deref()) {
                let _ = tx.send(row.clone()).await;
            }
        }
        return;
    }
    // Reconcile late deletes exactly once. This set retains keys, not rows;
    // each self-join scan has its own set and its own bounded row channel.
    if let Some(view) = view {
        let historical = match view.rows() {
            Ok(rows) => rows,
            Err(_) => {
                failure.expired();
                return;
            }
        };
        for image in historical {
            let (key, row) = match image {
                Ok(image) => image,
                Err(_) => {
                    failure.expired();
                    return;
                }
            };
            if !overlay_seen.contains(key.as_ref()) && !ctx.overlay.contains_key(key.as_ref()) {
                if let Some(row) = row.as_ref() {
                    if tx.send(row.clone()).await.is_err() {
                        return;
                    }
                }
            }
        }
    }
    for (key, row) in ctx.overlay.iter() {
        if !overlay_seen.contains(key) {
            if let Some(row) = row {
                if tx.send(row.clone()).await.is_err() {
                    return;
                }
            }
        }
    }
}

/// The sync half of the hand-off: pulls rows the producer pushes.
///
/// `blocking_recv` is legal here because the relational executor runs on a
/// `spawn_blocking` thread (see the `result_stream` module). Dropping this iterator
/// closes the channel, which is what tells the producer to stop.
struct ScanIter {
    rx: mpsc::Receiver<Row>,
}

/// Run a scan producer while converting an unexpected panic into a query
/// failure. The synchronous `TableProvider` iterator can only observe channel
/// closure; without this guard a panic would look exactly like a clean EOF and
/// could silently truncate a result.
async fn guard_scan_producer<F>(future: F, failure: ScanFailure)
where
    F: Future<Output = ()>,
{
    if let Err(payload) = AssertUnwindSafe(future).catch_unwind().await {
        let reason = payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| payload.downcast_ref::<&str>().copied())
            .unwrap_or("unknown panic");
        failure.record(format!("scan producer panicked: {reason}"));
    }
}

impl Iterator for ScanIter {
    type Item = Row;

    fn next(&mut self) -> Option<Row> {
        self.rx.blocking_recv()
    }
}

/// A [`TableProvider`] that streams `keyspace.table` from storage on demand.
///
/// Holds no rows. Each `scan()` opens a fresh bounded channel and spawns its own
/// producer, so the provider is re-scannable (see the module docs on self-joins)
/// and never accumulates.
pub struct StreamingTable {
    schema: RelSchema,
    ctx: Arc<ScanContext>,
    /// Captured at load time so `scan()` — which is sync and may run on a
    /// blocking thread with no runtime of its own — can still spawn a producer.
    handle: Handle,
    failure: ScanFailure,
    scan_buffer_rows: usize,
    mvcc_view: Option<crate::mvcc::MvccTableView>,
}

impl StreamingTable {
    pub(crate) fn with_point_lookup(mut self, point: Option<PointLookup>) -> Self {
        // Providers are uniquely owned until installed in the catalog.
        Arc::get_mut(&mut self.ctx)
            .expect("unpublished scan context")
            .point = point;
        self
    }
    pub(crate) fn with_mvcc_view(mut self, view: crate::mvcc::MvccTableView) -> Self {
        self.mvcc_view = Some(view);
        self
    }
}

impl fmt::Debug for StreamingTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamingTable")
            .field("table", &self.ctx.table_id)
            .field("columns", &self.schema.columns.len())
            .finish()
    }
}

impl TableProvider for StreamingTable {
    fn schema(&self) -> &RelSchema {
        &self.schema
    }

    fn scan(&self) -> Box<dyn Iterator<Item = Row> + Send> {
        let (tx, rx) = mpsc::channel(self.scan_buffer_rows);
        self.handle.spawn(guard_scan_producer(
            produce_scan(
                self.ctx.clone(),
                tx,
                self.failure.clone(),
                self.mvcc_view.clone(),
            ),
            self.failure.clone(),
        ));
        Box::new(ScanIter { rx })
    }
}

/// Open `keyspace.table` for streaming scans, resolving its schema and decode
/// context up front.
///
/// Reads **no rows**: the returned [`StreamingTable`] pulls from storage only
/// when scanned. `failure` is the query-wide slot a scan records a storage error
/// into; the caller must check it after execution (see [`ScanFailure`]).
///
/// # Errors
///
/// - [`LoadError::NoSuchTable`] if the table is not in the schema snapshot (the
///   R15 guard — never returns an empty relation for a missing table).
/// - [`LoadError::Storage`] if a column type string fails to parse. A storage
///   error *during* a scan cannot surface here — it lands in `failure`.
pub async fn load_table(
    engine: &Arc<StorageEngine>,
    schema: &Schema,
    keyspace: &str,
    table: &str,
    failure: ScanFailure,
) -> Result<StreamingTable, LoadError> {
    load_table_with_overlay(
        engine,
        schema,
        keyspace,
        table,
        failure,
        Default::default(),
        SCAN_BUFFER_ROWS,
        None,
    )
    .await
}

// Context pass-through for the scan: engine, schema, table identity, the scan
// failure accumulator, the uncommitted-write overlay, the bounded read buffer
// and the cluster reader. See the note on `load_catalog_with_mvcc`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn load_table_with_overlay(
    engine: &Arc<StorageEngine>,
    schema: &Schema,
    keyspace: &str,
    table: &str,
    failure: ScanFailure,
    overlay: std::collections::HashMap<Vec<Value>, Option<Row>>,
    scan_buffer_rows: usize,
    cluster_read: Option<Arc<dyn RangeRead>>,
) -> Result<StreamingTable, LoadError> {
    let snapshot = schema.snapshot();

    // R15 guard: existence is decided by schema metadata, never by an empty
    // stream. A missing table errors; an existing empty table loads zero rows.
    let meta = snapshot
        .tables
        .get(&(keyspace.to_string(), table.to_string()))
        .ok_or_else(|| LoadError::NoSuchTable {
            keyspace: keyspace.to_string(),
            table: table.to_string(),
        })?;

    // Column context for the canonical CQL decomposition, in declared order.
    let col_names: Vec<String> = meta.columns.keys().cloned().collect();
    let col_types = meta
        .columns
        .values()
        .map(|c| ferrosa_row_bridge::parse_cql_type_in_keyspace(&c.column_type, keyspace, schema))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| LoadError::Storage(format!("failed to resolve column type: {e}")))?;
    let rel_schema = rel_schema_for(&col_names, &col_types);
    let pk_idx = pk_indices(meta);
    let ck_idx = ck_indices(meta);
    let storage_to_table = storage_to_table_indices(meta);

    // The keyspace replication strategy the cluster read uses to fan out. Only
    // needed (and only parsed) when this deployment has a cluster read path.
    let replication = match &cluster_read {
        Some(_) => {
            let keyspace_meta = snapshot.keyspaces.get(keyspace).ok_or_else(|| {
                LoadError::Storage(format!(
                    "keyspace {keyspace} has no metadata to resolve a replication strategy from"
                ))
            })?;
            ferrosa_cluster::ring::strategy::ReplicationStrategy::try_from(
                &keyspace_meta.replication,
            )
            .map_err(|error| {
                LoadError::Storage(format!(
                    "keyspace {keyspace} has an invalid replication strategy: {error}"
                ))
            })?
        }
        // Unused by the local path; a harmless default keeps the field total.
        None => ferrosa_cluster::ring::strategy::ReplicationStrategy::Simple {
            replication_factor: 1,
        },
    };

    // No scan happens here: the provider reads from storage only when scanned
    // (full-table, no key bounds), one bounded window of rows at a time.
    Ok(StreamingTable {
        schema: rel_schema,
        ctx: Arc::new(ScanContext {
            engine: engine.clone(),
            table_id: TableId::new(keyspace, table),
            col_names,
            col_types,
            pk_idx,
            ck_idx,
            storage_to_table,
            overlay: Arc::new(overlay),
            cluster_read,
            replication,
            point: None,
        }),
        handle: Handle::current(),
        failure,
        scan_buffer_rows,
        mvcc_view: None,
    })
}

/// Merge a transaction's uncommitted scalar row mutations into the sparse
/// snapshot overlay so PostgreSQL transactions observe their own writes.
pub(crate) fn apply_pending_writes(
    engine: &StorageEngine,
    schema: &Schema,
    keyspace: &str,
    table: &str,
    overlay: &mut std::collections::HashMap<Vec<Value>, Option<Row>>,
    writes: &[crate::mvcc::PgWrite],
) -> Result<(), String> {
    let codec = TableCodec::build(keyspace, table, schema)?;
    apply_pending_writes_with_partition_keys(
        engine,
        &codec,
        keyspace,
        table,
        overlay,
        writes.iter().map(|w| &w.0),
        ApplyOutputs::default(),
    )
}

/// Keep only the keys explicitly staged by the transaction. Other snapshot
/// images must remain live so changes after query construction are observed.
pub(crate) fn staged_overlay(
    engine: &StorageEngine,
    schema: &Schema,
    keyspace: &str,
    table: &str,
    overlay: &mut std::collections::HashMap<Vec<Value>, Option<Row>>,
    writes: &[crate::mvcc::PgWrite],
    view: Option<&crate::mvcc::MvccTableView>,
) -> Result<(), crate::mvcc::MvccCommitError> {
    use crate::mvcc::MvccCommitError;
    let storage_error =
        |message: String| MvccCommitError::Storage(ferrosa_common::Error::InvalidData(message));
    let codec = TableCodec::build(keyspace, table, schema).map_err(storage_error)?;
    // Resolve only the keys the transaction actually staged. Building a whole
    // snapshot overlay here made even one own write clone unrelated payloads.
    if let Some(view) = view {
        view.ensure_active()?;
        for write in writes
            .iter()
            .map(|write| &write.0)
            .filter(|write| write.keyspace == keyspace && write.table == table)
        {
            if ferrosa_storage::table_tombstone::is_table_tombstone_key(&write.key) {
                continue;
            }
            let pk = ferrosa_row_bridge::decode_pk(&write.key, codec.pk.len());
            for row in &write.rows {
                let ck = ferrosa_row_bridge::decode_clustering(&row.clustering, codec.ck.len());
                let key = codec
                    .pk
                    .iter()
                    .zip(pk.iter())
                    .chain(codec.ck.iter().zip(ck.iter()))
                    .map(|(index, part)| {
                        let cell = ferrosa_row_bridge::decode_value(&codec.types[*index], part)
                            .map_err(|error| storage_error(error.to_string()))?;
                        cql_to_value(&cell).map_err(storage_error)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                if let Some(image) = view.row(&key)? {
                    overlay.insert(key, (*image).clone());
                }
            }
        }
    }
    let mut keys = std::collections::HashMap::new();
    apply_pending_writes_with_partition_keys(
        engine,
        &codec,
        keyspace,
        table,
        overlay,
        writes.iter().map(|write| &write.0),
        ApplyOutputs {
            partition_keys: Some(&mut keys),
            ..Default::default()
        },
    )
    .map_err(storage_error)?;
    overlay.retain(|key, _| keys.contains_key(key));
    if let Some(view) = view {
        view.ensure_active()?;
    }
    Ok(())
}

///
/// `writes` is an iterator of `&Mutation` (not `&[PgWrite]`) so a caller that
/// already owns the mutations — the transactional-`COPY` commit's
/// `prepare_row_changes` — does not have to CLONE the whole write-set into a
/// throwaway `Vec<PgWrite>` first. That clone was a full extra copy of every
/// buffered row (a megabyte-per-thousand-rows of pure waste on the commit's
/// peak), since this function makes a single pass.
///
/// The per-table decode projections arrive as a prebuilt [`TableCodec`] rather
/// than being rebuilt here, so a caller that visits many mutations of one table
/// pays for them once.
///
/// `before_images`, when supplied, records the **storage** before-image of every
/// key this call had to read (i.e. every key the transaction's overlay did not
/// already hold). A streaming caller can then attach the correct before-image to
/// each partition's row-version metadata without a second storage read — the
/// commit path used to read every row twice for exactly this reason.
/// Env-gated attribution counters for the commit path's per-row storage read.
/// Temporary measurement scaffolding (see `FERROSA_PG_COMMIT_PROFILE`).
pub(crate) static READ_IMAGE_CALLS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static READ_IMAGE_NS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
fn commit_profile_enabled() -> bool {
    std::env::var_os("FERROSA_PG_COMMIT_PROFILE").is_some()
}

#[derive(Default)]
pub(crate) struct ApplyOutputs<'a> {
    /// Per-key partition bytes, when the caller needs them for the commit payload.
    pub partition_keys: Option<&'a mut std::collections::HashMap<Vec<Value>, Vec<u8>>>,
    /// Before-image of every key this call had to read from storage, so a streaming
    /// caller can attach it to that partition's row-version metadata instead of
    /// reading the row a second time.
    pub before_images: Option<&'a mut std::collections::HashMap<Vec<Value>, Option<Row>>>,
    /// Batch-prefetched storage before-images, keyed by SQL key, produced by
    /// [`prefetch_before_images`] against ONE store-view snapshot.
    ///
    /// When an entry is present it is used instead of a per-row [`read_row_image`]
    /// call: the value is the SAME pre-transaction storage image the per-row read
    /// would return, so an absent entry means "not prefetched — read it now" while a
    /// present `Some(None)` means "read, and there is no live row". That distinction
    /// is what lets a batched prefetch stand in for the per-row read without changing
    /// which keys see a before-image.
    pub before_cache: Option<&'a std::collections::HashMap<Vec<Value>, Option<Row>>>,
}

pub(crate) fn apply_pending_writes_with_partition_keys<'a, I>(
    engine: &StorageEngine,
    codec: &TableCodec,
    keyspace: &str,
    table: &str,
    overlay: &mut std::collections::HashMap<Vec<Value>, Option<Row>>,
    writes: I,
    outputs: ApplyOutputs<'_>,
) -> Result<(), String>
where
    I: IntoIterator<Item = &'a ferrosa_storage::Mutation>,
{
    let mut partition_keys = outputs.partition_keys;
    let mut before_images = outputs.before_images;
    let before_cache = outputs.before_cache;
    for mutation in writes {
        if mutation.keyspace != keyspace || mutation.table != table {
            continue;
        }
        // A whole-table tombstone (`TRUNCATE`) is buffered as a partition-tombstone
        // MARKER under a RESERVED partition key, not as a data row: its key bytes are
        // the marker magic, not a value of any key column. Decoding them as this
        // table's declared key type is meaningless — for a length-constrained key (a
        // PK-less table's synthetic `_sys_ck_` uuid, or an `int`) it fails loud and
        // takes the whole statement or transaction with it. The marker carries no row
        // image, so skip it here; the COMMIT path applies and replicates it itself
        // (the cluster committer routes it to every serving node at CL=ALL).
        if ferrosa_storage::table_tombstone::is_table_tombstone_key(&mutation.key) {
            continue;
        }
        let pk_parts = ferrosa_row_bridge::decode_pk(&mutation.key, codec.pk.len());
        for mutation_row in &mutation.rows {
            let ck_parts =
                ferrosa_row_bridge::decode_clustering(&mutation_row.clustering, codec.ck.len());
            let mut key = Vec::with_capacity(codec.pk.len() + codec.ck.len());
            let mut values = vec![Value::Null; codec.names.len()];
            for (index, part) in codec.pk.iter().zip(pk_parts.iter()) {
                let cql = ferrosa_row_bridge::decode_value(&codec.types[*index], part)
                    .map_err(|error| error.to_string())?;
                let value = cql_to_value(&cql)?;
                key.push(value.clone());
                values[*index] = value;
            }
            for (index, part) in codec.ck.iter().zip(ck_parts.iter()) {
                let cql = ferrosa_row_bridge::decode_value(&codec.types[*index], part)
                    .map_err(|error| error.to_string())?;
                let value = cql_to_value(&cql)?;
                key.push(value.clone());
                values[*index] = value;
            }
            if let Some(partition_keys) = partition_keys.as_deref_mut() {
                partition_keys.insert(key.clone(), mutation.key.key.as_bytes().to_vec());
            }

            let base = if let Some(snapshot_row) = overlay.get(&key) {
                snapshot_row.clone()
            } else if let Some(cached) = before_cache.and_then(|cache| cache.get(&key)) {
                // Prefetched batch before-image: byte-for-byte what the per-row
                // read below would return, but resolved once per key across a whole
                // window of mutations against one store view (see
                // `prefetch_before_images`).
                if let Some(before) = before_images.as_deref_mut() {
                    before.insert(key.clone(), cached.clone());
                }
                cached.clone()
            } else {
                let started = commit_profile_enabled().then(std::time::Instant::now);
                let image = read_row_image(engine, codec, mutation, &mutation_row.clustering)?
                    .map(|(_, row)| row);
                if let Some(started) = started {
                    let elapsed = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
                    READ_IMAGE_CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    READ_IMAGE_NS.fetch_add(elapsed, std::sync::atomic::Ordering::Relaxed);
                }
                if let Some(before) = before_images.as_deref_mut() {
                    before.insert(key.clone(), image.clone());
                }
                image
            };
            if let Some(base) = base {
                values = base.0;
            }
            if !mutation_row.deletion.is_live() {
                overlay.insert(key, None);
                continue;
            }
            for (storage_index, cell) in &mutation_row.cells {
                let storage_index = usize::from(*storage_index & 0x3fff);
                let Some(table_index) = codec.storage_to_table.get(storage_index).copied() else {
                    continue;
                };
                values[table_index] = match &cell.value {
                    Some(bytes) => {
                        let cql =
                            ferrosa_row_bridge::decode_value(&codec.types[table_index], bytes)
                                .map_err(|error| error.to_string())?;
                        cql_to_value(&cql)?
                    }
                    None => Value::Null,
                };
            }
            overlay.insert(key, Some(Row::new(values)));
        }
    }
    Ok(())
}

/// Read one PostgreSQL row image by its storage partition and clustering key.
/// Returns the SQL primary-key tuple with the decoded row for MVCC history.
///
/// The per-table projections come from a prebuilt [`TableCodec`]; hoisting them
/// out of this call is what keeps a 1.1M-row commit from rebuilding the same
/// schema projection once per row.
pub(crate) fn read_row_image(
    engine: &StorageEngine,
    codec: &TableCodec,
    mutation: &ferrosa_storage::Mutation,
    clustering: &[u8],
) -> Result<Option<(Vec<Value>, Row)>, String> {
    let table_id = TableId::new(&mutation.keyspace, &mutation.table);
    let looked_up = engine
        .read_clustering_row(&table_id, &mutation.key, clustering)
        .map_err(|error| error.to_string())?;
    let Some(partition) = looked_up else {
        return Ok(None);
    };
    partition_row_image(codec, &mutation.keyspace, &mutation.table, &partition)
}

/// Decode one storage [`Partition`](ferrosa_sstable::types::Partition) into a SQL
/// primary-key tuple and the row image the MVCC history stores.
///
/// Shared by the per-row [`read_row_image`] and the batched
/// [`prefetch_before_images`] so both produce byte-identical before-images — the
/// batch path is a different read SHAPE, never a different answer.
pub(crate) fn partition_row_image(
    codec: &TableCodec,
    keyspace: &str,
    table: &str,
    partition: &ferrosa_sstable::types::Partition,
) -> Result<Option<(Vec<Value>, Row)>, String> {
    let rows = ferrosa_row_bridge::partition_to_rows_with_clustering(
        partition,
        &codec.names,
        &codec.types,
        &codec.pk,
        &codec.ck,
        &codec.storage_to_table,
    )
    .map_err(|error| error.in_table(format!("{keyspace}.{table}")).to_string())?;
    let Some((_, cells)) = rows.into_iter().next() else {
        return Ok(None);
    };
    let values: Vec<Value> = cells
        .iter()
        .map(|cell| match cell {
            Some(value) => cql_to_value(value),
            None => Ok(Value::Null),
        })
        .collect::<Result<_, _>>()?;
    let key = codec
        .pk
        .iter()
        .chain(codec.ck.iter())
        .map(|index| values[*index].clone())
        .collect();
    Ok(Some((key, Row::new(values))))
}

/// Batch-prefetch the storage before-images a chunk of mutations will need, in ONE
/// store-view snapshot, so the per-mutation overlay merge does not issue a point read
/// per row.
///
/// The commit's before-image read is one point read of the PRE-transaction row for
/// every key the transaction writes (the row-version metadata needs it). Issued one
/// row at a time that pays the storage view/schema/tombstone fixed cost once per row;
/// this resolves the whole chunk against one view and returns a `SQL key -> Option<Row>`
/// map the caller hands back through [`ApplyOutputs::before_cache`].
///
/// Keys already present in `overlay` are skipped: the merge uses the overlay's
/// accumulated row as its base and never reads storage for them. A key the overlay
/// gains LATER — an earlier mutation in the same chunk wrote it — is read here anyway,
/// which is harmless: the merge simply does not consult it.
pub(crate) fn prefetch_before_images<'a, I>(
    engine: &StorageEngine,
    codec: &TableCodec,
    keyspace: &str,
    table: &str,
    overlay: &std::collections::HashMap<Vec<Value>, Option<Row>>,
    writes: I,
) -> Result<std::collections::HashMap<Vec<Value>, Option<Row>>, String>
where
    I: IntoIterator<Item = &'a ferrosa_storage::Mutation>,
{
    let mut requests: Vec<(ferrosa_common::DecoratedKey, Vec<u8>)> = Vec::new();
    let mut keys: Vec<Vec<Value>> = Vec::new();
    for mutation in writes {
        if mutation.keyspace != keyspace || mutation.table != table {
            continue;
        }
        if ferrosa_storage::table_tombstone::is_table_tombstone_key(&mutation.key) {
            continue;
        }
        let pk_parts = ferrosa_row_bridge::decode_pk(&mutation.key, codec.pk.len());
        for mutation_row in &mutation.rows {
            let ck_parts =
                ferrosa_row_bridge::decode_clustering(&mutation_row.clustering, codec.ck.len());
            let mut key = Vec::with_capacity(codec.pk.len() + codec.ck.len());
            for (index, part) in codec.pk.iter().zip(pk_parts.iter()) {
                let cql = ferrosa_row_bridge::decode_value(&codec.types[*index], part)
                    .map_err(|error| error.to_string())?;
                key.push(cql_to_value(&cql)?);
            }
            for (index, part) in codec.ck.iter().zip(ck_parts.iter()) {
                let cql = ferrosa_row_bridge::decode_value(&codec.types[*index], part)
                    .map_err(|error| error.to_string())?;
                key.push(cql_to_value(&cql)?);
            }
            if overlay.contains_key(&key) {
                continue;
            }
            requests.push((mutation.key.clone(), mutation_row.clustering.clone()));
            keys.push(key);
        }
    }
    let mut cache = std::collections::HashMap::with_capacity(requests.len());
    if requests.is_empty() {
        return Ok(cache);
    }
    let table_id = TableId::new(keyspace, table);
    let images = engine
        .read_clustering_rows_batch(&table_id, &requests)
        .map_err(|error| error.to_string())?;
    for (key, partition) in keys.into_iter().zip(images) {
        let image = match partition {
            Some(partition) => {
                partition_row_image(codec, keyspace, table, &partition)?.map(|(_, row)| row)
            }
            None => None,
        };
        cache.insert(key, image);
    }
    Ok(cache)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_sql::ColumnType;

    fn jsonb_cell(text: &str) -> ferrosa_jsonb::JsonbValue {
        use ferrosa_jsonb::{parse_text, JsonbValue, Limits, LimitsConfig};
        let limits = Limits::from_config_with_env(&LimitsConfig::default(), &|_| None, 64 << 20)
            .expect("default limits");
        let enc = parse_text(text.as_bytes(), &limits).expect("valid json");
        JsonbValue::from_encoded(enc).expect("valid cell")
    }

    /// PG-T160-2: a jsonb cell crosses the storage boundary byte-for-byte in
    /// both directions, keeping scale variants (`1.0`) intact.
    #[test]
    fn storage_provider_round_trips_a_jsonb_cell() {
        let doc = jsonb_cell(r#"{"a":[1.0,{"b":12345678901234567890.5e300}],"s":"x"}"#);
        let cql = CqlValue::Jsonb(doc.clone());
        let sql = super::cql_to_value(&cql).expect("jsonb is representable");
        match &sql {
            Value::Jsonb(got) => assert_eq!(got.as_bytes(), doc.as_bytes()),
            other => panic!("expected Value::Jsonb, got {other:?}"),
        }
        match crate::query::value_to_cql(&sql, &CqlType::Jsonb, &crate::jsonb_wire::test_limits()) {
            Ok(back) => assert_eq!(back, cql),
            Err(_) => panic!("a jsonb value must bind to a jsonb column"),
        }
        // A non-jsonb, non-text value for a jsonb column is refused, never
        // coerced. (Text is parsed and validated: T-161a.)
        let limits = crate::jsonb_wire::test_limits();
        assert!(crate::query::value_to_cql(&Value::Int(1), &CqlType::Jsonb, &limits).is_err());
        assert!(
            crate::query::value_to_cql(&Value::Text("{".into()), &CqlType::Jsonb, &limits).is_err()
        );
    }

    fn cql_to_value(value: &CqlValue) -> Value {
        super::cql_to_value(value).expect("test value should be representable")
    }

    #[tokio::test]
    async fn scan_producer_panic_is_reported_as_query_failure() {
        let failure = ScanFailure::default();
        guard_scan_producer(async { panic!("injected scan failure") }, failure.clone()).await;

        assert_eq!(
            failure.take().as_deref(),
            Some("scan producer panicked: injected scan failure"),
            "a producer panic must not be mistaken for a clean end of scan"
        );
    }

    use ferrosa_common::cell::CellValue;
    use ferrosa_common::key::{DecoratedKey, PartitionKey};
    use ferrosa_common::CqlValue;
    use ferrosa_schema::{
        AuthContext, AuthMethod, ClusteringOrder, ColumnKind, ColumnMetadata, DeploymentMode,
        EnvSecretsProvider, KeyspaceMetadata, PasswordHasher, PasswordPolicy, RateLimitConfig,
        ReplicationParams, Schema, SchemaConfig, TableMetadata, TableParams, TestAuditSink,
    };
    use ferrosa_sql::TableProvider;
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row as StorageRow};
    use ferrosa_storage::{
        CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
        TableId,
    };
    use indexmap::IndexMap;
    use std::collections::{HashMap, HashSet};
    use std::path::Path;
    use std::time::Duration;
    use uuid::Uuid;

    // ── Pure conversion tests (no infra) ──────────────────────────────────

    #[test]
    fn owned_row_conversion_moves_text_and_blob_allocations() {
        let mut text = String::with_capacity(1024);
        text.push_str("owned text");
        let mut ascii = String::with_capacity(512);
        ascii.push_str("owned ascii");
        let mut blob = Vec::with_capacity(2048);
        blob.extend_from_slice(b"owned blob");
        let allocations = [
            (text.as_ptr(), text.capacity()),
            (ascii.as_ptr(), ascii.capacity()),
            (blob.as_ptr(), blob.capacity()),
        ];
        let values = super::decoded_row_values(vec![
            Some(CqlValue::Text(text)),
            Some(CqlValue::Ascii(ascii)),
            Some(CqlValue::Blob(blob)),
            None,
            Some(CqlValue::Int(42)),
        ])
        .unwrap();
        for (value, (pointer, capacity)) in values.iter().zip(allocations) {
            let (actual_pointer, actual_capacity) = match value {
                Value::Text(text) => (text.as_ptr(), text.capacity()),
                Value::Bytea(bytes) => (bytes.as_ptr(), bytes.capacity()),
                _ => panic!("expected an owned payload"),
            };
            assert_eq!(actual_pointer, pointer, "payload must move without a copy");
            assert_eq!(actual_capacity, capacity, "retain the original allocation");
        }
        assert_eq!(values[3], Value::Null);
        assert_eq!(values[4], Value::Int(42));
    }

    #[test]
    fn owned_row_conversion_preserves_values_and_errors() {
        use num_bigint::BigInt;
        let values = vec![
            CqlValue::Null,
            CqlValue::Int(42),
            CqlValue::Bigint(i64::MIN),
            CqlValue::Counter(7),
            CqlValue::Smallint(-3),
            CqlValue::Tinyint(5),
            CqlValue::Text("text".into()),
            CqlValue::Ascii("ascii".into()),
            CqlValue::Boolean(true),
            CqlValue::Float(1.5_f32.to_bits()),
            CqlValue::Double((-0.0_f64).to_bits()),
            CqlValue::Uuid(Uuid::nil()),
            CqlValue::Timeuuid(Uuid::nil()),
            CqlValue::Blob(vec![0, 255]),
            CqlValue::Timestamp(1000),
            CqlValue::Timestamp(i64::MAX),
            CqlValue::Date(u32::MAX),
            CqlValue::Time(1_500_000),
            CqlValue::Inet("::1".parse().unwrap()),
            CqlValue::Decimal {
                scale: 2,
                unscaled: BigInt::from(12300),
            },
            CqlValue::Varint(BigInt::from(9_999_999_999_i64)),
            CqlValue::Duration {
                months: 1,
                days: 2,
                nanos: 3,
            },
            CqlValue::List(vec![CqlValue::Int(1)]),
        ];
        for value in values {
            let expected = super::cql_to_value(&value);
            let actual =
                super::decoded_row_values(vec![Some(value)]).map(|mut values| values.remove(0));
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn cql_to_value_maps_supported_scalars() {
        assert_eq!(cql_to_value(&CqlValue::Null), Value::Null);
        assert_eq!(cql_to_value(&CqlValue::Int(42)), Value::Int(42));
        assert_eq!(
            cql_to_value(&CqlValue::Bigint(9_000_000_000)),
            Value::Int(9_000_000_000)
        );
        assert_eq!(cql_to_value(&CqlValue::Counter(7)), Value::Int(7));
        assert_eq!(cql_to_value(&CqlValue::Smallint(-3)), Value::Int(-3));
        assert_eq!(cql_to_value(&CqlValue::Tinyint(5)), Value::Int(5));
        assert_eq!(
            cql_to_value(&CqlValue::Text("hi".to_string())),
            Value::Text("hi".to_string())
        );
        assert_eq!(
            cql_to_value(&CqlValue::Ascii("a".to_string())),
            Value::Text("a".to_string())
        );
        assert_eq!(cql_to_value(&CqlValue::Boolean(true)), Value::Bool(true));
    }

    #[test]
    fn cql_to_value_rejects_unrepresentable_values() {
        assert!(super::cql_to_value(&CqlValue::Duration {
            months: 1,
            days: 2,
            nanos: 3
        })
        .is_err());
        assert!(super::cql_to_value(&CqlValue::List(vec![CqlValue::Int(1)])).is_err());
        assert!(super::cql_to_value(&CqlValue::Timestamp(i64::MAX)).is_err());
    }

    #[test]
    fn cql_to_value_maps_temporal_network_and_numeric() {
        use num_bigint::BigInt;
        use std::net::IpAddr;
        // Timestamp: CQL millis → engine micros.
        assert_eq!(
            cql_to_value(&CqlValue::Timestamp(1_705_315_800_000)),
            Value::Timestamp(1_705_315_800_000_000)
        );
        // Date: CQL epoch-centered days (2^31) → signed days since Unix epoch.
        // 2^31 is the CQL encoding of day 0 (1970-01-01).
        assert_eq!(cql_to_value(&CqlValue::Date(2_147_483_648)), Value::Date(0));
        assert_eq!(cql_to_value(&CqlValue::Date(2_147_483_649)), Value::Date(1));
        // Time: CQL nanos → engine micros.
        assert_eq!(cql_to_value(&CqlValue::Time(1_500_000)), Value::Time(1500));
        // Inet passes through.
        let ip: IpAddr = "10.0.0.5".parse().unwrap();
        assert_eq!(cql_to_value(&CqlValue::Inet(ip)), Value::Inet(ip));
        // Decimal → normalized Numeric.
        assert_eq!(
            cql_to_value(&CqlValue::Decimal {
                scale: 2,
                unscaled: BigInt::from(12345)
            }),
            Value::numeric(BigInt::from(12345), 2)
        );
        // Varint → Numeric scale 0.
        assert_eq!(
            cql_to_value(&CqlValue::Varint(BigInt::from(9_999_999_999i64))),
            Value::numeric(BigInt::from(9_999_999_999i64), 0)
        );
    }

    #[test]
    fn cql_to_value_maps_uuid_timeuuid_and_blob() {
        // These now map through to the widened Value variants (no longer lossy).
        let u = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        assert_eq!(cql_to_value(&CqlValue::Uuid(u)), Value::Uuid(u));
        assert_eq!(cql_to_value(&CqlValue::Timeuuid(u)), Value::Uuid(u));
        assert_eq!(
            cql_to_value(&CqlValue::Blob(vec![1, 2, 3])),
            Value::Bytea(vec![1, 2, 3])
        );
    }

    #[test]
    fn cql_to_value_maps_floats() {
        // `Float`/`Double` carry IEEE-754 bit patterns; reconstruct + widen.
        assert_eq!(
            cql_to_value(&CqlValue::Double(1.5f64.to_bits())),
            Value::float(1.5)
        );
        assert_eq!(
            cql_to_value(&CqlValue::Float((-0.5f32).to_bits())),
            Value::float(-0.5)
        );
        // Zero bits decode to 0.0 (not NULL).
        assert_eq!(cql_to_value(&CqlValue::Double(0)), Value::float(0.0));
        assert_eq!(cql_to_value(&CqlValue::Float(0)), Value::float(0.0));
    }

    #[test]
    fn rel_schema_uses_the_pg_types_map_for_every_cql_type() {
        use crate::pg_types::pg_type_of;
        use ferrosa_common::cql_type::names::SCALAR_TYPES;
        let types: Vec<CqlType> = SCALAR_TYPES.to_vec();
        let names: Vec<String> = (0..types.len()).map(|i| format!("c{i}")).collect();
        let rel = rel_schema_for(&names, &types);
        let cols: Vec<&Column> = rel.columns.iter().collect();
        assert_eq!(cols.len(), types.len());
        for (col, ty) in cols.iter().zip(&types) {
            assert_eq!(col.ty, pg_type_of(ty).column_type, "{ty:?}");
        }
    }

    #[test]
    fn load_error_display_distinguishes_variants() {
        let nst = LoadError::NoSuchTable {
            keyspace: "ks".to_string(),
            table: "t".to_string(),
        };
        assert!(nst.to_string().contains("no such table"));
        assert!(nst.to_string().contains("ks.t"));
        let storage = LoadError::Storage("boom".to_string());
        assert!(storage.to_string().contains("boom"));
    }

    // ── Real-engine integration tests ─────────────────────────────────────
    //
    // These run a real `StorageEngine` against a temp directory with no S3 /
    // Docker / cluster — fully local, no `live-infra-tests` feature, no env
    // vars. The storage `TableSchema` (cell layout) and the `ferrosa_schema`
    // `TableMetadata` (column metadata the loader reads) are built to describe
    // the SAME table so the round trip is consistent.

    fn schema_config() -> SchemaConfig {
        SchemaConfig {
            hasher: PasswordHasher::Bcrypt { cost: 4 },
            password_policy: PasswordPolicy::permissive(),
            auth_method: AuthMethod::Password,
            rate_limit: RateLimitConfig::default(),
            audit_sink: Box::new(TestAuditSink::new()),
            secrets: Box::new(EnvSecretsProvider),
            mode: DeploymentMode::Development,
        }
    }

    fn superuser() -> AuthContext {
        AuthContext {
            role: "cassandra".to_string(),
            is_superuser: true,
            must_change_password: false,
        }
    }

    fn column(name: &str, kind: ColumnKind, ty: &str, position: i32) -> ColumnMetadata {
        ColumnMetadata {
            name: name.to_string(),
            kind,
            position,
            column_type: ty.to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        }
    }

    /// A `ferrosa_schema::Schema` with keyspace `ks` and table
    /// `t(id text PK, ck int CK, name text, score int)` — declared in that
    /// order — created through the public DDL API.
    fn schema_with_table() -> Schema {
        let schema = Schema::new(schema_config()).expect("schema bootstraps");
        let auth = superuser();

        schema
            .create_keyspace(
                KeyspaceMetadata {
                    name: "ks".to_string(),
                    durable_writes: true,
                    replication: ReplicationParams {
                        strategy: "SimpleStrategy".to_string(),
                        options: {
                            let mut o = HashMap::new();
                            o.insert("replication_factor".to_string(), "1".to_string());
                            o
                        },
                    },
                },
                &auth,
            )
            .expect("create keyspace");

        let mut columns = IndexMap::new();
        columns.insert(
            "id".to_string(),
            column("id", ColumnKind::PartitionKey, "text", 0),
        );
        columns.insert(
            "ck".to_string(),
            column("ck", ColumnKind::Clustering, "int", 0),
        );
        columns.insert(
            "name".to_string(),
            column("name", ColumnKind::Regular, "text", 0),
        );
        columns.insert(
            "score".to_string(),
            column("score", ColumnKind::Regular, "int", 0),
        );

        schema
            .create_table(
                TableMetadata {
                    keyspace: "ks".to_string(),
                    name: "t".to_string(),
                    id: Uuid::new_v4(),
                    columns,
                    partition_key: vec!["id".to_string()],
                    clustering_key: vec![("ck".to_string(), ClusteringOrder::Asc)],
                    params: TableParams::default(),
                    flags: HashSet::new(),
                    extensions: HashMap::new(),
                    is_system: false,
                },
                &auth,
            )
            .expect("create table");

        schema
    }

    fn engine_config(dir: &Path) -> StorageEngineConfig {
        StorageEngineConfig {
            commit_log: CommitLogConfig {
                segment_size: 256 * 1024,
                max_segment_age: Duration::from_secs(60),
                sync_strategy: SyncStrategyConfig::Batch,
                batch: Default::default(),
                log_dir: dir.join("commitlog"),
                checkpoint_dir: dir.join("commitlog"),
                archive: None,
            },
            compaction: CompactionConfig::from_env(dir.join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            local_disk_free_reserve_bytes: 0,
            flush_threshold_bytes: 4096,
            memtable_backpressure_bytes: u64::MAX,
            flush_max_age_secs: 5,
            data_dir: dir.to_path_buf(),
            index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
            auth_enabled: false,
            auth_warn: false,
            max_pending_replay_mutations_without_schema: 1024,
            memtable_num_shards: 64,
            cache_hot_window_secs: 900,
            write_verify: false,
        }
    }

    /// Storage-layer schema for `ks.t`. The cell layout must match the
    /// `ferrosa_schema` metadata: PK `id` (UTF8), CK `ck` (Int32), regular
    /// `name` (UTF8) + `score` (Int32). Storage orders static+regular cells by
    /// Cassandra's column-name comparator, so `name` < `score` => indices 0, 1.
    fn storage_schema() -> ferrosa_common::schema::TableSchema {
        use ferrosa_common::schema::{ColumnDefinition, TableSchema};
        TableSchema {
            keyspace: "ks".to_string(),
            table: "t".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![ColumnDefinition {
                name: "ck".to_string(),
                type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            }],
            static_columns: vec![],
            regular_columns: vec![
                ColumnDefinition {
                    name: "name".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
                ColumnDefinition {
                    name: "score".to_string(),
                    type_name: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
                },
            ],
            extensions: Default::default(),
        }
    }

    /// A storage `Row` for clustering value `ck`, regular `name` + `score`.
    /// Cell ordinals follow the column-name comparator (name=0, score=1).
    fn storage_row(ck: i32, name: &str, score: i32, ts: i64) -> StorageRow {
        StorageRow {
            clustering: ck.to_be_bytes().to_vec(),
            cells: vec![
                (0, CellValue::live(name.as_bytes().to_vec(), ts)),
                (1, CellValue::live(score.to_be_bytes().to_vec(), ts)),
            ],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(ts),
        }
    }

    /// Drive a provider's scan the way the server does: on a blocking thread.
    ///
    /// The bounded channel's receive blocks, which is legal on the blocking
    /// pool and a panic on an async worker — so a test that scanned inline
    /// would be testing something the server never does.
    async fn scan_rows(table: Arc<StreamingTable>) -> Vec<Row> {
        tokio::task::spawn_blocking(move || table.scan().collect())
            .await
            .expect("scan task")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn load_table_streams_rows_in_declared_order() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageEngine::new(engine_config(dir.path()), None).unwrap());
        engine.register_table(storage_schema()).unwrap();

        let schema = schema_with_table();
        let tid = TableId::new("ks", "t");

        // Two partitions, the first with two clustering rows.
        let key_a = DecoratedKey::new(PartitionKey::new(b"alpha".to_vec()));
        let key_b = DecoratedKey::new(PartitionKey::new(b"beta".to_vec()));
        engine
            .write(&tid, &key_a, storage_row(1, "ann", 10, 1000), 1000)
            .unwrap();
        engine
            .write(&tid, &key_a, storage_row(2, "amy", 20, 1001), 1001)
            .unwrap();
        engine
            .write(&tid, &key_b, storage_row(1, "bob", 30, 1002), 1002)
            .unwrap();

        let table = Arc::new(
            load_table(&engine, &schema, "ks", "t", ScanFailure::default())
                .await
                .expect("load succeeds"),
        );

        // Schema is in declared order: id, ck, name, score.
        let cols: Vec<&str> = table
            .schema()
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(cols, vec!["id", "ck", "name", "score"]);
        assert_eq!(table.schema().columns[0].ty, ColumnType::Text); // id text
        assert_eq!(table.schema().columns[1].ty, ColumnType::Int); // ck int
        assert_eq!(table.schema().columns[3].ty, ColumnType::Int); // score int

        let mut rows: Vec<Row> = scan_rows(table.clone()).await;
        assert_eq!(rows.len(), 3, "two partitions, three logical rows");

        // Sort for deterministic assertions: by (id, ck).
        rows.sort_by(|a, b| {
            let (Value::Text(ia), Value::Text(ib)) = (a.get(0), b.get(0)) else {
                panic!("id should be text");
            };
            let (Value::Int(ca), Value::Int(cb)) = (a.get(1), b.get(1)) else {
                panic!("ck should be int");
            };
            ia.cmp(ib).then(ca.cmp(cb))
        });

        // alpha/ck=1 -> ann/10
        assert_eq!(rows[0].0[0], Value::Text("alpha".to_string()));
        assert_eq!(rows[0].0[1], Value::Int(1));
        assert_eq!(rows[0].0[2], Value::Text("ann".to_string()));
        assert_eq!(rows[0].0[3], Value::Int(10));
        // alpha/ck=2 -> amy/20
        assert_eq!(rows[1].0[1], Value::Int(2));
        assert_eq!(rows[1].0[2], Value::Text("amy".to_string()));
        assert_eq!(rows[1].0[3], Value::Int(20));
        // beta/ck=1 -> bob/30
        assert_eq!(rows[2].0[0], Value::Text("beta".to_string()));
        assert_eq!(rows[2].0[2], Value::Text("bob".to_string()));
        assert_eq!(rows[2].0[3], Value::Int(30));

        engine.shutdown().unwrap();
    }

    /// Behavioral RED: construct the real query catalog before a row first
    /// enters MVCC, then apply an update and scan. The static overlay leaks amy.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn query_constructed_before_first_mvcc_change_reads_its_before_image() {
        use ferrosa_sql::Catalog;
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageEngine::new(engine_config(dir.path()), None).unwrap());
        engine.register_table(storage_schema()).unwrap();
        let schema = schema_with_table();
        let tid = TableId::new("ks", "t");
        let key = DecoratedKey::new(PartitionKey::new(b"alpha".to_vec()));
        engine
            .write(&tid, &key, storage_row(1, "ann", 10, 1000), 1000)
            .unwrap();
        let manager = Arc::new(crate::mvcc::MvccManager::default());
        let snapshot = manager.snapshot();
        let stmt = match ferrosa_sql::parse_statement("SELECT * FROM ks.t").unwrap() {
            ferrosa_sql::Statement::Select(stmt) => *stmt,
            _ => panic!("SELECT required"),
        };
        let (catalog, failure) = crate::query::load_catalog_with_mvcc(
            &engine,
            &schema,
            &stmt,
            "ks",
            Some(&manager),
            Some(&snapshot),
            None,
            None,
            None,
        )
        .await
        .unwrap();
        let before = Row::new(vec![
            Value::Text("alpha".into()),
            Value::Int(1),
            Value::Text("ann".into()),
            Value::Int(10),
        ]);
        let after = Row::new(vec![
            Value::Text("alpha".into()),
            Value::Int(1),
            Value::Text("amy".into()),
            Value::Int(20),
        ]);
        manager
            .commit(
                &manager.snapshot(),
                &HashSet::new(),
                vec![crate::mvcc::RowChange {
                    table: "ks.t".into(),
                    key: vec![Value::Text("alpha".into()), Value::Int(1)],
                    partition_key: b"alpha".to_vec(),
                    before: Some(before.clone()),
                    after: Some(after),
                }],
                || engine.write(&tid, &key, storage_row(1, "amy", 20, 2000), 2000),
            )
            .unwrap();
        let table = catalog.resolve("ks", "t").unwrap();
        let rows: Vec<Row> = tokio::task::spawn_blocking(move || table.scan().collect())
            .await
            .unwrap();
        assert!(failure.take().is_none());
        assert_eq!(rows, vec![before.clone()]);
        // A deletion after construction must be restored once on EVERY scan;
        // two self-join scans cannot consume one another's historical rows.
        let mut tombstone = storage_row(1, "", 0, 3000);
        tombstone.cells.clear();
        tombstone.deletion = DeletionTime::new(3000, 3);
        manager
            .commit(
                &manager.snapshot(),
                &HashSet::new(),
                vec![crate::mvcc::RowChange {
                    table: "ks.t".into(),
                    key: vec![Value::Text("alpha".into()), Value::Int(1)],
                    partition_key: b"alpha".to_vec(),
                    before: Some(Row::new(vec![
                        Value::Text("alpha".into()),
                        Value::Int(1),
                        Value::Text("amy".into()),
                        Value::Int(20),
                    ])),
                    after: None,
                }],
                || engine.write(&tid, &key, tombstone, 3000),
            )
            .unwrap();
        for _ in 0..2 {
            let table = catalog.resolve("ks", "t").unwrap();
            let rows: Vec<Row> = tokio::task::spawn_blocking(move || table.scan().collect())
                .await
                .unwrap();
            assert_eq!(rows, vec![before.clone()]);
        }
        let staged = vec![crate::mvcc::PgWrite(ferrosa_storage::Mutation::new(
            "ks".into(),
            "t".into(),
            key.clone(),
            vec![storage_row(1, "own", 40, 4000)],
            4000,
        ))];
        let (own_catalog, own_failure) = crate::query::load_catalog_with_mvcc(
            &engine,
            &schema,
            &stmt,
            "ks",
            Some(&manager),
            Some(&snapshot),
            Some(&staged),
            None,
            None,
        )
        .await
        .unwrap();
        let own_table = own_catalog.resolve("ks", "t").unwrap();
        let rows: Vec<Row> = tokio::task::spawn_blocking(move || own_table.scan().collect())
            .await
            .unwrap();
        assert_eq!(
            rows,
            vec![Row::new(vec![
                Value::Text("alpha".into()),
                Value::Int(1),
                Value::Text("own".into()),
                Value::Int(40)
            ])]
        );
        assert!(own_failure.take().is_none());
        // A catalog owns a lease, not a historical root: reaping invalidates it
        // and the real query error mapper preserves serialization SQLSTATE.
        manager.expire_all_snapshots_for_test();
        let table = catalog.resolve("ks", "t").unwrap();
        let rows: Vec<Row> = tokio::task::spawn_blocking(move || table.scan().collect())
            .await
            .unwrap();
        assert!(rows.is_empty());
        let message = crate::query::check_scan_failure(&failure).expect("expired scan must fail");
        match message {
            crate::messages::BackendMessage::ErrorResponse { fields } => assert!(fields
                .iter()
                .any(|(tag, value)| *tag == b'C' && value == "40001")),
            other => panic!("expected SQLSTATE40001, got {other:?}"),
        }
        let racing_snapshot = manager.snapshot();
        crate::mvcc::MvccManager::expire_after_liveness_for_test(manager.clone());
        let expired_load = crate::query::load_catalog_with_mvcc(
            &engine,
            &schema,
            &stmt,
            "ks",
            Some(&manager),
            Some(&racing_snapshot),
            Some(&staged),
            None,
            None,
        )
        .await;
        match expired_load {
            Err(crate::messages::BackendMessage::ErrorResponse { fields }) => assert!(fields
                .iter()
                .any(|(tag, value)| *tag == b'C' && value == "40001")),
            _ => {
                panic!("expiry after liveness must refuse overlay construction with SQLSTATE40001")
            }
        }
        drop(catalog);
        drop(own_catalog);
        engine.shutdown().unwrap();
    }

    /// Deterministic retained-key allocation/lifetime curve; row values are
    /// compared separately. No wall-clock performance threshold is involved.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_scan_key_capacity_grows_with_rows_and_drop_releases_it() {
        use std::sync::atomic::Ordering::Relaxed;
        let mut curve = Vec::new();
        for size in [128, 1024, 4096] {
            let dir = tempfile::tempdir().unwrap();
            let mut config = engine_config(dir.path());
            config.commit_log.segment_size = 1024 * 1024;
            let engine = Arc::new(StorageEngine::new(config, None).unwrap());
            engine.register_table(storage_schema()).unwrap();
            let schema = schema_with_table();
            let key = DecoratedKey::new(PartitionKey::new(b"alpha".to_vec()));
            engine
                .write_atomic_batch(vec![ferrosa_storage::Mutation::new(
                    "ks".into(),
                    "t".into(),
                    key,
                    (0..size).map(|i| storage_row(i, "row", i, 1000)).collect(),
                    1000,
                )])
                .unwrap();
            let baseline_failure = ScanFailure::default();
            let baseline = Arc::new(
                load_table(&engine, &schema, "ks", "t", baseline_failure.clone())
                    .await
                    .unwrap(),
            );
            let baseline_rows = scan_rows(baseline.clone()).await;
            assert_eq!(baseline_rows.len(), size as usize);
            assert_eq!(
                baseline_failure.1.peak.load(Relaxed),
                0,
                "ordinary scan keeps no emitted-key set"
            );
            let manager = crate::mvcc::MvccManager::default();
            let snapshot = manager.snapshot();
            let failure = ScanFailure::default();
            let table = Arc::new(
                load_table(&engine, &schema, "ks", "t", failure.clone())
                    .await
                    .unwrap()
                    .with_mvcc_view(manager.table_view(&snapshot, "ks.t")),
            );
            let rows = scan_rows(table.clone()).await;
            assert_eq!(rows, baseline_rows);
            assert_eq!(failure.1.peak.load(Relaxed), size as usize);
            assert_eq!(failure.1.live.load(Relaxed), 0);
            curve.push((
                size,
                baseline_failure.1.capacity.load(Relaxed),
                failure.1.capacity.load(Relaxed),
            ));
            // Model LIMIT/cancellation: consume one physical row then drop the
            // iterator while the bounded producer still owns emitted keys.
            let cancelled_table = table.clone();
            tokio::task::spawn_blocking(move || {
                let mut scan = cancelled_table.scan();
                assert!(scan.next().is_some());
                drop(scan);
            })
            .await
            .unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                while failure.1.live.load(Relaxed) != 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("cancelled producer must release its keys");
            assert!(failure.take().is_none());
            drop(table);
            drop(snapshot);
            // The producer's parameter lease drops after its ScanSeen local.
            // Wait for the actual registry state, not that earlier destructor.
            tokio::time::timeout(Duration::from_secs(5), async {
                while manager.active_snapshot_count() != 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("cancelled producer must release its lease");
            drop(baseline);
            engine.shutdown().unwrap();
        }
        if let Ok(path) = std::env::var("FERROSA_MVCC_SCAN_EVIDENCE") {
            std::fs::write(
                path,
                format!("(rows, baseline key capacity, live key capacity): {curve:?}\n"),
            )
            .unwrap();
        }
    }

    /// Behavioral RED at real catalog construction: unrelated MVCC rows must
    /// not be cloned for a read-only catalog or a catalog with one own write.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn catalog_construction_does_not_copy_unrelated_mvcc_rows() {
        use crate::mvcc::{MvccManager, PgWrite, RowChange};
        use ferrosa_sql::Catalog;
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageEngine::new(engine_config(dir.path()), None).unwrap());
        engine.register_table(storage_schema()).unwrap();
        let schema = schema_with_table();
        let stmt = match ferrosa_sql::parse_statement("SELECT * FROM ks.t").unwrap() {
            ferrosa_sql::Statement::Select(stmt) => *stmt,
            _ => panic!("SELECT required"),
        };
        let mut curve = Vec::new();
        for size in [128, 1024, 4096] {
            let manager = MvccManager::default();
            let changes = (0..size)
                .map(|i| RowChange {
                    table: "ks.t".into(),
                    key: vec![Value::Text("alpha".into()), Value::Int(i)],
                    partition_key: b"alpha".to_vec(),
                    before: None,
                    after: Some(Row::new(vec![
                        Value::Text("alpha".into()),
                        Value::Int(i),
                        Value::Text("resident".into()),
                        Value::Int(i),
                    ])),
                })
                .collect();
            manager
                .commit(&manager.snapshot(), &HashSet::new(), changes, || Ok(()))
                .unwrap();
            let snapshot = manager.snapshot();
            let staged = vec![PgWrite(ferrosa_storage::Mutation::new(
                "ks".into(),
                "t".into(),
                DecoratedKey::new(PartitionKey::new(b"alpha".to_vec())),
                vec![storage_row(1, "own", 40, 4000)],
                4000,
            ))];
            let mut counts = Vec::new();
            for writes in [None, Some(staged.as_slice())] {
                let count = MvccManager::count_allocations_for_test(|| {
                    let (catalog, _) = crate::query::load_catalog_with_mvcc(
                        &engine,
                        &schema,
                        &stmt,
                        "ks",
                        Some(&manager),
                        Some(&snapshot),
                        writes,
                        None,
                        None,
                    )
                    .now_or_never()
                    .expect("catalog metadata construction must complete immediately")
                    .unwrap();
                    assert!(catalog.resolve("ks", "t").is_some());
                });
                counts.push(count);
            }
            curve.push((size, counts));
        }
        if let Ok(path) = std::env::var("FERROSA_MVCC_CATALOG_EVIDENCE") {
            std::fs::write(path, format!("(resident rows, [read-only allocations, one-own-write allocations]): {curve:?}\n")).unwrap();
        }
        assert!(
            curve[2].1[0] < curve[0].1[0] * 4,
            "read-only allocation curve {curve:?}"
        );
        assert!(
            curve[2].1[1] < curve[0].1[1] * 4,
            "own-write allocation curve {curve:?}"
        );
        engine.shutdown().unwrap();
    }

    /// `scan()` must be repeatable.
    ///
    /// A self-join (`FROM t JOIN t`) resolves both sides to ONE provider and
    /// scans it twice. A single-shot channel would hand the second scan an
    /// empty relation — a wrong answer that no error reports. Each scan gets
    /// its own producer, so both see the whole table.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scan_is_repeatable_so_a_self_join_sees_every_row() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageEngine::new(engine_config(dir.path()), None).unwrap());
        engine.register_table(storage_schema()).unwrap();
        let schema = schema_with_table();
        let tid = TableId::new("ks", "t");

        let key_a = DecoratedKey::new(PartitionKey::new(b"alpha".to_vec()));
        let key_b = DecoratedKey::new(PartitionKey::new(b"beta".to_vec()));
        engine
            .write(&tid, &key_a, storage_row(1, "ann", 10, 1000), 1000)
            .unwrap();
        engine
            .write(&tid, &key_b, storage_row(1, "bob", 30, 1002), 1002)
            .unwrap();

        let table = Arc::new(
            load_table(&engine, &schema, "ks", "t", ScanFailure::default())
                .await
                .expect("load succeeds"),
        );

        let first = scan_rows(table.clone()).await;
        let second = scan_rows(table.clone()).await;

        assert_eq!(first.len(), 2, "first scan sees the whole table");
        assert_eq!(
            second.len(),
            2,
            "the SECOND scan must see the whole table too, not an empty relation"
        );

        engine.shutdown().unwrap();
    }

    /// A scan of a table the engine does not have registered yields zero rows
    /// and records NO failure — the empty stream is the storage layer's answer
    /// for an unregistered table, and the R15 guard above is what distinguishes
    /// that from a missing table. This pins the failure slot to real storage
    /// errors so it cannot quietly become a second existence check.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scan_records_no_failure_when_storage_is_healthy() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageEngine::new(engine_config(dir.path()), None).unwrap());
        engine.register_table(storage_schema()).unwrap();
        let schema = schema_with_table();

        let failure = ScanFailure::default();
        let table = Arc::new(
            load_table(&engine, &schema, "ks", "t", failure.clone())
                .await
                .expect("load succeeds"),
        );
        let rows = scan_rows(table).await;

        assert_eq!(rows.len(), 0);
        assert_eq!(
            failure.take(),
            None,
            "a healthy empty scan must not record a failure"
        );

        engine.shutdown().unwrap();
    }

    /// PG-Tcf7ca2cc: a partition with one corrupt simple cell fails the scan
    /// with an error that names the table and column. The corrupt row is never
    /// emitted as a row with a NULL; the healthy partition still reads.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scan_records_failure_naming_table_for_a_corrupt_cell() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageEngine::new(engine_config(dir.path()), None).unwrap());
        engine.register_table(storage_schema()).unwrap();
        let schema = schema_with_table();
        let tid = TableId::new("ks", "t");

        // `name` (ordinal 0) is text: invalid UTF-8 is corrupt. (The engine
        // rejects wrong-width fixed types at write, but not bad text.)
        let mut corrupt = storage_row(1, "bad", 0, 1000);
        corrupt.cells[0] = (0, CellValue::live(vec![0xff, 0xfe], 1000));
        let key = DecoratedKey::new(PartitionKey::new(b"corrupt".to_vec()));
        engine.write(&tid, &key, corrupt, 1000).unwrap();

        let failure = ScanFailure::default();
        let table = Arc::new(
            load_table(&engine, &schema, "ks", "t", failure.clone())
                .await
                .expect("load succeeds"),
        );
        let rows = scan_rows(table).await;

        let message = failure.take().expect("a corrupt cell must fail the scan");
        assert!(message.contains("ks.t"), "must name the table: {message}");
        assert!(message.contains("name"), "must name the column: {message}");
        assert!(
            rows.is_empty(),
            "a corrupt row must not be emitted with a NULL: {rows:?}"
        );

        engine.shutdown().unwrap();
    }

    /// The failure slot keeps the FIRST error and survives being taken.
    ///
    /// Two tables in one query share one slot; the second collapse is a
    /// consequence of the first and must not overwrite the cause.
    #[test]
    fn scan_failure_keeps_the_first_error_and_takes_once() {
        let failure = ScanFailure::default();
        assert_eq!(failure.take(), None, "empty slot takes as None");

        failure.record("first".to_string());
        failure.record("second".to_string());

        // A clone shares the slot: this is how every provider in a catalog
        // reports into the one place the query layer checks.
        let seen = failure.clone().take();
        assert_eq!(seen.as_deref(), Some("first"), "first failure wins");
        assert_eq!(failure.take(), None, "taking empties the slot");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn load_table_missing_table_is_no_such_table() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageEngine::new(engine_config(dir.path()), None).unwrap());
        let schema = schema_with_table();

        // "ghost" is not in the schema snapshot -> NoSuchTable, NOT empty Ok.
        let err = load_table(&engine, &schema, "ks", "ghost", ScanFailure::default())
            .await
            .expect_err("missing table must error");
        match err {
            LoadError::NoSuchTable { keyspace, table } => {
                assert_eq!(keyspace, "ks");
                assert_eq!(table, "ghost");
            }
            other => panic!("expected NoSuchTable, got {other:?}"),
        }

        engine.shutdown().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn load_table_existing_empty_table_is_ok_zero_rows() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(StorageEngine::new(engine_config(dir.path()), None).unwrap());
        engine.register_table(storage_schema()).unwrap();
        let schema = schema_with_table();

        // Registered + declared, but no rows written: distinct from NoSuchTable.
        let table = Arc::new(
            load_table(&engine, &schema, "ks", "t", ScanFailure::default())
                .await
                .expect("existing empty table loads ok"),
        );
        assert_eq!(table.schema().width(), 4, "schema still has all 4 columns");
        assert_eq!(
            scan_rows(table).await.len(),
            0,
            "empty table yields zero rows"
        );

        engine.shutdown().unwrap();
    }

    /// OWED RED-FIRST GUARD for the batched before-image read landed in b9900eb3.
    ///
    /// That commit replaced a point read per row with one batched read per
    /// 4096-mutation window (`prefetch_before_images` -> `read_clustering_rows_batch`)
    /// and shipped WITHOUT the differential guard it owed. This is that guard.
    ///
    /// The SAME request set is driven through BOTH reads:
    ///   * the PER-ROW path — `query::prepare_row_changes`, which calls
    ///     `read_row_image` once per row; and
    ///   * the BATCHED path — `query::prepare_accord_writes`, which resolves a whole
    ///     4096-row window of before-images against ONE store view.
    ///
    /// Their per-partition row-version metadata must be byte-identical: batching is a
    /// different read SHAPE, never a different ANSWER. The request set deliberately
    /// includes a MISSING key (a mutation whose key has no live storage row, so its
    /// before-image is `None`) and crosses the 4096-row window boundary
    /// (4096 + 64 mutations), so the batching walks two windows and must carry no
    /// overlay / before-cache state incorrectly across the seam.
    #[test]
    fn batched_before_images_match_per_row_across_a_chunk_boundary() {
        /// The prefetch window `prepare_accord_writes` walks in
        /// (`BEFORE_IMAGE_PREFETCH_CHUNK`). Hard-coded in the test on purpose: if the
        /// production window moves, this guard must be revisited rather than silently
        /// no longer crossing it.
        const CHUNK_BOUNDARY: usize = 4096;
        const TOTAL: usize = CHUNK_BOUNDARY + 64;
        const SEEDED: usize = 8;
        const TS: i64 = 5_000;

        let dir = tempfile::tempdir().unwrap();
        let engine = StorageEngine::new(engine_config(dir.path()), None).unwrap();
        engine.register_table(storage_schema()).unwrap();
        let schema = schema_with_table();
        let tid = TableId::new("ks", "t");

        // Seed a handful of PRE-EXISTING rows, so a few mutations have a live
        // before-image while the rest are genuinely absent.
        for i in 0..SEEDED {
            let key = DecoratedKey::new(PartitionKey::new(format!("k{i}").into_bytes()));
            engine
                .write(
                    &tid,
                    &key,
                    storage_row(1, "old", 100 + i as i32, 1_000),
                    1_000,
                )
                .unwrap();
        }

        // One mutation per distinct PK. The first SEEDED reuse the seeded keys (present
        // before-image); the rest are MISSING (no storage row).
        let mutations: Vec<ferrosa_storage::Mutation> = (0..TOTAL)
            .map(|i| ferrosa_storage::Mutation {
                mutation_id: [0x42u8; 16],
                keyspace: "ks".to_string(),
                table: "t".to_string(),
                key: DecoratedKey::new(PartitionKey::new(format!("k{i}").into_bytes())),
                rows: vec![storage_row(1, "new", i as i32, TS)],
                timestamp: TS,
            })
            .collect();
        assert!(
            mutations.len() > CHUNK_BOUNDARY,
            "the request set must cross the {CHUNK_BOUNDARY}-row prefetch window"
        );

        // Per-row oracle.
        let changes = crate::query::prepare_row_changes(&engine, &schema, &mutations)
            .expect("the per-row path must build row versions");
        let mut by_partition: HashMap<Vec<u8>, Vec<crate::mvcc::RowChange>> = HashMap::new();
        for change in changes {
            by_partition
                .entry(change.partition_key.clone())
                .or_default()
                .push(change);
        }

        // Batched path (4096-row prefetch windows). The TEST sink collects the streamed
        // writes; production streams them into the disk-backed staged set.
        let mut writes = Vec::new();
        let count =
            crate::query::prepare_accord_writes_streaming(&engine, &schema, &mutations, &mut |w| {
                writes.push(w);
                Ok(())
            })
            .expect("the batched path must build row versions");
        assert_eq!(
            count,
            mutations.len(),
            "one Accord write per mutation, in order"
        );

        let mut saw_present_before = false;
        let mut saw_missing_before = false;
        for (write, mutation) in writes.iter().zip(&mutations) {
            let partition_key = mutation.key.key.as_bytes().to_vec();
            assert_eq!(
                write.key, partition_key,
                "write order must follow mutation order"
            );
            let expected = by_partition
                .get(&partition_key)
                .expect("every mutation partition has a row-version entry");

            let (storage, metadata) =
                ferrosa_storage::accord::decode_postgres_mvcc_mutation(&write.mutation)
                    .expect("the batched path must emit a valid MVCC envelope");
            let metadata = metadata.expect("a data row carries row-version metadata");
            let got = crate::row_change_codec::decode_partition(metadata).unwrap();
            assert_eq!(
                &got, expected,
                "batched before-images must equal the per-row answer for {partition_key:?}"
            );

            saw_present_before |= got.iter().any(|change| change.before.is_some());
            saw_missing_before |= got.iter().any(|change| change.before.is_none());

            let mut bytes = vec![0; mutation.serialized_size()];
            mutation.serialize_into(&mut bytes);
            assert_eq!(
                storage,
                bytes.as_slice(),
                "the storage mutation must survive the batched path unchanged"
            );
        }
        assert!(
            saw_present_before,
            "the request set must include a PRESENT key (a live before-image)"
        );
        assert!(
            saw_missing_before,
            "the request set must include a MISSING key (an absent before-image)"
        );

        engine.shutdown().unwrap();
    }

    /// `prepare_accord_writes` must reuse ONE serialize buffer without ever letting a
    /// previous, larger mutation's tail leak into a smaller frame, and must MOVE the
    /// partition-key bytes out of the consumed mutation rather than copy them.
    ///
    /// The request set deliberately alternates LARGE → small → LARGE → small
    /// serialized sizes: a buffer that only truncated (instead of clearing and
    /// resizing to the exact `serialized_size`) would carry the predecessor's tail
    /// into the short frame. Each emitted mutation is compared byte-for-byte against a
    /// FRESH-buffer serialization of the same mutation, and each emitted key against
    /// the `as_bytes().to_vec()` copy the old path made.
    #[test]
    fn prepare_accord_writes_reuses_one_buffer_without_leaking_a_stale_tail() {
        const TS: i64 = 7_000;

        let dir = tempfile::tempdir().unwrap();
        let engine = StorageEngine::new(engine_config(dir.path()), None).unwrap();
        engine.register_table(storage_schema()).unwrap();
        let schema = schema_with_table();

        let widths = [64usize, 1, 200, 2];
        let mutations: Vec<ferrosa_storage::Mutation> = widths
            .iter()
            .enumerate()
            .map(|(i, width)| ferrosa_storage::Mutation {
                mutation_id: [0x5Au8; 16],
                keyspace: "ks".to_string(),
                table: "t".to_string(),
                key: DecoratedKey::new(PartitionKey::new(format!("k{i}").into_bytes())),
                rows: vec![storage_row(1, &"n".repeat(*width), 100 + i as i32, TS)],
                timestamp: TS,
            })
            .collect();

        // Fresh-buffer oracle, and the copied-key oracle the moved path must match.
        let fresh: Vec<Vec<u8>> = mutations
            .iter()
            .map(|mutation| {
                let mut bytes = vec![0; mutation.serialized_size()];
                mutation.serialize_into(&mut bytes);
                bytes
            })
            .collect();
        let expected_keys: Vec<Vec<u8>> = mutations
            .iter()
            .map(|mutation| mutation.key.key.as_bytes().to_vec())
            .collect();
        assert!(
            fresh[0].len() != fresh[1].len() && fresh[2].len() != fresh[3].len(),
            "the request set must vary in serialized size, else it proves nothing"
        );

        let mut writes = Vec::new();
        let count =
            crate::query::prepare_accord_writes_streaming(&engine, &schema, &mutations, &mut |w| {
                writes.push(w);
                Ok(())
            })
            .expect("the batched path builds row versions");
        assert_eq!(count, widths.len());
        assert_eq!(writes.len(), widths.len());

        for (index, write) in writes.iter().enumerate() {
            assert_eq!(
                write.key, expected_keys[index],
                "the MOVED partition-key bytes must equal the copied path's bytes"
            );
            let (storage, metadata) =
                ferrosa_storage::accord::decode_postgres_mvcc_mutation(&write.mutation)
                    .expect("a valid MVCC envelope");
            assert!(
                metadata.is_some(),
                "a written row carries row-version metadata"
            );
            assert_eq!(
                storage, fresh[index],
                "the reused buffer must serialize mutation {index} exactly as a fresh \
                 one would (a stale tail from the previous mutation would show here)"
            );
        }

        engine.shutdown().unwrap();
    }

    /// The PostgreSQL COMMIT write-set must NEVER be materialized.
    ///
    /// `prepare_accord_writes_streaming` used to RETURN a `Vec<TransactionWrite>` holding every
    /// encoded payload — the single P0 OOM finding (`stream-returns-vec`). It now drives a caller
    /// sink, and the production sink (`StagedAccordWriteSetBuilder`) keeps the payload bulk on
    /// disk behind a bounded, tunable buffer. This test pins the four invariants:
    ///
    /// - **Data preserved** — the staged read-back is byte-identical to the collecting oracle.
    /// - **Complete** — every mutation yields exactly one write; none is dropped.
    /// - **Ordered** — keys follow mutation order.
    /// - **Capacity** — a write-set far larger than the buffer is accepted and spilled, never
    ///   refused or truncated.
    ///
    /// Residency is asserted STRUCTURALLY (resident bytes ≤ buffer; the bulk spilled), which is
    /// load-independent and safe for the per-PR gate. An absolute byte bound is deliberately
    /// NOT here — that measures the runner, not the code; it lives behind the nightly perf gate.
    ///
    /// The negative control at the end reproduces the REMOVED shape (a collecting sink) and shows
    /// it holds the whole write-set, so the bounded-residency assertion is not vacuous.
    #[test]
    fn accord_write_set_is_never_materialized_and_preserves_every_write() {
        const TS: i64 = 9_000;
        const TOTAL: usize = 5_000;
        const BUFFER: u64 = 1024;

        let dir = tempfile::tempdir().unwrap();
        let engine = StorageEngine::new(engine_config(dir.path()), None).unwrap();
        engine.register_table(storage_schema()).unwrap();
        let schema = schema_with_table();

        let mutations: Vec<ferrosa_storage::Mutation> = (0..TOTAL)
            .map(|i| ferrosa_storage::Mutation {
                mutation_id: [0x33u8; 16],
                keyspace: "ks".to_string(),
                table: "t".to_string(),
                key: DecoratedKey::new(PartitionKey::new(format!("k{i:08}").into_bytes())),
                rows: vec![storage_row(1, &"v".repeat(1 + i % 64), i as i32, TS)],
                timestamp: TS,
            })
            .collect();

        // Production sink: a bounded, disk-backed staged write-set builder.
        let mut builder =
            ferrosa_storage::accord::StagedAccordWriteSetBuilder::with_threshold_bytes(BUFFER);
        let count = crate::query::prepare_accord_writes_streaming(
            &engine,
            &schema,
            &mutations,
            &mut |write| builder.accept(write),
        )
        .expect("the streaming build must succeed");
        let staged = builder.finish().expect("staging must finish");

        // Complete: one write per mutation, none dropped.
        assert_eq!(count, TOTAL, "one Accord write per mutation");
        assert_eq!(staged.len(), TOTAL, "no write is dropped");

        // Ordered: keys follow mutation order.
        for (index, (keyspace, key)) in staged.keys().iter().enumerate() {
            assert_eq!(keyspace, "ks");
            assert_eq!(
                key.as_slice(),
                format!("k{index:08}").as_bytes(),
                "write order must follow mutation order"
            );
        }

        // Residency (structural): the payload bulk left memory; the resident buffer is bounded.
        assert!(
            staged.payloads_arc().resident_bytes() <= BUFFER,
            "resident payload must stay within the tunable buffer"
        );
        assert!(
            staged.payloads_arc().spilled_to_disk(),
            "a write-set far larger than the buffer must spill, not stay resident"
        );

        // Data preserved: differential against the collecting oracle over the same input.
        let mut collected: Vec<ferrosa_storage::accord::TransactionWrite> = Vec::new();
        let collected_count = crate::query::prepare_accord_writes_streaming(
            &engine,
            &schema,
            &mutations,
            &mut |write| {
                collected.push(write);
                Ok(())
            },
        )
        .expect("the collecting build must succeed");
        assert_eq!(collected_count, count);
        assert_eq!(
            staged.to_resident().expect("materialize the staged set"),
            collected,
            "the staged path must deliver byte-identical writes to the collecting path"
        );

        // NEGATIVE CONTROL: the collecting sink holds the whole write-set — the removed shape — so
        // the bounded-residency assertion above is measuring the streaming, not a constant.
        let collected_bytes: usize = collected
            .iter()
            .map(|write| write.key.len() + write.mutation.len())
            .sum();
        assert!(
            collected_bytes as u64 > BUFFER,
            "the control must exceed the buffer, else the guard is vacuous: {collected_bytes} bytes"
        );

        engine.shutdown().unwrap();
    }
}
