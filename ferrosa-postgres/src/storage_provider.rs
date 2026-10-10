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
pub struct ScanFailure(Arc<Mutex<Option<String>>>);

impl ScanFailure {
    /// Record `message` unless a failure is already recorded.
    ///
    /// Poisoning is impossible in practice (the guarded value is a `String` and
    /// nothing panics while held), but recovering rather than unwrapping means a
    /// poisoned lock still reports the error instead of masking it with a panic
    /// on the way out.
    pub fn record(&self, message: String) {
        let mut slot = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if slot.is_none() {
            *slot = Some(message);
        }
    }

    /// Take the recorded failure, if any, leaving the slot empty.
    pub fn take(&self) -> Option<String> {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).take()
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
}

/// Drain `range_iter` into `tx`, decoding one partition at a time.
///
/// Returns — releasing the storage stream and the engine handle — on any of the
/// three exits: the stream ends, storage errors (recorded in `failure` first),
/// or the consumer drops its receiver. Nothing is left running behind a
/// cancelled or short-circuited query.
async fn produce_scan(ctx: Arc<ScanContext>, tx: mpsc::Sender<Row>, failure: ScanFailure) {
    let mut stream = ctx.engine.range_iter(&ctx.table_id, None, None);
    let mut overlay_seen = std::collections::HashSet::new();
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
            let values: Vec<Value> = match cql_row
                .iter()
                .map(|cell| match cell {
                    Some(value) => cql_to_value(value),
                    None => Ok(Value::Null),
                })
                .collect()
            {
                Ok(values) => values,
                Err(error) => {
                    failure.record(format!(
                        "scan of {}.{} cannot represent a stored value: {error}",
                        ctx.table_id.keyspace, ctx.table_id.table
                    ));
                    return;
                }
            };
            let key: Vec<Value> = ctx
                .pk_idx
                .iter()
                .chain(ctx.ck_idx.iter())
                .map(|index| values[*index].clone())
                .collect();
            // `row.clone()` here is REQUIRED, not wasteful. `ctx.overlay` is an
            // `Arc` shared by every scan in one query (a self-join opens two
            // independent scans over the same overlay), so the substituted row
            // must be CLONED out — mutating the overlay (`remove`) to move the
            // row would make the second scan miss a restored historical row and
            // silently return a wrong result. The key clone is likewise needed
            // because `values` is MOVED into `Row::new` on the no-overlay path.
            // Both copies are sparse (one per MVCC-changed key, not per table
            // row) and are the documented, correctness-required exceptions
            // (t_110dd8a5).
            let row = match ctx.overlay.get(&key) {
                Some(Some(row)) => {
                    overlay_seen.insert(key);
                    row.clone()
                }
                Some(None) => {
                    overlay_seen.insert(key);
                    continue;
                }
                None => Row::new(values),
            };
            if tx.send(row).await.is_err() {
                // The consumer is gone: the executor short-circuited, errored,
                // or the connection dropped. Not a failure — stop producing.
                return;
            }
        }
    }
    // A snapshot can contain a row deleted from current storage, or omit a row
    // inserted after its read timestamp. Re-add historical rows absent from the
    // latest storage scan; the overlay remains sparse and table scans stay
    // streamed.
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
            produce_scan(self.ctx.clone(), tx, self.failure.clone()),
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
    )
    .await
}

pub(crate) async fn load_table_with_overlay(
    engine: &Arc<StorageEngine>,
    schema: &Schema,
    keyspace: &str,
    table: &str,
    failure: ScanFailure,
    overlay: std::collections::HashMap<Vec<Value>, Option<Row>>,
    scan_buffer_rows: usize,
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
        }),
        handle: Handle::current(),
        failure,
        scan_buffer_rows,
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
    apply_pending_writes_with_partition_keys(
        engine,
        schema,
        keyspace,
        table,
        overlay,
        writes.iter().map(|w| &w.0),
        None,
    )
}

///
/// `writes` is an iterator of `&Mutation` (not `&[PgWrite]`) so a caller that
/// already owns the mutations — the transactional-`COPY` commit's
/// `prepare_row_changes` — does not have to CLONE the whole write-set into a
/// throwaway `Vec<PgWrite>` first. That clone was a full extra copy of every
/// buffered row (a megabyte-per-thousand-rows of pure waste on the commit's
/// peak), since this function makes a single pass.
pub(crate) fn apply_pending_writes_with_partition_keys<'a, I>(
    engine: &StorageEngine,
    schema: &Schema,
    keyspace: &str,
    table: &str,
    overlay: &mut std::collections::HashMap<Vec<Value>, Option<Row>>,
    writes: I,
    mut partition_keys: Option<&mut std::collections::HashMap<Vec<Value>, Vec<u8>>>,
) -> Result<(), String>
where
    I: IntoIterator<Item = &'a ferrosa_storage::Mutation>,
{
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
            ferrosa_row_bridge::parse_cql_type_in_keyspace(&column.column_type, keyspace, schema)
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    let pk = pk_indices(meta);
    let ck = ck_indices(meta);
    let storage_to_table = storage_to_table_indices(meta);

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
        let pk_parts = ferrosa_row_bridge::decode_pk(&mutation.key, pk.len());
        for mutation_row in &mutation.rows {
            let ck_parts =
                ferrosa_row_bridge::decode_clustering(&mutation_row.clustering, ck.len());
            let mut key = Vec::with_capacity(pk.len() + ck.len());
            let mut values = vec![Value::Null; names.len()];
            for (index, part) in pk.iter().zip(pk_parts.iter()) {
                let cql = ferrosa_row_bridge::decode_value(&types[*index], part)
                    .map_err(|error| error.to_string())?;
                let value = cql_to_value(&cql)?;
                key.push(value.clone());
                values[*index] = value;
            }
            for (index, part) in ck.iter().zip(ck_parts.iter()) {
                let cql = ferrosa_row_bridge::decode_value(&types[*index], part)
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
            } else {
                read_row_image(engine, schema, mutation, &mutation_row.clustering)?
                    .map(|(_, row)| row)
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
                let Some(table_index) = storage_to_table.get(storage_index).copied() else {
                    continue;
                };
                values[table_index] = match &cell.value {
                    Some(bytes) => {
                        let cql = ferrosa_row_bridge::decode_value(&types[table_index], bytes)
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
pub(crate) fn read_row_image(
    engine: &StorageEngine,
    schema: &Schema,
    mutation: &ferrosa_storage::Mutation,
    clustering: &[u8],
) -> Result<Option<(Vec<Value>, Row)>, String> {
    let snapshot = schema.snapshot();
    let meta = snapshot
        .tables
        .get(&(mutation.keyspace.clone(), mutation.table.clone()))
        .ok_or_else(|| {
            format!(
                "table {}.{} is not registered",
                mutation.keyspace, mutation.table
            )
        })?;
    let names: Vec<String> = meta.columns.keys().cloned().collect();
    let types = meta
        .columns
        .values()
        .map(|column| {
            ferrosa_row_bridge::parse_cql_type_in_keyspace(
                &column.column_type,
                &mutation.keyspace,
                schema,
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    let pk = pk_indices(meta);
    let ck = ck_indices(meta);
    let storage_to_table = storage_to_table_indices(meta);
    let Some(partition) = engine
        .read_clustering_row(
            &TableId::new(&mutation.keyspace, &mutation.table),
            &mutation.key,
            clustering,
        )
        .map_err(|error| error.to_string())?
    else {
        return Ok(None);
    };
    let rows = ferrosa_row_bridge::partition_to_rows_with_clustering(
        &partition,
        &names,
        &types,
        &pk,
        &ck,
        &storage_to_table,
    )
    .map_err(|error| {
        error
            .in_table(format!("{}.{}", mutation.keyspace, mutation.table))
            .to_string()
    })?;
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
    let key = pk
        .iter()
        .chain(ck.iter())
        .map(|index| values[*index].clone())
        .collect();
    Ok(Some((key, Row::new(values))))
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
}
