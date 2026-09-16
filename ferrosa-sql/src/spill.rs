//! Module: Spill adapter — bounded-memory backing for the relational engine's
//! BLOCKING operators (`sort`, `hash_aggregate`, `hash_join`, DISTINCT dedup).
//! Correctness: correct when the orders defined here rank rows EXACTLY as the
//!   in-memory operators did — [`SqlOrder::Sql`] must match `exec::order_cmp`
//!   and [`SqlOrder::Canonical`] must agree with `Value`'s structural `Eq`
//!   (equal iff `Ordering::Equal`), so switching an operator onto the spilling
//!   path changes its memory behavior and nothing observable.
//! Last revised: 2026-09-15
//! Last changed: Created for forge t_50d99192.
//!
//! # Why
//!
//! A blocking operator cannot emit its first output row before it has consumed
//! its whole input, so pipelining is unavailable to it — which is precisely why
//! it must SPILL. The previous operators buffered everything: `sort` held the
//! full input, `hash_aggregate` held a group table up to input size, and
//! `hash_join` materialized the ENTIRE right stream into a build map *and*
//! accumulated every output row, so a skewed key made peak memory
//! `O(left x right)`.
//!
//! The owner decision on t_50d99192 is explicit: the fix is spill, **not** a
//! cap. A bound on a RESULT turns a legitimate query into a failure, so the
//! invariant each operator now holds is
//!
//! > given an input larger than the in-memory threshold, the operator returns
//! > EVERY row via spill — never truncating, never refusing a query it could
//! > have answered.
//!
//! # What this reuses
//!
//! Nothing here reimplements sorting or merging. [`ferrosa_storage::external_sort`]
//! already provides the bounded external merge sort ferrosa-cql and
//! ferrosa-graph both use: accumulate to a byte threshold, spill sorted runs to
//! a temp dir, cascade-merge down to a bounded fan-in, k-way merge on finish,
//! and fail loud on any spill/merge I/O error. The temp dir is held by a
//! [`ferrosa_storage::TempSortTableReservation`] whose `Drop` removes it, so a
//! cancelled query cleans up exactly like a completed one.
//!
//! # Work bounds vs result bounds
//!
//! Nothing in this module bounds a RESULT. The only knob is
//! [`SpillCtx::threshold_bytes`], which bounds *resident memory before a
//! spill* — a work bound. It is deliberately not named after rows and is never
//! reused as a row cap.

use std::cmp::Ordering;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime};

use ferrosa_storage::external_sort::{ExternalSorter, SortedRows, SpillOrder, SpillRow};
use ferrosa_storage::TempSortTableReservation;
use serde::{Deserialize, Serialize};

use crate::exec::{order_cmp, SortKey};
use crate::types::{Row, Value};

/// Env var naming the directory spilled query state is written under. A node
/// that wants `<data_dir>/tmp` sets this (or injects its own [`SpillReserver`]).
pub const ENV_TEMP_DIR: &str = "FERROSA_SQL_TEMP_DIR";

/// Age past which a leftover temp directory is considered orphaned by the
/// startup sweep. Comfortably longer than any live query, so the sweep can
/// never delete a directory another running query still owns.
pub const ORPHAN_SWEEP_AGE: Duration = Duration::from_secs(60 * 60);

/// A spill or merge failure. Every variant is loud: the engine surfaces it to
/// the client rather than returning a short result, because a dropped run would
/// silently lose rows — strictly worse than the cap this replaced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpillError {
    /// Which operator hit it (`sort`, `hash_aggregate`, `hash_join`, `dedup`).
    pub operator: &'static str,
    /// What went wrong, with the underlying I/O context.
    pub detail: String,
}

impl SpillError {
    pub fn new(operator: &'static str, detail: impl Into<String>) -> Self {
        Self {
            operator,
            detail: detail.into(),
        }
    }
}

impl std::fmt::Display for SpillError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} spill failed: {}", self.operator, self.detail)
    }
}

impl std::error::Error for SpillError {}

// ---------------------------------------------------------------------------
// The spillable row
// ---------------------------------------------------------------------------

/// One row travelling through a spilling operator, tagged with the position(s)
/// it came from.
///
/// The tags are what let a sort-based operator reproduce the in-memory
/// operator's OUTPUT order exactly. `hash_aggregate` must emit groups in
/// first-seen order and `dedup` must keep first-occurrence order, neither of
/// which is the sort order the operator internally needs; `hash_join` emits in
/// left-input order. Each therefore sorts by what the algorithm needs, then
/// restores order with a second sort on `(seq, seq2)`.
///
/// `seq2` is only used by the join (the right-hand row's position); every other
/// operator leaves it zero.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SeqRow {
    pub seq: u64,
    pub seq2: u64,
    pub row: Row,
}

impl SeqRow {
    pub fn new(seq: u64, row: Row) -> Self {
        Self { seq, seq2: 0, row }
    }
}

impl SpillRow for SeqRow {
    /// Approximate footprint for threshold accounting. Need not be exact — only
    /// monotonic in real memory use, so the buffer spills before the process is
    /// starved.
    fn estimated_bytes(&self) -> usize {
        2 * std::mem::size_of::<u64>() + row_bytes(&self.row)
    }
}

/// Bytes a row occupies: the `Value` slots plus each value's heap payload.
pub fn row_bytes(row: &Row) -> usize {
    row.0.len() * std::mem::size_of::<Value>()
        + row.0.iter().map(value_payload_bytes).sum::<usize>()
}

/// Payload bytes of a `Value` beyond its fixed enum size.
fn value_payload_bytes(v: &Value) -> usize {
    match v {
        Value::Text(s) => s.len(),
        Value::Bytea(b) => b.len(),
        // A BigInt's magnitude is a Vec<u32>; four bytes per 32-bit limb.
        Value::Numeric { unscaled, .. } => unscaled.iter_u32_digits().count() * 4,
        _ => 0,
    }
}

// ---------------------------------------------------------------------------
// Orders
// ---------------------------------------------------------------------------

/// A **total, type-aware** order on values, consistent with `Value`'s
/// structural `Eq`: `canonical_cmp(a, b) == Equal` exactly when `a == b`.
///
/// This is what grouping and DISTINCT need, and it is NOT the SQL comparator.
/// `Value::sql_cmp` returns UNKNOWN for mismatched types, which the ORDER BY
/// comparator folds to `Equal` — sorting group keys that way would merge
/// `Int(1)` and `Text("1")` into one group, silently changing results. Ordering
/// by type tag first keeps every distinct key distinct.
///
/// Group/DISTINCT output order never depends on this: both operators restore
/// their original order with a second sort on the sequence tag.
pub fn canonical_cmp(a: &Value, b: &Value) -> Ordering {
    let (ta, tb) = (type_tag(a), type_tag(b));
    if ta != tb {
        return ta.cmp(&tb);
    }
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Int(x), Value::Int(y)) => x.cmp(y),
        (Value::Text(x), Value::Text(y)) => x.cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        // `OrderedFloat` is a total order (NaN included) and its `Eq` matches.
        (Value::Float(x), Value::Float(y)) => x.cmp(y),
        (Value::Uuid(x), Value::Uuid(y)) => x.cmp(y),
        (Value::Bytea(x), Value::Bytea(y)) => x.cmp(y),
        (Value::Timestamp(x), Value::Timestamp(y)) => x.cmp(y),
        (Value::Date(x), Value::Date(y)) => x.cmp(y),
        (Value::Time(x), Value::Time(y)) => x.cmp(y),
        (Value::Inet(x), Value::Inet(y)) => x.cmp(y),
        // `Value::numeric` stores normalized pairs, so structural equality on
        // `(unscaled, scale)` IS numeric equality — which is the property this
        // order has to agree with.
        (
            Value::Numeric {
                unscaled: ux,
                scale: sx,
            },
            Value::Numeric {
                unscaled: uy,
                scale: sy,
            },
        ) => ux.cmp(uy).then(sx.cmp(sy)),
        // Unreachable: equal tags imply the same variant.
        _ => Ordering::Equal,
    }
}

/// Discriminant ordinal, so different variants never compare `Equal`.
fn type_tag(v: &Value) -> u8 {
    match v {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Int(_) => 2,
        Value::Float(_) => 3,
        Value::Numeric { .. } => 4,
        Value::Text(_) => 5,
        Value::Bytea(_) => 6,
        Value::Uuid(_) => 7,
        Value::Timestamp(_) => 8,
        Value::Date(_) => 9,
        Value::Time(_) => 10,
        Value::Inet(_) => 11,
    }
}

/// Lexicographic [`canonical_cmp`] over the given columns (or the whole row).
fn canonical_cols_cmp(a: &Row, b: &Row, cols: Option<&[usize]>) -> Ordering {
    match cols {
        Some(cols) => {
            for &c in cols {
                let ord = canonical_cmp(&a.0[c], &b.0[c]);
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            Ordering::Equal
        }
        None => {
            for (x, y) in a.0.iter().zip(b.0.iter()) {
                let ord = canonical_cmp(x, y);
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            a.0.len().cmp(&b.0.len())
        }
    }
}

/// How a spilling operator orders the rows it is sorting.
///
/// Every variant breaks ties on `(seq, seq2)`. That makes each order **total**,
/// which is what makes the spilling sort deterministic and stable regardless of
/// how the k-way merge resolves equal keys: a stable in-memory `sort_by`
/// preserved input order for free, and the tie-break is how that guarantee
/// survives being split across run files.
#[derive(Debug, Clone)]
pub enum SqlOrder {
    /// `ORDER BY`: the SQL comparator (`NULLS LAST` ascending, `NULLS FIRST`
    /// descending, UNKNOWN treated as equal) over the given keys.
    Sql(Vec<SortKey>),
    /// Grouping / DISTINCT: [`canonical_cmp`] over the given columns, or the
    /// whole row when `None`.
    Canonical(Option<Vec<usize>>),
    /// Restore the producer's original order.
    Seq,
}

impl SpillOrder<SeqRow> for SqlOrder {
    fn compare(&self, a: &SeqRow, b: &SeqRow) -> Ordering {
        let primary = match self {
            SqlOrder::Sql(keys) => {
                let mut ord = Ordering::Equal;
                for k in keys {
                    ord = order_cmp(&a.row.0[k.col], &b.row.0[k.col], k.dir);
                    if ord != Ordering::Equal {
                        break;
                    }
                }
                ord
            }
            SqlOrder::Canonical(cols) => canonical_cols_cmp(&a.row, &b.row, cols.as_deref()),
            SqlOrder::Seq => Ordering::Equal,
        };
        primary.then(a.seq.cmp(&b.seq)).then(a.seq2.cmp(&b.seq2))
    }
}

// ---------------------------------------------------------------------------
// Reservations, stats, context
// ---------------------------------------------------------------------------

/// Reserves a cancellable temp directory for one spilling operator.
///
/// Injected so a node can place query temp state wherever it wants — the
/// operator decisions on t_2487eeb7 require the location to be configurable per
/// node, defaulting to `<data_dir>/tmp`. The returned reservation's `Drop`
/// removes the directory, so a cancelled, failed or completed query all clean up
/// through the same path.
pub trait SpillReserver: Send + Sync + std::fmt::Debug {
    fn reserve(&self, label: &'static str) -> Result<TempSortTableReservation, SpillError>;
}

/// The default reserver: one subdirectory per operator under a root directory.
#[derive(Debug, Clone)]
pub struct DirReserver {
    root: PathBuf,
}

impl DirReserver {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
}

impl SpillReserver for DirReserver {
    fn reserve(&self, label: &'static str) -> Result<TempSortTableReservation, SpillError> {
        std::fs::create_dir_all(&self.root).map_err(|e| {
            SpillError::new(
                label,
                format!("create temp root {}: {e}", self.root.display()),
            )
        })?;
        let path = self.root.join(format!(
            "{label}-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&path).map_err(|e| {
            SpillError::new(label, format!("create temp dir {}: {e}", path.display()))
        })?;
        Ok(TempSortTableReservation::claim_dir(path))
    }
}

/// Observability for the spill path: whether it engaged, how much was resident
/// at peak, and which directories were reserved.
///
/// `max_resident_rows` is the checkable form of "bounded peak" — it is what the
/// operator invariant tests assert against the number of rows processed.
#[derive(Debug, Default)]
pub struct SpillStats {
    spilled: AtomicBool,
    runs: AtomicU64,
    max_resident_rows: AtomicUsize,
    reserved: Mutex<Vec<PathBuf>>,
}

impl SpillStats {
    /// Whether any operator spilled a run to disk.
    pub fn spilled(&self) -> bool {
        self.spilled.load(AtomicOrdering::Relaxed)
    }

    /// Total run files written across this context's operators.
    pub fn runs(&self) -> u64 {
        self.runs.load(AtomicOrdering::Relaxed)
    }

    /// Peak rows held in memory by any single operator.
    pub fn max_resident_rows(&self) -> usize {
        self.max_resident_rows.load(AtomicOrdering::Relaxed)
    }

    /// Temp directories reserved through this context (cleanup assertions).
    pub fn reserved_paths(&self) -> Vec<PathBuf> {
        self.reserved.lock().expect("stats lock").clone()
    }

    fn note_spilled(&self, runs: u64) {
        self.spilled.store(true, AtomicOrdering::Relaxed);
        self.runs.store(runs, AtomicOrdering::Relaxed);
    }

    fn note_resident(&self, rows: usize) {
        self.max_resident_rows
            .fetch_max(rows, AtomicOrdering::Relaxed);
    }

    fn note_reserved(&self, path: PathBuf) {
        self.reserved.lock().expect("stats lock").push(path);
    }
}

/// Everything a spilling operator needs: where to put temp state, how many
/// bytes it may hold before spilling, and where to report what it did.
///
/// Cheap to clone (both fields are `Arc`), so operators keep their own handle.
#[derive(Debug, Clone)]
pub struct SpillCtx {
    reserver: Arc<dyn SpillReserver>,
    /// The single WORK bound in the engine's blocking operators: resident bytes
    /// before a spill. It bounds memory, never rows — no result is ever capped.
    threshold_bytes: u64,
    stats: Arc<SpillStats>,
}

impl SpillCtx {
    pub fn new(reserver: Arc<dyn SpillReserver>, threshold_bytes: u64) -> Self {
        Self {
            reserver,
            threshold_bytes: threshold_bytes.max(1),
            stats: Arc::new(SpillStats::default()),
        }
    }

    pub fn threshold_bytes(&self) -> u64 {
        self.threshold_bytes
    }

    pub fn stats(&self) -> &SpillStats {
        &self.stats
    }

    /// Reserve a cancellable temp directory for `label`'s spilled state.
    pub fn reserve(&self, label: &'static str) -> Result<TempSortTableReservation, SpillError> {
        let reservation = self.reserver.reserve(label)?;
        self.stats.note_reserved(reservation.path().to_path_buf());
        Ok(reservation)
    }
}

impl Default for SpillCtx {
    /// The process-wide default: spill under [`default_temp_root`], at the
    /// storage engine's own detected spill threshold (`FERROSA_RANGE_SPILL_*`).
    ///
    /// Constructing it also runs the one-shot orphan sweep, so a node that
    /// restarted mid-query does not leave its temp tables behind forever.
    fn default() -> Self {
        let root = default_temp_root();
        sweep_orphans_once(&root);
        Self::new(
            Arc::new(DirReserver::new(root)),
            ferrosa_storage::process_spill_threshold_bytes(),
        )
    }
}

/// Where query temp state goes by default: `$FERROSA_SQL_TEMP_DIR` when set,
/// else `<system temp>/ferrosa-sql`. A node overrides it either through that
/// variable or by injecting its own [`SpillReserver`] pointed at `<data_dir>/tmp`.
pub fn default_temp_root() -> PathBuf {
    match std::env::var(ENV_TEMP_DIR) {
        Ok(dir) if !dir.trim().is_empty() => PathBuf::from(dir),
        _ => std::env::temp_dir().join("ferrosa-sql"),
    }
}

/// Run [`sweep_orphaned_temp_dirs`] at most once per process, logging what it
/// reclaimed (or why it could not).
fn sweep_orphans_once(root: &Path) {
    static SWEPT: OnceLock<()> = OnceLock::new();
    SWEPT.get_or_init(|| match sweep_orphaned_temp_dirs(root, ORPHAN_SWEEP_AGE) {
        Ok(0) => {}
        Ok(n) => tracing::info!(
            removed = n,
            root = %root.display(),
            "ferrosa-sql: swept orphaned query temp directories left by a previous run"
        ),
        Err(e) => tracing::warn!(
            root = %root.display(),
            %e,
            "ferrosa-sql: could not sweep orphaned query temp directories"
        ),
    });
}

/// Remove temp directories under `root` older than `older_than`, returning how
/// many were reclaimed.
///
/// This is the restart sweep. Age is the discriminator on purpose: a directory
/// younger than the threshold may belong to a query running in another process
/// sharing the root, and deleting a live query's runs would lose rows. A
/// missing root is not an error — there is simply nothing to reclaim.
pub fn sweep_orphaned_temp_dirs(root: &Path, older_than: Duration) -> Result<usize, SpillError> {
    let entries = match std::fs::read_dir(root) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => {
            return Err(SpillError::new(
                "sweep",
                format!("read temp root {}: {e}", root.display()),
            ))
        }
    };
    let now = SystemTime::now();
    let mut removed = 0usize;
    for entry in entries {
        let entry =
            entry.map_err(|e| SpillError::new("sweep", format!("read {}: {e}", root.display())))?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let age = entry
            .metadata()
            .and_then(|m| m.modified())
            .ok()
            .and_then(|m| now.duration_since(m).ok());
        match age {
            Some(age) if age >= older_than => {
                // A failure to reclaim one orphan must not abort the sweep, but
                // it is never silent: the next restart tries again and the
                // warning says which directory is stuck.
                if let Err(e) = std::fs::remove_dir_all(&path) {
                    tracing::warn!(path = %path.display(), %e, "ferrosa-sql: orphan sweep could not remove temp dir");
                } else {
                    removed += 1;
                }
            }
            _ => {}
        }
    }
    Ok(removed)
}

// ---------------------------------------------------------------------------
// The spilling sort
// ---------------------------------------------------------------------------

/// A push-per-row, spill-backed sort.
///
/// Producers hand rows over AS THEY ARE PRODUCED, so no caller ever builds the
/// interim `Vec` the operators used to take. Memory is bounded by
/// [`SpillCtx::threshold_bytes`] independently of how many rows pass through.
pub struct SpillSort {
    reservation: TempSortTableReservation,
    sorter: ExternalSorter<SeqRow, SqlOrder>,
    stats: Arc<SpillStats>,
    label: &'static str,
    buffered: usize,
    next_seq: u64,
}

impl std::fmt::Debug for SpillSort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpillSort")
            .field("label", &self.label)
            .field("temp_dir", &self.reservation.path())
            .field("rows_pushed", &self.next_seq)
            .finish()
    }
}

impl SpillSort {
    /// Reserve temp state and start sorting under `order`.
    pub fn new(ctx: &SpillCtx, order: SqlOrder, label: &'static str) -> Result<Self, SpillError> {
        let reservation = ctx.reserve(label)?;
        let sorter = ExternalSorter::new(reservation.path(), order, ctx.threshold_bytes());
        Ok(Self {
            reservation,
            sorter,
            stats: Arc::clone(&ctx.stats),
            label,
            buffered: 0,
            next_seq: 0,
        })
    }

    /// Push one row, tagging it with its arrival position.
    pub fn push(&mut self, row: Row) -> Result<(), SpillError> {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.push_tagged(SeqRow::new(seq, row))
    }

    /// Push a row that already carries the tags the caller wants preserved.
    pub fn push_tagged(&mut self, item: SeqRow) -> Result<(), SpillError> {
        let runs_before = self.sorter.run_count();
        self.sorter
            .push(item)
            .map_err(|e| SpillError::new(self.label, format!("write run: {e}")))?;
        if self.sorter.run_count() > runs_before {
            // The buffer just spilled and is empty again.
            self.buffered = 0;
            self.stats.note_spilled(self.sorter.run_count() as u64);
        } else {
            self.buffered += 1;
            self.stats.note_resident(self.buffered);
        }
        Ok(())
    }

    /// How many rows have been pushed so far.
    pub fn pushed(&self) -> u64 {
        self.next_seq
    }

    /// Finish the sort and stream the merged rows WITHOUT re-materializing
    /// them. The reservation moves into the stream, so spilled runs live exactly
    /// as long as a consumer can still pull from them and are removed when the
    /// stream is dropped — exhausted, cancelled or abandoned alike.
    pub fn finish(self) -> Result<SpillSortStream, SpillError> {
        let spilled = self.sorter.spilled();
        let runs = self.sorter.run_count();
        let temp_dir = self.reservation.path().display().to_string();
        let sorted = self
            .sorter
            .finish()
            .map_err(|e| SpillError::new(self.label, format!("merge runs: {e}")))?;
        if spilled {
            tracing::info!(
                operator = self.label,
                rows = self.next_seq,
                runs,
                temp_sort_table = %temp_dir,
                "ferrosa-sql: blocking operator spilled to a cancellable temp-sort table"
            );
        }
        Ok(SpillSortStream {
            _reservation: self.reservation,
            label: self.label,
            inner: sorted,
        })
    }
}

/// The merged output of a [`SpillSort`], streamed one row at a time.
pub struct SpillSortStream {
    /// Dropped with the stream; its `Drop` removes the temp directory.
    _reservation: TempSortTableReservation,
    label: &'static str,
    inner: SortedRows<SeqRow, SqlOrder>,
}

impl Iterator for SpillSortStream {
    type Item = Result<SeqRow, SpillError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|r| r.map_err(|e| SpillError::new(self.label, format!("read merged run: {e}"))))
    }
}

// ---------------------------------------------------------------------------
// The replayable buffer (join inner side)
// ---------------------------------------------------------------------------

/// A bounded, **replayable** row buffer: rows are pushed once and iterated any
/// number of times, in push order.
///
/// The join needs this and a sorter cannot provide it. For one join key the
/// operator must emit the full cross product of that key's left and right rows,
/// so the right-hand group has to be read once per left row. Holding it in a
/// `Vec` is exactly the skew case that made peak memory `O(left x right)`; once
/// the group crosses the threshold this writes it to a run file inside the
/// query's reservation and replays it from there.
pub struct ReplayBuffer {
    dir: PathBuf,
    label: &'static str,
    threshold_bytes: u64,
    stats: Arc<SpillStats>,
    mem: Arc<Vec<SeqRow>>,
    mem_bytes: u64,
    /// Set once the group crossed the threshold; `mem` is then empty and every
    /// row lives in this file.
    file: Option<PathBuf>,
    writer: Option<BufWriter<std::fs::File>>,
    len: usize,
    generation: usize,
}

impl ReplayBuffer {
    /// Buffer rows inside `dir` (the operator's own reservation).
    pub fn new(dir: &Path, ctx: &SpillCtx, label: &'static str) -> Self {
        Self {
            dir: dir.to_path_buf(),
            label,
            threshold_bytes: ctx.threshold_bytes(),
            stats: Arc::clone(&ctx.stats),
            mem: Arc::new(Vec::new()),
            mem_bytes: 0,
            file: None,
            writer: None,
            len: 0,
            generation: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Drop every buffered row and any file backing them, ready for the next
    /// key group. Fails loud if the file cannot be removed — a stale run left
    /// behind would be replayed into the next group's output.
    pub fn clear(&mut self) -> Result<(), SpillError> {
        self.mem = Arc::new(Vec::new());
        self.mem_bytes = 0;
        self.writer = None;
        if let Some(path) = self.file.take() {
            std::fs::remove_file(&path).map_err(|e| {
                SpillError::new(
                    self.label,
                    format!("remove group run {}: {e}", path.display()),
                )
            })?;
        }
        self.len = 0;
        self.generation += 1;
        Ok(())
    }

    /// Append one row.
    pub fn push(&mut self, item: SeqRow) -> Result<(), SpillError> {
        self.len += 1;
        if self.writer.is_some() {
            return self.write_one(&item);
        }
        self.mem_bytes += item.estimated_bytes() as u64;
        Arc::get_mut(&mut self.mem)
            .expect("no replay outlives a push")
            .push(item);
        self.stats.note_resident(self.mem.len());
        if self.mem_bytes >= self.threshold_bytes {
            self.spill_to_file()?;
        }
        Ok(())
    }

    /// Move everything resident into a run file and keep writing there.
    fn spill_to_file(&mut self) -> Result<(), SpillError> {
        let path = self.dir.join(format!("group-{:08}.bin", self.generation));
        let file = std::fs::File::create(&path).map_err(|e| {
            SpillError::new(
                self.label,
                format!("create group run {}: {e}", path.display()),
            )
        })?;
        self.writer = Some(BufWriter::new(file));
        self.file = Some(path);
        let resident =
            std::mem::take(Arc::get_mut(&mut self.mem).expect("no replay outlives a push"));
        for item in &resident {
            self.write_one(item)?;
        }
        self.mem_bytes = 0;
        self.stats.note_spilled(self.stats.runs() + 1);
        Ok(())
    }

    fn write_one(&mut self, item: &SeqRow) -> Result<(), SpillError> {
        let bytes = serde_json::to_vec(item)
            .map_err(|e| SpillError::new(self.label, format!("encode group row: {e}")))?;
        let len = u32::try_from(bytes.len())
            .map_err(|_| SpillError::new(self.label, "group row exceeds 4 GiB"))?;
        let w = self.writer.as_mut().expect("writer present");
        w.write_all(&len.to_le_bytes())
            .map_err(|e| SpillError::new(self.label, format!("write group row len: {e}")))?;
        w.write_all(&bytes)
            .map_err(|e| SpillError::new(self.label, format!("write group row: {e}")))?;
        Ok(())
    }

    /// Iterate the buffered rows in push order. Safe to call repeatedly.
    pub fn replay(&mut self) -> Result<ReplayIter, SpillError> {
        if let Some(w) = self.writer.as_mut() {
            w.flush()
                .map_err(|e| SpillError::new(self.label, format!("flush group run: {e}")))?;
        }
        match &self.file {
            Some(path) => {
                let file = std::fs::File::open(path).map_err(|e| {
                    SpillError::new(
                        self.label,
                        format!("open group run {}: {e}", path.display()),
                    )
                })?;
                Ok(ReplayIter::File {
                    reader: BufReader::new(file),
                    label: self.label,
                })
            }
            None => Ok(ReplayIter::Mem {
                rows: Arc::clone(&self.mem),
                next: 0,
            }),
        }
    }
}

/// One replay pass over a [`ReplayBuffer`].
pub enum ReplayIter {
    Mem {
        rows: Arc<Vec<SeqRow>>,
        next: usize,
    },
    File {
        reader: BufReader<std::fs::File>,
        label: &'static str,
    },
}

impl Iterator for ReplayIter {
    type Item = Result<SeqRow, SpillError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ReplayIter::Mem { rows, next } => {
                let item = rows.get(*next)?.clone();
                *next += 1;
                Some(Ok(item))
            }
            ReplayIter::File { reader, label } => read_record(reader, label).transpose(),
        }
    }
}

/// Read one length-prefixed JSON record, or `Ok(None)` at a clean EOF.
///
/// A truncated record is a corrupt run and fails loud — treating it as a clean
/// end would silently drop rows, which is the failure this whole module exists
/// to prevent.
fn read_record<R: Read>(r: &mut R, label: &'static str) -> Result<Option<SeqRow>, SpillError> {
    let mut len_buf = [0u8; 4];
    match r.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(SpillError::new(label, format!("read group row len: {e}"))),
    }
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).map_err(|e| {
        SpillError::new(
            label,
            format!("truncated group run record (want {len} bytes): {e}"),
        )
    })?;
    serde_json::from_slice(&buf)
        .map(Some)
        .map_err(|e| SpillError::new(label, format!("decode group row: {e}")))
}

// ---------------------------------------------------------------------------
// Lookahead over a fallible row stream
// ---------------------------------------------------------------------------

/// A one-row lookahead over a fallible stream.
///
/// `Peekable` cannot serve the merge join: peeking a `Result` would force the
/// join to decide what to do with an error it has not consumed yet. This keeps
/// the error in the slot until the join takes it, so an I/O failure surfaces at
/// the point it is read and never turns into a short result.
pub struct Lookahead<I: Iterator<Item = Result<SeqRow, SpillError>>> {
    iter: I,
    head: Option<Result<SeqRow, SpillError>>,
}

impl<I: Iterator<Item = Result<SeqRow, SpillError>>> Lookahead<I> {
    pub fn new(mut iter: I) -> Self {
        let head = iter.next();
        Self { iter, head }
    }

    /// The next row without consuming it, or `None` at end of stream.
    /// An error sitting in the slot reads as "a row is present" — the caller
    /// takes it with [`Lookahead::take`] and propagates it.
    pub fn peek(&self) -> Option<&Result<SeqRow, SpillError>> {
        self.head.as_ref()
    }

    /// The value of column `col` in the next row, cloned so the caller can hold
    /// it across a [`Lookahead::next_row`] without borrowing this stream.
    ///
    /// A pending error is returned HERE rather than read as end of stream. That
    /// distinction is the whole point: a merge join that treated an unreadable
    /// run as "no more rows" would return a short join result and call it
    /// success.
    pub fn peek_key(&mut self, col: usize) -> Result<Option<Value>, SpillError> {
        match &self.head {
            None => Ok(None),
            Some(Ok(r)) => Ok(Some(r.row.0[col].clone())),
            Some(Err(_)) => Err(self
                .head
                .take()
                .expect("matched Some")
                .expect_err("matched Err")),
        }
    }

    /// Consume the next row, propagating a spill error instead of ending.
    pub fn next_row(&mut self) -> Result<Option<SeqRow>, SpillError> {
        match self.take() {
            None => Ok(None),
            Some(Ok(r)) => Ok(Some(r)),
            Some(Err(e)) => Err(e),
        }
    }

    pub fn take(&mut self) -> Option<Result<SeqRow, SpillError>> {
        let head = self.head.take();
        self.head = self.iter.next();
        head
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::SortDir;

    fn ctx_in(dir: &Path, threshold: u64) -> SpillCtx {
        SpillCtx::new(Arc::new(DirReserver::new(dir)), threshold)
    }

    fn row(vals: Vec<Value>) -> Row {
        Row::new(vals)
    }

    /// The load-bearing property of [`canonical_cmp`]: it must be an order whose
    /// `Equal` is exactly `Value`'s structural equality, because that is what
    /// grouping and DISTINCT key on. If the two ever disagreed, a sort-based
    /// GROUP BY would merge distinct keys or split equal ones.
    #[test]
    fn canonical_order_equals_structural_equality() {
        let values = vec![
            Value::Null,
            Value::Bool(false),
            Value::Bool(true),
            Value::Int(1),
            Value::float(1.0),
            Value::Text("1".into()),
            Value::Text("a".into()),
            Value::Bytea(vec![1]),
            Value::Timestamp(1),
            Value::Date(1),
            Value::Time(1),
            Value::numeric(num_bigint::BigInt::from(15), 1),
        ];
        for a in &values {
            for b in &values {
                assert_eq!(
                    canonical_cmp(a, b) == Ordering::Equal,
                    a == b,
                    "canonical_cmp disagrees with Eq for {a:?} vs {b:?}"
                );
            }
        }
    }

    /// `1.5` and `1.50` are the same `Value` (it normalizes), so the order that
    /// groups them must call them equal too.
    #[test]
    fn canonical_order_treats_equal_numerics_as_one_key() {
        let a = Value::numeric(num_bigint::BigInt::from(15), 1);
        let b = Value::numeric(num_bigint::BigInt::from(150), 2);
        assert_eq!(a, b);
        assert_eq!(canonical_cmp(&a, &b), Ordering::Equal);
    }

    #[test]
    fn sql_order_matches_the_in_memory_order_cmp_and_breaks_ties_by_arrival() {
        let order = SqlOrder::Sql(vec![SortKey {
            col: 0,
            dir: SortDir::Asc,
        }]);
        let a = SeqRow::new(0, row(vec![Value::Int(1)]));
        let b = SeqRow::new(1, row(vec![Value::Int(1)]));
        let n = SeqRow::new(2, row(vec![Value::Null]));
        // Equal keys fall back to arrival order: that is what makes the spilling
        // sort stable across run files.
        assert_eq!(order.compare(&a, &b), Ordering::Less);
        // ASC puts NULLs last, exactly like `order_cmp`.
        assert_eq!(order.compare(&a, &n), Ordering::Less);
    }

    #[test]
    fn spill_sort_returns_every_row_and_drops_its_temp_dir() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = ctx_in(dir.path(), 1);
        let mut sort = SpillSort::new(
            &ctx,
            SqlOrder::Sql(vec![SortKey {
                col: 0,
                dir: SortDir::Asc,
            }]),
            "sort",
        )
        .unwrap();
        for i in (0..200i64).rev() {
            sort.push(row(vec![Value::Int(i)])).unwrap();
        }
        let reserved = ctx.stats().reserved_paths();
        assert_eq!(reserved.len(), 1);

        let stream = sort.finish().unwrap();
        let got: Vec<i64> = stream
            .map(|r| match r.unwrap().row.0[0] {
                Value::Int(n) => n,
                ref v => panic!("unexpected {v:?}"),
            })
            .collect();
        assert_eq!(got, (0..200).collect::<Vec<_>>());
        assert!(ctx.stats().spilled());
        assert!(!reserved[0].exists(), "the stream's Drop cleans up");
    }

    #[test]
    fn replay_buffer_replays_identically_from_memory_and_from_disk() {
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<SeqRow> = (0..50)
            .map(|i| SeqRow::new(i, row(vec![Value::Int(i as i64), Value::Text("x".into())])))
            .collect();

        // Threshold high enough that nothing spills.
        let mem_ctx = ctx_in(dir.path(), 1 << 30);
        let mut mem = ReplayBuffer::new(dir.path(), &mem_ctx, "hash_join");
        // Threshold of one byte forces the file path from the first push.
        let disk_ctx = ctx_in(dir.path(), 1);
        let mut disk = ReplayBuffer::new(dir.path(), &disk_ctx, "hash_join");
        for r in &rows {
            mem.push(r.clone()).unwrap();
            disk.push(r.clone()).unwrap();
        }

        // Replaying twice must yield the same rows both times, from both backings.
        for _ in 0..2 {
            let from_mem: Vec<SeqRow> = mem.replay().unwrap().map(|r| r.unwrap()).collect();
            let from_disk: Vec<SeqRow> = disk.replay().unwrap().map(|r| r.unwrap()).collect();
            assert_eq!(from_mem, rows);
            assert_eq!(from_disk, rows);
        }
        assert!(disk_ctx.stats().spilled());
    }

    #[test]
    fn replay_buffer_clear_forgets_the_previous_group() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = ctx_in(dir.path(), 1);
        let mut buf = ReplayBuffer::new(dir.path(), &ctx, "hash_join");
        buf.push(SeqRow::new(0, row(vec![Value::Int(1)]))).unwrap();
        buf.clear().unwrap();
        assert!(buf.is_empty());
        assert_eq!(buf.replay().unwrap().count(), 0);
        buf.push(SeqRow::new(1, row(vec![Value::Int(2)]))).unwrap();
        let got: Vec<SeqRow> = buf.replay().unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(got, vec![SeqRow::new(1, row(vec![Value::Int(2)]))]);
    }

    /// A truncated run must fail loud rather than read as a clean end of group.
    #[test]
    fn a_truncated_group_run_is_an_error_not_a_short_read() {
        let mut encoded = Vec::new();
        let item = SeqRow::new(0, row(vec![Value::Text("hello".into())]));
        let bytes = serde_json::to_vec(&item).unwrap();
        encoded.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        encoded.extend_from_slice(&bytes[..bytes.len() - 2]); // truncate

        let err = read_record(&mut encoded.as_slice(), "hash_join").unwrap_err();
        assert!(format!("{err}").contains("truncated"), "{err}");
    }

    #[test]
    fn sweep_removes_only_directories_older_than_the_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let fresh = dir.path().join("sort-1-fresh");
        std::fs::create_dir(&fresh).unwrap();
        // Age threshold of zero makes every directory eligible.
        assert_eq!(
            sweep_orphaned_temp_dirs(dir.path(), Duration::from_secs(0)).unwrap(),
            1
        );
        assert!(!fresh.exists());

        let live = dir.path().join("sort-2-live");
        std::fs::create_dir(&live).unwrap();
        assert_eq!(
            sweep_orphaned_temp_dirs(dir.path(), Duration::from_secs(3600)).unwrap(),
            0,
            "a directory younger than the threshold may belong to a live query"
        );
        assert!(live.exists());
    }

    #[test]
    fn sweeping_a_missing_root_is_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("never-created");
        assert_eq!(
            sweep_orphaned_temp_dirs(&missing, Duration::from_secs(0)).unwrap(),
            0
        );
    }

    #[test]
    fn reserving_under_a_regular_file_fails_loud() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let reserver = DirReserver::new(file.path());
        let err = match reserver.reserve("sort") {
            Err(e) => e,
            Ok(_) => panic!("reserving under a regular file must not succeed"),
        };
        assert_eq!(err.operator, "sort");
        assert!(format!("{err}").contains("spill failed"), "{err}");
    }
}
