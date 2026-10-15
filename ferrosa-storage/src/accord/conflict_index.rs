//! Per-shard index of in-flight transactions for Accord conflict detection.
//!
//! The [`ConflictIndex`] tracks all in-flight writes within a single shard
//! executor, providing:
//!
//! - **O(1)** exact-key conflict lookup via `HashMap`
//! - **O(log n)** range overlap detection via `BTreeMap`
//! - **Indexed column projections** for transactional secondary index queries
//!
//! All access must be through a single-threaded shard executor — the index
//! is intentionally `!Sync` (it uses non-atomic interior state).
//!
//! # No hard bound: the index GROWS
//!
//! There is deliberately **no cap** on how many `(key, transaction)`
//! registrations the index holds. A PreAccept registers its transaction under
//! EVERY key in the write-set, so a fixed capacity was a hard floor on the
//! largest decidable transaction: the live `pgbench -i` load (a single
//! ~1,000,112-key transactional COPY) hit the historical 100 000-entry cap, every
//! replica refused the PreAccept, and the coordinator reported an opaque "Accord
//! quorum unavailable" on a fully healthy cluster. The index must hold a
//! write-set of any size, so it GROWS instead of refusing.
//!
//! Growth (rather than a disk spill) is the right shape here because the index
//! holds only *in-flight* registrations: every entry is removed by
//! [`gc_applied`](ConflictIndex::gc_applied) / [`remove`](ConflictIndex::remove)
//! once its transaction applies, so the resident set is a working set, not a
//! store. The lookups stay exactly as cheap as before — exact-key is an O(1) hash
//! lookup, range overlap is O(log n + k) — which a spilled-file index could not
//! promise on the PreAccept hot path.
//!
//! The per-key execution-timestamp high-water-mark (see
//! [`max_conflicting_timestamp`](ConflictIndex::max_conflicting_timestamp)) is
//! likewise unbounded: it used to skip new keys once full, which silently lost
//! the skipped key's post-GC conflict — a real-time inversion and a missed
//! conflict (t_813caf39). It is stored ON the key's own entry (see `KeyEntry`),
//! not in a parallel map, so the key's bytes are held once.
//!
//! # Residency
//!
//! [`single_key`](ConflictIndex) is a `HashMap<Vec<u8>, KeyEntry>`: ONE map that
//! holds each key's bytes, its in-flight writes, and its HWM together. The common
//! case is exactly one in-flight write per key, so the writes buffer is seeded at
//! capacity ONE instead of the four a bare `Vec` reserves for a >1-byte element,
//! and the HWM no longer re-stores the key in a second map. Measured with a
//! counting allocator at N = 200,000 keys, the whole index falls from 601 to 235
//! bytes per key — the writes buffer alone from 448 to 112, the parallel HWM map
//! from 82 to 0 — which projects to ~573 MiB → ~224 MiB at the live N = 1,000,112
//! (a ~349 MiB cut). `tests/conflict_index_residency.rs` (a per-PR structural /
//! same-run-ratio guard) and `tests/conflict_index_residency_absolute.rs` (the
//! absolute projection, gated out of the per-PR lane like `accord::perf`) measure
//! it and guard both factors.

use ferrosa_common::accord::{Timestamp, TxnId};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

/// Reserved conflict key shared by PostgreSQL transactions to establish a
/// cluster-wide serialization order. Its high-water mark lives in a named field,
/// not a per-key entry, so it survives regardless of how many ordinary keys the
/// index tracks.
pub const POSTGRES_TRANSACTION_MARKER_KEY: &[u8] = b"\0ferrosa:postgres:serializable:v1";
/// PostgreSQL begin and commit barriers share this key to order their Accord
/// transactions. BEGIN-only barriers do not advance the data-commit marker.
pub const POSTGRES_TRANSACTION_BARRIER_KEY: &[u8] = b"\0ferrosa:postgres:barrier:v1";
/// Prefix of the synthetic conflict key that carries a PostgreSQL transaction's
/// TABLE-level read of a scanned table (`schema.table`).
///
/// A transaction that SCANNED a table registers this key alongside its write keys,
/// so the READ participates in the DISTRIBUTED Accord ordering: a later transaction
/// that scanned the same table conflicts with it, and its captured snapshot is
/// refused (fail loud; the front end reports SQLSTATE 40001) when the table was
/// written after that snapshot — the write-skew case. A row-level POINT read
/// (`WHERE pk = <literal>`) records its row instead and does NOT register this key,
/// so two transactions on DISJOINT keys of one table still commit concurrently
/// (RT-2). Table granularity is deliberately conservative: a table-level scan
/// conflicts with ANY write to that table, which is exactly a scan's phantom
/// guard.
pub const POSTGRES_READ_TABLE_KEY_PREFIX: &[u8] = b"\0ferrosa:postgres:read-table:v1:";

/// The synthetic read-table conflict key for a fully-qualified `schema.table`.
#[must_use]
pub fn postgres_read_table_key(qualified_table: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(POSTGRES_READ_TABLE_KEY_PREFIX.len() + qualified_table.len());
    key.extend_from_slice(POSTGRES_READ_TABLE_KEY_PREFIX);
    key.extend_from_slice(qualified_table.as_bytes());
    key
}

/// Whether `key` is a synthetic read-table conflict key
/// ([`POSTGRES_READ_TABLE_KEY_PREFIX`]).
#[must_use]
pub fn is_postgres_read_table_key(key: &[u8]) -> bool {
    key.starts_with(POSTGRES_READ_TABLE_KEY_PREFIX)
}

/// Prefix of the synthetic conflict key that carries a PostgreSQL transaction's
/// WRITE of a whole table: one per table the transaction wrote rows into.
///
/// Registering it makes the table's WRITE watermark visible to a transaction that
/// SCANNED that table: the scanner registers a READ of this key
/// ([`postgres_read_key`]) and is refused `SnapshotStale` — fail loud, SQLSTATE
/// `40001` — when a write to the table committed after the scanner's snapshot,
/// **even when the writer never scanned the table itself**. That is the half the
/// coarse table-level read cannot reach: a writer that never read the table
/// registers no read-table key, so a stale scan against ITS write was caught only
/// by the LOCAL check (FMEA CL-72 residual).
///
/// It is a WATERMARK, not a data key: a writer registers it but does not conflict
/// on it, so two transactions writing DISJOINT rows of one table still commit
/// concurrently (RT-2). Only a SCANNER consults it, through the READ registration.
pub const POSTGRES_WRITE_TABLE_KEY_PREFIX: &[u8] = b"\0ferrosa:postgres:write-table:v1:";

/// The synthetic whole-table WRITE watermark key for a fully-qualified
/// `schema.table` ([`POSTGRES_WRITE_TABLE_KEY_PREFIX`]).
#[must_use]
pub fn postgres_write_table_key(qualified_table: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(POSTGRES_WRITE_TABLE_KEY_PREFIX.len() + qualified_table.len());
    key.extend_from_slice(POSTGRES_WRITE_TABLE_KEY_PREFIX);
    key.extend_from_slice(qualified_table.as_bytes());
    key
}

/// Whether `key` is a synthetic whole-table WRITE watermark key
/// ([`POSTGRES_WRITE_TABLE_KEY_PREFIX`]).
#[must_use]
pub fn is_postgres_write_table_key(key: &[u8]) -> bool {
    key.starts_with(POSTGRES_WRITE_TABLE_KEY_PREFIX)
}

/// Prefix of a READ registration: a key a transaction CONSULTED without writing.
///
/// A read is registered under this prefix so it is distinguishable from a write:
/// two READERS of one key register the same read key and, because the staleness
/// check consults a read key's UNDERLYING key ([`postgres_read_lookup_key`]) and
/// never the read key itself, they never conflict with each other. A reader does
/// conflict with a WRITER of the key it read — that is the read-write conflict the
/// distributed ordering must catch, and it is decided by the writer's own
/// registration under the underlying key.
pub const POSTGRES_READ_KEY_PREFIX: &[u8] = b"\0ferrosa:postgres:read:v1:";

/// The READ registration key for the key `underlying` a transaction read.
#[must_use]
pub fn postgres_read_key(underlying: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(POSTGRES_READ_KEY_PREFIX.len() + underlying.len());
    key.extend_from_slice(POSTGRES_READ_KEY_PREFIX);
    key.extend_from_slice(underlying);
    key
}

/// The key a staleness check must consult for `key`: the underlying key a READ
/// registration named, or `key` itself when it is not a read registration.
///
/// This is what makes read registrations distinguishable: a reader consults what
/// it READ (and so sees a writer of that key), while the read key it registered
/// under is never itself consulted — so read-read never conflicts.
#[must_use]
pub fn postgres_read_lookup_key(key: &[u8]) -> &[u8] {
    key.strip_prefix(POSTGRES_READ_KEY_PREFIX).unwrap_or(key)
}

/// Default pre-allocation hint for [`ConflictIndex::new`] — a starting buffer
/// size for the hot maps, never a bound on how much they may hold.
pub const DEFAULT_CONFLICT_INDEX_RESERVE: usize = 1024;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Status of an in-flight transaction in the ConflictIndex.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnStatus {
    PreAccepted,
    Accepted,
    Committed,
    Applied,
}

/// Entry for a single in-flight write.
#[derive(Debug, Clone)]
pub struct InFlightWrite {
    pub txn_id: TxnId,
    pub t0: Timestamp,
    /// Commit timestamp. `None` until the transaction is committed.
    pub accord_ts: Option<Timestamp>,
    pub status: TxnStatus,
}

/// Token range for range operations.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TokenRange {
    pub start: i64,
    pub end: i64,
}

impl TokenRange {
    /// Returns true if this range overlaps with `other`.
    ///
    /// Ranges are inclusive on both ends: `[start, end]`.
    fn overlaps(&self, other: &TokenRange) -> bool {
        self.start <= other.end && other.start <= self.end
    }
}

/// One key's entry in the [`ConflictIndex`]: its in-flight writes together with
/// its per-key execution-timestamp high-water-mark.
///
/// Holding the key's bytes once (in the map key) and the HWM on the same entry
/// replaces a parallel `HashMap<Vec<u8>, Timestamp>` that stored every key a
/// second time.
struct KeyEntry {
    /// In-flight writes registered on this key. Almost every key holds exactly
    /// ONE at a time: a PreAccept registers the transaction once per key and the
    /// key is dropped as soon as that transaction is GC'd. A plain `Vec` reserved
    /// capacity FOUR for that single element (Rust's first-growth rule for an
    /// element larger than one byte) — three slots never used. The buffer is
    /// seeded at capacity one here; a second concurrent write on the same key is
    /// the rare case and the `Vec` grows normally when it happens.
    writes: Vec<InFlightWrite>,
    /// Highest committed **execution** timestamp ever seen for this key, raised by
    /// [`set_commit_ts`](ConflictIndex::set_commit_ts) and monotonic. Unlike
    /// `writes`, it is retained across [`gc_applied`](ConflictIndex::gc_applied) so
    /// a later append still bumps past an already-applied one — the GC-boundary
    /// real-time inversion (t_813caf39). An entry with empty `writes` and a
    /// `Some(hwm)` is kept for exactly this reason.
    hwm: Option<Timestamp>,
}

// ---------------------------------------------------------------------------
// ConflictIndex
// ---------------------------------------------------------------------------

/// Per-shard index of in-flight transactions for conflict detection.
///
/// Designed to be owned by a single-threaded shard executor. Not `Sync`.
pub struct ConflictIndex {
    /// Single-partition writes: O(1) exact-key lookup. Each key's bytes live
    /// here ONCE, alongside its in-flight writes and its per-key HWM.
    single_key: HashMap<Vec<u8>, KeyEntry>,

    /// Range-spanning operations: O(log n) range overlap.
    range_ops: BTreeMap<TokenRange, BTreeSet<(Timestamp, TxnId)>>,

    /// Indexed column projections for transactional 2i.
    indexed_writes: HashMap<String, HashMap<Vec<u8>, Vec<TxnId>>>,

    /// The PostgreSQL marker must never lose its high-water mark: stale-snapshot
    /// validation depends on it. Kept as a named field (not on a per-key entry)
    /// because the marker's HWM must survive even after the marker's live entry is
    /// GC'd and its `KeyEntry` is dropped.
    postgres_marker_hwm: Option<Timestamp>,
    postgres_barrier_hwm: Option<Timestamp>,

    /// Total live registrations (single-key + range), for [`len`](Self::len) and
    /// [`is_empty`](Self::is_empty). There is no cap — it grows with the
    /// in-flight write-sets and shrinks as they apply.
    current_entries: usize,
}

impl ConflictIndex {
    /// Create a new conflict index.
    ///
    /// `reserve` is a **pre-allocation hint** for the hot maps — a buffer-size
    /// tuning, never a bound. The index grows past it without limit: neither
    /// [`register`](Self::register) nor [`register_range`](Self::register_range)
    /// can refuse. Pass [`DEFAULT_CONFLICT_INDEX_RESERVE`] unless the caller has a
    /// better guess at its working-set size.
    pub fn new(reserve: usize) -> Self {
        Self {
            single_key: HashMap::with_capacity(reserve),
            range_ops: BTreeMap::new(),
            indexed_writes: HashMap::new(),
            postgres_marker_hwm: None,
            postgres_barrier_hwm: None,
            current_entries: 0,
        }
    }

    /// Register a new in-flight transaction on a single key.
    ///
    /// Always succeeds: the index grows to hold the key. A PreAccept registers
    /// its transaction under EVERY key of the write-set and is all-or-nothing,
    /// so a refusal here would silently drop a conflict (two conflicting txns
    /// could both commit — a lost update). There is deliberately no refusal path.
    pub fn register(&mut self, key: &[u8], entry: InFlightWrite) {
        // Seed the writes buffer at capacity ONE: the common case is a single
        // in-flight write per key, and a bare `Vec` would reserve four slots for
        // it. An existing key's buffer grows normally on its rare second write.
        self.single_key
            .entry(key.to_vec())
            .or_insert_with(|| KeyEntry {
                writes: Vec::with_capacity(1),
                hwm: None,
            })
            .writes
            .push(entry);
        self.current_entries += 1;
    }

    /// Register a range operation.
    ///
    /// Always succeeds: the index grows to hold the range. See
    /// [`register`](Self::register) for why there is no refusal path.
    pub fn register_range(&mut self, range: TokenRange, ts: Timestamp, txn_id: TxnId) {
        self.range_ops
            .entry(range)
            .or_default()
            .insert((ts, txn_id));
        self.current_entries += 1;
    }

    /// Register an indexed column write.
    ///
    /// Indexed writes do not count toward the capacity limit since they
    /// are secondary projections of already-registered transactions.
    pub fn register_indexed_write(&mut self, column: &str, value: &[u8], txn_id: TxnId) {
        self.indexed_writes
            .entry(column.to_string())
            .or_default()
            .entry(value.to_vec())
            .or_default()
            .push(txn_id);
    }

    /// Returns the maximum **effective** timestamp of all conflicting
    /// transactions for a single key — each entry's agreed execution timestamp
    /// (`accord_ts`) once known, else its proposed `t0`. O(1) lookup.
    ///
    /// PreAccept bumps a new transaction past this value, so it MUST be the
    /// conflicting txns' serialization point (their execution `t`), not their
    /// proposed `t0`: a committed txn whose `t` was bumped far past its `t0`
    /// (past an earlier, now-GC'd conflict) would otherwise be under-counted, and
    /// a later txn assigned an execution `t` below it — a real-time inversion that
    /// reorders accumulating list elements (t_813caf39).
    pub fn max_conflicting_timestamp(&self, key: &[u8]) -> Option<Timestamp> {
        let entry = self.single_key.get(key);
        let live = entry.and_then(|e| e.writes.iter().map(|w| w.accord_ts.unwrap_or(w.t0)).max());
        // Fold in the per-key high-water-mark, which survives GC of the live
        // entries — so a later append still bumps past an already-applied one. It
        // lives on the key's own entry, so the key's bytes are read once.
        let hwm = if key == POSTGRES_TRANSACTION_MARKER_KEY {
            self.postgres_marker_hwm
                .into_iter()
                .chain(entry.and_then(|e| e.hwm))
                .max()
        } else if key == POSTGRES_TRANSACTION_BARRIER_KEY {
            self.postgres_barrier_hwm
                .into_iter()
                .chain(entry.and_then(|e| e.hwm))
                .max()
        } else {
            entry.and_then(|e| e.hwm)
        };
        match (live, hwm) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        }
    }

    /// Record a transaction's agreed execution timestamp on every entry it owns,
    /// so subsequent [`max_conflicting_timestamp`](Self::max_conflicting_timestamp)
    /// lookups bump past its real serialization point rather than its stale `t0`.
    /// Called when the timestamp is finalized (Accept / Commit).
    ///
    /// Also raises the per-key execution-timestamp **high-water-mark** on the
    /// key's own entry for every key the transaction touches, so the bump survives
    /// [`gc_applied`] once the transaction is applied and its live entry is
    /// removed. No-op on the live entries if the txn is already GC'd, but the HWM
    /// is still raised.
    ///
    /// [`gc_applied`]: Self::gc_applied
    pub fn set_commit_ts(&mut self, txn_id: &TxnId, t: Timestamp) {
        for (key, entry) in self.single_key.iter_mut() {
            let mut touches_key = false;
            for w in entry.writes.iter_mut() {
                if w.txn_id == *txn_id {
                    w.accord_ts = Some(t);
                    touches_key = true;
                }
            }
            if touches_key {
                if key.as_slice() == POSTGRES_TRANSACTION_MARKER_KEY {
                    self.postgres_marker_hwm =
                        Some(self.postgres_marker_hwm.map_or(t, |current| current.max(t)));
                } else if key.as_slice() == POSTGRES_TRANSACTION_BARRIER_KEY {
                    self.postgres_barrier_hwm = Some(
                        self.postgres_barrier_hwm
                            .map_or(t, |current| current.max(t)),
                    );
                } else {
                    // Monotonic raise on the key's own entry — no second key copy.
                    entry.hwm = Some(entry.hwm.map_or(t, |current| current.max(t)));
                }
            }
        }
    }

    /// Returns the maximum `t0` of conflicting range operations that
    /// overlap with the given range.
    pub fn max_conflicting_range_timestamp(&self, range: &TokenRange) -> Option<Timestamp> {
        let mut max_ts: Option<Timestamp> = None;
        for (stored_range, txn_set) in &self.range_ops {
            if stored_range.overlaps(range) {
                for (ts, _txn_id) in txn_set {
                    match max_ts {
                        None => max_ts = Some(*ts),
                        Some(current_max) if *ts > current_max => max_ts = Some(*ts),
                        _ => {}
                    }
                }
            }
        }
        max_ts
    }

    /// Returns all conflicting transaction IDs where `t0_gamma < t0`.
    ///
    /// Used for building the PreAccept dependency set.
    pub fn deps_before_t0(&self, key: &[u8], t0: &Timestamp) -> HashSet<TxnId> {
        let mut deps = HashSet::new();
        if let Some(entry) = self.single_key.get(key) {
            for w in &entry.writes {
                if w.t0 < *t0 {
                    deps.insert(w.txn_id);
                }
            }
        }
        deps
    }

    /// Returns all conflicting transaction IDs where `t0_gamma < t`.
    ///
    /// Used for building the Accept dependency set. Note: this compares
    /// each entry's `t0` against the provided `t` (the commit timestamp),
    /// not against another `t0`.
    pub fn deps_before_t(&self, key: &[u8], t: &Timestamp) -> HashSet<TxnId> {
        let mut deps = HashSet::new();
        if let Some(entry) = self.single_key.get(key) {
            for w in &entry.writes {
                if w.t0 < *t {
                    deps.insert(w.txn_id);
                }
            }
        }
        deps
    }

    /// Whether `key` already carries an in-flight entry for `txn_id`.
    ///
    /// Used to make [`ConflictIndex::register`] on the coordinator's OWN commit
    /// path idempotent: a replica that already indexed the txn at PreAccept must
    /// not index it a second time at Commit.
    pub fn has_write(&self, key: &[u8], txn_id: &TxnId) -> bool {
        self.single_key
            .get(key)
            .is_some_and(|entry| entry.writes.iter().any(|w| w.txn_id == *txn_id))
    }

    /// Remove a completed transaction from all indexes.
    ///
    /// Scans single-key, range, and indexed-write maps for entries
    /// matching the given `txn_id` and removes them. Decrements the
    /// entry count for each removal from single-key and range maps.
    pub fn remove(&mut self, txn_id: &TxnId) {
        // Remove from single-key index.
        let mut empty_keys = Vec::new();
        for (key, entry) in &mut self.single_key {
            let before = entry.writes.len();
            entry.writes.retain(|w| w.txn_id != *txn_id);
            let removed = before - entry.writes.len();
            self.current_entries = self.current_entries.saturating_sub(removed);
            // Keep the entry while its HWM is set: the entry IS the per-key HWM.
            if entry.writes.is_empty() && entry.hwm.is_none() {
                empty_keys.push(key.clone());
            }
        }
        for key in empty_keys {
            self.single_key.remove(&key);
        }

        // Remove from range index.
        let mut empty_ranges = Vec::new();
        for (range, txn_set) in &mut self.range_ops {
            let before = txn_set.len();
            txn_set.retain(|(_ts, tid)| *tid != *txn_id);
            let removed = before - txn_set.len();
            self.current_entries = self.current_entries.saturating_sub(removed);
            if txn_set.is_empty() {
                empty_ranges.push(range.clone());
            }
        }
        for range in empty_ranges {
            self.range_ops.remove(&range);
        }

        // Remove from indexed writes.
        let mut empty_columns = Vec::new();
        for (column, value_map) in &mut self.indexed_writes {
            let mut empty_values = Vec::new();
            for (value, txn_ids) in value_map.iter_mut() {
                txn_ids.retain(|tid| *tid != *txn_id);
                if txn_ids.is_empty() {
                    empty_values.push(value.clone());
                }
            }
            for value in empty_values {
                value_map.remove(&value);
            }
            if value_map.is_empty() {
                empty_columns.push(column.clone());
            }
        }
        for column in empty_columns {
            self.indexed_writes.remove(&column);
        }
    }

    /// Mark a transaction as Applied across all index entries.
    ///
    /// Updates the status of all single-key entries for this transaction
    /// to `TxnStatus::Applied`, making them eligible for garbage collection
    /// via [`gc_applied`](ConflictIndex::gc_applied).
    pub fn mark_applied(&mut self, txn_id: &TxnId) {
        for entry in self.single_key.values_mut() {
            for w in entry.writes.iter_mut() {
                if w.txn_id == *txn_id {
                    w.status = TxnStatus::Applied;
                }
            }
        }
    }

    /// Garbage-collect all entries with status `TxnStatus::Applied`.
    ///
    /// Removes applied entries from single-key, range, and indexed-write
    /// maps. Decrements `current_entries` for each removal from single-key
    /// and range maps.
    ///
    /// The caller is responsible for only marking transactions as Applied
    /// once all dependents have been resolved.
    pub fn gc_applied(&mut self) {
        // Collect applied TxnIds from single_key for range/indexed cleanup.
        let mut applied_txn_ids = Vec::new();
        let mut removed = 0usize;

        self.single_key.retain(|_key, entry| {
            let before = entry.writes.len();
            for w in entry.writes.iter() {
                if w.status == TxnStatus::Applied {
                    applied_txn_ids.push(w.txn_id);
                }
            }
            entry.writes.retain(|w| w.status != TxnStatus::Applied);
            removed += before - entry.writes.len();
            // An entry with no live writes but a raised HWM must be KEPT: it is the
            // surviving post-GC conflict (t_813caf39).
            !entry.writes.is_empty() || entry.hwm.is_some()
        });

        // Remove applied transactions from range_ops.
        let mut empty_ranges = Vec::new();
        for (range, txn_set) in &mut self.range_ops {
            let before = txn_set.len();
            txn_set.retain(|(_ts, tid)| !applied_txn_ids.contains(tid));
            removed += before - txn_set.len();
            if txn_set.is_empty() {
                empty_ranges.push(range.clone());
            }
        }
        for range in empty_ranges {
            self.range_ops.remove(&range);
        }

        // Remove applied transactions from indexed_writes.
        let mut empty_columns = Vec::new();
        for (column, value_map) in &mut self.indexed_writes {
            let mut empty_values = Vec::new();
            for (value, txn_ids) in value_map.iter_mut() {
                txn_ids.retain(|tid| !applied_txn_ids.contains(tid));
                if txn_ids.is_empty() {
                    empty_values.push(value.clone());
                }
            }
            for value in empty_values {
                value_map.remove(&value);
            }
            if value_map.is_empty() {
                empty_columns.push(column.clone());
            }
        }
        for column in empty_columns {
            self.indexed_writes.remove(&column);
        }

        self.current_entries = self.current_entries.saturating_sub(removed);
    }

    /// Current number of entries (single-key + range).
    pub fn len(&self) -> usize {
        self.current_entries
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.current_entries == 0
    }

    /// Look up indexed write projections for a given column and value.
    pub fn get_indexed_writes(&self, column: &str, value: &[u8]) -> Option<&[TxnId]> {
        self.indexed_writes
            .get(column)
            .and_then(|value_map| value_map.get(value))
            .map(|v| v.as_slice())
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create a Timestamp with the given time value (other fields zero).
    fn ts(time: u64) -> Timestamp {
        Timestamp {
            epoch: 0,
            time,
            seq: 0,
            node: 1,
        }
    }

    /// Helper: create a TxnId from a Timestamp time value.
    fn txn(time: u64) -> TxnId {
        TxnId(ts(time))
    }

    /// Helper: create an InFlightWrite with given txn_id and t0 time.
    fn write_entry(t0_time: u64) -> InFlightWrite {
        InFlightWrite {
            txn_id: txn(t0_time),
            t0: ts(t0_time),
            accord_ts: None,
            status: TxnStatus::PreAccepted,
        }
    }

    // -----------------------------------------------------------------------
    // Test 1: Single key register + lookup
    // -----------------------------------------------------------------------

    #[test]
    fn conflict_index_single_key_register_lookup() {
        let mut idx = ConflictIndex::new(100);
        let key = b"partition-1";

        // Register T1 (t0 = 10).
        idx.register(key, write_entry(10));
        assert_eq!(idx.max_conflicting_timestamp(key), Some(ts(10)));

        // Register T2 with higher t0 (t0 = 20).
        idx.register(key, write_entry(20));
        assert_eq!(idx.max_conflicting_timestamp(key), Some(ts(20)));
    }

    /// Root cause of the Accord list-append real-time inversion (t_813caf39): a
    /// committed transaction's serialization point is its EXECUTION timestamp
    /// (`accord_ts`), not its proposed `t0`. `max_conflicting_timestamp` — used by
    /// PreAccept to bump a new txn past existing conflicts — must reflect the
    /// execution timestamp. Otherwise a txn A that bumped its execution `t` far
    /// past its `t0` (past an earlier, now-GC'd conflict) is under-counted, and a
    /// later txn B is assigned an execution `t` BELOW A — a real-time inversion
    /// (and, for accumulating lists, an out-of-order element).
    #[test]
    fn max_conflicting_timestamp_reflects_committed_execution_ts_not_t0() {
        let mut idx = ConflictIndex::new(100);
        let key = b"k";
        // Txn A: proposed t0 = 500, but its AGREED execution t = 1000 (it bumped
        // past an earlier, higher-t0 conflict that has since applied + been GC'd).
        idx.register(
            key,
            InFlightWrite {
                txn_id: txn(500),
                t0: ts(500),
                accord_ts: Some(ts(1000)),
                status: TxnStatus::Committed,
            },
        );

        assert_eq!(
            idx.max_conflicting_timestamp(key),
            Some(ts(1000)),
            "a committed txn's execution timestamp (accord_ts) — not its stale t0 — \
             must be its conflict timestamp, so a later PreAccept bumps past it"
        );
    }

    /// The GC-boundary edge of the list-append inversion (t_813caf39): a
    /// committed append's execution timestamp must survive `gc_applied` as a
    /// per-key high-water-mark, so a LATER append on the same key still bumps past
    /// it even though the earlier append's live entry is gone. Without the HWM,
    /// `max_conflicting_timestamp` returns `None` after GC and the later append
    /// mints a lower timestamp → it sorts before the earlier one (mid-list).
    #[test]
    fn max_conflicting_timestamp_survives_gc_via_per_key_hwm() {
        let mut idx = ConflictIndex::new(100);
        let key = b"k";
        let id = txn(500);
        idx.register(
            key,
            InFlightWrite {
                txn_id: id,
                t0: ts(500),
                accord_ts: Some(ts(1000)),
                status: TxnStatus::Committed,
            },
        );
        idx.set_commit_ts(&id, ts(1000)); // records the per-key HWM

        // The txn applies and is GC'd — its live entry is removed.
        idx.mark_applied(&id);
        idx.gc_applied();
        assert_eq!(
            idx.max_conflicting_timestamp(key),
            None.or(Some(ts(1000))),
            "the committed execution timestamp must survive gc_applied as a per-key HWM"
        );

        // A different key is unaffected (no false HWM bleed across keys).
        assert_eq!(idx.max_conflicting_timestamp(b"other"), None);
    }

    /// The PostgreSQL marker's high-water mark must survive regardless of how
    /// many ordinary keys the index tracks: stale-snapshot validation depends on
    /// it. The ordinary per-key HWM is likewise unbounded (nothing is skipped),
    /// so an ordinary key's post-GC conflict is retained too.
    #[test]
    fn postgres_marker_hwm_survives_alongside_many_normal_keys() {
        let mut idx = ConflictIndex::new(1);
        // Far more ordinary keys than any old HWM bound.
        for i in 0..1_000u64 {
            let id = txn(100 + i);
            idx.register(
                &i.to_be_bytes(),
                InFlightWrite {
                    txn_id: id,
                    t0: ts(100 + i),
                    accord_ts: Some(ts(100 + i)),
                    status: TxnStatus::Committed,
                },
            );
            idx.set_commit_ts(&id, ts(100 + i));
            idx.mark_applied(&id);
        }
        idx.gc_applied();
        // An ordinary key's HWM is retained (it was never skipped).
        assert_eq!(
            idx.max_conflicting_timestamp(&5u64.to_be_bytes()),
            Some(ts(105)),
            "an ordinary key's committed execution timestamp must survive gc_applied"
        );

        let marker = txn(20);
        idx.register(
            POSTGRES_TRANSACTION_MARKER_KEY,
            InFlightWrite {
                txn_id: marker,
                t0: ts(20),
                accord_ts: Some(ts(20)),
                status: TxnStatus::Committed,
            },
        );
        idx.set_commit_ts(&marker, ts(20));
        idx.mark_applied(&marker);
        idx.gc_applied();

        assert_eq!(
            idx.max_conflicting_timestamp(POSTGRES_TRANSACTION_MARKER_KEY),
            Some(ts(20)),
            "snapshot validation must retain the global marker timestamp regardless of \
             how many ordinary keys are tracked"
        );
    }

    // -----------------------------------------------------------------------
    // Test 2: No false positives across keys
    // -----------------------------------------------------------------------

    #[test]
    fn conflict_index_single_key_no_false_positives() {
        let mut idx = ConflictIndex::new(100);

        idx.register(b"key-A", write_entry(10));

        // Querying a different key must return None.
        assert_eq!(idx.max_conflicting_timestamp(b"key-B"), None);
    }

    // -----------------------------------------------------------------------
    // Test 3: Range overlap detection
    // -----------------------------------------------------------------------

    #[test]
    fn conflict_index_range_overlap_detection() {
        let mut idx = ConflictIndex::new(100);

        let range1 = TokenRange {
            start: 100,
            end: 200,
        };
        idx.register_range(range1, ts(10), txn(10));

        // Overlapping query range [150, 250] — should find conflict.
        let query_overlap = TokenRange {
            start: 150,
            end: 250,
        };
        assert_eq!(
            idx.max_conflicting_range_timestamp(&query_overlap),
            Some(ts(10))
        );

        // Non-overlapping query range [201, 300] — no conflict.
        let query_disjoint = TokenRange {
            start: 201,
            end: 300,
        };
        assert_eq!(idx.max_conflicting_range_timestamp(&query_disjoint), None);
    }

    // -----------------------------------------------------------------------
    // Test 4: deps_before_t0 filter
    // -----------------------------------------------------------------------

    #[test]
    fn conflict_index_deps_before_t0_filter() {
        let mut idx = ConflictIndex::new(100);
        let key = b"key";

        idx.register(key, write_entry(5));
        idx.register(key, write_entry(10));
        idx.register(key, write_entry(15));

        // deps_before_t0(key, t0=12) should return T1(5) and T2(10), not T3(15).
        let deps = idx.deps_before_t0(key, &ts(12));
        assert_eq!(deps.len(), 2);
        assert!(deps.contains(&txn(5)));
        assert!(deps.contains(&txn(10)));
        assert!(!deps.contains(&txn(15)));
    }

    // -----------------------------------------------------------------------
    // Test 5: deps_before_t filter
    // -----------------------------------------------------------------------

    #[test]
    fn conflict_index_deps_before_t_filter() {
        let mut idx = ConflictIndex::new(100);
        let key = b"key";

        idx.register(key, write_entry(5));
        idx.register(key, write_entry(10));

        // deps_before_t(key, t=8) should return T1(5) only, not T2(10).
        let deps = idx.deps_before_t(key, &ts(8));
        assert_eq!(deps.len(), 1);
        assert!(deps.contains(&txn(5)));
        assert!(!deps.contains(&txn(10)));
    }

    // -----------------------------------------------------------------------
    // Test 6: Remove after applied
    // -----------------------------------------------------------------------

    #[test]
    fn conflict_index_remove_after_applied() {
        let mut idx = ConflictIndex::new(100);
        let key = b"key";

        idx.register(key, write_entry(10));
        assert_eq!(idx.max_conflicting_timestamp(key), Some(ts(10)));
        assert_eq!(idx.len(), 1);

        idx.remove(&txn(10));
        assert_eq!(idx.max_conflicting_timestamp(key), None);
        assert!(idx.is_empty());
    }

    // -----------------------------------------------------------------------
    // Test 7: The reserve hint is a hint, never a bound
    // -----------------------------------------------------------------------

    /// The constructor argument is a pre-allocation HINT: the index grows past
    /// it and NEVER refuses a registration. (Before the fix this test asserted
    /// the inverse — that the 4th registration failed — which re-encoded the
    /// defect as the specification.)
    #[test]
    fn conflict_index_grows_past_its_reserve_hint() {
        let mut idx = ConflictIndex::new(1); // reserve of ONE entry

        idx.register(b"k1", write_entry(1));
        idx.register(b"k2", write_entry(2));
        idx.register(b"k3", write_entry(3));

        // The 4th registration must ALSO land: the reserve is a hint, not a cap.
        idx.register(b"k4", write_entry(4));
        assert_eq!(
            idx.len(),
            4,
            "no registration may be refused at the reserve"
        );
        assert_eq!(idx.max_conflicting_timestamp(b"k4"), Some(ts(4)));
    }

    // -----------------------------------------------------------------------
    // Test 8: Verify ConflictIndex is !Sync
    // -----------------------------------------------------------------------

    #[test]
    fn conflict_index_concurrent_single_threaded() {
        // ConflictIndex uses HashMap (which is !Sync for mutable access).
        // We verify it is Send but document that all access must go through
        // a single-threaded shard executor.
        //
        // The type is Send (it contains only owned data), but concurrent
        // mutable access is prevented by Rust's ownership rules — only one
        // &mut reference can exist at a time. This is the desired property
        // for a shard-local data structure.
        fn assert_send<T: Send>() {}
        assert_send::<ConflictIndex>();

        // Verify the index works correctly in a single-threaded context
        // with interleaved operations.
        let mut idx = ConflictIndex::new(100);
        idx.register(b"k1", write_entry(1));
        idx.register(b"k2", write_entry(2));
        assert_eq!(idx.max_conflicting_timestamp(b"k1"), Some(ts(1)));
        idx.remove(&txn(1));
        assert_eq!(idx.max_conflicting_timestamp(b"k1"), None);
        assert_eq!(idx.max_conflicting_timestamp(b"k2"), Some(ts(2)));
    }

    // -----------------------------------------------------------------------
    // Test 9: Indexed writes projection
    // -----------------------------------------------------------------------

    #[test]
    fn conflict_index_indexed_writes_projection() {
        let mut idx = ConflictIndex::new(100);

        let t1 = txn(10);
        idx.register_indexed_write("age", b"25", t1);

        // Query indexed_writes for ("age", "25") should return T1.
        let result = idx.get_indexed_writes("age", b"25");
        assert!(result.is_some());
        let txn_ids = result.unwrap();
        assert_eq!(txn_ids.len(), 1);
        assert_eq!(txn_ids[0], t1);

        // Query for a different value should return None.
        assert!(idx.get_indexed_writes("age", b"30").is_none());

        // Query for a different column should return None.
        assert!(idx.get_indexed_writes("name", b"25").is_none());
    }

    // -----------------------------------------------------------------------
    // Test 10: GC respects deps — only removes Applied
    // -----------------------------------------------------------------------

    #[test]
    fn conflict_index_gc_respects_deps() {
        let mut idx = ConflictIndex::new(100);
        let key = b"users:alice";

        // Register T1 and T2 on the same key. T2 depends on T1.
        idx.register(key, write_entry(100));
        idx.register(key, write_entry(200));
        assert_eq!(idx.len(), 2);

        // Verify T2 sees T1 as a dependency.
        let deps = idx.deps_before_t0(key, &ts(200));
        assert_eq!(deps.len(), 1);
        assert!(deps.contains(&txn(100)));

        // Mark T1 as Applied and GC.
        idx.mark_applied(&txn(100));
        idx.gc_applied();

        // T1 should be removed.
        assert_eq!(idx.len(), 1);

        // T2 should still be present (not applied).
        assert_eq!(idx.max_conflicting_timestamp(key), Some(ts(200)));
    }

    // -----------------------------------------------------------------------
    // Test 11: GC after apply removes entry completely
    // -----------------------------------------------------------------------

    #[test]
    fn conflict_index_gc_after_apply() {
        let mut idx = ConflictIndex::new(100);
        let key = b"orders:123";

        idx.register(key, write_entry(100));
        assert_eq!(idx.len(), 1);

        idx.mark_applied(&txn(100));
        idx.gc_applied();

        assert_eq!(idx.len(), 0);
        assert!(idx.is_empty());

        // No deps should remain.
        let deps = idx.deps_before_t0(key, &ts(200));
        assert!(deps.is_empty());
    }

    // -----------------------------------------------------------------------
    // GROWTH invariants — the index must never refuse, truncate or cap a
    // write-set. The historical fixed cap was 100_000 entries; every test below
    // crosses it. Their failure mode BEFORE the fix is exactly the live failure:
    // `register` returned `Err(ConflictIndexFull)` and the registration was lost.
    // -----------------------------------------------------------------------

    /// INVARIANT (no false negative past the old boundary): a write-set may hold
    /// far more keys than the historical 100_000-entry cap, and EVERY registered
    /// key must still be found afterwards. The old index refused the writes past
    /// the cap, so `len()` froze at 100_000 and the tail keys were silently lost —
    /// the live `pgbench -i` failure at 1,000,112 keys.
    #[test]
    fn registers_every_key_of_a_write_set_far_past_the_old_boundary() {
        const OLD_CAP: usize = 100_000;
        const N: usize = 1_000_112;
        let mut idx = ConflictIndex::new(OLD_CAP);
        for i in 0..N as u64 {
            let entry = InFlightWrite {
                txn_id: txn(1),
                t0: ts(1),
                accord_ts: Some(ts(1)),
                status: TxnStatus::PreAccepted,
            };
            idx.register(&i.to_be_bytes(), entry);
        }
        assert_eq!(
            idx.len(),
            N,
            "every key of the write-set must be registered — no cap may truncate it"
        );
        for i in [0usize, OLD_CAP - 1, OLD_CAP, N - 1] {
            assert_eq!(
                idx.max_conflicting_timestamp(&(i as u64).to_be_bytes()),
                Some(ts(1)),
                "key {i} must still be found after growth past the old cap"
            );
        }
    }

    /// INVARIANT (conflict straddling the old boundary): two transactions that
    /// share a key are still detected as conflicting when one of them registers
    /// while the index already holds more than the old 100_000 entries on OTHER
    /// keys. Before the fix the second registration was refused, so the shared-key
    /// conflict vanished and two conflicting txns could both commit — a lost update.
    #[test]
    fn detects_a_conflict_straddling_the_old_boundary() {
        const OLD_CAP: usize = 100_000;
        let mut idx = ConflictIndex::new(OLD_CAP);
        let shared: &[u8] = b"shared-key";
        let a = txn(10);
        let b = txn(20);

        // Fill up to just under the old cap on unrelated keys.
        for i in 0..(OLD_CAP as u64 - 1) {
            idx.register(
                &(i ^ 0x8000_0000_0000_0000).to_be_bytes(),
                InFlightWrite {
                    txn_id: txn(1),
                    t0: ts(1),
                    accord_ts: None,
                    status: TxnStatus::PreAccepted,
                },
            );
        }
        // A registers the shared key as the index crosses the old boundary.
        idx.register(
            shared,
            InFlightWrite {
                txn_id: a,
                t0: ts(10),
                accord_ts: None,
                status: TxnStatus::PreAccepted,
            },
        );
        // The index grows well past the old boundary.
        for i in 0..1_000u64 {
            idx.register(
                &(i ^ 0x4000_0000_0000_0000).to_be_bytes(),
                InFlightWrite {
                    txn_id: txn(1),
                    t0: ts(1),
                    accord_ts: None,
                    status: TxnStatus::PreAccepted,
                },
            );
        }
        // B registers the SAME shared key far past the old cap.
        idx.register(
            shared,
            InFlightWrite {
                txn_id: b,
                t0: ts(20),
                accord_ts: None,
                status: TxnStatus::PreAccepted,
            },
        );

        assert_eq!(
            idx.len(),
            (OLD_CAP as u64 - 1 + 2 + 1_000) as usize,
            "every registration must land — none may be refused at the old boundary"
        );
        let deps = idx.deps_before_t0(shared, &ts(1000));
        assert!(
            deps.contains(&a) && deps.contains(&b),
            "the shared-key conflict straddling the old boundary must be detected: {deps:?}"
        );
    }

    /// INVARIANT (all-or-nothing): registering a whole write-set registers EVERY
    /// key; there is no capacity at which a prefix is accepted and the tail dropped.
    #[test]
    fn registers_a_large_multi_key_write_set_all_or_nothing() {
        const N: usize = 250_000;
        let mut idx = ConflictIndex::new(1024);
        let id = txn(7);
        for i in 0..N as u64 {
            idx.register(
                &i.to_be_bytes(),
                InFlightWrite {
                    txn_id: id,
                    t0: ts(7),
                    accord_ts: None,
                    status: TxnStatus::PreAccepted,
                },
            );
        }
        assert_eq!(idx.len(), N, "the whole write-set must be registered");
        assert_eq!(
            idx.deps_before_t0(&((N as u64) - 1).to_be_bytes(), &ts(8))
                .len(),
            1,
            "the last key of the write-set must resolve — no dropped tail"
        );
    }

    /// INVARIANT (no missed conflict after growth + GC): the per-key execution
    /// high-water-mark survives `gc_applied` even after the index has grown far
    /// past the old 100_000-entry boundary, so a LATER txn on a GC'd key still
    /// bumps past the earlier one (t_813caf39). The old bounded HWM map SKIPPED
    /// new keys once full, silently losing their post-GC conflict — a lost update.
    #[test]
    fn detects_conflict_after_growth_and_gc_past_the_old_boundary() {
        const OLD_CAP: usize = 100_000;
        let mut idx = ConflictIndex::new(OLD_CAP);
        let key: &[u8] = b"gc-key";
        let id = txn(42);
        idx.register(
            key,
            InFlightWrite {
                txn_id: id,
                t0: ts(42),
                accord_ts: Some(ts(900)),
                status: TxnStatus::Committed,
            },
        );
        idx.set_commit_ts(&id, ts(900));

        // Grow far past the old boundary on unrelated keys.
        for i in 0..(OLD_CAP as u64 + 5) {
            idx.register(
                &(i ^ 0x2000_0000_0000_0000).to_be_bytes(),
                InFlightWrite {
                    txn_id: txn(1),
                    t0: ts(1),
                    accord_ts: None,
                    status: TxnStatus::PreAccepted,
                },
            );
        }

        idx.mark_applied(&id);
        idx.gc_applied();
        assert_eq!(
            idx.max_conflicting_timestamp(key),
            Some(ts(900)),
            "the GC'd conflict must still be detected after the index grew past the old boundary"
        );
    }

    /// The read-table key is a RESERVED key, and both halves are load-bearing: the
    /// conflict index treats these keys uniformly with row keys, so a shape drift
    /// would silently start colliding with real data. Pin the namespace and the
    /// constructor/predicate agreement.
    #[test]
    fn the_read_table_key_stays_in_the_reserved_namespace() {
        let key = super::postgres_read_table_key("public.doctors");
        assert!(
            key.starts_with(b"\0ferrosa:"),
            "reserved namespace, got {:?}",
            String::from_utf8_lossy(&key)
        );
        assert!(
            key.ends_with(b"public.doctors"),
            "carries the qualified table"
        );
        assert!(
            super::is_postgres_read_table_key(&key),
            "the predicate must accept exactly what the constructor produces"
        );
        assert!(
            !super::is_postgres_read_table_key(b"doctor-a"),
            "a row key must never be mistaken for a read-table key"
        );
    }

    /// A READ registration is distinguishable from a WRITE, and that distinction is
    /// load-bearing: two readers of one key must never conflict, while a reader must
    /// still see a WRITER of the key it read. The read key is namespaced away from
    /// the key it consults, and the staleness check resolves a read key back to the
    /// key it named — never to the read key itself.
    #[test]
    fn a_read_registration_consults_the_key_it_read_not_the_read_key() {
        let written = super::postgres_write_table_key("public.doctors");
        let read = super::postgres_read_key(&written);
        assert_ne!(
            read, written,
            "a read registration must be a distinct key from the write it observes"
        );
        assert_eq!(
            super::postgres_read_lookup_key(&read),
            written.as_slice(),
            "a read consults the key it READ, so it sees a writer of that key"
        );
        assert_eq!(
            super::postgres_read_lookup_key(&written),
            written.as_slice(),
            "a plain key is consulted as itself"
        );
        // Two readers register the SAME read key, and nothing consults a read key:
        // so read-read can never conflict. (The lookup of a read key is its
        // underlying key, which carries only WRITER registrations.)
        let read_again = super::postgres_read_key(&written);
        assert_eq!(read, read_again, "two readers register the same read key");
        assert!(
            !super::is_postgres_write_table_key(&read),
            "a read registration must not be mistaken for the table-write watermark it consults"
        );
    }
}
