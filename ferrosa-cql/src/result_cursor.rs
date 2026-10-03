//! Server-side result cursors for CQL results that cannot be produced in scan
//! order.
//!
//! An `ORDER BY` over a non-clustering column cannot emit its first row until
//! it has seen every row, and a `DISTINCT` over an arbitrary projection has to
//! remember what it has already emitted. The router used to compute such a
//! result into one `Vec`, slice a page out of it by offset, and throw the rest
//! away — so the whole result sat on the heap, and every page re-ran the scan
//! and the sort (O(N²/page) for a paged client).
//!
//! A [`ResultCursor`] is the result computed ONCE, held on disk as the sorted
//! (or, with no `ORDER BY`, arrival-ordered) runs of an
//! [`ferrosa_storage::ExternalSorter`], and read a page at a time. Between
//! pages it is parked in the node's [`ResultCursorRegistry`] and the client
//! holds a signed [`CursorToken`] naming it.
//!
//! # What a parked cursor holds
//!
//! Only files and a merge head. A cursor never holds a live storage scan, a
//! scan-pool slot or a blocking thread between pages: the scan runs to
//! completion inside the request that builds the cursor, so a client that
//! stops fetching pins nothing but disk until the idle TTL deletes it. An
//! in-memory result is moved to disk before it is parked
//! ([`ferrosa_storage::SortedRows::into_disk_backed`]), so its heap cost is
//! one reader buffer and one row per run (at most the sorter's fan-in), not up
//! to a spill threshold of rows.
//!
//! # Lifecycle
//!
//! - **Built** by the first page's request, which must first get a
//!   [`CursorPermit`] (at most `max_open` per node; a request past that is
//!   refused with `Overloaded` before it scans anything).
//! - **Parked** between pages. A parked cursor is deleted, with its spill
//!   directory, when it sits idle past `idle_ttl` (swept by a background task
//!   and on every registry call), when the connection that parked it closes
//!   ([`ResultCursorRegistry::close_owner`]), or when its last page is read.
//! - **Checked out** while a page is read. It is removed from the registry,
//!   so a concurrent request with the same token finds nothing and errors. If
//!   the request is cancelled (its future dropped), the cursor drops with it
//!   and its directory is removed: cancellation cleans up by construction.
//!
//! # Fail loud
//!
//! A token that names a cursor this node no longer has — expired, closed,
//! already exhausted, issued by another node or before a restart — is an
//! error that says which, never a silent restart of the query and never an
//! empty or partial page presented as the end of the result.
//!
//! # Locks
//!
//! None. The registry is a copy-on-write map behind an [`ArcSwap`]; a parked
//! cursor sits in an [`ArcSwapOption`] that is only ever swapped out, which
//! hands its sole owner the value. Counters are atomics.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use arc_swap::{ArcSwap, ArcSwapOption};
use ferrosa_storage::{RowOrder, SortedRows, TempSortTableReservation};

use crate::error::CqlError;
use crate::types::CqlValue;

/// One result row.
pub type Row = Vec<Option<CqlValue>>;

/// First bytes of every cursor token payload. A pre-cursor server reads a
/// paging state's first four bytes as a big-endian partition-key length;
/// `0xFF` makes that length larger than any token, so an old node answers a
/// cursor token with "partition key truncated" instead of misreading it.
pub(crate) const CURSOR_TOKEN_MAGIC: [u8; 3] = [0xFF, b'R', b'C'];

/// Layout version of [`CursorToken`]. Bump it when the payload changes; a
/// token of another version is refused with a message naming both.
pub const CURSOR_TOKEN_VERSION: u8 = 1;

/// magic + version + epoch + id + seq + fingerprint.
const TOKEN_PAYLOAD_LEN: usize = 3 + 1 + 8 + 8 + 8 + 16;

/// Default idle lifetime of a parked cursor.
pub const DEFAULT_CURSOR_IDLE_TTL: Duration = Duration::from_secs(300);
/// Default number of cursors one node keeps open at once.
pub const DEFAULT_MAX_OPEN_CURSORS: usize = 256;

/// The opaque `paging_state` handed to a client whose result is a cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CursorToken {
    /// Random per-registry value. A token from another node, or from this
    /// node before a restart, carries a different epoch.
    pub epoch: u64,
    /// The cursor's id within its registry.
    pub id: u64,
    /// How many pages the cursor had served when this token was issued. Only
    /// the token for the cursor's current position is accepted.
    pub seq: u64,
    /// Hash of the query and role the cursor answers.
    pub fingerprint: [u8; 16],
}

impl CursorToken {
    /// Serialize and sign (the same HMAC as every other paging state).
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(TOKEN_PAYLOAD_LEN + 32);
        buf.extend_from_slice(&CURSOR_TOKEN_MAGIC);
        buf.push(CURSOR_TOKEN_VERSION);
        buf.extend_from_slice(&self.epoch.to_be_bytes());
        buf.extend_from_slice(&self.id.to_be_bytes());
        buf.extend_from_slice(&self.seq.to_be_bytes());
        buf.extend_from_slice(&self.fingerprint);
        debug_assert_eq!(buf.len(), TOKEN_PAYLOAD_LEN);
        crate::paging::sign_paging_payload(buf)
    }

    /// Verify the signature, then parse. A signed token that is not a cursor
    /// token (a scan-position or offset paging state from an older server, or
    /// from a query that paged another way) is refused with an explanation.
    pub fn decode(bytes: &[u8]) -> Result<Self, CqlError> {
        let payload = crate::paging::verify_paging_payload(bytes)?;
        if !payload.starts_with(&CURSOR_TOKEN_MAGIC) {
            return Err(CqlError::Invalid(
                "paging_state is not a result-cursor token: it is a scan-position paging \
                 state, issued by an older server version or by a query that pages another \
                 way. This query pages through a server-side result cursor; re-run it from \
                 its first page"
                    .into(),
            ));
        }
        let version = payload[CURSOR_TOKEN_MAGIC.len()..]
            .first()
            .copied()
            .ok_or_else(|| CqlError::Protocol("paging_state: cursor token truncated".into()))?;
        if version != CURSOR_TOKEN_VERSION {
            return Err(CqlError::Invalid(format!(
                "paging_state is a version-{version} result-cursor token; this server reads \
                 version {CURSOR_TOKEN_VERSION}. Re-run the query from its first page"
            )));
        }
        if payload.len() != TOKEN_PAYLOAD_LEN {
            return Err(CqlError::Protocol(format!(
                "paging_state: cursor token is {} bytes, expected {TOKEN_PAYLOAD_LEN}",
                payload.len()
            )));
        }
        let u64_at = |at: usize| {
            u64::from_be_bytes(
                payload[at..at + 8]
                    .try_into()
                    .expect("length checked above"),
            )
        };
        let mut fingerprint = [0u8; 16];
        fingerprint.copy_from_slice(&payload[28..44]);
        Ok(Self {
            epoch: u64_at(4),
            id: u64_at(12),
            seq: u64_at(20),
            fingerprint,
        })
    }
}

/// Tunables for a [`ResultCursorRegistry`].
#[derive(Debug, Clone, Copy)]
pub struct ResultCursorConfig {
    /// How long a parked cursor may sit unread before it is deleted.
    pub idle_ttl: Duration,
    /// Cursors one node keeps open at once (parked or being read).
    pub max_open: usize,
}

impl Default for ResultCursorConfig {
    fn default() -> Self {
        Self {
            idle_ttl: DEFAULT_CURSOR_IDLE_TTL,
            max_open: DEFAULT_MAX_OPEN_CURSORS,
        }
    }
}

impl ResultCursorConfig {
    /// `FERROSA_CQL_RESULT_CURSOR_TTL_SECS` and `FERROSA_CQL_RESULT_CURSOR_MAX`
    /// override the defaults. A value that does not parse as a positive number
    /// is reported and ignored.
    pub fn from_env() -> Self {
        let mut config = Self::default();
        if let Some(secs) = positive_env("FERROSA_CQL_RESULT_CURSOR_TTL_SECS") {
            config.idle_ttl = Duration::from_secs(secs);
        }
        if let Some(max) = positive_env("FERROSA_CQL_RESULT_CURSOR_MAX") {
            config.max_open = usize::try_from(max).unwrap_or(usize::MAX);
        }
        config
    }
}

fn positive_env(name: &str) -> Option<u64> {
    let raw = std::env::var(name).ok()?;
    match raw.trim().parse::<u64>() {
        Ok(n) if n > 0 => Some(n),
        _ => {
            tracing::warn!(
                variable = name,
                value = %raw,
                "ignoring result-cursor setting: not a positive integer"
            );
            None
        }
    }
}

/// A slot in the node's open-cursor budget. Held by a cursor from before its
/// scan starts until it is dropped, whatever drops it.
pub struct CursorPermit {
    open: Arc<AtomicUsize>,
}

impl Drop for CursorPermit {
    fn drop(&mut self) {
        self.open.fetch_sub(1, Ordering::AcqRel);
    }
}

/// One page read from a cursor.
pub struct CursorPage {
    pub rows: Vec<Row>,
    /// Whether rows remain after this page.
    pub more: bool,
}

/// A computed result, read a page at a time. See the module docs.
pub struct ResultCursor {
    // Field order is drop order: the readers close before the directory they
    // read from is removed.
    rows: Option<SortedRows<Row, RowOrder>>,
    lookahead: Option<Row>,
    order: RowOrder,
    /// Rows still owed under the query's `LIMIT`, if it has one.
    remaining_limit: Option<u64>,
    /// `true` when the stored rows are already the projected result rows.
    /// `false` when they are full table rows that each page projects.
    projected: bool,
    fingerprint: [u8; 16],
    owner: String,
    seq: u64,
    spool: TempSortTableReservation,
    _permit: CursorPermit,
}

impl std::fmt::Debug for ResultCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResultCursor")
            .field("seq", &self.seq)
            .field("owner", &self.owner)
            .field("remaining_limit", &self.remaining_limit)
            .field("projected", &self.projected)
            .field("spill_dir", &self.spool.path())
            .finish_non_exhaustive()
    }
}

impl ResultCursor {
    /// Wrap a finished sort. `spool` owns the directory `rows` reads from.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        rows: SortedRows<Row, RowOrder>,
        order: RowOrder,
        spool: TempSortTableReservation,
        permit: CursorPermit,
        limit: Option<u64>,
        projected: bool,
        fingerprint: [u8; 16],
        owner: String,
    ) -> Self {
        Self {
            rows: Some(rows),
            lookahead: None,
            order,
            remaining_limit: limit,
            projected,
            fingerprint,
            owner,
            seq: 0,
            spool,
            _permit: permit,
        }
    }

    /// The directory holding this cursor's runs.
    pub fn spill_dir(&self) -> &Path {
        self.spool.path()
    }

    /// Pages served so far.
    pub fn seq(&self) -> u64 {
        self.seq
    }

    fn pull(&mut self) -> Result<Option<Row>, CqlError> {
        if let Some(row) = self.lookahead.take() {
            return Ok(Some(row));
        }
        let Some(rows) = self.rows.as_mut() else {
            return Ok(None);
        };
        rows.next()
            .transpose()
            .map_err(|e| CqlError::ServerError(format!("result cursor: read spilled run: {e}")))
    }

    fn peek_more(&mut self) -> Result<bool, CqlError> {
        if self.lookahead.is_none() {
            self.lookahead = self.pull()?;
        }
        Ok(self.lookahead.is_some())
    }

    /// Read the next page of at most `cap` rows. `project` turns a batch of
    /// stored full rows into result rows (one for one) when the cursor stores
    /// full rows; it is not called for a cursor that stores projected rows.
    pub fn next_page(
        &mut self,
        cap: usize,
        project: impl FnOnce(Vec<Row>) -> Result<Vec<Row>, CqlError>,
    ) -> Result<CursorPage, CqlError> {
        assert!(cap > 0, "a zero-row page would never advance the cursor");
        let cap = match self.remaining_limit {
            Some(left) => cap.min(usize::try_from(left).unwrap_or(usize::MAX)),
            None => cap,
        };
        let mut batch = Vec::with_capacity(cap.min(4096));
        while batch.len() < cap {
            match self.pull()? {
                Some(row) => batch.push(row),
                None => break,
            }
        }
        let pulled = batch.len();
        let rows = if self.projected || batch.is_empty() {
            batch
        } else {
            project(batch)?
        };
        if rows.len() != pulled {
            return Err(CqlError::ServerError(format!(
                "result cursor: projecting {pulled} rows produced {}; projection must be one \
                 row in, one row out",
                rows.len()
            )));
        }
        if let Some(left) = self.remaining_limit.as_mut() {
            *left = left.saturating_sub(pulled as u64);
        }
        self.seq += 1;
        let more = self.remaining_limit != Some(0) && self.peek_more()?;
        Ok(CursorPage { rows, more })
    }

    /// Read every remaining row. For a client that asked for an unpaged
    /// result: the protocol requires all of it in one response.
    pub fn drain(
        &mut self,
        mut project: impl FnMut(Vec<Row>) -> Result<Vec<Row>, CqlError>,
    ) -> Result<Vec<Row>, CqlError> {
        const BATCH: usize = 4096;
        let mut out = Vec::new();
        loop {
            let page = self.next_page(BATCH, &mut project)?;
            out.extend(page.rows);
            if !page.more {
                return Ok(out);
            }
        }
    }

    /// Move any in-memory remainder to disk so the parked cursor holds no
    /// more than its merge head.
    fn make_disk_backed(&mut self) -> Result<(), CqlError> {
        let Some(rows) = self.rows.take() else {
            return Ok(());
        };
        let rows = rows
            .into_disk_backed(self.spool.path(), self.order.clone())
            .map_err(|e| CqlError::ServerError(format!("result cursor: park result: {e}")))?;
        self.rows = Some(rows);
        Ok(())
    }
}

/// A parked cursor plus what a request must match to take it.
struct Parked {
    cursor: ArcSwapOption<ResultCursor>,
    fingerprint: [u8; 16],
    seq: u64,
    owner: String,
    spill_dir: PathBuf,
    parked_at: Instant,
}

/// Counters a registry keeps for operators and tests.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CursorStats {
    pub open: usize,
    pub parked: usize,
    pub refused: u64,
    pub expired: u64,
    pub closed: u64,
}

/// The node's parked result cursors. See the module docs.
pub struct ResultCursorRegistry {
    config: ResultCursorConfig,
    epoch: u64,
    next_id: AtomicU64,
    open: Arc<AtomicUsize>,
    parked: ArcSwap<HashMap<u64, Arc<Parked>>>,
    sweeper_started: AtomicBool,
    refused: AtomicU64,
    expired: AtomicU64,
    closed: AtomicU64,
}

impl Default for ResultCursorRegistry {
    fn default() -> Self {
        Self::new(ResultCursorConfig::default())
    }
}

impl ResultCursorRegistry {
    pub fn new(config: ResultCursorConfig) -> Self {
        assert!(
            config.max_open > 0,
            "a registry must admit at least one cursor"
        );
        assert!(
            !config.idle_ttl.is_zero(),
            "a zero idle TTL expires every cursor"
        );
        Self {
            config,
            epoch: rand::random::<u64>(),
            next_id: AtomicU64::new(1),
            open: Arc::new(AtomicUsize::new(0)),
            parked: ArcSwap::from_pointee(HashMap::new()),
            sweeper_started: AtomicBool::new(false),
            refused: AtomicU64::new(0),
            expired: AtomicU64::new(0),
            closed: AtomicU64::new(0),
        }
    }

    pub fn config(&self) -> ResultCursorConfig {
        self.config
    }

    pub fn stats(&self) -> CursorStats {
        CursorStats {
            open: self.open.load(Ordering::Acquire),
            parked: self.parked.load().len(),
            refused: self.refused.load(Ordering::Relaxed),
            expired: self.expired.load(Ordering::Relaxed),
            closed: self.closed.load(Ordering::Relaxed),
        }
    }

    /// Spill directories of the parked cursors (observability and tests).
    pub fn parked_spill_dirs(&self) -> Vec<PathBuf> {
        self.parked
            .load()
            .values()
            .map(|p| p.spill_dir.clone())
            .collect()
    }

    /// Reserve a slot for a new cursor, or refuse with `Overloaded` when the
    /// node already has `max_open`. Call BEFORE scanning, so a refused query
    /// costs nothing.
    pub fn admit(&self) -> Result<CursorPermit, CqlError> {
        self.sweep_expired_at(Instant::now());
        let max = self.config.max_open;
        let admitted = self
            .open
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < max).then_some(n + 1)
            });
        match admitted {
            Ok(_) => Ok(CursorPermit {
                open: self.open.clone(),
            }),
            Err(n) => {
                self.refused.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    open = n,
                    max,
                    "refusing a query that needs a result cursor: the node has its maximum \
                     open (FERROSA_CQL_RESULT_CURSOR_MAX)"
                );
                Err(CqlError::Overloaded(format!(
                    "this node already holds {n} open result cursors (the maximum, \
                     FERROSA_CQL_RESULT_CURSOR_MAX={max}); retry after other paged ORDER BY / \
                     DISTINCT reads finish or expire"
                )))
            }
        }
    }

    /// Park `cursor` for its next page and return the token naming it. The
    /// cursor's in-memory remainder is moved to disk first.
    pub fn park(
        self: &Arc<Self>,
        mut cursor: ResultCursor,
        id: Option<u64>,
    ) -> Result<CursorToken, CqlError> {
        cursor.make_disk_backed()?;
        let id = id.unwrap_or_else(|| self.next_id.fetch_add(1, Ordering::Relaxed));
        let token = CursorToken {
            epoch: self.epoch,
            id,
            seq: cursor.seq,
            fingerprint: cursor.fingerprint,
        };
        let parked = Arc::new(Parked {
            fingerprint: cursor.fingerprint,
            seq: cursor.seq,
            owner: cursor.owner.clone(),
            spill_dir: cursor.spill_dir().to_path_buf(),
            parked_at: Instant::now(),
            cursor: ArcSwapOption::from_pointee(cursor),
        });
        self.parked.rcu(|map| {
            let mut next = HashMap::clone(map);
            next.insert(id, parked.clone());
            next
        });
        self.ensure_sweeper();
        Ok(token)
    }

    /// Take the cursor `token` names, for `fingerprint`'s query, out of the
    /// registry. Every way this can fail names its cause.
    pub fn take(
        &self,
        token: &CursorToken,
        fingerprint: &[u8; 16],
    ) -> Result<(u64, ResultCursor), CqlError> {
        if token.epoch != self.epoch {
            return Err(CqlError::Invalid(
                "paging_state names a result cursor from another node, or from this node before \
                 it restarted; cursors live on the node that built them. Re-run the query from \
                 its first page"
                    .into(),
            ));
        }
        let now = Instant::now();
        self.sweep_expired_at(now);
        let map = self.parked.load_full();
        let Some(parked) = map.get(&token.id).cloned() else {
            return Err(CqlError::Invalid(format!(
                "paging_state names result cursor {} which this node no longer holds: it was \
                 idle longer than {}s, the connection that opened it closed, its last page was \
                 already read, or another request is reading it. Re-run the query from its \
                 first page",
                token.id,
                self.config.idle_ttl.as_secs()
            )));
        };
        if &parked.fingerprint != fingerprint || &token.fingerprint != fingerprint {
            return Err(CqlError::Invalid(
                "paging_state belongs to a different query or role than the one it was sent \
                 with"
                    .into(),
            ));
        }
        if parked.seq != token.seq {
            return Err(CqlError::Invalid(format!(
                "paging_state is stale: result cursor {} has served {} pages and this token is \
                 for page {}. Use the paging_state from the most recent page",
                token.id, parked.seq, token.seq
            )));
        }
        // Remove exactly this parked version. Only the request whose removal
        // commits gets it; a concurrent one sees `None` below.
        let previous = self.parked.rcu(|map| {
            let mut next = HashMap::clone(map);
            if next.get(&token.id).is_some_and(|p| Arc::ptr_eq(p, &parked)) {
                next.remove(&token.id);
            }
            next
        });
        let won = previous
            .get(&token.id)
            .is_some_and(|p| Arc::ptr_eq(p, &parked));
        let taken = if won { parked.cursor.swap(None) } else { None };
        let Some(cursor) = taken else {
            return Err(CqlError::Invalid(format!(
                "paging_state names result cursor {} which another request is reading or has \
                 just expired. Re-run the query from its first page",
                token.id
            )));
        };
        let cursor = Arc::try_unwrap(cursor).map_err(|_| {
            CqlError::ServerError(format!(
                "result cursor {}: still shared after removal; this is a bug",
                token.id
            ))
        })?;
        Ok((token.id, cursor))
    }

    /// Delete every cursor parked by `owner` (a connection's peer address):
    /// the connection closed, so nothing will ask for their next pages.
    pub fn close_owner(&self, owner: &str) -> usize {
        let removed = self.remove_where(|p| p.owner == owner);
        if removed > 0 {
            self.closed.fetch_add(removed as u64, Ordering::Relaxed);
            tracing::info!(
                owner,
                cursors = removed,
                "deleted result cursors: their connection closed"
            );
        }
        removed
    }

    /// Delete every cursor idle past the TTL as of `now`.
    pub fn sweep_expired_at(&self, now: Instant) -> usize {
        let ttl = self.config.idle_ttl;
        let expired = |p: &Parked| now.saturating_duration_since(p.parked_at) >= ttl;
        if !self.parked.load().values().any(|p| expired(p)) {
            return 0;
        }
        let removed = self.remove_where(expired);
        if removed > 0 {
            self.expired.fetch_add(removed as u64, Ordering::Relaxed);
            tracing::info!(
                cursors = removed,
                idle_ttl_secs = ttl.as_secs(),
                "deleted idle result cursors"
            );
        }
        removed
    }

    /// Remove the parked cursors matching `pred` and drop them (which removes
    /// their spill directories). Returns how many this call removed.
    fn remove_where(&self, pred: impl Fn(&Parked) -> bool) -> usize {
        let previous = self.parked.rcu(|map| {
            map.iter()
                .filter(|(_, p)| !pred(p))
                .map(|(id, p)| (*id, p.clone()))
                .collect::<HashMap<_, _>>()
        });
        let mut removed = 0;
        for parked in previous.values().filter(|p| pred(p)) {
            // Swapping out drops the cursor here unless a concurrent take
            // already won it, in which case that request owns it.
            if parked.cursor.swap(None).is_some() {
                removed += 1;
            }
        }
        removed
    }

    /// Start the idle sweeper the first time a cursor is parked. It holds only
    /// a weak reference and exits when the registry is dropped.
    fn ensure_sweeper(self: &Arc<Self>) {
        if self.sweeper_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            self.sweeper_started.store(false, Ordering::Release);
            tracing::warn!(
                "result-cursor sweeper not started (no tokio runtime); idle cursors are deleted \
                 only on the next registry call"
            );
            return;
        };
        let weak: Weak<Self> = Arc::downgrade(self);
        let period =
            (self.config.idle_ttl / 4).clamp(Duration::from_millis(50), Duration::from_secs(30));
        handle.spawn(async move {
            let mut tick = tokio::time::interval(period);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let Some(registry) = weak.upgrade() else {
                    return;
                };
                registry.sweep_expired_at(Instant::now());
            }
        });
    }
}

/// Hash of what a cursor answers: the role, the keyspace and the statement.
/// A token presented with any other query or by any other role is refused.
pub fn query_fingerprint(role: &str, keyspace: &str, statement_debug: &str) -> [u8; 16] {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    for part in [role, keyspace, statement_debug] {
        h.update((part.len() as u64).to_be_bytes());
        h.update(part.as_bytes());
    }
    let digest = h.finalize();
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest[..16]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_storage::ExternalSorter;

    fn int_row(v: i32) -> Row {
        vec![Some(CqlValue::Int(v))]
    }

    fn val(row: &Row) -> i32 {
        match row[0] {
            Some(CqlValue::Int(v)) => v,
            ref other => panic!("expected int, got {other:?}"),
        }
    }

    /// A cursor over `0..n` (pushed in reverse, sorted ascending) whose spool
    /// is a fresh directory under `root`.
    fn cursor(
        registry: &ResultCursorRegistry,
        root: &Path,
        n: i32,
        threshold: u64,
        owner: &str,
    ) -> ResultCursor {
        let permit = registry.admit().unwrap();
        let dir = tempfile::Builder::new().tempdir_in(root).unwrap().keep();
        let order = RowOrder::new(vec![(0, true)]);
        let mut sorter = ExternalSorter::new(&dir, order.clone(), threshold);
        for v in (0..n).rev() {
            sorter.push(int_row(v)).unwrap();
        }
        ResultCursor::new(
            sorter.finish().unwrap(),
            order,
            TempSortTableReservation::claim_dir(dir),
            permit,
            None,
            true,
            [7; 16],
            owner.to_string(),
        )
    }

    fn no_projection(_: Vec<Row>) -> Result<Vec<Row>, CqlError> {
        unreachable!("projected cursors never project")
    }

    #[test]
    fn token_round_trips_and_is_signed() {
        let token = CursorToken {
            epoch: 1,
            id: 2,
            seq: 3,
            fingerprint: [9; 16],
        };
        let bytes = token.encode();
        assert_eq!(CursorToken::decode(&bytes).unwrap(), token);
        let mut forged = bytes.clone();
        forged[10] ^= 1;
        assert!(matches!(
            CursorToken::decode(&forged),
            Err(CqlError::Protocol(m)) if m.contains("signature")
        ));
    }

    /// An existing driver resends whatever paging state it was given. A
    /// scan-position state from an older server must get a clear error, and a
    /// cursor token sent to an older-format decoder must not parse as a key.
    #[test]
    fn foreign_and_future_paging_states_are_refused_by_name() {
        let legacy = crate::paging::PagingState {
            partition_key: 40u64.to_be_bytes().to_vec(),
            clustering_key: Vec::new(),
            remaining_in_partition: false,
        }
        .encode();
        let err = CursorToken::decode(&legacy).unwrap_err().to_string();
        assert!(err.contains("not a result-cursor token"), "{err}");

        let mut future = CURSOR_TOKEN_MAGIC.to_vec();
        future.push(CURSOR_TOKEN_VERSION + 1);
        future.extend_from_slice(&[0; TOKEN_PAYLOAD_LEN - 4]);
        let err = CursorToken::decode(&crate::paging::sign_paging_payload(future))
            .unwrap_err()
            .to_string();
        assert!(err.contains("version-2"), "{err}");

        let token = CursorToken {
            epoch: 1,
            id: 1,
            seq: 0,
            fingerprint: [0; 16],
        }
        .encode();
        let err = crate::paging::PagingState::decode(&token)
            .unwrap_err()
            .to_string();
        assert!(err.contains("result-cursor token"), "{err}");
        // The pre-cursor layout reads the first four bytes as a key length.
        let as_len = u32::from_be_bytes(token[..4].try_into().unwrap()) as usize;
        assert!(
            as_len > token.len(),
            "an old decoder must see a truncated key"
        );
    }

    #[test]
    fn pages_resume_the_same_cursor_and_finish() {
        let root = tempfile::tempdir().unwrap();
        let registry = Arc::new(ResultCursorRegistry::default());
        let mut c = cursor(&registry, root.path(), 25, 16, "peer");
        let first = c.next_page(10, no_projection).unwrap();
        assert!(first.more);
        let mut got: Vec<i32> = first.rows.iter().map(val).collect();
        let mut token = registry.park(c, None).unwrap();
        for _ in 0..10 {
            let (id, mut c) = registry.take(&token, &[7; 16]).unwrap();
            let page = c.next_page(10, no_projection).unwrap();
            got.extend(page.rows.iter().map(val));
            if !page.more {
                break;
            }
            token = registry.park(c, Some(id)).unwrap();
        }
        assert_eq!(got, (0..25).collect::<Vec<_>>());
        assert_eq!(
            registry.stats().open,
            0,
            "an exhausted cursor releases its slot"
        );
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }

    #[test]
    fn a_parked_in_memory_cursor_moves_to_disk() {
        let root = tempfile::tempdir().unwrap();
        let registry = Arc::new(ResultCursorRegistry::default());
        let mut c = cursor(&registry, root.path(), 50, u64::MAX, "peer");
        assert!(!c.rows.as_ref().unwrap().is_disk_backed());
        c.next_page(5, no_projection).unwrap();
        let token = registry.park(c, None).unwrap();
        let (_, c) = registry.take(&token, &[7; 16]).unwrap();
        assert!(c.rows.as_ref().unwrap().is_disk_backed());
    }

    #[test]
    fn expiry_deletes_the_spill_dir_and_the_token_errors() {
        let root = tempfile::tempdir().unwrap();
        let registry = Arc::new(ResultCursorRegistry::default());
        let mut c = cursor(&registry, root.path(), 30, 16, "peer");
        let dir = c.spill_dir().to_path_buf();
        c.next_page(5, no_projection).unwrap();
        let token = registry.park(c, None).unwrap();
        assert!(dir.exists());
        let later = Instant::now() + registry.config().idle_ttl + Duration::from_secs(1);
        assert_eq!(registry.sweep_expired_at(later), 1);
        assert!(!dir.exists(), "expiry must delete the spill directory");
        assert_eq!(registry.stats().open, 0);
        let err = registry.take(&token, &[7; 16]).unwrap_err().to_string();
        assert!(err.contains("no longer holds"), "{err}");
    }

    #[test]
    fn closing_the_owner_connection_deletes_its_cursors_only() {
        let root = tempfile::tempdir().unwrap();
        let registry = Arc::new(ResultCursorRegistry::default());
        let mine = cursor(&registry, root.path(), 10, 16, "a:1");
        let theirs = cursor(&registry, root.path(), 10, 16, "b:2");
        let (mine_dir, theirs_dir) = (
            mine.spill_dir().to_path_buf(),
            theirs.spill_dir().to_path_buf(),
        );
        let mine_token = registry.park(mine, None).unwrap();
        registry.park(theirs, None).unwrap();
        assert_eq!(registry.close_owner("a:1"), 1);
        assert!(!mine_dir.exists());
        assert!(theirs_dir.exists());
        assert!(registry.take(&mine_token, &[7; 16]).is_err());
        assert_eq!(registry.stats().parked, 1);
    }

    #[test]
    fn a_cancelled_page_read_deletes_the_cursor() {
        let root = tempfile::tempdir().unwrap();
        let registry = Arc::new(ResultCursorRegistry::default());
        let c = cursor(&registry, root.path(), 10, 16, "peer");
        let dir = c.spill_dir().to_path_buf();
        let token = registry.park(c, None).unwrap();
        let (_, taken) = registry.take(&token, &[7; 16]).unwrap();
        // The request reading the page is dropped mid-flight.
        drop(taken);
        assert!(!dir.exists());
        assert_eq!(registry.stats().open, 0);
        assert!(registry.take(&token, &[7; 16]).is_err());
    }

    #[test]
    fn wrong_query_stale_page_and_other_epoch_are_refused() {
        let root = tempfile::tempdir().unwrap();
        let registry = Arc::new(ResultCursorRegistry::default());
        let mut c = cursor(&registry, root.path(), 30, 16, "peer");
        c.next_page(5, no_projection).unwrap();
        let first = registry.park(c, None).unwrap();

        let err = registry.take(&first, &[8; 16]).unwrap_err().to_string();
        assert!(err.contains("different query"), "{err}");

        let (id, mut c) = registry.take(&first, &[7; 16]).unwrap();
        c.next_page(5, no_projection).unwrap();
        let second = registry.park(c, Some(id)).unwrap();
        let err = registry.take(&first, &[7; 16]).unwrap_err().to_string();
        assert!(err.contains("stale"), "{err}");

        let other = CursorToken {
            epoch: second.epoch.wrapping_add(1),
            ..second
        };
        let err = registry.take(&other, &[7; 16]).unwrap_err().to_string();
        assert!(err.contains("another node"), "{err}");
        assert!(registry.take(&second, &[7; 16]).is_ok());
    }

    #[test]
    fn admission_is_bounded_and_refusal_is_loud() {
        let registry = ResultCursorRegistry::new(ResultCursorConfig {
            max_open: 2,
            ..ResultCursorConfig::default()
        });
        let a = registry.admit().unwrap();
        let _b = registry.admit().unwrap();
        assert!(matches!(registry.admit(), Err(CqlError::Overloaded(_))));
        assert_eq!(registry.stats().refused, 1);
        drop(a);
        assert!(registry.admit().is_ok());
    }

    #[test]
    fn limit_counts_across_pages() {
        let root = tempfile::tempdir().unwrap();
        let registry = Arc::new(ResultCursorRegistry::default());
        let mut c = cursor(&registry, root.path(), 30, 16, "peer");
        c.remaining_limit = Some(12);
        let a = c.next_page(5, no_projection).unwrap();
        let b = c.next_page(5, no_projection).unwrap();
        let last = c.next_page(5, no_projection).unwrap();
        assert_eq!((a.rows.len(), b.rows.len(), last.rows.len()), (5, 5, 2));
        assert!(a.more && b.more && !last.more);
    }

    #[tokio::test]
    async fn the_sweeper_deletes_idle_cursors_without_further_requests() {
        let root = tempfile::tempdir().unwrap();
        let registry = Arc::new(ResultCursorRegistry::new(ResultCursorConfig {
            idle_ttl: Duration::from_millis(100),
            ..ResultCursorConfig::default()
        }));
        let c = cursor(&registry, root.path(), 10, 16, "peer");
        let dir = c.spill_dir().to_path_buf();
        registry.park(c, None).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while dir.exists() {
            assert!(
                Instant::now() < deadline,
                "sweeper never deleted {}",
                dir.display()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(registry.stats().open, 0);
        assert_eq!(registry.stats().expired, 1);
    }
}
