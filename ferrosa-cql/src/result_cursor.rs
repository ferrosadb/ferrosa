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
//! stops fetching pins nothing but disk until it expires. An in-memory result
//! is moved to disk before it is parked
//! ([`ferrosa_storage::SortedRows::into_disk_backed`]), so its heap cost is one
//! reader buffer and one row per run (at most the sorter's fan-in), not up to a
//! spill threshold of rows.
//!
//! The stored rows are FINAL result rows (projected when the cursor was built),
//! each prefixed by its sort key, so a page needs nothing but the cursor: any
//! node holding it can serve it, with no statement or schema context.
//!
//! # Any coordinator can ask for the next page
//!
//! Cassandra drivers treat `paging_state` as portable. scylla-rust-driver pins
//! an iterator to its first node only until that node fails, then retries the
//! remaining pages on the next node in its plan, with the same paging state.
//! So the token names the node that owns the cursor, and a node that receives
//! a token for another node's cursor forwards the page request over internode
//! (`MsgType::ResultCursorPage`, [`forward_page`]) and relays the owner's
//! reply. It forwards only to a peer that advertised
//! `ferrosa_net::handshake::CAP_RESULT_CURSOR_PAGE`; an older node does not
//! know the message type and would drop the whole connection, so it gets a
//! named error instead.
//!
//! # Lifecycle
//!
//! - **Built** by the first page's request, which must first get a
//!   [`CursorPermit`] (at most `max_open` per node; a request past that is
//!   refused with `Overloaded` before it scans anything).
//! - **Parked** between pages. A parked cursor is deleted, with its spill
//!   directory, when it sits idle past `idle_ttl`, when its last page is read,
//!   or `close_grace` after the connection that parked it closes
//!   ([`ResultCursorRegistry::close_owner`]) unless a page is fetched in the
//!   meantime (from any connection, on any node). Expiry is swept by a
//!   background task and on every registry call.
//! - **Checked out** while a page is read. It is removed from the registry,
//!   so a concurrent request with the same token finds nothing and errors. If
//!   the request is cancelled (its future dropped), the cursor drops with it
//!   and its directory is removed: cancellation cleans up by construction.
//!
//! # Fail loud
//!
//! A token that names a cursor its owner no longer has — expired, closed and
//! past its grace, already exhausted, the owner restarted or unreachable — is
//! an error that says which, never a silent restart of the query and never an
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
use uuid::Uuid;

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
///
/// - 1: epoch, id, seq, fingerprint (never released).
/// - 2: adds the owner node's host id, so any node can forward the page.
pub const CURSOR_TOKEN_VERSION: u8 = 2;

/// magic + version + owner + epoch + id + seq + fingerprint.
const TOKEN_PAYLOAD_LEN: usize = 3 + 1 + 16 + 8 + 8 + 8 + 16;

/// Default idle lifetime of a parked cursor.
pub const DEFAULT_CURSOR_IDLE_TTL: Duration = Duration::from_secs(300);
/// Default number of cursors one node keeps open at once.
pub const DEFAULT_MAX_OPEN_CURSORS: usize = 256;
/// Default time a cursor survives the connection that parked it closing.
pub const DEFAULT_CURSOR_CLOSE_GRACE: Duration = Duration::from_secs(30);

/// The opaque `paging_state` handed to a client whose result is a cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CursorToken {
    /// The node holding the cursor (its host id).
    pub owner: Uuid,
    /// Random per-registry value. The owner refuses a token from before it
    /// restarted: same host id, different epoch.
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
        buf.extend_from_slice(self.owner.as_bytes());
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
        let owner = Uuid::from_slice(&payload[4..20]).expect("16 bytes, length checked above");
        let mut fingerprint = [0u8; 16];
        fingerprint.copy_from_slice(&payload[44..60]);
        Ok(Self {
            owner,
            epoch: u64_at(20),
            id: u64_at(28),
            seq: u64_at(36),
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
    /// How long a parked cursor survives the connection that parked it
    /// closing. A driver whose connection broke retries the next page on
    /// another connection or node; zero deletes at close.
    pub close_grace: Duration,
}

impl Default for ResultCursorConfig {
    fn default() -> Self {
        Self {
            idle_ttl: DEFAULT_CURSOR_IDLE_TTL,
            max_open: DEFAULT_MAX_OPEN_CURSORS,
            close_grace: DEFAULT_CURSOR_CLOSE_GRACE,
        }
    }
}

impl ResultCursorConfig {
    /// `FERROSA_CQL_RESULT_CURSOR_TTL_SECS`, `FERROSA_CQL_RESULT_CURSOR_MAX`
    /// and `FERROSA_CQL_RESULT_CURSOR_CLOSE_GRACE_SECS` override the defaults.
    /// A value that does not parse is reported and ignored.
    pub fn from_env() -> Self {
        let mut config = Self::default();
        if let Some(secs) = env_u64("FERROSA_CQL_RESULT_CURSOR_TTL_SECS", false) {
            config.idle_ttl = Duration::from_secs(secs);
        }
        if let Some(max) = env_u64("FERROSA_CQL_RESULT_CURSOR_MAX", false) {
            config.max_open = usize::try_from(max).unwrap_or(usize::MAX);
        }
        if let Some(secs) = env_u64("FERROSA_CQL_RESULT_CURSOR_CLOSE_GRACE_SECS", true) {
            config.close_grace = Duration::from_secs(secs);
        }
        config
    }
}

fn env_u64(name: &str, zero_ok: bool) -> Option<u64> {
    let raw = std::env::var(name).ok()?;
    match raw.trim().parse::<u64>() {
        Ok(n) if n > 0 || zero_ok => Some(n),
        _ => {
            tracing::warn!(
                variable = name,
                value = %raw,
                "ignoring result-cursor setting: not a valid count of seconds/cursors"
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
    /// Leading sort-key columns on every stored row, stripped before a row is
    /// returned.
    key_len: usize,
    /// Rows still owed under the query's `LIMIT`, if it has one.
    remaining_limit: Option<u64>,
    fingerprint: [u8; 16],
    seq: u64,
    spool: TempSortTableReservation,
    _permit: CursorPermit,
}

impl std::fmt::Debug for ResultCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResultCursor")
            .field("seq", &self.seq)
            .field("key_len", &self.key_len)
            .field("remaining_limit", &self.remaining_limit)
            .field("spill_dir", &self.spool.path())
            .finish_non_exhaustive()
    }
}

impl ResultCursor {
    /// Wrap a finished sort of final result rows, each prefixed by `key_len`
    /// sort-key columns. `spool` owns the directory `rows` reads from.
    pub fn new(
        rows: SortedRows<Row, RowOrder>,
        order: RowOrder,
        key_len: usize,
        spool: TempSortTableReservation,
        permit: CursorPermit,
        limit: Option<u64>,
        fingerprint: [u8; 16],
    ) -> Self {
        Self {
            rows: Some(rows),
            lookahead: None,
            order,
            key_len,
            remaining_limit: limit,
            fingerprint,
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

    /// Read the next page of at most `cap` rows.
    pub fn next_page(&mut self, cap: usize) -> Result<CursorPage, CqlError> {
        assert!(cap > 0, "a zero-row page would never advance the cursor");
        let cap = match self.remaining_limit {
            Some(left) => cap.min(usize::try_from(left).unwrap_or(usize::MAX)),
            None => cap,
        };
        let mut rows = Vec::with_capacity(cap.min(4096));
        while rows.len() < cap {
            match self.pull()? {
                Some(mut row) => {
                    if row.len() < self.key_len {
                        return Err(CqlError::ServerError(format!(
                            "result cursor: stored row has {} columns, fewer than its {}-column \
                             sort key",
                            row.len(),
                            self.key_len
                        )));
                    }
                    row.drain(..self.key_len);
                    rows.push(row);
                }
                None => break,
            }
        }
        if let Some(left) = self.remaining_limit.as_mut() {
            *left = left.saturating_sub(rows.len() as u64);
        }
        self.seq += 1;
        let more = self.remaining_limit != Some(0) && self.peek_more()?;
        Ok(CursorPage { rows, more })
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
    /// The client connection that parked it (`None`: parked for a page
    /// forwarded from another node).
    owner: Option<String>,
    /// Set once `owner` closed, so a second close does not extend the grace.
    owner_closed: AtomicBool,
    spill_dir: PathBuf,
    /// When it is deleted unless a page is fetched first, in milliseconds
    /// since the registry's `base`: the idle TTL, lowered to the close grace
    /// once its connection closes. Atomic so a close updates it in place.
    deadline_ms: AtomicU64,
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
    node: Uuid,
    epoch: u64,
    /// Origin of the `deadline_ms` clock.
    base: Instant,
    next_id: AtomicU64,
    open: Arc<AtomicUsize>,
    parked: ArcSwap<HashMap<u64, Arc<Parked>>>,
    sweeper_started: AtomicBool,
    refused: AtomicU64,
    expired: AtomicU64,
    closed: AtomicU64,
}

impl Default for ResultCursorRegistry {
    /// A registry for a node with the nil host id: single-node tests only.
    fn default() -> Self {
        Self::new(ResultCursorConfig::default(), Uuid::nil())
    }
}

impl ResultCursorRegistry {
    /// A registry for the node whose host id is `node`.
    pub fn new(config: ResultCursorConfig, node: Uuid) -> Self {
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
            node,
            epoch: rand::random::<u64>(),
            base: Instant::now(),
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

    /// `at` on the registry's deadline clock (saturating at its origin).
    fn ms(&self, at: Instant) -> u64 {
        u64::try_from(at.saturating_duration_since(self.base).as_millis()).unwrap_or(u64::MAX)
    }

    /// The host id of the node this registry belongs to.
    pub fn node(&self) -> Uuid {
        self.node
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
    /// cursor's in-memory remainder is moved to disk first. `owner` is the
    /// client connection whose closing starts the close grace; `None` (a page
    /// served for another node) leaves only the idle TTL.
    pub fn park(
        self: &Arc<Self>,
        mut cursor: ResultCursor,
        id: Option<u64>,
        owner: Option<String>,
    ) -> Result<CursorToken, CqlError> {
        cursor.make_disk_backed()?;
        let id = id.unwrap_or_else(|| self.next_id.fetch_add(1, Ordering::Relaxed));
        let token = CursorToken {
            owner: self.node,
            epoch: self.epoch,
            id,
            seq: cursor.seq,
            fingerprint: cursor.fingerprint,
        };
        let parked = Arc::new(Parked {
            fingerprint: cursor.fingerprint,
            seq: cursor.seq,
            owner,
            owner_closed: AtomicBool::new(false),
            spill_dir: cursor.spill_dir().to_path_buf(),
            deadline_ms: AtomicU64::new(self.ms(Instant::now() + self.config.idle_ttl)),
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
        if token.owner != self.node {
            return Err(CqlError::Invalid(format!(
                "paging_state names a result cursor on node {}, not this node ({}); its page \
                 must be fetched from or forwarded to that node",
                token.owner, self.node
            )));
        }
        if token.epoch != self.epoch {
            return Err(CqlError::Invalid(format!(
                "paging_state names a result cursor that node {} held before it restarted; \
                 cursors do not survive a restart. Re-run the query from its first page",
                self.node
            )));
        }
        let now = Instant::now();
        self.sweep_expired_at(now);
        let map = self.parked.load_full();
        let Some(parked) = map.get(&token.id).cloned() else {
            return Err(CqlError::Invalid(format!(
                "paging_state names result cursor {} which node {} no longer holds: it was idle \
                 longer than {}s, the connection that opened it closed more than {}s ago, its \
                 last page was already read, or another request is reading it. Re-run the \
                 query from its first page",
                token.id,
                self.node,
                self.config.idle_ttl.as_secs(),
                self.config.close_grace.as_secs()
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

    /// Serve the next page of the local cursor `token` names and, if rows
    /// remain, park it again. The whole page round trip, shared by a client
    /// request on this node and a page request forwarded from another node.
    pub fn serve_page(
        self: &Arc<Self>,
        token: &CursorToken,
        fingerprint: &[u8; 16],
        cap: usize,
        owner: Option<String>,
    ) -> Result<(Vec<Row>, Option<CursorToken>), CqlError> {
        let (id, mut cursor) = self.take(token, fingerprint)?;
        let page = cursor.next_page(cap)?;
        let next = if page.more {
            Some(self.park(cursor, Some(id), owner)?)
        } else {
            None
        };
        Ok((page.rows, next))
    }

    /// The connection `owner` (a client's peer address) closed. Each cursor it
    /// parked now expires `close_grace` from now unless a page is fetched
    /// first — a driver whose connection broke retries the page on another
    /// connection or node. With a zero grace they are deleted at once.
    pub fn close_owner(&self, owner: &str) -> usize {
        let mine = |p: &Parked| p.owner.as_deref() == Some(owner);
        if self.config.close_grace.is_zero() {
            let removed = self.remove_where(mine);
            if removed > 0 {
                self.closed.fetch_add(removed as u64, Ordering::Relaxed);
                tracing::info!(
                    owner,
                    cursors = removed,
                    "deleted result cursors: connection closed"
                );
            }
            return removed;
        }
        let grace_deadline = self.ms(Instant::now() + self.config.close_grace);
        let mut affected = 0;
        for parked in self.parked.load().values().filter(|p| mine(p)) {
            if !parked.owner_closed.swap(true, Ordering::AcqRel) {
                parked
                    .deadline_ms
                    .fetch_min(grace_deadline, Ordering::AcqRel);
                affected += 1;
            }
        }
        if affected > 0 {
            self.closed.fetch_add(affected as u64, Ordering::Relaxed);
            tracing::info!(
                owner,
                cursors = affected,
                grace_secs = self.config.close_grace.as_secs(),
                "result cursors' connection closed; they expire after the close grace unless read"
            );
        }
        affected
    }

    /// Delete every cursor past its deadline as of `now`.
    pub fn sweep_expired_at(&self, now: Instant) -> usize {
        let now_ms = self.ms(now);
        let expired = |p: &Parked| now_ms >= p.deadline_ms.load(Ordering::Acquire);
        if !self.parked.load().values().any(|p| expired(p)) {
            return 0;
        }
        let removed = self.remove_where(expired);
        if removed > 0 {
            self.expired.fetch_add(removed as u64, Ordering::Relaxed);
            tracing::info!(cursors = removed, "deleted expired result cursors");
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
        let shortest = self
            .config
            .idle_ttl
            .min(if self.config.close_grace.is_zero() {
                self.config.idle_ttl
            } else {
                self.config.close_grace
            });
        let period = (shortest / 4).clamp(Duration::from_millis(50), Duration::from_secs(30));
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

// ── Forwarding a page request to the cursor's owner ─────────────────────────

/// Body of `MsgType::ResultCursorPage` (JSON).
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ForwardPageRequest {
    /// The client's paging state, as received.
    pub token: Vec<u8>,
    /// Fingerprint of the query the client sent it with, computed on the
    /// forwarding node; the owner checks it against the cursor's.
    pub fingerprint: [u8; 16],
    /// The client's page size.
    pub page_cap: u32,
}

/// Body of `MsgType::ResultCursorPageReply` (JSON).
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum ForwardPageReply {
    /// The page, and the token for the next one if rows remain.
    Page {
        rows: Vec<Row>,
        next_token: Option<Vec<u8>>,
    },
    /// The owner refused (expired, restarted, stale, other query...). The
    /// message is the owner's named error; `overloaded` keeps its error class.
    Refused { message: String, overloaded: bool },
}

impl ForwardPageReply {
    fn from_result(result: Result<(Vec<Row>, Option<CursorToken>), CqlError>) -> Self {
        match result {
            Ok((rows, next)) => Self::Page {
                rows,
                next_token: next.map(|t| t.encode()),
            },
            Err(CqlError::Overloaded(message)) => Self::Refused {
                message,
                overloaded: true,
            },
            Err(other) => Self::Refused {
                message: other.to_string(),
                overloaded: false,
            },
        }
    }
}

/// Serves `MsgType::ResultCursorPage` on the node that owns the cursor.
///
/// Reading a page is bounded file I/O on the cursor's spill runs; it holds no
/// scan slot and no blocking-pool thread, and the requesting node waits on the
/// internode Data lane's deadline.
pub struct ResultCursorPageHandler {
    registry: Arc<ResultCursorRegistry>,
}

impl ResultCursorPageHandler {
    pub fn new(registry: Arc<ResultCursorRegistry>) -> Self {
        Self { registry }
    }

    /// Serve one decoded request (also the unit-testable core).
    pub fn serve(&self, request: &ForwardPageRequest) -> ForwardPageReply {
        let result = CursorToken::decode(&request.token).and_then(|token| {
            let cap = usize::try_from(request.page_cap)
                .unwrap_or(usize::MAX)
                .max(1);
            self.registry
                .serve_page(&token, &request.fingerprint, cap, None)
        });
        ForwardPageReply::from_result(result)
    }
}

#[async_trait::async_trait]
impl ferrosa_net::rpc::RpcHandler for ResultCursorPageHandler {
    async fn handle(
        &self,
        from: ferrosa_net::rpc::PeerId,
        msg: ferrosa_net::message::Message,
    ) -> Option<ferrosa_net::message::Message> {
        let ferrosa_net::message::Message::ResultCursorPage(body) = msg else {
            tracing::error!(peer = %from.0, "result-cursor page handler got another message type");
            return None;
        };
        let reply = match serde_json::from_slice::<ForwardPageRequest>(&body) {
            Ok(request) => self.serve(&request),
            Err(e) => ForwardPageReply::Refused {
                message: format!("result-cursor page request could not be decoded: {e}"),
                overloaded: false,
            },
        };
        match serde_json::to_vec(&reply) {
            Ok(bytes) => Some(ferrosa_net::message::Message::ResultCursorPageReply(
                bytes.into(),
            )),
            Err(e) => {
                // Replying nothing would leave the requester waiting out its
                // lane deadline; reply with the encode failure instead.
                tracing::error!(peer = %from.0, %e, "result-cursor page reply could not be encoded");
                let refused = ForwardPageReply::Refused {
                    message: format!("result-cursor page reply could not be encoded: {e}"),
                    overloaded: false,
                };
                serde_json::to_vec(&refused)
                    .ok()
                    .map(|b| ferrosa_net::message::Message::ResultCursorPageReply(b.into()))
            }
        }
    }
}

/// Fetch the next page of `token`'s cursor from its owner node over
/// internode, on the Data lane (its deadline bounds the wait).
///
/// Refuses by name — never sends — when there is no internode layer, no live
/// connection to the owner, or the owner did not advertise
/// [`ferrosa_net::handshake::CAP_RESULT_CURSOR_PAGE`] (an older node, which
/// would drop the connection on the unknown message type).
pub async fn forward_page(
    peers: Option<&Arc<ferrosa_net::peer::PeerManager>>,
    token: &CursorToken,
    raw_token: &[u8],
    fingerprint: [u8; 16],
    page_cap: usize,
) -> Result<(Vec<Row>, Option<Vec<u8>>), CqlError> {
    let owner = token.owner;
    let Some(peers) = peers else {
        return Err(CqlError::Invalid(format!(
            "paging_state names a result cursor on node {owner}, and this node has no \
             internode connection to forward the page request. Send it to that node or \
             re-run the query from its first page"
        )));
    };
    match peers.peer_capabilities(owner).await {
        None => {
            return Err(CqlError::Invalid(format!(
                "paging_state names a result cursor on node {owner}, which this node cannot \
                 reach (down, removed, or not connected). Re-run the query from its first page"
            )))
        }
        Some(caps) if caps & ferrosa_net::handshake::CAP_RESULT_CURSOR_PAGE == 0 => {
            return Err(CqlError::Invalid(format!(
                "paging_state names a result cursor on node {owner}, which runs a version that \
                 cannot serve forwarded cursor pages. Send the page request to that node, or \
                 re-run the query from its first page"
            )))
        }
        Some(_) => {}
    }
    let request = ForwardPageRequest {
        token: raw_token.to_vec(),
        fingerprint,
        page_cap: u32::try_from(page_cap).unwrap_or(u32::MAX),
    };
    let body = serde_json::to_vec(&request)
        .map_err(|e| CqlError::ServerError(format!("result cursor: encode page request: {e}")))?;
    let reply = peers
        .send(
            owner,
            ferrosa_net::message::Message::ResultCursorPage(body.into()),
            ferrosa_net::codec::Lane::Data,
        )
        .await
        .map_err(|e| {
            CqlError::ServerError(format!(
                "forwarding a result-cursor page request to node {owner} failed: {e}"
            ))
        })?;
    let ferrosa_net::message::Message::ResultCursorPageReply(bytes) = reply else {
        return Err(CqlError::ServerError(format!(
            "node {owner} answered a result-cursor page request with {:?}",
            reply.msg_type()
        )));
    };
    let reply: ForwardPageReply = serde_json::from_slice(&bytes).map_err(|e| {
        CqlError::ServerError(format!(
            "node {owner} sent an undecodable result-cursor page reply: {e}"
        ))
    })?;
    match reply {
        ForwardPageReply::Page { rows, next_token } => Ok((rows, next_token)),
        ForwardPageReply::Refused {
            message,
            overloaded: true,
        } => Err(CqlError::Overloaded(message)),
        ForwardPageReply::Refused { message, .. } => Err(CqlError::Invalid(message)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_storage::ExternalSorter;

    fn val(row: &Row) -> i32 {
        match row[0] {
            Some(CqlValue::Int(v)) => v,
            ref other => panic!("expected int, got {other:?}"),
        }
    }

    /// A cursor over `0..n` (pushed in reverse, sorted ascending) whose spool
    /// is a fresh directory under `root`. Each stored row carries its value
    /// twice: once as the one-column sort key, once as the result.
    fn cursor(
        registry: &ResultCursorRegistry,
        root: &Path,
        n: i32,
        threshold: u64,
    ) -> ResultCursor {
        let permit = registry.admit().unwrap();
        let dir = tempfile::Builder::new().tempdir_in(root).unwrap().keep();
        let order = RowOrder::new(vec![(0, true)]);
        let mut sorter = ExternalSorter::new(&dir, order.clone(), threshold);
        for v in (0..n).rev() {
            sorter
                .push(vec![Some(CqlValue::Int(v)), Some(CqlValue::Int(v))])
                .unwrap();
        }
        ResultCursor::new(
            sorter.finish().unwrap(),
            order,
            1,
            TempSortTableReservation::claim_dir(dir),
            permit,
            None,
            [7; 16],
        )
    }

    fn registry() -> Arc<ResultCursorRegistry> {
        Arc::new(ResultCursorRegistry::new(
            ResultCursorConfig::default(),
            Uuid::from_u128(0xA),
        ))
    }

    fn owned(s: &str) -> Option<String> {
        Some(s.to_string())
    }

    #[test]
    fn token_round_trips_and_is_signed() {
        let token = CursorToken {
            owner: Uuid::from_u128(5),
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
    /// scan-position state from an older server, and a cursor token of another
    /// layout version, get a clear error; a cursor token sent to an
    /// older-format decoder must not parse as a key.
    #[test]
    fn foreign_and_other_version_paging_states_are_refused_by_name() {
        let legacy = crate::paging::PagingState {
            partition_key: 40u64.to_be_bytes().to_vec(),
            clustering_key: Vec::new(),
            remaining_in_partition: false,
        }
        .encode();
        let err = CursorToken::decode(&legacy).unwrap_err().to_string();
        assert!(err.contains("not a result-cursor token"), "{err}");

        // Version 1 (no owner id), and a future version 3.
        for (version, len) in [
            (1u8, 44usize),
            (CURSOR_TOKEN_VERSION + 1, TOKEN_PAYLOAD_LEN),
        ] {
            let mut other = CURSOR_TOKEN_MAGIC.to_vec();
            other.push(version);
            other.resize(len, 0);
            let err = CursorToken::decode(&crate::paging::sign_paging_payload(other))
                .unwrap_err()
                .to_string();
            assert!(err.contains(&format!("version-{version}")), "{err}");
        }

        let token = CursorToken {
            owner: Uuid::nil(),
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
    fn pages_resume_the_same_cursor_strip_the_key_and_finish() {
        let root = tempfile::tempdir().unwrap();
        let registry = registry();
        let mut c = cursor(&registry, root.path(), 25, 16);
        let first = c.next_page(10).unwrap();
        assert!(first.more);
        assert!(
            first.rows.iter().all(|r| r.len() == 1),
            "the sort key is stripped"
        );
        let mut got: Vec<i32> = first.rows.iter().map(val).collect();
        let mut token = Some(registry.park(c, None, owned("peer")).unwrap());
        for _ in 0..10 {
            let Some(t) = token else { break };
            let (rows, next) = registry
                .serve_page(&t, &[7; 16], 10, owned("peer"))
                .unwrap();
            got.extend(rows.iter().map(val));
            token = next;
        }
        assert!(token.is_none());
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
        let registry = registry();
        let mut c = cursor(&registry, root.path(), 50, u64::MAX);
        assert!(!c.rows.as_ref().unwrap().is_disk_backed());
        c.next_page(5).unwrap();
        let token = registry.park(c, None, None).unwrap();
        let (_, c) = registry.take(&token, &[7; 16]).unwrap();
        assert!(c.rows.as_ref().unwrap().is_disk_backed());
    }

    #[test]
    fn expiry_deletes_the_spill_dir_and_the_token_errors() {
        let root = tempfile::tempdir().unwrap();
        let registry = registry();
        let mut c = cursor(&registry, root.path(), 30, 16);
        let dir = c.spill_dir().to_path_buf();
        c.next_page(5).unwrap();
        let token = registry.park(c, None, None).unwrap();
        assert!(dir.exists());
        let later = Instant::now() + registry.config().idle_ttl + Duration::from_secs(1);
        assert_eq!(registry.sweep_expired_at(later), 1);
        assert!(!dir.exists(), "expiry must delete the spill directory");
        assert_eq!(registry.stats().open, 0);
        let err = registry.take(&token, &[7; 16]).unwrap_err().to_string();
        assert!(err.contains("no longer holds"), "{err}");
    }

    /// A closed connection's cursors survive the close grace (a driver retries
    /// the page elsewhere), only theirs are affected, and they expire at the
    /// end of the grace — not the idle TTL.
    #[test]
    fn a_closed_connections_cursor_survives_the_grace_then_expires() {
        let root = tempfile::tempdir().unwrap();
        let registry = registry();
        let mine = cursor(&registry, root.path(), 10, 16);
        let theirs = cursor(&registry, root.path(), 10, 16);
        let (mine_dir, theirs_dir) = (
            mine.spill_dir().to_path_buf(),
            theirs.spill_dir().to_path_buf(),
        );
        registry.park(mine, None, owned("a:1")).unwrap();
        registry.park(theirs, None, owned("b:2")).unwrap();
        assert_eq!(registry.close_owner("a:1"), 1);
        assert!(mine_dir.exists(), "the grace keeps it");

        let within = Instant::now() + registry.config().close_grace / 2;
        assert_eq!(registry.sweep_expired_at(within), 0);
        let past_grace = Instant::now() + registry.config().close_grace + Duration::from_secs(1);
        assert!(past_grace < Instant::now() + registry.config().idle_ttl);
        assert_eq!(registry.sweep_expired_at(past_grace), 1);
        assert!(!mine_dir.exists());
        assert!(
            theirs_dir.exists(),
            "another connection's cursor is untouched"
        );
        assert_eq!(registry.stats().parked, 1);
    }

    /// Reading a page within the grace re-parks the cursor with a fresh idle
    /// TTL, so a client that moved to another connection keeps it.
    #[test]
    fn a_page_read_within_the_grace_keeps_the_cursor() {
        let root = tempfile::tempdir().unwrap();
        let registry = registry();
        let c = cursor(&registry, root.path(), 30, 16);
        let token = registry.park(c, None, owned("a:1")).unwrap();
        registry.close_owner("a:1");
        let (_, next) = registry
            .serve_page(&token, &[7; 16], 5, owned("b:2"))
            .unwrap();
        assert!(next.is_some());
        let past_grace = Instant::now() + registry.config().close_grace + Duration::from_secs(1);
        assert_eq!(registry.sweep_expired_at(past_grace), 0);
    }

    #[test]
    fn a_zero_grace_deletes_at_close() {
        let root = tempfile::tempdir().unwrap();
        let registry = Arc::new(ResultCursorRegistry::new(
            ResultCursorConfig {
                close_grace: Duration::ZERO,
                ..ResultCursorConfig::default()
            },
            Uuid::nil(),
        ));
        let c = cursor(&registry, root.path(), 10, 16);
        let dir = c.spill_dir().to_path_buf();
        registry.park(c, None, owned("a:1")).unwrap();
        assert_eq!(registry.close_owner("a:1"), 1);
        assert!(!dir.exists());
    }

    #[test]
    fn a_cancelled_page_read_deletes_the_cursor() {
        let root = tempfile::tempdir().unwrap();
        let registry = registry();
        let c = cursor(&registry, root.path(), 10, 16);
        let dir = c.spill_dir().to_path_buf();
        let token = registry.park(c, None, None).unwrap();
        let (_, taken) = registry.take(&token, &[7; 16]).unwrap();
        // The request reading the page is dropped mid-flight.
        drop(taken);
        assert!(!dir.exists());
        assert_eq!(registry.stats().open, 0);
        assert!(registry.take(&token, &[7; 16]).is_err());
    }

    #[test]
    fn wrong_query_stale_page_restart_and_other_owner_are_refused() {
        let root = tempfile::tempdir().unwrap();
        let registry = registry();
        let mut c = cursor(&registry, root.path(), 30, 16);
        c.next_page(5).unwrap();
        let first = registry.park(c, None, None).unwrap();

        let err = registry.take(&first, &[8; 16]).unwrap_err().to_string();
        assert!(err.contains("different query"), "{err}");

        let (id, mut c) = registry.take(&first, &[7; 16]).unwrap();
        c.next_page(5).unwrap();
        let second = registry.park(c, Some(id), None).unwrap();
        let err = registry.take(&first, &[7; 16]).unwrap_err().to_string();
        assert!(err.contains("stale"), "{err}");

        let restarted = CursorToken {
            epoch: second.epoch.wrapping_add(1),
            ..second
        };
        let err = registry.take(&restarted, &[7; 16]).unwrap_err().to_string();
        assert!(err.contains("before it restarted"), "{err}");

        let elsewhere = CursorToken {
            owner: Uuid::from_u128(0xB),
            ..second
        };
        let err = registry.take(&elsewhere, &[7; 16]).unwrap_err().to_string();
        assert!(err.contains("not this node"), "{err}");
        assert!(registry.take(&second, &[7; 16]).is_ok());
    }

    #[test]
    fn admission_is_bounded_and_refusal_is_loud() {
        let registry = ResultCursorRegistry::new(
            ResultCursorConfig {
                max_open: 2,
                ..ResultCursorConfig::default()
            },
            Uuid::nil(),
        );
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
        let registry = registry();
        let mut c = cursor(&registry, root.path(), 30, 16);
        c.remaining_limit = Some(12);
        let a = c.next_page(5).unwrap();
        let b = c.next_page(5).unwrap();
        let last = c.next_page(5).unwrap();
        assert_eq!((a.rows.len(), b.rows.len(), last.rows.len()), (5, 5, 2));
        assert!(a.more && b.more && !last.more);
    }

    /// The owner-side handler serves a forwarded request, and turns every
    /// refusal into a reply carrying the named error (never a silent `None`,
    /// which would leave the requester waiting out its deadline).
    #[test]
    fn the_page_handler_serves_and_refuses_by_name() {
        let root = tempfile::tempdir().unwrap();
        let registry = registry();
        let c = cursor(&registry, root.path(), 12, 16);
        let token = registry.park(c, None, owned("a:1")).unwrap();
        let handler = ResultCursorPageHandler::new(registry.clone());
        let reply = handler.serve(&ForwardPageRequest {
            token: token.encode(),
            fingerprint: [7; 16],
            page_cap: 5,
        });
        let ForwardPageReply::Page { rows, next_token } = reply else {
            panic!("expected a page, got {reply:?}");
        };
        assert_eq!(
            rows.iter().map(val).collect::<Vec<_>>(),
            vec![0, 1, 2, 3, 4]
        );
        let next = CursorToken::decode(&next_token.unwrap()).unwrap();
        assert_eq!(next.owner, registry.node());

        let stale = handler.serve(&ForwardPageRequest {
            token: token.encode(),
            fingerprint: [7; 16],
            page_cap: 5,
        });
        assert!(
            matches!(&stale, ForwardPageReply::Refused { message, overloaded: false } if message.contains("stale")),
            "{stale:?}"
        );
    }

    #[tokio::test]
    async fn the_sweeper_deletes_idle_cursors_without_further_requests() {
        let root = tempfile::tempdir().unwrap();
        let registry = Arc::new(ResultCursorRegistry::new(
            ResultCursorConfig {
                idle_ttl: Duration::from_millis(100),
                ..ResultCursorConfig::default()
            },
            Uuid::nil(),
        ));
        let c = cursor(&registry, root.path(), 10, 16);
        let dir = c.spill_dir().to_path_buf();
        registry.park(c, None, None).unwrap();
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
