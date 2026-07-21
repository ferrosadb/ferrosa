//! Module: Connection-independent, txn-id-keyed CQL transaction registry.
//!
//! Replaces the per-connection `Arc<Mutex<CqlTransaction>>` (connection.rs) with a
//! server-side map keyed by an opaque 128-bit transaction id. The id rides the CQL
//! surface (`BEGIN TRANSACTION` returns it as a result row; `... IN TRANSACTION
//! <id>` on DML), so BEGIN/UPDATE/COMMIT are no longer pinned to one TCP
//! connection — deleting the connection-affinity desync bug class (the ~24%
//! `:info` in the Elle list-append cert: "COMMIT outside of a transaction" /
//! "nested transactions" from statements landing on different tasks).
//!
//! Correctness: an entry records its authenticated owner (a statement on a
//! non-owned id FAILS LOUD — FMEA F1 hijack); the registry count is bounded (F2
//! OOM) and fails loud at capacity; every open transaction carries an absolute
//! deadline, enforced both lazily (on the next stage/commit) and actively (by the
//! background reaper via [`TransactionRegistry::reap_expired`], constraint 8 /
//! A1b, default 10s). Time is injected (`now: Instant`) so the deadline logic is
//! deterministically testable with no wall-clock dependence.
//!
//! Last revised: 2026-07-20
//! Last changed: New module — Phase A of the unified transaction manager (t_3120ec2f).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use uuid::Uuid;

use crate::error::CqlError;
use crate::session::CqlTransaction;
use ferrosa_storage::accord::TransactionWrite;

/// Server-wide (per-node) shared registry handle. `parking_lot::Mutex` (not the
/// async mutex) is deliberate: every registry operation is synchronous — `commit`
/// takes the owned [`CqlTransaction`] OUT via [`TransactionRegistry::take_for_commit`]
/// and drives the async Accord commit WITHOUT holding this lock, so one node's
/// transactions never serialize on the registry lock across an await.
pub type SharedTransactionRegistry = Arc<Mutex<TransactionRegistry>>;

/// How often the background reaper sweeps for past-deadline transactions (A1b).
/// Short relative to the 10 s default timeout so an abandoned transaction is
/// reclaimed within ~one interval of its deadline.
pub const REAPER_SWEEP_INTERVAL: Duration = Duration::from_secs(1);

/// Default open-transaction timeout (constraint 8 / A1b). A transaction idle past
/// this budget is aborted by the reaper without any client statement — which also
/// bounds MVCC version retention to ~one window in later phases.
pub const DEFAULT_OPEN_TIMEOUT: Duration = Duration::from_secs(10);

/// Hard cluster maximum a `BEGIN ... USING TIMEOUT` override may request. An
/// override above this FAILS LOUD rather than being silently clamped.
pub const MAX_OPEN_TIMEOUT: Duration = Duration::from_secs(600);

/// Maximum concurrent open transactions the registry will hold (F2 OOM bound,
/// Power-of-10 Rule 3: every server-side dynamic collection has a hard cap). A
/// `BEGIN` past this FAILS LOUD.
pub const DEFAULT_MAX_ENTRIES: usize = 10_000;

/// Bounded retries when minting a fresh id, so id generation cannot loop forever
/// on the (astronomically unreachable) event of a 128-bit collision.
const ID_MINT_ATTEMPTS: usize = 8;

/// An opaque 128-bit CQL transaction id. Rendered/parsed as canonical UUID text so
/// it rides a standard CQL string on the wire (stock Cassandra drivers unmodified)
/// and is unguessable (FMEA F1) with negligible collision odds (F9).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct CqlTxnId(Uuid);

impl CqlTxnId {
    /// Mint a fresh random (v4) transaction id.
    pub fn generate() -> Self {
        Self(Uuid::new_v4())
    }

    /// Parse an id from its canonical UUID text (the `<id>` in `IN TRANSACTION
    /// <id>`). FAILS LOUD on malformed text so a client typo cannot alias an id.
    pub fn parse(text: &str) -> Result<Self, CqlError> {
        Uuid::parse_str(text.trim())
            .map(Self)
            .map_err(|_| CqlError::Invalid(format!("malformed transaction id: {text:?}")))
    }
}

impl std::fmt::Display for CqlTxnId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// One open transaction's server-side state: its staging buffer plus the metadata
/// needed to authorize and expire it. In-RAM metadata only (Phase A stages the
/// write-set in the `CqlTransaction` buffer; the NVMe temp-table staging store
/// lands in Phase D).
struct TransactionEntry {
    /// The real staging buffer + commit machine (reused unchanged from session.rs).
    txn: CqlTransaction,
    /// Authenticated principal that opened this transaction (F1 auth scope).
    owner: String,
    /// Absolute abort deadline; `now >= deadline` means expired.
    deadline: Instant,
    /// The timeout budget this transaction was opened with (for the timeout error).
    timeout: Duration,
}

/// Connection-independent transaction registry: `txn-id -> TransactionEntry`.
///
/// Lifecycle: [`begin`](Self::begin) -> [`stage`](Self::stage)* ->
/// [`take_for_commit`](Self::take_for_commit) | [`abort`](Self::abort). The
/// background reaper calls [`reap_expired`](Self::reap_expired) to actively evict
/// transactions past their deadline.
pub struct TransactionRegistry {
    entries: HashMap<CqlTxnId, TransactionEntry>,
    max_entries: usize,
    default_timeout: Duration,
    max_timeout: Duration,
}

impl Default for TransactionRegistry {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_ENTRIES, DEFAULT_OPEN_TIMEOUT, MAX_OPEN_TIMEOUT)
    }
}

impl TransactionRegistry {
    /// Construct a registry with explicit bounds. Prefer [`Default`] for the
    /// production defaults (10 000 entries, 10 s default / 600 s max timeout).
    pub fn new(max_entries: usize, default_timeout: Duration, max_timeout: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            max_entries,
            default_timeout,
            max_timeout,
        }
    }

    /// Build a shared (Arc + Mutex) registry with the production defaults —
    /// convenience for front-end wiring so callers need not name `parking_lot`.
    pub fn shared_default() -> SharedTransactionRegistry {
        Arc::new(Mutex::new(Self::default()))
    }

    /// Number of currently open transactions.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// `true` when no transactions are open.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// `true` if `id` names a currently open transaction (inspection/tests).
    pub fn contains(&self, id: CqlTxnId) -> bool {
        self.entries.contains_key(&id)
    }

    /// The registry's default open-transaction timeout (for a `BEGIN` without an
    /// explicit `USING TIMEOUT`).
    pub fn default_timeout(&self) -> Duration {
        self.default_timeout
    }

    /// `BEGIN [TRANSACTION] [USING TIMEOUT <ms>]`: open a transaction owned by
    /// `owner`, deadlined at `now + timeout`, and return its fresh id.
    ///
    /// FAILS LOUD when the registry is at capacity (F2) or when `timeout_override`
    /// exceeds the hard cluster max (never silently clamp — silent degradation is
    /// a bug). `timeout_override = None` uses the configured default.
    pub fn begin(
        &mut self,
        owner: &str,
        now: Instant,
        timeout_override: Option<Duration>,
    ) -> Result<CqlTxnId, CqlError> {
        if self.entries.len() >= self.max_entries {
            return Err(CqlError::Overloaded(format!(
                "transaction registry at capacity ({} open); retry after in-flight \
                 transactions commit or time out",
                self.max_entries
            )));
        }
        let timeout = self.resolve_timeout(timeout_override)?;
        let id = self.fresh_id()?;
        let mut txn = CqlTransaction::new();
        // A registry entry only ever holds an OPEN transaction; opening a fresh
        // machine cannot fail (assertion — Power-of-10 Rule 5).
        txn.begin()
            .expect("a fresh CqlTransaction always opens on begin()");
        self.entries.insert(
            id,
            TransactionEntry {
                txn,
                owner: owner.to_string(),
                deadline: now + timeout,
                timeout,
            },
        );
        Ok(id)
    }

    /// Stage one DML write under an owned, open, not-yet-expired transaction.
    ///
    /// FAILS LOUD on unknown id, wrong owner (F1), or an elapsed deadline (the
    /// entry is evicted and nothing is staged — never persist past the budget).
    pub fn stage(
        &mut self,
        id: CqlTxnId,
        owner: &str,
        now: Instant,
        write: TransactionWrite,
    ) -> Result<(), CqlError> {
        self.authorize(id, owner, now)?;
        // `authorize` proved the entry is present and left it in place on success.
        let entry = self
            .entries
            .get_mut(&id)
            .expect("entry present after a successful authorize");
        entry.txn.stage(write)
    }

    /// Poison an owned, open transaction after a statement failed inside it, so the
    /// next `COMMIT` fails loud rather than committing a partial write-set. Unknown
    /// or unowned ids are ignored (best-effort marking; the failing statement has
    /// already surfaced its own error).
    pub fn poison(&mut self, id: CqlTxnId, owner: &str) {
        if let Some(entry) = self.entries.get_mut(&id) {
            if entry.owner == owner {
                entry.txn.poison();
            }
        }
    }

    /// `COMMIT TRANSACTION <id>`: remove the entry and return its owned staging
    /// machine so the caller can drive the (async) Accord commit WITHOUT holding
    /// the registry lock across the await (avoids serializing every transaction on
    /// one node). FAILS LOUD on unknown id, wrong owner (F1), or elapsed deadline.
    pub fn take_for_commit(
        &mut self,
        id: CqlTxnId,
        owner: &str,
        now: Instant,
    ) -> Result<CqlTransaction, CqlError> {
        self.authorize(id, owner, now)?;
        let entry = self
            .entries
            .remove(&id)
            .expect("entry present after a successful authorize");
        Ok(entry.txn)
    }

    /// Verify `id` is an owned, open, not-yet-expired transaction WITHOUT staging
    /// anything — used to scope a `SELECT ... IN TRANSACTION <id>` (Phase A reads
    /// committed state; MVCC snapshot isolation lands in Phase D). An expired entry
    /// is evicted and surfaced as a timeout, exactly like a stage/commit.
    pub fn ensure_active(
        &mut self,
        id: CqlTxnId,
        owner: &str,
        now: Instant,
    ) -> Result<(), CqlError> {
        self.authorize(id, owner, now)
    }

    /// `ROLLBACK TRANSACTION <id>`: drop an owned transaction, discarding its
    /// buffer. No deadline check — rollback of an expired transaction is still a
    /// clean no-op abort. FAILS LOUD on unknown id or wrong owner (F1).
    pub fn abort(&mut self, id: CqlTxnId, owner: &str) -> Result<(), CqlError> {
        match self.entries.get(&id) {
            None => Err(Self::unknown(id)),
            Some(entry) if entry.owner != owner => Err(Self::forbidden(id)),
            Some(_) => {
                self.entries.remove(&id);
                Ok(())
            }
        }
    }

    /// Actively evict every transaction whose deadline has elapsed (the reaper
    /// sweep, A1b). Returns the ids that were aborted so the caller can log/meter
    /// them. Live transactions are untouched.
    pub fn reap_expired(&mut self, now: Instant) -> Vec<CqlTxnId> {
        let expired: Vec<CqlTxnId> = self
            .entries
            .iter()
            .filter(|(_, e)| now >= e.deadline)
            .map(|(id, _)| *id)
            .collect();
        for id in &expired {
            self.entries.remove(id);
        }
        expired
    }

    /// Authorize an operation on `id` by `owner` at `now`: the id must exist, be
    /// owned by `owner` (F1), and not be past its deadline. An expired entry is
    /// EVICTED here and surfaced as a timeout — the single shared enforcement point
    /// for `stage`/`take_for_commit`.
    fn authorize(&mut self, id: CqlTxnId, owner: &str, now: Instant) -> Result<(), CqlError> {
        let entry = match self.entries.get(&id) {
            None => return Err(Self::unknown(id)),
            Some(e) => e,
        };
        if entry.owner != owner {
            return Err(Self::forbidden(id));
        }
        if now >= entry.deadline {
            let elapsed = now.duration_since(entry.deadline) + entry.timeout;
            let timeout_ms = entry.timeout.as_millis() as u64;
            let elapsed_ms = elapsed.as_millis() as u64;
            self.entries.remove(&id);
            return Err(CqlError::TransactionTimeout {
                timeout_ms,
                elapsed_ms,
            });
        }
        Ok(())
    }

    /// Resolve the effective timeout for a new transaction: an override (bounded by
    /// the hard cluster max — an override above it FAILS LOUD rather than being
    /// silently clamped) or the configured default.
    fn resolve_timeout(&self, timeout_override: Option<Duration>) -> Result<Duration, CqlError> {
        match timeout_override {
            Some(t) if t > self.max_timeout => Err(CqlError::Invalid(format!(
                "requested transaction timeout {}ms exceeds the cluster maximum {}ms",
                t.as_millis(),
                self.max_timeout.as_millis()
            ))),
            Some(t) => Ok(t),
            None => Ok(self.default_timeout),
        }
    }

    /// Mint a fresh id not already in use. Bounded retries (Power-of-10 Rule 2);
    /// exhausting them FAILS LOUD rather than looping forever.
    fn fresh_id(&self) -> Result<CqlTxnId, CqlError> {
        for _ in 0..ID_MINT_ATTEMPTS {
            let id = CqlTxnId::generate();
            if !self.entries.contains_key(&id) {
                return Ok(id);
            }
        }
        Err(CqlError::ServerError(
            "could not mint a unique transaction id".to_string(),
        ))
    }

    fn unknown(id: CqlTxnId) -> CqlError {
        CqlError::Invalid(format!(
            "unknown transaction {id} (already committed, rolled back, or timed out)"
        ))
    }

    fn forbidden(id: CqlTxnId) -> CqlError {
        CqlError::Unauthorized(format!("transaction {id} is owned by another principal"))
    }
}

/// Spawn the background open-transaction reaper (A1b). Every
/// [`REAPER_SWEEP_INTERVAL`] it actively aborts and evicts every transaction past
/// its deadline, so an abandoned transaction is reclaimed without any client
/// statement (bounding both RAM — F2 — and, in later phases, MVCC version
/// retention). The sweep decision ([`TransactionRegistry::reap_expired`]) is a
/// pure, unit-tested function; this loop only clocks it. Runs for the lifetime of
/// the node.
pub fn spawn_transaction_reaper(
    registry: SharedTransactionRegistry,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(REAPER_SWEEP_INTERVAL);
        loop {
            ticker.tick().await;
            let reaped = registry.lock().reap_expired(Instant::now());
            if !reaped.is_empty() {
                tracing::warn!(
                    count = reaped.len(),
                    "reaped past-deadline transaction(s) (open-transaction timeout)"
                );
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tw(key: &[u8]) -> TransactionWrite {
        TransactionWrite {
            keyspace: "ks".to_string(),
            key: key.to_vec(),
            mutation: b"m".to_vec(),
        }
    }

    fn registry() -> TransactionRegistry {
        TransactionRegistry::default()
    }

    #[test]
    fn begin_returns_a_fresh_open_id() {
        let mut reg = registry();
        let now = Instant::now();
        let id = reg.begin("alice", now, None).unwrap();
        assert!(
            reg.contains(id),
            "the returned id names an open transaction"
        );
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn two_begins_yield_distinct_ids() {
        let mut reg = registry();
        let now = Instant::now();
        let a = reg.begin("alice", now, None).unwrap();
        let b = reg.begin("alice", now, None).unwrap();
        assert_ne!(a, b, "each BEGIN mints a distinct id (F9 uniqueness)");
        assert_eq!(reg.len(), 2);
    }

    #[test]
    fn stage_buffers_writes_on_an_owned_open_id() {
        let mut reg = registry();
        let now = Instant::now();
        let id = reg.begin("alice", now, None).unwrap();
        reg.stage(id, "alice", now, tw(b"a")).unwrap();
        reg.stage(id, "alice", now, tw(b"b")).unwrap();
        // Draining for commit yields the buffered write-set in order.
        let txn = reg.take_for_commit(id, "alice", now).unwrap();
        assert_eq!(txn.staged_len(), 2);
    }

    #[test]
    fn operations_on_an_unknown_id_fail_loud() {
        let mut reg = registry();
        let now = Instant::now();
        let ghost = CqlTxnId::generate();
        assert!(reg.stage(ghost, "alice", now, tw(b"a")).is_err());
        assert!(reg.take_for_commit(ghost, "alice", now).is_err());
        assert!(reg.abort(ghost, "alice").is_err());
    }

    #[test]
    fn a_second_principal_is_rejected_f1_hijack() {
        let mut reg = registry();
        let now = Instant::now();
        let id = reg.begin("alice", now, None).unwrap();

        let staged = reg.stage(id, "mallory", now, tw(b"a"));
        assert!(matches!(staged, Err(CqlError::Unauthorized(_))));
        let committed = reg.take_for_commit(id, "mallory", now);
        assert!(matches!(committed, Err(CqlError::Unauthorized(_))));
        assert!(matches!(
            reg.abort(id, "mallory"),
            Err(CqlError::Unauthorized(_))
        ));

        // Alice's transaction is untouched by Mallory's rejected attempts.
        assert!(reg.contains(id));
        reg.abort(id, "alice").unwrap();
    }

    #[test]
    fn take_for_commit_removes_the_entry() {
        let mut reg = registry();
        let now = Instant::now();
        let id = reg.begin("alice", now, None).unwrap();
        let _txn = reg.take_for_commit(id, "alice", now).unwrap();
        assert!(!reg.contains(id), "committing removes the entry");
        assert_eq!(reg.len(), 0);
    }

    #[test]
    fn abort_removes_the_entry() {
        let mut reg = registry();
        let now = Instant::now();
        let id = reg.begin("alice", now, None).unwrap();
        reg.abort(id, "alice").unwrap();
        assert!(!reg.contains(id));
    }

    #[test]
    fn begin_past_capacity_fails_loud_f2() {
        let mut reg = TransactionRegistry::new(2, DEFAULT_OPEN_TIMEOUT, MAX_OPEN_TIMEOUT);
        let now = Instant::now();
        reg.begin("alice", now, None).unwrap();
        reg.begin("alice", now, None).unwrap();
        let over = reg.begin("alice", now, None);
        assert!(
            matches!(over, Err(CqlError::Overloaded(_))),
            "a BEGIN past the registry cap must fail loud (F2 OOM bound)"
        );
    }

    #[test]
    fn timeout_override_above_max_is_rejected() {
        let mut reg = registry();
        let now = Instant::now();
        let over = reg.begin(
            "alice",
            now,
            Some(MAX_OPEN_TIMEOUT + Duration::from_secs(1)),
        );
        assert!(
            matches!(over, Err(CqlError::Invalid(_))),
            "an override above the hard max fails loud (never silently clamp)"
        );
    }

    #[test]
    fn timeout_override_within_max_is_honored() {
        let mut reg = registry();
        let base = Instant::now();
        // A 5s override: at base+4s it is still live, at base+6s it is expired.
        let id = reg
            .begin("alice", base, Some(Duration::from_secs(5)))
            .unwrap();
        reg.stage(id, "alice", base + Duration::from_secs(4), tw(b"a"))
            .unwrap();
        let expired = reg.stage(id, "alice", base + Duration::from_secs(6), tw(b"b"));
        assert!(matches!(expired, Err(CqlError::TransactionTimeout { .. })));
    }

    #[test]
    fn reap_expired_evicts_past_deadline_and_keeps_live_a1b() {
        let mut reg = TransactionRegistry::new(
            DEFAULT_MAX_ENTRIES,
            Duration::from_secs(10),
            MAX_OPEN_TIMEOUT,
        );
        let base = Instant::now();
        let short = reg
            .begin("alice", base, Some(Duration::from_secs(1)))
            .unwrap();
        let long = reg
            .begin("alice", base, Some(Duration::from_secs(60)))
            .unwrap();

        // Sweep at base+2s: the 1s transaction is past deadline, the 60s is live.
        let reaped = reg.reap_expired(base + Duration::from_secs(2));
        assert_eq!(
            reaped,
            vec![short],
            "only the expired transaction is reaped"
        );
        assert!(!reg.contains(short));
        assert!(
            reg.contains(long),
            "the live transaction survives the sweep"
        );
    }

    #[test]
    fn lazy_timeout_evicts_on_operation_of_an_expired_id() {
        let mut reg = registry();
        let base = Instant::now();
        let id = reg
            .begin("alice", base, Some(Duration::from_secs(1)))
            .unwrap();
        // No reaper ran; a stage past the deadline must itself fail loud + evict.
        let staged = reg.stage(id, "alice", base + Duration::from_secs(2), tw(b"a"));
        assert!(matches!(staged, Err(CqlError::TransactionTimeout { .. })));
        assert!(!reg.contains(id), "the expired entry is evicted on access");
    }

    #[test]
    fn id_round_trips_through_text() {
        let id = CqlTxnId::generate();
        let text = id.to_string();
        let parsed = CqlTxnId::parse(&text).unwrap();
        assert_eq!(id, parsed, "an id survives Display -> parse round trip");
        assert!(CqlTxnId::parse("not-a-uuid").is_err());
    }
}
