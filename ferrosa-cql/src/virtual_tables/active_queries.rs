//! Active queries virtual table.
//!
//! [`QueryTracker`] maintains a live map of currently executing queries.
//! [`ActiveQueriesTable`] exposes that map as a [`VirtualTable`] readable
//! via CQL `SELECT` from `system_observability.active_queries`.
//!
//! ## Cost model
//!
//! Registration happens on **every** routed statement, but the map is only
//! ever read by a `SELECT` against `system_observability.active_queries` — a
//! diagnostics surface nobody touches under load. Eagerly publishing each
//! request therefore spends a sharded-map insert + remove, several owned
//! `String`s, and two clock reads per operation to feed a table that is almost
//! always empty *and* unread.
//!
//! [`QueryTracker::from_env`] therefore honours an explicit opt-out
//! (`FERROSA_ACTIVE_QUERIES_TRACKING=0`); when disabled, [`begin_guarded`]
//! returns `None` and does no work at all, while `total_executed` still counts
//! completions. The default stays **on** so operators keep the table; the
//! switch exists to take the cost off a measured hot path.
//!
//! [`begin_guarded`]: QueryTracker::begin_guarded

use dashmap::DashMap;
use ferrosa_common::{CellValue, DataType};
use ferrosa_schema::virtual_table::{
    RowPredicate, SubscriptionMode, VirtualColumnDef, VirtualRow, VirtualTable,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Environment switch for the tracking cost. Absent or any value other than
/// the explicit off-tokens leaves tracking **enabled**.
pub const TRACKING_ENV: &str = "FERROSA_ACTIVE_QUERIES_TRACKING";

/// Parse the [`TRACKING_ENV`] value. `None` (unset/unparseable) means enabled.
fn tracking_enabled_from(raw: Option<&str>) -> bool {
    match raw {
        None => true,
        Some(v) => !matches!(v.trim(), "0" | "false" | "off" | "no"),
    }
}

// ---------------------------------------------------------------------------
// QueryInfo
// ---------------------------------------------------------------------------

/// Metadata about a single in-flight query.
#[derive(Clone)]
pub struct QueryInfo {
    pub query_id: u64,
    pub client_address: String,
    pub username: String,
    pub query_text: String,
    pub keyspace: String,
    pub start_time: Instant,
    /// Wall-clock start in milliseconds since UNIX epoch, captured once on
    /// `begin()` so rows can report a stable `start_time` column.
    pub start_epoch_ms: i64,
    /// Always `"executing"` today. A `&'static str` rather than an owned
    /// `String` so the per-request registration does not allocate for a
    /// constant.
    pub state: &'static str,
}

// ---------------------------------------------------------------------------
// QueryTracker
// ---------------------------------------------------------------------------

/// Tracks currently executing queries.
///
/// Designed for concurrent use without cloning all active queries on every
/// request. Reads take a point-in-time snapshot for the virtual table.
pub struct QueryTracker {
    active: DashMap<u64, QueryInfo>,
    next_id: AtomicU64,
    total_executed: AtomicU64,
    /// When false, [`begin_guarded`](Self::begin_guarded) is a no-op: no id,
    /// no insert, no clock reads. Set once at construction.
    enabled: bool,
}

impl QueryTracker {
    /// Create an empty tracker with tracking **enabled**.
    pub fn new() -> Self {
        Self::new_gated(true)
    }

    /// Create an empty tracker with tracking explicitly on or off.
    pub fn new_gated(enabled: bool) -> Self {
        Self {
            active: DashMap::new(),
            next_id: AtomicU64::new(1),
            total_executed: AtomicU64::new(0),
            enabled,
        }
    }

    /// Create a tracker gated by [`TRACKING_ENV`] (default: enabled).
    pub fn from_env() -> Self {
        let raw = std::env::var(TRACKING_ENV).ok();
        Self::new_gated(tracking_enabled_from(raw.as_deref()))
    }

    /// Whether per-request registration is enabled.
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// Register a new query and return its opaque ID.
    ///
    /// The caller must call [`complete`](Self::complete) with the returned ID
    /// when the query finishes (or use [`begin_guarded`](Self::begin_guarded)
    /// for automatic cleanup). Unlike `begin_guarded`, this raw entry point
    /// always records, regardless of the `enabled` gate — it is the direct API
    /// used by the tracker's own tests.
    pub fn begin(&self, query: &str, keyspace: &str, client: &str, username: &str) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let info = QueryInfo {
            query_id: id,
            client_address: client.to_string(),
            username: username.to_string(),
            query_text: query.to_string(),
            keyspace: keyspace.to_string(),
            start_time: Instant::now(),
            start_epoch_ms: now_ms,
            state: "executing",
        };
        self.active.insert(id, info);
        id
    }

    /// Like [`begin`](Self::begin) but returns a [`QueryGuard`] that calls
    /// `complete` automatically when dropped.
    ///
    /// Returns `None` when tracking is disabled, so the hot path pays nothing
    /// beyond a load of the gate.
    pub fn begin_guarded(
        self: &Arc<Self>,
        query: &str,
        keyspace: &str,
        client: &str,
        username: &str,
    ) -> Option<QueryGuard> {
        if !self.enabled {
            return None;
        }
        let id = self.begin(query, keyspace, client, username);
        Some(QueryGuard {
            tracker: Arc::clone(self),
            id,
        })
    }

    /// Mark a query as complete. If `id` is not found this is a no-op.
    pub fn complete(&self, id: u64) {
        if self.active.remove(&id).is_some() {
            self.total_executed.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Number of queries currently in flight.
    pub fn active_count(&self) -> usize {
        self.active.len()
    }

    /// Total number of queries that have completed since this tracker was
    /// created.
    pub fn total_executed(&self) -> u64 {
        self.total_executed.load(Ordering::Relaxed)
    }

    /// Snapshot of all active query infos, for use by [`ActiveQueriesTable`].
    fn snapshot(&self) -> Vec<ActiveQuerySnapshot> {
        let now = Instant::now();
        self.active
            .iter()
            .map(|info| ActiveQuerySnapshot {
                query_id: info.query_id,
                client_address: info.client_address.clone(),
                username: info.username.clone(),
                query_text: info.query_text.clone(),
                keyspace: info.keyspace.clone(),
                start_epoch_ms: info.start_epoch_ms,
                elapsed_ms: now.duration_since(info.start_time).as_millis() as i64,
                state: info.state,
            })
            .collect()
    }
}

impl Default for QueryTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// A point-in-time snapshot of a single active query, used to build rows.
struct ActiveQuerySnapshot {
    query_id: u64,
    client_address: String,
    username: String,
    query_text: String,
    keyspace: String,
    start_epoch_ms: i64,
    elapsed_ms: i64,
    state: &'static str,
}

// ---------------------------------------------------------------------------
// QueryGuard
// ---------------------------------------------------------------------------

/// RAII guard that automatically calls [`QueryTracker::complete`] on drop.
///
/// Obtain via [`QueryTracker::begin_guarded`].
pub struct QueryGuard {
    tracker: Arc<QueryTracker>,
    id: u64,
}

impl Drop for QueryGuard {
    fn drop(&mut self) {
        self.tracker.complete(self.id);
    }
}

// ---------------------------------------------------------------------------
// ActiveQueriesTable
// ---------------------------------------------------------------------------

/// Virtual table that surfaces all in-flight queries.
///
/// Columns (in order):
/// 0. `query_id`       — BigInt (u64 cast to i64)
/// 1. `client_address` — Text
/// 2. `username`       — Text
/// 3. `query_text`     — Text
/// 4. `keyspace`       — Text
/// 5. `start_time`     — BigInt (epoch ms)
/// 6. `elapsed_ms`     — BigInt
/// 7. `state`          — Text
///
/// Primary key: `query_id` (column 0).
pub struct ActiveQueriesTable {
    tracker: Arc<QueryTracker>,
    columns: Vec<VirtualColumnDef>,
}

impl ActiveQueriesTable {
    /// Create a new table backed by `tracker`.
    pub fn new(tracker: Arc<QueryTracker>) -> Self {
        let columns = vec![
            VirtualColumnDef {
                name: "query_id".to_string(),
                data_type: DataType::BigInt,
            },
            VirtualColumnDef {
                name: "client_address".to_string(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "username".to_string(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "query_text".to_string(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "keyspace".to_string(),
                data_type: DataType::Text,
            },
            VirtualColumnDef {
                name: "start_time".to_string(),
                data_type: DataType::BigInt,
            },
            VirtualColumnDef {
                name: "elapsed_ms".to_string(),
                data_type: DataType::BigInt,
            },
            VirtualColumnDef {
                name: "state".to_string(),
                data_type: DataType::Text,
            },
        ];
        Self { tracker, columns }
    }
}

impl VirtualTable for ActiveQueriesTable {
    fn name(&self) -> &str {
        "active_queries"
    }

    fn keyspace(&self) -> &str {
        "system_observability"
    }

    fn columns(&self) -> &[VirtualColumnDef] {
        &self.columns
    }

    fn primary_key_columns(&self) -> &[usize] {
        &[0]
    }

    fn visit_rows(&self, _predicate: Option<&RowPredicate>, visit: &mut dyn FnMut(VirtualRow)) {
        for s in self.tracker.snapshot() {
            let cells = vec![
                CellValue::live((s.query_id as i64).to_be_bytes().to_vec(), 0),
                CellValue::live(s.client_address.into_bytes(), 0),
                CellValue::live(s.username.into_bytes(), 0),
                CellValue::live(s.query_text.into_bytes(), 0),
                CellValue::live(s.keyspace.into_bytes(), 0),
                CellValue::live(s.start_epoch_ms.to_be_bytes().to_vec(), 0),
                CellValue::live(s.elapsed_ms.to_be_bytes().to_vec(), 0),
                CellValue::live(s.state.as_bytes().to_vec(), 0),
            ];
            visit(VirtualRow { cells });
        }
    }

    fn subscription_mode(&self) -> SubscriptionMode {
        SubscriptionMode::DemandDriven {
            default_interval: std::time::Duration::from_millis(500),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_queries_table_metadata() {
        let tracker = Arc::new(QueryTracker::new());
        let table = ActiveQueriesTable::new(tracker);
        assert_eq!(table.name(), "active_queries");
        assert_eq!(table.columns().len(), 8);
    }

    #[test]
    fn tracks_query_lifecycle() {
        let tracker = Arc::new(QueryTracker::new());
        let id = tracker.begin("SELECT * FROM users", "test_ks", "10.0.0.1", "admin");
        assert_eq!(tracker.active_count(), 1);

        let table = ActiveQueriesTable::new(tracker.clone());
        assert_eq!(table.read(None).len(), 1);

        tracker.complete(id);
        assert_eq!(table.read(None).len(), 0);
    }

    #[test]
    fn query_guard_auto_completes() {
        let tracker = Arc::new(QueryTracker::new());
        {
            let _guard = tracker
                .begin_guarded("SELECT 1", "ks", "10.0.0.1", "admin")
                .expect("tracking enabled by default");
            assert_eq!(tracker.active_count(), 1);
        }
        assert_eq!(tracker.active_count(), 0);
    }

    #[test]
    fn total_executed_counts_completions() {
        let tracker = Arc::new(QueryTracker::new());
        let id1 = tracker.begin("SELECT 1", "ks", "127.0.0.1", "user");
        let id2 = tracker.begin("SELECT 2", "ks", "127.0.0.1", "user");
        assert_eq!(tracker.total_executed(), 0);
        tracker.complete(id1);
        assert_eq!(tracker.total_executed(), 1);
        tracker.complete(id2);
        assert_eq!(tracker.total_executed(), 2);
    }

    #[test]
    fn complete_unknown_id_is_noop() {
        let tracker = QueryTracker::new();
        // Should not panic.
        tracker.complete(9999);
        assert_eq!(tracker.active_count(), 0);
        assert_eq!(tracker.total_executed(), 0);
    }

    #[test]
    fn row_has_eight_cells() {
        let tracker = Arc::new(QueryTracker::new());
        tracker.begin("INSERT INTO t VALUES (1)", "myks", "192.168.0.1", "bob");
        let table = ActiveQueriesTable::new(tracker);
        let rows = table.read(None);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].cells.len(), 8);
    }

    #[test]
    fn multiple_queries_tracked_independently() {
        let tracker = Arc::new(QueryTracker::new());
        let id1 = tracker.begin("SELECT a", "ks1", "10.0.0.1", "alice");
        let _id2 = tracker.begin("SELECT b", "ks2", "10.0.0.2", "bob");
        assert_eq!(tracker.active_count(), 2);
        tracker.complete(id1);
        assert_eq!(tracker.active_count(), 1);
    }

    // ---- Gate: opt-out leaves the hot path free ---------------------------

    #[test]
    fn gated_off_begin_guarded_is_a_noop() {
        let tracker = Arc::new(QueryTracker::new_gated(false));
        let guard = tracker.begin_guarded("SELECT 1", "ks", "10.0.0.1", "admin");
        assert!(
            guard.is_none(),
            "disabled tracker must not hand out a guard"
        );
        assert_eq!(tracker.active_count(), 0, "nothing may be registered");
        assert_eq!(tracker.next_id.load(Ordering::Relaxed), 1, "no id burned");
        drop(guard);
        // No completion was recorded because nothing was ever inserted.
        assert_eq!(tracker.total_executed(), 0);
    }

    #[test]
    fn gated_on_begin_guarded_registers() {
        let tracker = Arc::new(QueryTracker::new_gated(true));
        let guard = tracker.begin_guarded("SELECT 1", "ks", "10.0.0.1", "admin");
        assert!(guard.is_some());
        assert_eq!(tracker.active_count(), 1);
        drop(guard);
        assert_eq!(tracker.active_count(), 0);
    }

    #[test]
    fn env_tokens_map_to_gate() {
        assert!(tracking_enabled_from(None), "unset means enabled");
        assert!(tracking_enabled_from(Some("1")));
        assert!(tracking_enabled_from(Some("true")));
        assert!(tracking_enabled_from(Some("yes")));
        assert!(tracking_enabled_from(Some(" on ")));
        assert!(!tracking_enabled_from(Some("0")));
        assert!(!tracking_enabled_from(Some("false")));
        assert!(!tracking_enabled_from(Some("off")));
        assert!(!tracking_enabled_from(Some("no")));
    }
}
