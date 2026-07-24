//! `system_observability.connections` virtual table.
//!
//! Tracks active CQL connections and exposes them as a queryable virtual
//! table. Each row represents one live TCP connection to the CQL server.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwap;
use ferrosa_common::{CellValue, DataType};
use ferrosa_schema::{RowPredicate, SubscriptionMode, VirtualColumnDef, VirtualRow, VirtualTable};

/// Metadata about a single active CQL connection.
#[derive(Clone)]
pub struct ConnectionInfo {
    /// Human-readable peer address (IP string).
    pub peer_address: String,
    /// Peer port number.
    pub peer_port: u16,
    /// Connection lifecycle state: `"startup"`, `"authenticating"`, or `"ready"`.
    pub state: String,
    /// Authenticated username, if authentication has completed.
    pub username: Option<String>,
    /// When the connection was first registered.
    pub connected_at: Instant,
    /// Total requests processed on this connection.
    ///
    /// A shared atomic so the per-request increment on the hot path is a single
    /// `fetch_add` with no map mutation — the tracker's `ArcSwap<HashMap>` is
    /// snapshotted (a cheap `Arc` load) and the counter bumped in place. The old
    /// per-request `rcu` cloned the entire connection map on every query, which
    /// profiled at ~22% of write-path CPU under concurrency (t_f0f17a55).
    /// Cloning `ConnectionInfo` (register / lifecycle `rcu`) shares the same
    /// atomic via the `Arc`, so counts survive state/username updates.
    pub requests_served: Arc<AtomicU64>,
    /// CQL native protocol version negotiated (e.g. `5`).
    pub protocol_version: u8,
}

/// Concurrent registry of active CQL connections.
///
/// Uses clone-on-write `ArcSwap` snapshots so reads never acquire locks. Writes
/// are expected to be small, bounded metadata updates at connection lifecycle
/// boundaries.
pub struct ConnectionTracker {
    connections: ArcSwap<HashMap<SocketAddr, ConnectionInfo>>,
}

impl ConnectionTracker {
    /// Create an empty tracker.
    pub fn new() -> Self {
        Self {
            connections: ArcSwap::new(Arc::new(HashMap::new())),
        }
    }

    /// Register a new connection. Replaces any existing entry for `addr`.
    pub fn register(&self, addr: SocketAddr, info: ConnectionInfo) {
        self.connections.rcu(|current| {
            let mut next = (**current).clone();
            next.insert(addr, info.clone());
            Arc::new(next)
        });
    }

    /// Remove a connection from the tracker.
    pub fn deregister(&self, addr: &SocketAddr) {
        self.connections.rcu(|current| {
            let mut next = (**current).clone();
            next.remove(addr);
            Arc::new(next)
        });
    }

    /// Update the lifecycle state of a connection.
    pub fn update_state(&self, addr: &SocketAddr, state: &str) {
        self.connections.rcu(|current| {
            let mut next = (**current).clone();
            if let Some(info) = next.get_mut(addr) {
                info.state = state.to_owned();
            }
            Arc::new(next)
        });
    }

    /// Record the authenticated username for a connection.
    pub fn update_username(&self, addr: &SocketAddr, username: &str) {
        self.connections.rcu(|current| {
            let mut next = (**current).clone();
            if let Some(info) = next.get_mut(addr) {
                info.username = Some(username.to_owned());
            }
            Arc::new(next)
        });
    }

    /// Atomically increment the request counter for a connection.
    ///
    /// Hot path (called per query): snapshot the map (`Arc` load, lock-free) and
    /// `fetch_add` the connection's shared atomic. NO map clone / `rcu` — see the
    /// `requests_served` field doc. A concurrent `deregister` that drops the
    /// entry just means the increment lands on an orphaned atomic (the
    /// connection is gone) — harmless.
    pub fn increment_requests(&self, addr: &SocketAddr) {
        if let Some(info) = self.connections.load().get(addr) {
            info.requests_served.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Return the number of currently tracked connections.
    pub fn active_count(&self) -> usize {
        self.connections.load().len()
    }
}

impl Default for ConnectionTracker {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Column definitions
// ---------------------------------------------------------------------------

/// Column layout for `system_observability.connections`.
///
/// Indices:
/// 0 – peer_address  (Text)      — partition key
/// 1 – peer_port     (Int)       — clustering key
/// 2 – state         (Text)
/// 3 – username      (Text)
/// 4 – idle_seconds  (Int)
/// 5 – requests_served (BigInt)
/// 6 – protocol_version (Int)
fn make_columns() -> Vec<VirtualColumnDef> {
    vec![
        VirtualColumnDef {
            name: "peer_address".to_owned(),
            data_type: DataType::Text,
        },
        VirtualColumnDef {
            name: "peer_port".to_owned(),
            data_type: DataType::Int,
        },
        VirtualColumnDef {
            name: "state".to_owned(),
            data_type: DataType::Text,
        },
        VirtualColumnDef {
            name: "username".to_owned(),
            data_type: DataType::Text,
        },
        VirtualColumnDef {
            name: "idle_seconds".to_owned(),
            data_type: DataType::Int,
        },
        VirtualColumnDef {
            name: "requests_served".to_owned(),
            data_type: DataType::BigInt,
        },
        VirtualColumnDef {
            name: "protocol_version".to_owned(),
            data_type: DataType::Int,
        },
    ]
}

// ---------------------------------------------------------------------------
// ConnectionsTable
// ---------------------------------------------------------------------------

/// Virtual table that exposes live CQL connection state.
///
/// Registered under `system_observability.connections`. Each row represents
/// one active TCP connection; primary key is `(peer_address, peer_port)`.
pub struct ConnectionsTable {
    tracker: Arc<ConnectionTracker>,
    columns: Vec<VirtualColumnDef>,
}

impl ConnectionsTable {
    /// Create a new table backed by the given [`ConnectionTracker`].
    pub fn new(tracker: Arc<ConnectionTracker>) -> Self {
        Self {
            tracker,
            columns: make_columns(),
        }
    }
}

impl VirtualTable for ConnectionsTable {
    fn name(&self) -> &str {
        "connections"
    }

    fn keyspace(&self) -> &str {
        "system_observability"
    }

    fn columns(&self) -> &[VirtualColumnDef] {
        &self.columns
    }

    /// Primary key: `peer_address` (col 0) + `peer_port` (col 1).
    fn primary_key_columns(&self) -> &[usize] {
        &[0, 1]
    }

    fn subscription_mode(&self) -> SubscriptionMode {
        SubscriptionMode::Pollable
    }

    fn read(&self, _predicate: Option<&RowPredicate>) -> Vec<VirtualRow> {
        let now = Instant::now();
        let snapshot = self.tracker.connections.load();

        snapshot
            .values()
            .map(|info| {
                let idle_secs = now
                    .duration_since(info.connected_at)
                    .as_secs()
                    .min(i32::MAX as u64) as i32;

                // Column 0: peer_address (Text)
                let peer_address = CellValue::live(info.peer_address.as_bytes().to_vec(), 0);

                // Column 1: peer_port (Int — 4 bytes big-endian)
                let peer_port = CellValue::live((info.peer_port as i32).to_be_bytes().to_vec(), 0);

                // Column 2: state (Text)
                let state = CellValue::live(info.state.as_bytes().to_vec(), 0);

                // Column 3: username (Text, NULL tombstone when absent)
                let username = match &info.username {
                    Some(u) => CellValue::live(u.as_bytes().to_vec(), 0),
                    None => CellValue::tombstone(0, 0),
                };

                // Column 4: idle_seconds (Int)
                let idle_seconds = CellValue::live(idle_secs.to_be_bytes().to_vec(), 0);

                // Column 5: requests_served (BigInt — 8 bytes big-endian)
                let requests_served = CellValue::live(
                    (info.requests_served.load(Ordering::Relaxed) as i64)
                        .to_be_bytes()
                        .to_vec(),
                    0,
                );

                // Column 6: protocol_version (Int)
                let protocol_version =
                    CellValue::live((info.protocol_version as i32).to_be_bytes().to_vec(), 0);

                VirtualRow {
                    cells: vec![
                        peer_address,
                        peer_port,
                        state,
                        username,
                        idle_seconds,
                        requests_served,
                        protocol_version,
                    ],
                }
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn make_tracker() -> Arc<ConnectionTracker> {
        Arc::new(ConnectionTracker::new())
    }

    fn sample_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
    }

    fn sample_info(port: u16) -> ConnectionInfo {
        ConnectionInfo {
            peer_address: "127.0.0.1".to_owned(),
            peer_port: port,
            state: "ready".to_owned(),
            username: Some("alice".to_owned()),
            connected_at: Instant::now(),
            requests_served: Arc::new(AtomicU64::new(42)),
            protocol_version: 5,
        }
    }

    #[test]
    fn connections_table_metadata() {
        let tracker = make_tracker();
        let table = ConnectionsTable::new(tracker);

        assert_eq!(table.name(), "connections");
        assert_eq!(table.keyspace(), "system_observability");
        assert_eq!(table.columns().len(), 7);

        // Verify column names in order.
        let names: Vec<&str> = table.columns().iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "peer_address",
                "peer_port",
                "state",
                "username",
                "idle_seconds",
                "requests_served",
                "protocol_version"
            ]
        );

        // Verify data types.
        assert_eq!(table.columns()[0].data_type, DataType::Text);
        assert_eq!(table.columns()[1].data_type, DataType::Int);
        assert_eq!(table.columns()[5].data_type, DataType::BigInt);
        assert_eq!(table.columns()[6].data_type, DataType::Int);

        // Primary key is [0, 1].
        assert_eq!(table.primary_key_columns(), &[0, 1]);
    }

    #[test]
    fn connections_table_reads_active() {
        let tracker = make_tracker();
        tracker.register(sample_addr(9042), sample_info(9042));
        tracker.register(sample_addr(9043), sample_info(9043));

        let table = ConnectionsTable::new(Arc::clone(&tracker));
        let rows = table.read(None);

        assert_eq!(
            rows.len(),
            2,
            "expected 2 rows for 2 registered connections"
        );

        for row in &rows {
            assert_eq!(row.cells.len(), 7, "each row must have 7 cells");

            // peer_address should decode to "127.0.0.1"
            let addr_bytes = row.cells[0]
                .value
                .as_deref()
                .expect("peer_address not null");
            assert_eq!(addr_bytes, b"127.0.0.1");

            // state should decode to "ready"
            let state_bytes = row.cells[2].value.as_deref().expect("state not null");
            assert_eq!(state_bytes, b"ready");

            // requests_served should be 42 (i64 big-endian)
            let req_bytes = row.cells[5]
                .value
                .as_deref()
                .expect("requests_served not null");
            let req: i64 = i64::from_be_bytes(req_bytes.try_into().unwrap());
            assert_eq!(req, 42);

            // protocol_version should be 5 (i32 big-endian)
            let pv_bytes = row.cells[6]
                .value
                .as_deref()
                .expect("protocol_version not null");
            let pv: i32 = i32::from_be_bytes(pv_bytes.try_into().unwrap());
            assert_eq!(pv, 5);
        }
    }

    #[test]
    fn connections_table_is_pollable() {
        let tracker = make_tracker();
        let table = ConnectionsTable::new(tracker);
        assert!(matches!(
            table.subscription_mode(),
            SubscriptionMode::Pollable
        ));
    }

    #[test]
    fn connections_table_empty_when_no_connections() {
        let tracker = make_tracker();
        let table = ConnectionsTable::new(tracker);
        assert_eq!(table.read(None).len(), 0);
    }

    #[test]
    fn connections_table_deregistered_connection_disappears() {
        let tracker = make_tracker();
        let addr = sample_addr(9042);
        tracker.register(addr, sample_info(9042));

        let table = ConnectionsTable::new(Arc::clone(&tracker));
        assert_eq!(table.read(None).len(), 1);

        tracker.deregister(&addr);
        assert_eq!(table.read(None).len(), 0);
    }

    #[test]
    fn tracker_null_username_produces_tombstone() {
        let tracker = make_tracker();
        let addr = sample_addr(9042);
        tracker.register(
            addr,
            ConnectionInfo {
                peer_address: "127.0.0.1".to_owned(),
                peer_port: 9042,
                state: "startup".to_owned(),
                username: None,
                connected_at: Instant::now(),
                requests_served: Arc::new(AtomicU64::new(0)),
                protocol_version: 5,
            },
        );

        let table = ConnectionsTable::new(Arc::clone(&tracker));
        let rows = table.read(None);
        assert_eq!(rows.len(), 1);

        // username cell (index 3) should be a tombstone when username is None
        assert!(rows[0].cells[3].is_tombstone(), "no username → tombstone");
    }

    #[test]
    fn tracker_update_state() {
        let tracker = make_tracker();
        let addr = sample_addr(9042);
        tracker.register(addr, sample_info(9042));
        tracker.update_state(&addr, "authenticating");

        let table = ConnectionsTable::new(Arc::clone(&tracker));
        let rows = table.read(None);
        let state_bytes = rows[0].cells[2].value.as_deref().expect("state");
        assert_eq!(state_bytes, b"authenticating");
    }

    #[test]
    fn tracker_increment_requests() {
        let tracker = make_tracker();
        let addr = sample_addr(9042);
        tracker.register(addr, sample_info(9042)); // starts at 42
        tracker.increment_requests(&addr);
        tracker.increment_requests(&addr);

        let table = ConnectionsTable::new(Arc::clone(&tracker));
        let rows = table.read(None);
        let req_bytes = rows[0].cells[5].value.as_deref().expect("requests_served");
        let req: i64 = i64::from_be_bytes(req_bytes.try_into().unwrap());
        assert_eq!(req, 44);
    }

    #[test]
    fn tracker_increment_requests_is_correct_under_concurrency() {
        // The hot-path increment is a single atomic fetch_add on a shared
        // counter (no map clone/rcu). Many threads bumping the same connection
        // must produce an exact total — this both proves correctness and pins
        // the no-clone contract that reclaimed ~22% of write CPU (t_f0f17a55).
        let tracker = Arc::new(make_tracker());
        let addr = sample_addr(9042);
        tracker.register(addr, sample_info(9042)); // starts at 42

        let threads = 8usize;
        let per_thread = 10_000usize;
        std::thread::scope(|scope| {
            for _ in 0..threads {
                let t = Arc::clone(&tracker);
                scope.spawn(move || {
                    for _ in 0..per_thread {
                        t.increment_requests(&addr);
                    }
                });
            }
        });

        let table = ConnectionsTable::new(Arc::clone(&tracker));
        let rows = table.read(None);
        let req_bytes = rows[0].cells[5].value.as_deref().expect("requests_served");
        let req: i64 = i64::from_be_bytes(req_bytes.try_into().unwrap());
        assert_eq!(req, 42 + (threads * per_thread) as i64);
    }

    #[test]
    fn tracker_active_count() {
        let tracker = make_tracker();
        assert_eq!(tracker.active_count(), 0);
        tracker.register(sample_addr(9042), sample_info(9042));
        assert_eq!(tracker.active_count(), 1);
        tracker.register(sample_addr(9043), sample_info(9043));
        assert_eq!(tracker.active_count(), 2);
        tracker.deregister(&sample_addr(9042));
        assert_eq!(tracker.active_count(), 1);
    }
}
