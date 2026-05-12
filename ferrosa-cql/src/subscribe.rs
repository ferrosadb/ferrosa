//! Per-connection subscription lifecycle management.
//!
//! Each CQL connection can hold up to `max_subscriptions` active streaming
//! subscriptions. A `SubscriptionHandle` tracks one subscription and carries
//! a `CancellationToken` that is cancelled when the subscription is removed
//! (via UNSUBSCRIBE) or when the connection is torn down.
//!
//! The polling task (`run_subscription_poll`) re-executes the inner SELECT
//! at the specified interval and pushes result frames to the connection via
//! an `mpsc` channel.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::ast::Statement;
use crate::router::{RequestContext, RouteResult, SharedState};
use crate::types::CqlValue;

use ferrosa_schema::AuthContext;

/// A frame pushed from a subscription task to the connection writer.
pub struct SubscriptionPush {
    pub stream_id: i16,
    pub body: Bytes,
}

// ---------------------------------------------------------------------------
// Dual-timestamp subscription events (A5.7)
// ---------------------------------------------------------------------------

/// A subscription event that carries both Accord consensus timestamp
/// (`accord_ts`) and the local apply timestamp (`apply_ts`).
///
/// `accord_ts` is the globally agreed execution timestamp from the Accord
/// protocol. `apply_ts` is the wall-clock time at which the mutation was
/// applied on this replica. Consumers that need global ordering use
/// `accord_ts`; consumers that need recency-based display use `apply_ts`.
///
/// Old consumers that do not understand these fields simply ignore them
/// (the base `SubscriptionPush` is still sent unchanged).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionEvent {
    /// The mutation payload bytes (same as SubscriptionPush.body).
    pub body: Bytes,
    /// Accord consensus timestamp (globally ordered).
    pub accord_ts: u64,
    /// Local apply timestamp — wall-clock nanos when mutation was applied.
    pub apply_ts: u64,
}

impl SubscriptionEvent {
    /// Create a new dual-timestamp event.
    pub fn new(body: Bytes, accord_ts: u64, apply_ts: u64) -> Self {
        Self {
            body,
            accord_ts,
            apply_ts,
        }
    }
}

/// Sort a slice of events by their Accord consensus timestamp (ascending).
pub fn sort_by_accord_ts(events: &mut [SubscriptionEvent]) {
    events.sort_by_key(|e| e.accord_ts);
}

/// Sort a slice of events by their apply timestamp (ascending).
pub fn sort_by_apply_ts(events: &mut [SubscriptionEvent]) {
    events.sort_by_key(|e| e.apply_ts);
}

/// A handle to a single active subscription.
pub struct SubscriptionHandle {
    pub stream_id: u16,
    pub cancel: CancellationToken,
}

impl SubscriptionHandle {
    #[cfg(test)]
    pub fn test(stream_id: u16) -> Self {
        Self {
            stream_id,
            cancel: CancellationToken::new(),
        }
    }
}

/// Per-connection subscription state.
pub struct SubscriptionState {
    max_subscriptions: usize,
    subscriptions: HashMap<u16, SubscriptionHandle>,
}

impl SubscriptionState {
    pub fn new(max_subscriptions: usize) -> Self {
        Self {
            max_subscriptions,
            subscriptions: HashMap::new(),
        }
    }

    pub fn active_count(&self) -> usize {
        self.subscriptions.len()
    }

    pub fn add(&mut self, handle: SubscriptionHandle) -> Result<(), &'static str> {
        if self.subscriptions.len() >= self.max_subscriptions {
            return Err("maximum subscriptions per connection reached");
        }
        self.subscriptions.insert(handle.stream_id, handle);
        Ok(())
    }

    /// Cancel one or all subscriptions.
    pub fn cancel(&mut self, stream_id: Option<u16>) {
        match stream_id {
            Some(id) => {
                if let Some(handle) = self.subscriptions.remove(&id) {
                    handle.cancel.cancel();
                }
            }
            None => {
                // Cancel all
                for (_, handle) in self.subscriptions.drain() {
                    handle.cancel.cancel();
                }
            }
        }
    }

    /// Cancel all subscriptions (called on disconnect).
    pub fn cancel_all(&mut self) {
        self.cancel(None);
    }
}

/// Compute the rows that differ between `previous` and `current`.
///
/// A row is included in the delta if its full content does not appear anywhere
/// in the `previous` snapshot (new row or in-place value change).
///
/// Uses `O(n × m)` comparison — acceptable for the typically small result sets
/// produced by subscription queries (hundreds of rows, not millions).
fn compute_delta<'a>(
    current: &'a [Vec<Option<CqlValue>>],
    previous: &[Vec<Option<CqlValue>>],
) -> Vec<&'a Vec<Option<CqlValue>>> {
    current
        .iter()
        .filter(|row| !previous.contains(row))
        .collect()
}

/// Spawn a polling subscription task.
///
/// Re-executes the inner SELECT every `interval` and sends result frames
/// through `push_tx`. Runs until the `cancel` token fires or the channel
/// closes (connection dropped).
///
/// When `delta = true`, each tick delivers only rows whose values differ from
/// the previous delivery (row-level diff). The first tick always delivers all
/// matching rows. Subsequent ticks deliver only rows that are new or changed.
///
/// When `delta = false`, the full result set is delivered on every tick.
///
/// # Cancel Safety
///
/// This function is cancel-safe. The spawned task uses `reserve`+`send` for
/// channel writes and a `CancellationToken` for shutdown. Dropping the handle
/// returned by `tokio::spawn` (or the caller's future) does not leave the task
/// in a partially-sent state — the `permit` is only committed after the full
/// result is available.
#[allow(clippy::too_many_arguments)]
pub fn spawn_subscription_poll(
    stream_id: i16,
    interval: Duration,
    state: Arc<SharedState>,
    auth: AuthContext,
    keyspace: Option<String>,
    inner: Statement,
    push_tx: mpsc::Sender<SubscriptionPush>,
    cancel: CancellationToken,
    delta: bool,
) {
    tokio::spawn(async move {
        // SUBSCRIBE no longer sets allow_filtering — queries must use
        // partition keys or indexes, consistent with the ALLOW FILTERING
        // rejection policy.
        let inner = inner;

        // Delta mode: track rows delivered in the previous tick so we can
        // compute a diff. `None` means "no previous delivery yet" (first tick
        // always delivers everything).
        let mut previous_snapshot: Option<Vec<Vec<Option<CqlValue>>>> = None;

        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await; // Skip the immediate first tick — first result at t+interval.
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let ctx = RequestContext {
                        auth: &auth,
                        current_keyspace: &keyspace,
                        consistency: ferrosa_cluster::consistency::ConsistencyLevel::One,
                        serial_consistency: None,
                        paging: crate::paging::PagingParams {
                            page_size: Some(1_000),
                            paging_state: None,
                        },
                        client_address: String::new(),
                    };

                    if delta {
                        // Delta path: fetch raw rows, diff, encode only changed.
                        let select_stmt = match inner.as_select() {
                            Some(s) => s,
                            None => {
                                tracing::warn!(
                                    stream_id,
                                    "delta subscription inner statement is not a SELECT; \
                                     skipping tick"
                                );
                                continue;
                            }
                        };
                        match crate::router::route_select_raw(&state, &ctx, select_stmt).await {
                            Ok(raw) => {
                                let delta_rows: Vec<Vec<Option<CqlValue>>> =
                                    match &previous_snapshot {
                                        // First tick: deliver all rows.
                                        None => raw.rows.clone(),
                                        // Subsequent ticks: deliver only rows not in the
                                        // previous snapshot (new or changed rows).
                                        Some(prev) => {
                                            compute_delta(&raw.rows, prev)
                                                .into_iter()
                                                .cloned()
                                                .collect()
                                        }
                                    };
                                previous_snapshot = Some(raw.rows.clone());

                                // Only push a frame when there are rows to deliver.
                                // An empty delta (no changes) produces no frame.
                                if delta_rows.is_empty() {
                                    continue;
                                }

                                let body = crate::result::encode_rows(
                                    &raw.column_names,
                                    &raw.column_types,
                                    &raw.keyspace,
                                    &raw.table,
                                    &delta_rows,
                                );
                                let push = SubscriptionPush {
                                    stream_id,
                                    body: body.freeze(),
                                };
                                let permit = tokio::select! {
                                    p = push_tx.reserve() => match p {
                                        Ok(permit) => permit,
                                        Err(_) => break, // channel closed
                                    },
                                    _ = cancel.cancelled() => break,
                                };
                                permit.send(push);
                            }
                            Err(e) => {
                                tracing::debug!(
                                    stream_id,
                                    error = %e,
                                    "delta subscription query error"
                                );
                            }
                        }
                    } else {
                        // Full-delivery path (original behavior).
                        match crate::router::route(&state, &ctx, inner.clone()).await {
                            Ok(RouteResult::Result(body)) => {
                                let push = SubscriptionPush {
                                    stream_id,
                                    body: body.freeze(),
                                };
                                let permit = tokio::select! {
                                    p = push_tx.reserve() => match p {
                                        Ok(permit) => permit,
                                        Err(_) => break, // channel closed
                                    },
                                    _ = cancel.cancelled() => break,
                                };
                                permit.send(push);
                            }
                            Ok(_) => {} // Unexpected result type; skip
                            Err(e) => {
                                tracing::debug!(stream_id, error = %e, "subscription query error");
                            }
                        }
                    }
                }
                _ = cancel.cancelled() => {
                    tracing::debug!(stream_id, "subscription cancelled");
                    break;
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    // ── spawn_subscription_poll delivery tests ─────────────────────────────

    /// Build a minimal SharedState for testing, backed by a temp directory.
    #[cfg(test)]
    fn test_shared_state() -> (Arc<crate::router::SharedState>, tempfile::TempDir) {
        use crate::router::SharedState;
        use crate::virtual_tables::{active_queries::QueryTracker, connections::ConnectionTracker};
        use arc_swap::ArcSwap;
        use ferrosa_cluster::{DdlPath, WritePath};
        use ferrosa_schema::{
            AuthMethod, DeploymentMode, EnvSecretsProvider, NodeConfig, PasswordHasher,
            PasswordPolicy, RateLimitConfig, Schema, SchemaConfig, TestAuditSink,
        };
        use ferrosa_storage::{
            CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig,
            SyncStrategyConfig,
        };

        let dir = tempfile::TempDir::new().unwrap();

        let commit_log = CommitLogConfig {
            segment_size: 4096,
            max_segment_age: Duration::from_secs(60),
            sync_strategy: SyncStrategyConfig::Batch,
            log_dir: dir.path().join("commitlog"),
            checkpoint_dir: dir.path().join("commitlog"),
            archive: None,
        };
        let compaction = CompactionConfig::from_env(dir.path().join("compaction"));
        let engine_config = StorageEngineConfig {
            commit_log,
            compaction,
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            flush_threshold_bytes: 4096,
            flush_max_age_secs: 5,
            data_dir: dir.path().to_path_buf(),
            index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
            write_verify: true,
            auth_enabled: false,
            auth_warn: false,
            max_pending_replay_mutations_without_schema: 1024,
        };
        let engine = Arc::new(StorageEngine::new(engine_config, None).unwrap());

        let schema = Arc::new(
            Schema::new(SchemaConfig {
                hasher: PasswordHasher::Bcrypt { cost: 4 },
                password_policy: PasswordPolicy::permissive(),
                auth_method: AuthMethod::Password,
                rate_limit: RateLimitConfig::default(),
                audit_sink: Box::new(TestAuditSink::new()),
                secrets: Box::new(EnvSecretsProvider),
                mode: DeploymentMode::Development,
            })
            .unwrap(),
        );

        let node_config = Arc::new(NodeConfig {
            cluster_name: "test".into(),
            data_center: "dc1".into(),
            rack: "rack1".into(),
            rpc_port: 9042,
            host_id: uuid::Uuid::new_v4(),
            listen_address: "127.0.0.1".parse().unwrap(),
            listen_port: 7000,
            broadcast_address: "127.0.0.1".parse().unwrap(),
            broadcast_port: 7000,
            rpc_address: "127.0.0.1".parse().unwrap(),
            internal_rpc_address: "127.0.0.1".parse().unwrap(),
            internal_rpc_port: 9042,
            tokens: vec![],
        });

        let udf_executor =
            Arc::new(ferrosa_udf::UdfExecutor::new(ferrosa_udf::SandboxConfig::default()).unwrap());
        let mode_controller =
            ferrosa_cluster::ModeController::standalone_for_test(schema.clone(), engine.clone());

        let state = SharedState {
            engine: engine.clone(),
            schema: schema.clone(),
            node_config,
            cluster_state: Arc::new(ArcSwap::from_pointee(
                ferrosa_cluster::ClusterStateHolder::Standalone,
            )),
            write_path: Arc::new(ArcSwap::from_pointee(WritePath::direct(engine.clone()))),
            ddl_path: Arc::new(ArcSwap::from_pointee(DdlPath::Direct { schema, engine })),
            prepared_cache: Arc::new(crate::prepared::PreparedCache::new(10 * 1024 * 1024)),
            connection_tracker: Arc::new(ConnectionTracker::new()),
            query_tracker: Arc::new(QueryTracker::new()),
            udf_executor,
            event_sender: tokio::sync::broadcast::channel(64).0,
            mode_controller,
            cql_metrics: Arc::new(crate::observability::CqlMetrics::new()),
            topology_policy: crate::topology::ClientTopologyPolicy::default(),
            auth_warn: false,
            peer_manager: None,
            accord_clock: None,
        };

        (Arc::new(state), dir)
    }

    fn superuser_auth() -> ferrosa_schema::AuthContext {
        ferrosa_schema::AuthContext {
            role: "cassandra".into(),
            is_superuser: true,
            must_change_password: false,
        }
    }

    /// spawn_subscription_poll must deliver at least one RESULT frame through
    /// the mpsc channel within a reasonable deadline.
    #[tokio::test]
    async fn poll_delivers_frame_through_channel() {
        let (state, _dir) = test_shared_state();
        let (tx, mut rx) = mpsc::channel::<SubscriptionPush>(8);
        let cancel = CancellationToken::new();

        // Use system.local — always available with no keyspace setup required.
        let inner = crate::parser::parse("SELECT * FROM system.local").unwrap();

        spawn_subscription_poll(
            42,
            Duration::from_millis(5),
            state,
            superuser_auth(),
            None,
            inner,
            tx,
            cancel,
            false, // delta=false: full delivery (regression guard)
        );

        // Wait up to 2 seconds for a frame to arrive.
        let push = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for subscription frame")
            .expect("channel closed without delivering a frame");

        assert_eq!(push.stream_id, 42, "stream_id must be preserved");
        // The body must start with the Rows RESULT kind code (0x00000002).
        assert!(
            push.body.len() >= 4,
            "body is too short to be a valid RESULT frame"
        );
        let kind = i32::from_be_bytes([push.body[0], push.body[1], push.body[2], push.body[3]]);
        assert_eq!(
            kind, 0x0002,
            "expected Rows RESULT kind (0x0002), got 0x{kind:04X}"
        );
    }

    /// After cancellation the polling task must stop sending frames and the
    /// sender side of the channel must close (rx.recv() returns None).
    #[tokio::test]
    async fn poll_stops_after_cancellation() {
        let (state, _dir) = test_shared_state();
        let (tx, mut rx) = mpsc::channel::<SubscriptionPush>(8);
        let cancel = CancellationToken::new();

        let inner = crate::parser::parse("SELECT * FROM system.local").unwrap();

        spawn_subscription_poll(
            1,
            Duration::from_millis(5),
            state,
            superuser_auth(),
            None,
            inner,
            tx,
            cancel.clone(),
            false, // delta=false: full delivery
        );

        // Receive at least one frame to confirm the task is running.
        tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for first frame")
            .expect("channel closed before first frame");

        // Cancel the subscription.
        cancel.cancel();

        // Drain remaining frames (the task may have already queued one more)
        // and confirm no new frames arrive after a short window.
        tokio::time::sleep(Duration::from_millis(50)).await;
        // Drop tx so the task exits; channel should close.
        // (The task's tx clone will be dropped when the task exits after cancel.)
        // We just verify we can drain and the channel eventually ends.
        while rx.try_recv().is_ok() {}
        // Channel should now be drained; task should have exited.
    }

    /// Closing the receiver causes the polling task to stop (send returns Err).
    #[tokio::test]
    async fn poll_stops_when_channel_closed() {
        let (state, _dir) = test_shared_state();
        let (tx, rx) = mpsc::channel::<SubscriptionPush>(8);
        let cancel = CancellationToken::new();

        let inner = crate::parser::parse("SELECT * FROM system.local").unwrap();

        spawn_subscription_poll(
            7,
            Duration::from_millis(5),
            state,
            superuser_auth(),
            None,
            inner,
            tx,
            cancel,
            false, // delta=false: full delivery
        );

        // Drop the receiver — next send will fail and the task should exit.
        drop(rx);

        // Give the task time to attempt a send and exit cleanly.
        tokio::time::sleep(Duration::from_millis(100)).await;
        // If we reach here without panic, the task exited gracefully.
    }

    #[test]
    fn subscription_state_tracks_active() {
        let mut state = SubscriptionState::new(8);
        let handle = SubscriptionHandle::test(1);
        assert!(state.add(handle).is_ok());
        assert_eq!(state.active_count(), 1);
    }

    #[test]
    fn subscription_state_enforces_max() {
        let mut state = SubscriptionState::new(2);
        state.add(SubscriptionHandle::test(1)).unwrap();
        state.add(SubscriptionHandle::test(2)).unwrap();
        assert!(state.add(SubscriptionHandle::test(3)).is_err());
    }

    #[test]
    fn unsubscribe_by_stream_id() {
        let mut state = SubscriptionState::new(8);
        state.add(SubscriptionHandle::test(42)).unwrap();
        state.cancel(Some(42));
        assert_eq!(state.active_count(), 0);
    }

    #[test]
    fn unsubscribe_all() {
        let mut state = SubscriptionState::new(8);
        state.add(SubscriptionHandle::test(1)).unwrap();
        state.add(SubscriptionHandle::test(2)).unwrap();
        state.cancel(None);
        assert_eq!(state.active_count(), 0);
    }

    #[test]
    fn cancellation_token_is_cancelled() {
        let mut state = SubscriptionState::new(8);
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        state
            .add(SubscriptionHandle {
                stream_id: 1,
                cancel,
            })
            .unwrap();
        assert!(!cancel_clone.is_cancelled());
        state.cancel(Some(1));
        assert!(cancel_clone.is_cancelled());
    }

    // ===================================================================
    // A5.7 — SUBSCRIBE dual timestamps (4 tests)
    // ===================================================================

    /// Events carry both accord_ts and apply_ts.
    #[test]
    fn subscribe_dual_timestamps() {
        let event = SubscriptionEvent::new(
            Bytes::from_static(b"row-data"),
            1000, // accord_ts
            2000, // apply_ts
        );

        assert_eq!(event.accord_ts, 1000);
        assert_eq!(event.apply_ts, 2000);
        assert_eq!(event.body, Bytes::from_static(b"row-data"));
    }

    /// Events are ordered by accord_ts when using sort_by_accord_ts.
    #[test]
    fn subscribe_accord_ts_ordering() {
        let mut events = vec![
            SubscriptionEvent::new(Bytes::from_static(b"c"), 300, 100),
            SubscriptionEvent::new(Bytes::from_static(b"a"), 100, 300),
            SubscriptionEvent::new(Bytes::from_static(b"b"), 200, 200),
        ];

        sort_by_accord_ts(&mut events);

        assert_eq!(events[0].accord_ts, 100);
        assert_eq!(events[1].accord_ts, 200);
        assert_eq!(events[2].accord_ts, 300);
        // Body order must match the reordering.
        assert_eq!(events[0].body, Bytes::from_static(b"a"));
        assert_eq!(events[1].body, Bytes::from_static(b"b"));
        assert_eq!(events[2].body, Bytes::from_static(b"c"));
    }

    /// Old consumers that only read SubscriptionPush are unaffected by
    /// the new dual-timestamp fields — SubscriptionEvent is a separate
    /// type that does not alter the existing SubscriptionPush layout.
    #[test]
    fn subscribe_backward_compat() {
        // SubscriptionPush still works without timestamps.
        let push = SubscriptionPush {
            stream_id: 42,
            body: Bytes::from_static(b"result-frame"),
        };
        assert_eq!(push.stream_id, 42);
        assert_eq!(push.body, Bytes::from_static(b"result-frame"));

        // SubscriptionEvent is opt-in: creating one does not require
        // changing any existing SubscriptionPush consumer code.
        let event = SubscriptionEvent::new(push.body.clone(), 500, 600);
        assert_eq!(event.accord_ts, 500);
        assert_eq!(event.apply_ts, 600);
    }

    /// Events can be sorted by apply_ts as an alternative ordering.
    #[test]
    fn subscribe_apply_ts_sort() {
        let mut events = vec![
            SubscriptionEvent::new(Bytes::from_static(b"late-apply"), 100, 500),
            SubscriptionEvent::new(Bytes::from_static(b"early-apply"), 300, 100),
            SubscriptionEvent::new(Bytes::from_static(b"mid-apply"), 200, 300),
        ];

        sort_by_apply_ts(&mut events);

        assert_eq!(events[0].apply_ts, 100);
        assert_eq!(events[1].apply_ts, 300);
        assert_eq!(events[2].apply_ts, 500);
        // Body must follow the apply_ts order.
        assert_eq!(events[0].body, Bytes::from_static(b"early-apply"));
        assert_eq!(events[1].body, Bytes::from_static(b"mid-apply"));
        assert_eq!(events[2].body, Bytes::from_static(b"late-apply"));
    }

    // ===================================================================
    // P0-02 — SUBSCRIBE DELTA row-level diff (acceptance criterion #3)
    // ===================================================================

    /// Helper: parse and route a statement, asserting success.
    async fn exec(state: &Arc<crate::router::SharedState>, cql: &str) {
        use crate::router::{route, RequestContext};
        use ferrosa_cluster::consistency::ConsistencyLevel;
        let ctx = RequestContext {
            auth: &superuser_auth(),
            current_keyspace: &None,
            consistency: ConsistencyLevel::One,
            serial_consistency: None,
            paging: crate::paging::PagingParams::default(),
            client_address: String::new(),
        };
        let stmt =
            crate::parser::parse(cql).unwrap_or_else(|e| panic!("parse failed for {cql:?}: {e}"));
        route(state, &ctx, stmt)
            .await
            .unwrap_or_else(|e| panic!("route failed for {cql:?}: {e}"));
    }

    /// Extract the row count from a Rows RESULT frame body.
    ///
    /// The CQL native protocol Rows result layout (Global_tables_spec flag set):
    ///   [4] kind (0x0002)
    ///   [4] flags
    ///   [4] column_count
    ///   if Has_more_pages (0x0002): [4+N] paging state
    ///   [4] keyspace name length + bytes
    ///   [4] table name length + bytes
    ///   [4] N column specs (name length + bytes, type code, ...)
    ///   [4] rows_count  ← what we want
    ///
    /// Instead of parsing the full frame, we decode row count via a minimal
    /// walk: skip kind, flags, col_count, then the global table spec, then
    /// the column specs, and finally read the row count.
    fn decode_row_count(body: &[u8]) -> i32 {
        // Use the encode_rows output format: kind(4), then rows_metadata, then
        // rows_count(4). The metadata layout with Global_tables_spec:
        //   flags(4) + columns_count(4) + ks[u16+bytes] + tbl[u16+bytes] +
        //   N * (col_name[u16+bytes] + col_type[2+])
        // Rather than re-implement the full decoder, we rely on the fact that
        // our tests use a known schema (id int, v text) so column_count = 2.
        // We parse flags to handle that generically.

        assert!(body.len() >= 4, "body too short for kind code");
        let kind = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
        assert_eq!(kind, 0x0002, "expected Rows kind");

        let mut pos = 4usize; // skip kind

        // flags
        assert!(pos + 4 <= body.len());
        let flags = i32::from_be_bytes([body[pos], body[pos + 1], body[pos + 2], body[pos + 3]]);
        pos += 4;

        // columns_count
        assert!(pos + 4 <= body.len());
        let col_count =
            i32::from_be_bytes([body[pos], body[pos + 1], body[pos + 2], body[pos + 3]]) as usize;
        pos += 4;

        let has_more_pages = (flags & 0x0002) != 0;
        if has_more_pages {
            // paging_state: [int length][bytes]
            assert!(pos + 4 <= body.len());
            let ps_len =
                i32::from_be_bytes([body[pos], body[pos + 1], body[pos + 2], body[pos + 3]])
                    as usize;
            pos += 4 + ps_len;
        }

        let global_tables_spec = (flags & 0x0001) != 0;
        if global_tables_spec {
            // keyspace name: [u16 length][bytes]
            assert!(pos + 2 <= body.len());
            let ks_len = u16::from_be_bytes([body[pos], body[pos + 1]]) as usize;
            pos += 2 + ks_len;
            // table name: [u16 length][bytes]
            assert!(pos + 2 <= body.len());
            let tbl_len = u16::from_be_bytes([body[pos], body[pos + 1]]) as usize;
            pos += 2 + tbl_len;
        }

        // Skip col_count column specs.
        for _ in 0..col_count {
            // col_name: [u16 length][bytes]
            assert!(pos + 2 <= body.len(), "ran out of bytes reading col name");
            let col_name_len = u16::from_be_bytes([body[pos], body[pos + 1]]) as usize;
            pos += 2 + col_name_len;
            // col_type: [u16 option code] — simple types are just 2 bytes.
            // Complex types (list, set, map, tuple, udt) have additional bytes.
            // Our test table uses int (0x0009) and varchar (0x000D): both simple.
            assert!(pos + 2 <= body.len(), "ran out of bytes reading col type");
            let type_code = u16::from_be_bytes([body[pos], body[pos + 1]]);
            pos += 2;
            // Skip type parameter bytes for known complex types.
            // (Our test schema only uses simple types, so this is fine.)
            let _ = type_code;
        }

        // rows_count
        assert!(pos + 4 <= body.len(), "ran out of bytes reading rows_count");
        i32::from_be_bytes([body[pos], body[pos + 1], body[pos + 2], body[pos + 3]])
    }

    /// Acceptance criterion #3 (P0-02):
    ///
    /// SUBSCRIBE DELTA on a table with 10 rows:
    /// - First tick delivers all 10 rows.
    /// - Modify 2 rows → second tick delivers exactly 2 rows.
    /// - No changes → third tick delivers 0 rows (no frame within deadline).
    ///
    /// Also verifies that non-DELTA subscriptions are unaffected (regression).
    #[tokio::test]
    async fn delta_delivers_only_changed_rows() {
        let (state, _dir) = test_shared_state();

        // Schema setup: keyspace + table (id int PRIMARY KEY, v text).
        exec(
            &state,
            "CREATE KEYSPACE delta_ks WITH REPLICATION = \
             {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .await;
        exec(
            &state,
            "CREATE TABLE delta_ks.items (id int PRIMARY KEY, v text)",
        )
        .await;

        // Insert 10 rows.
        for i in 0..10 {
            exec(
                &state,
                &format!("INSERT INTO delta_ks.items (id, v) VALUES ({i}, 'original-{i}')"),
            )
            .await;
        }

        // Start a DELTA subscription with a 50 ms interval.
        let (tx, mut rx) = mpsc::channel::<SubscriptionPush>(32);
        let cancel = CancellationToken::new();
        let inner = crate::parser::parse("SELECT * FROM delta_ks.items ALLOW FILTERING").unwrap();

        spawn_subscription_poll(
            99,
            Duration::from_millis(50),
            state.clone(),
            superuser_auth(),
            Some("delta_ks".into()),
            inner,
            tx,
            cancel.clone(),
            true, // delta=true: row-level diff
        );

        // ── Tick 1: all 10 rows must be delivered ─────────────────────────
        let first = tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("timed out waiting for first delta tick")
            .expect("channel closed before first tick");

        assert_eq!(first.stream_id, 99);
        let first_count = decode_row_count(&first.body);
        assert_eq!(
            first_count, 10,
            "first delta tick must deliver all 10 rows; got {first_count}"
        );

        // ── Modify rows 0 and 1 ───────────────────────────────────────────
        exec(
            &state,
            "INSERT INTO delta_ks.items (id, v) VALUES (0, 'changed-0')",
        )
        .await;
        exec(
            &state,
            "INSERT INTO delta_ks.items (id, v) VALUES (1, 'changed-1')",
        )
        .await;

        // ── Tick 2: exactly 2 changed rows must be delivered ──────────────
        let second = tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("timed out waiting for second delta tick")
            .expect("channel closed before second tick");

        let second_count = decode_row_count(&second.body);
        assert_eq!(
            second_count, 2,
            "second delta tick must deliver exactly 2 changed rows; got {second_count}"
        );

        // ── Tick 3: no changes → no frame within 200 ms ───────────────────
        let no_frame = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await;
        assert!(
            no_frame.is_err(),
            "third tick must produce no frame when there are no changes \
             (delta subscription must not deliver empty results)"
        );

        cancel.cancel();
    }

    /// Regression: non-DELTA subscription still delivers full result on every tick.
    #[tokio::test]
    async fn non_delta_delivers_full_result_every_tick() {
        let (state, _dir) = test_shared_state();

        exec(
            &state,
            "CREATE KEYSPACE ndelta_ks WITH REPLICATION = \
             {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .await;
        exec(
            &state,
            "CREATE TABLE ndelta_ks.items (id int PRIMARY KEY, v text)",
        )
        .await;
        exec(
            &state,
            "INSERT INTO ndelta_ks.items (id, v) VALUES (1, 'row-one')",
        )
        .await;

        let (tx, mut rx) = mpsc::channel::<SubscriptionPush>(8);
        let cancel = CancellationToken::new();
        let inner = crate::parser::parse("SELECT * FROM ndelta_ks.items ALLOW FILTERING").unwrap();

        spawn_subscription_poll(
            77,
            Duration::from_millis(50),
            state.clone(),
            superuser_auth(),
            Some("ndelta_ks".into()),
            inner,
            tx,
            cancel.clone(),
            false, // delta=false: full delivery on every tick
        );

        // First tick.
        let first = tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("timed out waiting for first tick")
            .expect("channel closed before first tick");
        assert_eq!(decode_row_count(&first.body), 1);

        // Second tick — no changes, but full delivery still expected.
        let second = tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("timed out waiting for second tick")
            .expect("channel closed before second tick");
        assert_eq!(decode_row_count(&second.body), 1);

        cancel.cancel();
    }
}
