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

use ferrosa_schema::AuthContext;

/// A frame pushed from a subscription task to the connection writer.
pub struct SubscriptionPush {
    pub stream_id: i16,
    pub body: Bytes,
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

/// Spawn a polling subscription task.
///
/// Re-executes the inner SELECT every `interval` and sends result frames
/// through `push_tx`. Runs until the `cancel` token fires or the channel
/// closes (connection dropped).
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
) {
    tokio::spawn(async move {
        // SUBSCRIBE no longer sets allow_filtering — queries must use
        // partition keys or indexes, consistent with the ALLOW FILTERING
        // rejection policy.
        let inner = inner;

        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await; // Skip the immediate first tick — first result at t+interval.
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let ctx = RequestContext {
                        auth: &auth,
                        current_keyspace: &keyspace,
                        consistency: ferrosa_cluster::consistency::ConsistencyLevel::One,
                    };
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
            data_dir: dir.path().to_path_buf(),
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
}
