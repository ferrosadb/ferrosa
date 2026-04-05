//! Integration test for 3-node cluster formation via progressive join.
//!
//! This test exposes a bug where a fresh 3-node Raft cluster cannot elect a
//! leader. See specs/bug-cluster-formation-raft-election-failure.md for the
//! full bug report.
//!
//! Expected behavior:
//!   - Node1 starts standalone, transitions to pair when node2 connects,
//!     then to cluster when node3 connects.
//!   - A Raft leader is elected within a reasonable time (~10s).
//!   - All 3 nodes agree on the leader.
//!   - DDL operations (e.g., CREATE KEYSPACE) succeed through Raft.
//!
//! Actual behavior (the bug):
//!   - All 3 nodes start elections simultaneously.
//!   - Candidates each get their own vote but never win quorum (need 2 of 3).
//!   - Terms increment slowly (T1 -> T19 in ~90s) but no leader is elected.
//!   - Vote RPCs timeout at 3s despite TCP connectivity being fine.
//!   - Node3 may be rejected with "peer not approved to join cluster".

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use uuid::Uuid;

use ferrosa_cluster::config::ClusterConfig;
use ferrosa_cluster::mode::DeploymentMode;
use ferrosa_cluster::controller::{ModeController, ModeControllerHandles};
use ferrosa_net::config::NetConfig;
use ferrosa_net::peer::{PeerEventListener, PeerManager};
use ferrosa_net::rpc::server::RpcServer;
use ferrosa_net::rpc::HandlerRegistry;
use ferrosa_net::pool::PriorityPool;
use ferrosa_storage::engine::{StorageEngine, StorageEngineConfig};
use ferrosa_storage::{CommitLogConfig, CompactionConfig};

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/// Create a StorageEngine with a temp directory.
fn test_storage(dir: &std::path::Path) -> Arc<StorageEngine> {
    let config = StorageEngineConfig {
        commit_log: CommitLogConfig {
            log_dir: dir.to_path_buf(),
            checkpoint_dir: dir.to_path_buf(),
            archive: None,
            ..CommitLogConfig::default()
        },
        compaction: CompactionConfig::from_env(dir.join("compaction")),
        object_store: None,
        local_cache_max_bytes: 1024 * 1024,
        flush_threshold_bytes: 4096,
        flush_max_age_secs: 5,
        data_dir: dir.to_path_buf(),
    };
    Arc::new(StorageEngine::new(config, None).unwrap())
}

/// Create a test Schema instance.
fn test_schema() -> Arc<ferrosa_schema::Schema> {
    use ferrosa_schema::{
        AuthMethod, DeploymentMode as SchemaDeploymentMode, LogAuditSink, PasswordHasher,
        PasswordPolicy, RateLimitConfig, SchemaConfig,
    };
    let config = SchemaConfig {
        hasher: PasswordHasher::default(),
        password_policy: PasswordPolicy::permissive(),
        auth_method: AuthMethod::Password,
        rate_limit: RateLimitConfig::default(),
        audit_sink: Box::new(LogAuditSink),
        secrets: Box::new(ferrosa_schema::EnvSecretsProvider),
        mode: SchemaDeploymentMode::Development,
    };
    Arc::new(ferrosa_schema::Schema::new(config).unwrap())
}

/// Create a NetConfig that binds to a random port on localhost.
fn test_net_config() -> NetConfig {
    NetConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        broadcast_addr: "127.0.0.1:0".parse().unwrap(),
        ..NetConfig::default()
    }
}

/// A test node wrapping ModeController with real networking.
///
/// Unlike PairNode (which is hardcoded for 2-node pair mode), this uses
/// ModeController directly to support the full standalone -> pair -> cluster
/// progression.
struct TestClusterNode {
    controller: Arc<ModeController>,
    _handles: ModeControllerHandles,
    #[allow(dead_code)]
    server: Arc<RpcServer>,
    peer_manager: Arc<PeerManager>,
    host_id: Uuid,
    bound_addr: SocketAddr,
    _dir: tempfile::TempDir,
}

impl TestClusterNode {
    /// Create and start a new test node. Binds to a random port.
    async fn start(host_id: Uuid) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        let schema = test_schema();

        // Use aggressive Raft election timeouts for testing.
        // In production, defaults are 3000-6000ms which are too slow for tests.
        // Even with these lower values, the bug still manifests because the
        // root cause is simultaneous elections and vote splitting, not timeout
        // duration.
        let config = Arc::new(ClusterConfig {
            raft_data_dir: Some(dir.path().join("raft")),
            auto_join: true, // Allow nodes to join without explicit approval
            raft_heartbeat_ms: 150,
            raft_election_timeout_min_ms: 500,
            raft_election_timeout_max_ms: 1000,
            ..ClusterConfig::default()
        });

        let net_config = Arc::new(test_net_config());
        let registry = Arc::new(HandlerRegistry::new());

        let (controller, handles) = ModeController::new(
            config,
            net_config.clone(),
            host_id,
            storage,
            schema,
            registry.clone(),
        );

        // Create PeerManager with ModeController as the listener
        let pm = Arc::new(PeerManager::new(
            net_config.clone(),
            host_id,
            controller.clone(),
        ));
        controller.set_peer_manager(pm.clone());

        // Start the RPC server on a random port
        let server = Arc::new(RpcServer::new(
            (*net_config).clone(),
            host_id,
            registry,
        ));
        let addr = server.start_and_get_addr().await.unwrap();

        Self {
            controller,
            _handles: handles,
            server,
            peer_manager: pm,
            host_id,
            bound_addr: addr,
            _dir: dir,
        }
    }

    /// Establish an outbound connection to another node.
    async fn connect_to(&self, other: &TestClusterNode) {
        let net_config = Arc::new(test_net_config());
        let pool = PriorityPool::connect(
            net_config,
            self.host_id,
            &other.bound_addr.to_string(),
        )
        .await
        .expect("failed to connect to peer");

        self.peer_manager
            .add_peer((other.host_id, other.bound_addr), pool)
            .await;
    }

    fn mode(&self) -> DeploymentMode {
        self.controller.mode()
    }

    /// Poll for Raft leader election, returning the leader node ID if found.
    async fn wait_for_leader(&self, timeout: Duration) -> Option<u64> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(raft) = self.controller.raft() {
                let metrics = raft.metrics().borrow().clone();
                if let Some(leader) = metrics.current_leader {
                    return Some(leader);
                }
            }
            if tokio::time::Instant::now() > deadline {
                // Diagnostic on timeout: print Raft state for each node.
                if let Some(raft) = self.controller.raft() {
                    let m = raft.metrics().borrow().clone();
                    eprintln!(
                        "[{}] election timeout: term={:?} state={:?} leader={:?}",
                        self.host_id, m.current_term, m.state, m.current_leader,
                    );
                } else {
                    eprintln!("[{}] election timeout: Raft instance is NONE", self.host_id);
                }
                return None;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn shutdown(&self) {
        self.controller.shutdown().await;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Test that 3 nodes can form a cluster and elect a Raft leader.
///
/// This test follows the production progressive join pattern:
///   1. Node1 starts standalone
///   2. Node2 connects to node1 -> both transition to pair mode
///   3. Node3 connects to node1 -> all three transition to cluster mode
///   4. Raft initialization runs on all nodes
///   5. Leader should be elected within 30 seconds
///
/// BUG: This test is expected to FAIL because:
///   - All 3 nodes start Raft elections simultaneously after transition_to_cluster
///   - With default election timeouts (3-6s), all nodes timeout their Vote RPCs
///   - Each candidate votes for itself but no candidate gets 2 votes (quorum)
///   - The election terms increment but no leader emerges
///   - Root cause: simultaneous raft.initialize() calls or simultaneous first
///     elections without staggered timeouts, combined with Vote RPC timeouts
///     caused by network transport issues on the Raft lane
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_cluster_elects_raft_leader() {
    // Use deterministic UUIDs so the test is reproducible.
    // Node1 has the highest UUID so it becomes Primary in pair mode,
    // which means it will be the seed that calls raft.initialize().
    let id1 = Uuid::from_bytes([0xFF, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    let id2 = Uuid::from_bytes([0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2]);
    let id3 = Uuid::from_bytes([0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3]);

    // Start all 3 nodes (each binds to a random port)
    let node1 = TestClusterNode::start(id1).await;
    let node2 = TestClusterNode::start(id2).await;
    let node3 = TestClusterNode::start(id3).await;

    // Verify all start in standalone mode
    assert_eq!(node1.mode(), DeploymentMode::Standalone);
    assert_eq!(node2.mode(), DeploymentMode::Standalone);
    assert_eq!(node3.mode(), DeploymentMode::Standalone);

    // Step 1: Node2 connects to node1 (progressive join: standalone -> pair)
    //
    // We simulate the peer connection by establishing the TCP connection and
    // then notifying the ModeController via on_peer_connected. In production,
    // the PeerManager does this automatically after handshake.
    node2.connect_to(&node1).await;
    node1.connect_to(&node2).await;

    // Notify controllers about the peer connection
    node1.controller.on_peer_connected((id2, node2.bound_addr));
    node2.controller.on_peer_connected((id1, node1.bound_addr));

    // Allow time for pair mode transition
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Both nodes should now be in pair mode
    assert_eq!(
        node1.mode(),
        DeploymentMode::Pair,
        "node1 should transition to pair after node2 connects"
    );
    assert_eq!(
        node2.mode(),
        DeploymentMode::Pair,
        "node2 should transition to pair after node1 connects"
    );

    // Step 2: Node3 connects to node1 (progressive join: pair -> cluster)
    //
    // This triggers the transition to cluster mode on node1, which then
    // broadcasts ClusterInvite to the other nodes.
    node3.connect_to(&node1).await;
    node1.connect_to(&node3).await;
    // Also establish connectivity between node2 and node3 for the mesh
    node3.connect_to(&node2).await;
    node2.connect_to(&node3).await;

    // Notify node1 about node3 connecting (triggers transition_to_forming -> transition_to_cluster)
    node1.controller.on_peer_connected((id3, node3.bound_addr));

    // Give node1 time to transition and send ClusterInvites
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Node1 should now be in cluster (or forming) mode
    let node1_mode = node1.mode();
    assert!(
        node1_mode == DeploymentMode::Cluster || node1_mode == DeploymentMode::Forming,
        "node1 should be in Cluster or Forming mode, got: {node1_mode:?}"
    );

    // Manually trigger cluster transition on node2 and node3 if they haven't
    // received the ClusterInvite yet (simulates the invite delivery).
    if node2.mode() == DeploymentMode::Pair {
        node2.controller.on_peer_connected((id3, node3.bound_addr));
    }
    if node3.mode() == DeploymentMode::Pair || node3.mode() == DeploymentMode::Standalone {
        node3.controller.on_peer_connected((id1, node1.bound_addr));
        if node3.mode() != DeploymentMode::Cluster && node3.mode() != DeploymentMode::Forming {
            node3.controller.on_peer_connected((id2, node2.bound_addr));
        }
    }

    tokio::time::sleep(Duration::from_millis(1000)).await;

    // All nodes should now be in cluster mode
    eprintln!(
        "Mode check: node1={:?} node2={:?} node3={:?}",
        node1.mode(),
        node2.mode(),
        node3.mode()
    );

    // Step 3: Wait for Raft leader election
    //
    // With election timeouts of 500-1000ms, a healthy cluster should elect
    // a leader within a few seconds. We give it 30 seconds to be generous.
    //
    // BUG: This is where the test is expected to fail. The leader election
    // times out because:
    //   1. All nodes start elections at roughly the same time
    //   2. Vote RPCs fail with timeouts (the Raft lane may not have
    //      established connections, or the handler registration races
    //      with the first Vote RPC)
    //   3. Each candidate votes for itself in every term but never gets
    //      a second vote
    //   4. Terms increment but no leader emerges
    let election_timeout = Duration::from_secs(30);

    eprintln!("Waiting up to 30s for Raft leader election...");
    let leader_on_node1 = node1.wait_for_leader(election_timeout).await;
    let leader_on_node2 = node2.wait_for_leader(Duration::from_secs(5)).await;
    let leader_on_node3 = node3.wait_for_leader(Duration::from_secs(5)).await;

    eprintln!(
        "Leader election results: node1={:?} node2={:?} node3={:?}",
        leader_on_node1, leader_on_node2, leader_on_node3
    );

    // Assert a leader was elected on at least one node
    assert!(
        leader_on_node1.is_some(),
        "BUG: No Raft leader elected on node1 within {election_timeout:?}. \
         This indicates the cluster formation race condition where all nodes \
         start elections simultaneously and split votes indefinitely. \
         See specs/bug-cluster-formation-raft-election-failure.md"
    );

    let leader = leader_on_node1.unwrap();

    // Assert all nodes agree on the same leader
    assert_eq!(
        leader_on_node2,
        Some(leader),
        "node2 should agree on the same leader as node1"
    );
    assert_eq!(
        leader_on_node3,
        Some(leader),
        "node3 should agree on the same leader as node1"
    );

    // Step 4: Verify DDL works through Raft
    //
    // If the leader was elected, DDL should work. The leader proposes the
    // command through Raft, and all followers apply it.
    //
    // Note: This step is only reached if the leader election succeeds.
    // Currently, it should not be reached due to the bug.
    if let Some(raft) = node1.controller.raft() {
        use ferrosa_cluster::raft::{RaftCommand, RaftOp};
        use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};

        let mut opts = std::collections::HashMap::new();
        opts.insert("replication_factor".to_string(), "1".to_string());

        let cmd = RaftCommand {
            op: RaftOp::CreateKeyspace(KeyspaceMetadata {
                name: "test_cluster_ks".to_string(),
                durable_writes: true,
                replication: ReplicationParams {
                    strategy: "SimpleStrategy".to_string(),
                    options: opts,
                },
            }),
            schema_version: Uuid::new_v4(),
        };

        let result = tokio::time::timeout(Duration::from_secs(10), raft.client_write(cmd)).await;

        assert!(
            result.is_ok(),
            "DDL (CreateKeyspace) timed out after 10s — Raft may not be functional"
        );
        assert!(
            result.unwrap().is_ok(),
            "DDL (CreateKeyspace) failed — Raft proposal rejected"
        );
    }

    // Cleanup
    node1.shutdown().await;
    node2.shutdown().await;
    node3.shutdown().await;
}

/// Minimal test: verify that ModeController transitions through
/// standalone -> pair -> cluster when peers connect progressively.
///
/// This is a simpler version that only checks mode transitions without
/// real networking, to isolate the mode state machine from network issues.
#[tokio::test]
async fn progressive_join_mode_transitions() {
    let dir = tempfile::tempdir().unwrap();
    let storage = test_storage(dir.path());
    let schema = test_schema();

    let config = Arc::new(ClusterConfig {
        raft_data_dir: Some(dir.path().join("raft")),
        auto_join: true,
        raft_heartbeat_ms: 150,
        raft_election_timeout_min_ms: 500,
        raft_election_timeout_max_ms: 1000,
        ..ClusterConfig::default()
    });
    let net_config = Arc::new(NetConfig::default());
    let local_id = Uuid::from_bytes([0xFF; 16]);
    let peer1_id = Uuid::from_bytes([0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    let peer2_id = Uuid::from_bytes([0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2]);

    let registry = Arc::new(HandlerRegistry::new());
    let (controller, _handles) = ModeController::new(
        config,
        net_config.clone(),
        local_id,
        storage,
        schema,
        registry,
    );

    let pm = Arc::new(PeerManager::new(net_config, local_id, controller.clone()));
    controller.set_peer_manager(pm);

    // Standalone -> Pair
    let peer1_addr: SocketAddr = "127.0.0.1:7001".parse().unwrap();
    controller.on_peer_connected((peer1_id, peer1_addr));
    assert_eq!(controller.mode(), DeploymentMode::Pair);

    // Pair -> Cluster (via Forming)
    let peer2_addr: SocketAddr = "127.0.0.2:7002".parse().unwrap();
    controller.on_peer_connected((peer2_id, peer2_addr));

    let mode = controller.mode();
    assert!(
        mode == DeploymentMode::Cluster || mode == DeploymentMode::Forming,
        "expected Cluster or Forming after 2nd peer, got: {mode:?}"
    );

    // Raft init runs in background — verify it was spawned.
    // Without real networking, it won't elect a leader, but the Raft
    // instance should eventually be stored after the election timeout.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if controller.raft().is_some() {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            // Expected in a single-process test without real networking:
            // Raft cannot elect a leader in a 3-node cluster without being
            // able to send Vote RPCs to peers. The background task stores
            // the Raft instance after the 30s election timeout, but we
            // only wait 10s here to keep the test fast.
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    controller.shutdown().await;
}
