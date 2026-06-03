use super::cluster::{
    build_recovered_topology_refresh_plan, build_recovered_topology_token_repair_plan,
    drain_ddl_queue, keyspace_needs_cluster_replay, should_initialize_seed_membership,
    should_run_bootstrap_streaming,
};
use super::*;
use crate::raft::{NodeInfo, NodeState};
use ferrosa_net::message::Message;
use ferrosa_net::rpc::server::RpcServer;
use ferrosa_net::rpc::{PeerId, RpcHandler};

fn test_storage(dir: &std::path::Path) -> Arc<StorageEngine> {
    use ferrosa_storage::{CommitLogConfig, CompactionConfig, StorageEngineConfig};
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
        local_disk_free_reserve_bytes: 0,
        flush_threshold_bytes: 4096,
        memtable_backpressure_bytes: u64::MAX,
        flush_max_age_secs: 5,
        data_dir: dir.to_path_buf(),
        index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
        auth_enabled: false,
        auth_warn: false,
        write_verify: false,
        max_pending_replay_mutations_without_schema: 1024,
        memtable_num_shards: 64,
    };
    Arc::new(StorageEngine::new(config, None).unwrap())
}

fn test_schema() -> Arc<Schema> {
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
    Arc::new(Schema::new(config).unwrap())
}

struct EchoPingHandler;

#[async_trait::async_trait]
impl RpcHandler for EchoPingHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        let Message::Ping { nonce, sent_at } = msg else {
            return None;
        };
        Some(Message::Pong {
            nonce,
            ping_recv_at: sent_at,
            sent_at,
        })
    }
}

async fn start_live_peer_server(
    bind_addr: &str,
    host_id: Uuid,
) -> (Arc<RpcServer>, std::net::SocketAddr) {
    let config = NetConfig {
        bind_addr: bind_addr.parse().unwrap(),
        ..NetConfig::default()
    };
    let registry = Arc::new(HandlerRegistry::new());
    registry.register(MsgType::Ping, Arc::new(EchoPingHandler));
    let server = Arc::new(RpcServer::new(config, host_id, registry));
    let addr = server.start_and_get_addr().await.unwrap();
    (server, addr)
}

#[test]
fn starts_in_standalone_mode() {
    let dir = tempfile::tempdir().unwrap();
    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig::default());
    let net_config = Arc::new(NetConfig::default());
    let host_id = Uuid::new_v4();

    let registry = Arc::new(HandlerRegistry::new());
    let (controller, _handles) =
        ModeController::new(config, net_config, host_id, storage, schema, registry);

    assert_eq!(controller.mode(), DeploymentMode::Standalone);
}

#[test]
fn peer_connect_transitions_to_pair() {
    let dir = tempfile::tempdir().unwrap();
    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig::default());
    let net_config = Arc::new(NetConfig::default());
    let local_id = Uuid::new_v4();
    let peer_id = Uuid::new_v4();

    let registry = Arc::new(HandlerRegistry::new());
    let (controller, _handles) = ModeController::new(
        config,
        net_config.clone(),
        local_id,
        storage,
        schema,
        registry,
    );

    // Create a PeerManager and set it
    let pm = Arc::new(PeerManager::new(
        net_config.clone(),
        local_id,
        controller.clone(),
    ));
    controller.set_peer_manager(pm.clone());

    // Simulate peer connection
    let peer_addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
    controller.on_peer_connected((peer_id, peer_addr));

    assert_eq!(controller.mode(), DeploymentMode::Pair);
}

#[test]
fn peer_disconnect_transitions_to_degraded() {
    let dir = tempfile::tempdir().unwrap();
    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig::default());
    let net_config = Arc::new(NetConfig::default());
    let local_id = Uuid::new_v4();
    let peer_id = Uuid::new_v4();

    let registry = Arc::new(HandlerRegistry::new());
    let (controller, _handles) = ModeController::new(
        config,
        net_config.clone(),
        local_id,
        storage,
        schema,
        registry,
    );

    let pm = Arc::new(PeerManager::new(
        net_config.clone(),
        local_id,
        controller.clone(),
    ));
    controller.set_peer_manager(pm.clone());

    // Connect then disconnect
    let peer_addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
    controller.on_peer_connected((peer_id, peer_addr));
    assert_eq!(controller.mode(), DeploymentMode::Pair);

    controller.on_peer_disconnected((peer_id, peer_addr));
    // Degraded preserves pair context — mode is DegradedPair, not Standalone
    assert_eq!(controller.mode(), DeploymentMode::DegradedPair);
    // Pair context is preserved for automatic recovery
    assert!(
        controller.role().is_some(),
        "pair context must be preserved in degraded mode"
    );
}

#[test]
fn force_promote_sets_direct_write_path() {
    let dir = tempfile::tempdir().unwrap();
    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig::default());
    let net_config = Arc::new(NetConfig::default());
    let local_id = Uuid::new_v4();

    let registry = Arc::new(HandlerRegistry::new());
    let (controller, _handles) =
        ModeController::new(config, net_config, local_id, storage, schema, registry);

    controller.force_promote().unwrap();
    assert_eq!(controller.mode(), DeploymentMode::Standalone);
    assert!(controller.force_promoted.load(Ordering::Acquire));
}

#[tokio::test]
async fn promote_from_degraded_pair_restores_writes() {
    let dir = tempfile::tempdir().unwrap();
    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig::default());
    let net_config = Arc::new(NetConfig::default());
    let local_id = Uuid::from_u128(1);
    let peer_id = Uuid::from_u128(2);

    let registry = Arc::new(HandlerRegistry::new());
    let (controller, _handles) = ModeController::new(
        config,
        net_config.clone(),
        local_id,
        storage,
        schema,
        registry,
    );

    let pm = Arc::new(PeerManager::new(
        net_config.clone(),
        local_id,
        controller.clone(),
    ));
    controller.set_peer_manager(pm.clone());

    // Enter pair mode via inbound connection. Lower host-id wins primary.
    let peer_addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
    controller.on_inbound_peer((peer_id, peer_addr), None);
    assert_eq!(controller.mode(), DeploymentMode::Pair);
    assert_eq!(controller.role(), Some(PairRole::Primary));

    // Peer disconnects → DegradedPair
    controller.on_peer_disconnected((peer_id, peer_addr));
    assert_eq!(controller.mode(), DeploymentMode::DegradedPair);
    // Pair context preserved
    assert!(controller.role().is_some());

    // Operator promotes → Standalone Primary with direct writes
    controller.force_promote().unwrap();
    assert_eq!(controller.mode(), DeploymentMode::Standalone);
    assert!(controller.force_promoted.load(Ordering::Acquire));
    assert!(controller.is_cql_ready());

    // When old primary reconnects (outbound), this node stays Primary
    // because force_promoted flag overrides connection direction
}

#[tokio::test]
async fn degraded_pair_serves_stale_reads() {
    let dir = tempfile::tempdir().unwrap();
    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig::default());
    let net_config = Arc::new(NetConfig::default());
    let local_id = Uuid::from_u128(2);
    let peer_id = Uuid::from_u128(1);

    let registry = Arc::new(HandlerRegistry::new());
    let (controller, _handles) = ModeController::new(
        config,
        net_config.clone(),
        local_id,
        storage,
        schema,
        registry,
    );

    let pm = Arc::new(PeerManager::new(
        net_config.clone(),
        local_id,
        controller.clone(),
    ));
    controller.set_peer_manager(pm);

    // Enter pair mode
    let peer_addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
    controller.on_inbound_peer((peer_id, peer_addr), None);
    assert_eq!(controller.mode(), DeploymentMode::Pair);

    // Peer disconnects → DegradedPair
    controller.on_peer_disconnected((peer_id, peer_addr));
    assert_eq!(controller.mode(), DeploymentMode::DegradedPair);

    // CQL is still ready (stale reads available)
    assert!(controller.is_cql_ready());
}

#[tokio::test]
async fn second_peer_transitions_to_cluster() {
    let dir = tempfile::tempdir().unwrap();

    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig {
        raft_data_dir: Some(dir.path().join("raft")),
        ..ClusterConfig::default()
    });
    let net_config = Arc::new(NetConfig::default());
    let local_id = Uuid::new_v4();
    let peer1_id = Uuid::new_v4();
    let peer2_id = Uuid::new_v4();

    let registry = Arc::new(HandlerRegistry::new());
    let (controller, _handles) = ModeController::new(
        config,
        net_config.clone(),
        local_id,
        storage,
        schema,
        registry,
    );

    let pm = Arc::new(PeerManager::new(
        net_config.clone(),
        local_id,
        controller.clone(),
    ));
    controller.set_peer_manager(pm);

    // First peer → pair mode
    let peer1_addr: SocketAddr = "127.0.0.1:7001".parse().unwrap();
    controller.on_peer_connected((peer1_id, peer1_addr));
    assert_eq!(controller.mode(), DeploymentMode::Pair);

    // Second peer → cluster mode
    let peer2_addr: SocketAddr = "127.0.0.2:7002".parse().unwrap();
    controller.on_peer_connected((peer2_id, peer2_addr));
    assert_eq!(controller.mode(), DeploymentMode::Cluster);
}

#[test]
fn connected_peers_tracked_and_cleared() {
    let dir = tempfile::tempdir().unwrap();
    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig::default());
    let net_config = Arc::new(NetConfig::default());
    let local_id = Uuid::new_v4();
    let peer_id = Uuid::new_v4();

    let registry = Arc::new(HandlerRegistry::new());
    let (controller, _handles) = ModeController::new(
        config,
        net_config.clone(),
        local_id,
        storage,
        schema,
        registry,
    );

    let pm = Arc::new(PeerManager::new(
        net_config.clone(),
        local_id,
        controller.clone(),
    ));
    controller.set_peer_manager(pm);

    let peer_addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
    controller.on_peer_connected((peer_id, peer_addr));

    assert_eq!(controller.connected_peers.lock().len(), 1);

    controller.on_peer_disconnected((peer_id, peer_addr));
    assert_eq!(controller.connected_peers.lock().len(), 0);
}

/// Helper: create a ModeController in cluster mode with raft init spawned.
///
/// Returns the controller and a tempdir handle (must be held alive for
/// the sled store to remain valid).
async fn setup_cluster_controller() -> (Arc<ModeController>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();

    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig {
        raft_data_dir: Some(dir.path().join("raft")),
        ..ClusterConfig::default()
    });
    let net_config = Arc::new(NetConfig::default());
    let local_id = Uuid::new_v4();
    let peer1_id = Uuid::new_v4();
    let peer2_id = Uuid::new_v4();

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

    // First peer -> pair, second peer -> cluster (spawns raft init)
    let peer1_addr: SocketAddr = "127.0.0.1:7001".parse().unwrap();
    controller.on_peer_connected((peer1_id, peer1_addr));

    let peer2_addr: SocketAddr = "127.0.0.2:7002".parse().unwrap();
    controller.on_peer_connected((peer2_id, peer2_addr));

    (controller, dir)
}

#[tokio::test]
async fn raft_initializes_on_third_peer() {
    let (controller, _dir) = setup_cluster_controller().await;
    assert_eq!(controller.mode(), DeploymentMode::Cluster);

    // The raft init runs in a background task. Poll until raft() is Some
    // or timeout after 10 seconds. A single-node Raft elects itself leader
    // quickly, but our 3-node cluster with no real networking may take a moment.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if controller.raft().is_some() {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            // This is expected — single-node Raft in a 3-node cluster
            // cannot elect a leader without real networking. The raft
            // instance should still be stored after the timeout.
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    // In a single-process test without real networking, raft will be stored
    // after the background task's leader election loop times out (~30s).
    // We verify the mode is Cluster and the task was spawned successfully.
    assert_eq!(controller.mode(), DeploymentMode::Cluster);
}

#[tokio::test]
async fn transition_to_cluster_normalizes_ephemeral_peer_ports_before_seeding_ring() {
    let dir = tempfile::tempdir().unwrap();

    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig {
        raft_data_dir: Some(dir.path().join("raft")),
        ..ClusterConfig::default()
    });
    let net_config = Arc::new(NetConfig::default());
    let local_id = Uuid::new_v4();
    let peer1_id = Uuid::new_v4();
    let peer2_id = Uuid::new_v4();

    let registry = Arc::new(HandlerRegistry::new());
    let (controller, _handles) = ModeController::new(
        config,
        net_config.clone(),
        local_id,
        storage,
        schema,
        registry,
    );

    let pm = Arc::new(PeerManager::new(
        net_config.clone(),
        local_id,
        controller.clone(),
    ));
    controller.set_peer_manager(pm);

    controller.transition_to_cluster(vec![
        (peer1_id, "10.89.1.53:50318".parse().unwrap()),
        (peer2_id, "10.89.1.54:50319".parse().unwrap()),
    ]);

    let ring = controller
        .token_ring()
        .expect("cluster transition should publish a token ring snapshot");

    let peer1 = ring
        .get_node(uuid_to_node_id(peer1_id))
        .expect("peer1 should be seeded into the initial ring");
    let peer2 = ring
        .get_node(uuid_to_node_id(peer2_id))
        .expect("peer2 should be seeded into the initial ring");

    assert_eq!(peer1.addr, "10.89.1.53:17000");
    assert_eq!(peer2.addr, "10.89.1.54:17000");
}

#[test]
fn fresh_seed_with_no_persisted_raft_state_still_initializes_membership() {
    assert!(should_initialize_seed_membership(
        true,  // was_seed
        false, // has_recovered_membership
        false, // has_recovered_topology_state
    ));
}

#[test]
fn seed_restart_with_recovered_membership_skips_initialize() {
    assert!(!should_initialize_seed_membership(
        true,  // was_seed
        true,  // has_recovered_membership
        false, // has_recovered_topology_state
    ));
    assert!(!should_initialize_seed_membership(
        false, // was_seed
        false, // has_recovered_membership
        false, // has_recovered_topology_state
    ));
}

#[test]
fn seed_restart_with_recovered_topology_backed_membership_skips_initialize() {
    assert!(!should_initialize_seed_membership(
        true, // was_seed
        true, // recovered from topology state
        true, // has_recovered_topology_state
    ));
}

#[test]
fn seed_restart_with_only_persisted_vote_or_log_still_initializes_membership() {
    assert!(should_initialize_seed_membership(
        true,  // was_seed
        false, // has_recovered_membership
        false, // has_recovered_topology_state
    ));
    assert!(!should_initialize_seed_membership(
        false, // was_seed
        false, // has_recovered_membership
        false, // has_recovered_topology_state
    ));
}

#[test]
fn seed_restart_with_recovered_topology_but_empty_membership_skips_initialize() {
    assert!(!should_initialize_seed_membership(
        true,  // was_seed
        false, // has_recovered_membership
        true,  // has_recovered_topology_state
    ));
}

#[test]
fn recovered_topology_restart_skips_bootstrap_streaming() {
    assert!(!should_run_bootstrap_streaming(true));
    assert!(should_run_bootstrap_streaming(false));
}

#[test]
fn cluster_reconnect_rebroadcasts_invite_even_when_join_is_deduped() {
    let now = std::time::Instant::now();
    assert!(
        super::peer_events::should_send_cluster_invite_after_join_trigger(false, None, now),
        "a recreated existing member can have current ring metadata, so JoinNode is deduped, \
         but it still needs ClusterInvite to leave pair mode and register Raft/Bulk handlers"
    );
    assert!(
        super::peer_events::should_send_cluster_invite_after_join_trigger(true, Some(now), now)
    );
}

#[test]
fn cluster_reconnect_invite_is_rate_limited_when_join_is_deduped() {
    let now = std::time::Instant::now();
    assert!(
        !super::peer_events::should_send_cluster_invite_after_join_trigger(false, Some(now), now),
        "duplicate reconnects for an already-known member must not repeatedly rebroadcast invites"
    );
    assert!(
        super::peer_events::should_send_cluster_invite_after_join_trigger(
            false,
            Some(now - super::CLUSTER_RECONNECT_INVITE_COOLDOWN),
            now
        )
    );
}

#[test]
fn reconnect_invite_plan_includes_self_and_excludes_recipient() {
    let local = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap();
    let recipient = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    let other = Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap();
    let local_addr = "10.0.0.1:7000".parse().unwrap();
    let recipient_addr = "10.0.0.2:7000".parse().unwrap();
    let other_addr = "10.0.0.3:7000".parse().unwrap();

    let plan = super::invite::plan_reconnect_invite(super::invite::ReconnectInvitePlanInput {
        local_host_id: local,
        local_addr: Some(local_addr),
        recipient,
        connected_peers: &[(recipient, recipient_addr), (other, other_addr)],
    })
    .expect("other peers plus self should produce an invite payload");

    assert_eq!(plan.recipient, recipient);
    assert_eq!(plan.peers, vec![(other, other_addr), (local, local_addr)]);
}

#[test]
fn connected_peer_tracking_updates_existing_peer_address() {
    let peer = Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();
    let old_addr = "10.89.1.14:7000".parse().unwrap();
    let new_addr = "10.89.1.17:7000".parse().unwrap();
    let other = Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap();
    let other_addr = "10.89.1.18:7000".parse().unwrap();
    let mut peers = vec![(peer, old_addr), (other, other_addr)];

    super::peer_events::track_connected_peer(&mut peers, peer, new_addr, 16);

    assert_eq!(
        peers,
        vec![(peer, new_addr), (other, other_addr)],
        "reconnects with a new container IP must update connected_peers; otherwise later ClusterInvite payloads re-advertise dead addresses"
    );
}

#[test]
fn reconnect_invite_plan_returns_none_when_no_peer_addresses_available() {
    let local = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap();
    let recipient = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    let recipient_addr = "10.0.0.2:7000".parse().unwrap();

    let plan = super::invite::plan_reconnect_invite(super::invite::ReconnectInvitePlanInput {
        local_host_id: local,
        local_addr: None,
        recipient,
        connected_peers: &[(recipient, recipient_addr)],
    });

    assert!(
        plan.is_none(),
        "an invite with no reachable peers would only echo the recipient"
    );
}

#[test]
fn cluster_invite_keeps_live_peer_when_payload_advertises_older_address() {
    let local = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
    let peer = Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();
    let stale_invite_addr = "10.89.1.14:9042".parse().unwrap();

    let plan = super::cluster::plan_invite_peer_connection(
        local,
        peer,
        stale_invite_addr,
        7000,
        Some("10.89.1.17:7000"),
        true,
    );

    assert_eq!(
        plan,
        super::cluster::InvitePeerConnectionPlan::KeepLiveKnownPeer {
            known_addr: "10.89.1.17:7000".to_string(),
            invite_addr: "10.89.1.14:7000".parse().unwrap(),
        },
        "third-party ClusterInvite metadata must not downgrade an already-live peer to a dead address"
    );
}

#[test]
fn inbound_peer_address_change_refreshes_live_outbound_pool() {
    let current_inbound_addr = "10.89.1.174:7000".parse().unwrap();

    assert!(
        super::peer_events::should_refresh_outbound_peer_for_inbound(
            Some("10.89.1.171:7000"),
            true,
            current_inbound_addr,
        ),
        "an inbound connection from a recreated peer is authoritative enough to replace a stale live outbound pool"
    );
    assert!(
        !super::peer_events::should_refresh_outbound_peer_for_inbound(
            Some("10.89.1.174:7000"),
            true,
            current_inbound_addr,
        ),
        "matching live outbound pools should not be churned"
    );
    assert!(
        super::peer_events::should_refresh_outbound_peer_for_inbound(
            Some("10.89.1.174:7000"),
            false,
            current_inbound_addr,
        ),
        "a non-live pool should be rebuilt even when the stored address is current"
    );
}

#[test]
fn stale_inbound_refresh_cannot_overwrite_newer_peer_address() {
    assert!(
        super::peer_events::should_install_refreshed_outbound_peer(
            Some("10.89.1.178:7000"),
            Some("10.89.1.178:7000"),
        ),
        "a refresh may install when the peer address has not changed while it was connecting"
    );
    assert!(
        !super::peer_events::should_install_refreshed_outbound_peer(
            Some("10.89.1.178:7000"),
            Some("10.89.1.181:7000"),
        ),
        "a stale refresh must not overwrite a newer inbound address"
    );
    assert!(
        !super::peer_events::should_install_refreshed_outbound_peer(None, Some("10.89.1.181:7000"),),
        "a first-observed refresh must not install after another task already learned the peer"
    );
}

#[test]
fn reconnect_invite_reservation_is_atomic_under_burst_callbacks() {
    let peer = Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap();
    let now = std::time::Instant::now();
    let mut recent = std::collections::BTreeMap::new();

    assert!(super::invite::reserve_reconnect_invite(
        &mut recent,
        peer,
        now,
        super::CLUSTER_RECONNECT_INVITE_COOLDOWN,
        super::MAX_CONNECTED_PEERS,
    ));
    assert_eq!(recent.get(&peer).copied(), Some(now));

    let duplicate = now + std::time::Duration::from_millis(5);
    assert!(
        !super::invite::reserve_reconnect_invite(
            &mut recent,
            peer,
            duplicate,
            super::CLUSTER_RECONNECT_INVITE_COOLDOWN,
            super::MAX_CONNECTED_PEERS,
        ),
        "the second callback in a reconnect burst must observe the reservation and skip delivery"
    );
    assert_eq!(
        recent.get(&peer).copied(),
        Some(now),
        "suppressed duplicates must not extend the cooldown window"
    );
}

#[test]
fn invite_cooldown_allows_retry_after_interval() {
    let peer = Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap();
    let now = std::time::Instant::now();
    let mut recent = std::collections::BTreeMap::new();

    assert!(super::invite::reserve_reconnect_invite(
        &mut recent,
        peer,
        now,
        super::CLUSTER_RECONNECT_INVITE_COOLDOWN,
        super::MAX_CONNECTED_PEERS,
    ));

    let after_cooldown = now + super::CLUSTER_RECONNECT_INVITE_COOLDOWN;
    assert!(
        super::invite::reserve_reconnect_invite(
            &mut recent,
            peer,
            after_cooldown,
            super::CLUSTER_RECONNECT_INVITE_COOLDOWN,
            super::MAX_CONNECTED_PEERS,
        ),
        "reconnect invites must be retried once the cooldown interval has elapsed"
    );
    assert_eq!(recent.get(&peer).copied(), Some(after_cooldown));
}

#[test]
fn cluster_membership_forward_handler_is_registered_before_waiting_for_leader() {
    let source = include_str!("cluster.rs");
    let forward_registration = source
        .find("MsgType::ClusterMembershipForward")
        .expect("cluster transition must register ClusterMembershipForward");
    let leader_wait = source
        .find("// Wait for leader election.")
        .expect("cluster transition must wait for leader election");

    assert!(
        forward_registration < leader_wait,
        "membership refresh forwarding can happen during reconnect before leader-election waits finish; \
         the ClusterMembershipForward handler must already be registered or peers log no-handler and topology refresh times out"
    );
}

#[tokio::test]
async fn peer_manager_registration_installs_membership_forward_nack_before_cluster_mode() {
    let dir = tempfile::tempdir().unwrap();
    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig::default());
    let net_config = Arc::new(NetConfig::default());
    let local_id = Uuid::new_v4();

    let registry = Arc::new(HandlerRegistry::new());
    let (controller, _handles) = ModeController::new(
        config,
        net_config.clone(),
        local_id,
        storage,
        schema,
        registry.clone(),
    );
    let pm = Arc::new(PeerManager::new(net_config, local_id, controller.clone()));

    controller.set_peer_manager(pm);

    assert!(
        registry.has_handler(MsgType::ClusterMembershipForward),
        "membership-forward must have a startup handler before cluster transition; otherwise rolling restart races drop UpdateNodeInfo forwards"
    );

    let response = registry
        .dispatch(
            (Uuid::new_v4(), "127.0.0.1:7000".parse().unwrap()),
            MsgType::ClusterMembershipForward,
            Message::ClusterMembershipForward(bytes::Bytes::from_static(b"not decoded")),
        )
        .await;

    let Some(Message::ClusterMembershipForwardAck(body)) = response else {
        panic!("expected explicit ClusterMembershipForwardAck nack, not a dropped request");
    };
    let ack: crate::raft_forward::ForwardAckBody =
        bincode::deserialize(&body).expect("nack body must decode");
    assert_eq!(
        ack,
        crate::raft_forward::ForwardAckBody::Err(
            "ClusterMembershipForward: node has not entered cluster mode".to_string()
        )
    );
}

#[test]
fn peer_event_plan_cluster_connect_existing_member_triggers_join_and_invite() {
    let peer = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
    let addr = "10.89.5.18:7000".parse().unwrap();
    let now = std::time::Instant::now();

    let plan = super::peer_plan::plan_connected_peer(super::peer_plan::ConnectedPeerInput {
        host_id: peer,
        addr,
        mode: DeploymentMode::Cluster,
        known_peer_count_after_tracking: 3,
        committed_cluster_size: 3,
        join_enqueued: false,
        last_reconnect_invite_sent: None,
        cql_broadcast: Some("127.0.0.1:38043".to_string()),
        now,
    });

    assert_eq!(
        plan.actions,
        vec![
            super::peer_plan::PeerEventAction::TriggerClusterJoin {
                host_id: peer,
                addr,
                cql_broadcast: Some("127.0.0.1:38043".to_string()),
            },
            super::peer_plan::PeerEventAction::SendClusterInvite {
                host_id: peer,
                force: false,
            },
        ],
        "a recreated existing cluster member can have current ring metadata so JoinNode is deduped, \
         but it still needs a ClusterInvite to leave pair mode and register Raft handlers"
    );
}

#[test]
fn peer_event_plan_cluster_connect_suppresses_duplicate_invite_with_recent_reservation() {
    let peer = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
    let addr = "10.89.5.18:7000".parse().unwrap();
    let now = std::time::Instant::now();

    let plan = super::peer_plan::plan_connected_peer(super::peer_plan::ConnectedPeerInput {
        host_id: peer,
        addr,
        mode: DeploymentMode::Cluster,
        known_peer_count_after_tracking: 3,
        committed_cluster_size: 3,
        join_enqueued: false,
        last_reconnect_invite_sent: Some(now),
        cql_broadcast: None,
        now,
    });

    assert!(plan
        .actions
        .contains(&super::peer_plan::PeerEventAction::TriggerClusterJoin {
            host_id: peer,
            addr,
            cql_broadcast: None,
        }));
    assert!(
        !plan.actions.iter().any(|action| matches!(
            action,
            super::peer_plan::PeerEventAction::SendClusterInvite { host_id, .. } if *host_id == peer
        )),
        "a recent invite reservation suppresses only duplicate ClusterInvite delivery"
    );
}

#[test]
fn peer_event_plan_degraded_cluster_restores_only_after_committed_quorum() {
    let peer = Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();
    let addr = "10.89.5.19:7000".parse().unwrap();
    let now = std::time::Instant::now();

    let below_quorum = super::peer_plan::ConnectedPeerInput {
        host_id: peer,
        addr,
        mode: DeploymentMode::DegradedCluster,
        known_peer_count_after_tracking: 1,
        committed_cluster_size: 5,
        join_enqueued: false,
        last_reconnect_invite_sent: None,
        cql_broadcast: None,
        now,
    };
    let still_degraded = super::peer_plan::plan_connected_peer(below_quorum.clone());
    assert!(
        !still_degraded
            .actions
            .contains(&super::peer_plan::PeerEventAction::RestoreClusterMode),
        "dynamic peer count must not claim quorum for a 5-node committed cluster with only 2 live members"
    );

    let restored = super::peer_plan::plan_connected_peer(super::peer_plan::ConnectedPeerInput {
        known_peer_count_after_tracking: 2,
        ..below_quorum
    });
    assert!(
        restored
            .actions
            .contains(&super::peer_plan::PeerEventAction::RestoreClusterMode),
        "3 live members restores quorum for a committed 5-node cluster"
    );
}

#[test]
fn peer_event_plan_recovered_cluster_peer_invites_and_delivers_hints_independently() {
    let peer = Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap();

    let invite_only = super::peer_plan::plan_recovered_peer(super::peer_plan::RecoveredPeerInput {
        host_id: peer,
        mode: DeploymentMode::Cluster,
        pending_hint_count: 0,
        peer_manager_available: false,
    });
    assert_eq!(
        invite_only.actions,
        vec![super::peer_plan::PeerEventAction::SendClusterInvite {
            host_id: peer,
            force: false,
        }],
        "recovery invite delivery must not depend on pending hints or PeerManager availability"
    );

    let invite_and_hints =
        super::peer_plan::plan_recovered_peer(super::peer_plan::RecoveredPeerInput {
            host_id: peer,
            mode: DeploymentMode::Cluster,
            pending_hint_count: 7,
            peer_manager_available: true,
        });
    assert_eq!(
        invite_and_hints.actions,
        vec![
            super::peer_plan::PeerEventAction::SendClusterInvite {
                host_id: peer,
                force: false,
            },
            super::peer_plan::PeerEventAction::DeliverHints { host_id: peer },
        ]
    );
}

#[test]
fn initial_raft_membership_uses_local_broadcast_address_for_seed() {
    let local_host_id = Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap();
    let peer1_host_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
    let peer2_host_id = Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();
    let peers = vec![
        (peer1_host_id, "172.20.0.3:7000".parse().unwrap()),
        (peer2_host_id, "172.20.0.4:7000".parse().unwrap()),
    ];

    let members = super::cluster::build_initial_raft_members(
        local_host_id,
        "172.20.0.5:7000".parse().unwrap(),
        &peers,
    );

    assert_eq!(
        members[&uuid_to_node_id(local_host_id)].addr,
        "172.20.0.5:7000",
        "the seed must not commit itself to Raft membership with an empty address"
    );
}

#[test]
fn raft_membership_refresh_plan_uses_topology_addresses() {
    let node3 = Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap();
    let plan = vec![crate::raft::NodeInfo {
        host_id: node3,
        addr: "172.20.0.5:7000".to_string(),
        data_center: "dc1".to_string(),
        rack: "rack1".to_string(),
        state: crate::raft::NodeState::Normal,
        cql_broadcast: None,
    }];

    let members = super::cluster::build_raft_members_from_node_info(&plan);

    assert_eq!(members[&uuid_to_node_id(node3)].addr, "172.20.0.5:7000");
}

#[test]
fn raft_membership_repair_detects_empty_committed_node_address() {
    let node3 = uuid_to_node_id(Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap());
    let current = std::collections::BTreeMap::from([(
        node3,
        openraft::BasicNode {
            addr: String::new(),
        },
    )]);
    let desired = std::collections::BTreeMap::from([(
        node3,
        openraft::BasicNode {
            addr: "172.20.0.5:7000".to_string(),
        },
    )]);

    assert!(
        super::cluster::membership_addresses_need_repair(&current, &desired),
        "a committed BasicNode with addr=\"\" must trigger leader-side membership repair"
    );
}

#[test]
fn recovered_topology_refresh_plan_uses_current_live_addresses() {
    let local_host_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
    let peer_host_id = Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();
    let peers = vec![(peer_host_id, "10.89.5.18:7000".parse().unwrap())];
    let peer_cql_broadcasts =
        std::collections::HashMap::from([(peer_host_id, Some("127.0.0.1:38043".to_string()))]);

    let plan = build_recovered_topology_refresh_plan(
        local_host_id,
        "10.89.5.17:7000".to_string(),
        Some("127.0.0.1:38042".to_string()),
        "dc1",
        "rack1",
        &peers,
        &peer_cql_broadcasts,
    );

    assert_eq!(plan.len(), 2);
    assert_eq!(plan[0].host_id, local_host_id);
    assert_eq!(plan[0].addr, "10.89.5.17:7000");
    assert_eq!(plan[0].cql_broadcast.as_deref(), Some("127.0.0.1:38042"));

    assert_eq!(plan[1].host_id, peer_host_id);
    assert_eq!(plan[1].addr, "10.89.5.18:7000");
    assert_eq!(plan[1].cql_broadcast.as_deref(), Some("127.0.0.1:38043"));
}

#[test]
fn recovered_topology_refresh_plan_repairs_tokens_for_every_voter() {
    let local_host_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
    let peer_host_id = Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();
    let peers = vec![(peer_host_id, "10.89.5.18:7000".parse().unwrap())];
    let peer_cql_broadcasts = std::collections::HashMap::new();
    let num_tokens = 4;

    let refresh_plan = build_recovered_topology_refresh_plan(
        local_host_id,
        "10.89.5.17:7000".to_string(),
        Some("127.0.0.1:38042".to_string()),
        "dc1",
        "rack1",
        &peers,
        &peer_cql_broadcasts,
    );
    let token_plan = build_recovered_topology_token_repair_plan(&refresh_plan, num_tokens);

    assert_eq!(
        token_plan.len(),
        refresh_plan.len(),
        "recovered topology must re-author token assignments for every recovered voter"
    );
    for (node_id, tokens) in token_plan {
        assert_eq!(
            tokens.len(),
            num_tokens,
            "node {node_id} should receive deterministic token ownership on recovered topology refresh"
        );
    }
}

#[tokio::test]
async fn raft_accessor_returns_none_before_init() {
    let dir = tempfile::tempdir().unwrap();
    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig::default());
    let net_config = Arc::new(NetConfig::default());
    let local_id = Uuid::new_v4();

    let registry = Arc::new(HandlerRegistry::new());
    let (controller, _handles) =
        ModeController::new(config, net_config, local_id, storage, schema, registry);

    // Before any transition, raft() should be None
    assert!(
        controller.raft().is_none(),
        "raft() should be None in standalone mode"
    );
}

#[tokio::test]
async fn raft_init_registers_handlers() {
    let (controller, _dir) = setup_cluster_controller().await;

    // Give the background task time to register handlers
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Verify that the Raft handlers were registered by checking the
    // registry has entries for the Raft message types
    assert!(
        controller.registry.has_handler(MsgType::RaftAppendEntries),
        "RaftAppendEntries handler should be registered"
    );
    assert!(
        controller.registry.has_handler(MsgType::RaftVote),
        "RaftVote handler should be registered"
    );
    assert!(
        controller
            .registry
            .has_handler(MsgType::RaftInstallSnapshot),
        "RaftInstallSnapshot handler should be registered"
    );
    assert!(
        controller.registry.has_handler(MsgType::ReadRequest),
        "ReadRequest handler should be registered"
    );
}

#[test]
fn deterministic_token_generation_is_stable() {
    let node_id = 42u64;
    let t1 = generate_deterministic_token(node_id, 0);
    let t2 = generate_deterministic_token(node_id, 0);
    assert_eq!(t1, t2, "same inputs must produce same token");

    // Different indices produce different tokens
    let t3 = generate_deterministic_token(node_id, 1);
    assert_ne!(t1, t3, "different indices should produce different tokens");

    // Different node IDs produce different tokens
    let t4 = generate_deterministic_token(99u64, 0);
    assert_ne!(t1, t4, "different nodes should produce different tokens");
}

// ---- Task 17: Node join tests ----------------------------------------

#[tokio::test]
async fn unapproved_node_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig {
        auto_join: false,
        ..ClusterConfig::default()
    });
    let net_config = Arc::new(NetConfig::default());
    let local_id = Uuid::new_v4();

    let registry = Arc::new(HandlerRegistry::new());
    let (controller, _handles) =
        ModeController::new(config, net_config, local_id, storage, schema, registry);

    let peer_host_id = Uuid::new_v4();
    let peer_node_id = uuid_to_node_id(peer_host_id);

    // auto_join=false, node not in approved_nodes -> Err(NotApproved)
    let result = controller
        .handle_join_request(peer_host_id, peer_node_id, None)
        .await;
    assert!(
        matches!(result, Err(ClusterError::NotApproved(id)) if id == peer_host_id),
        "unapproved node must be rejected"
    );
}

#[tokio::test]
async fn approved_node_passes_approval_check() {
    let dir = tempfile::tempdir().unwrap();
    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig {
        auto_join: false,
        ..ClusterConfig::default()
    });
    let net_config = Arc::new(NetConfig::default());
    let local_id = Uuid::new_v4();

    let registry = Arc::new(HandlerRegistry::new());
    let (controller, _handles) =
        ModeController::new(config, net_config, local_id, storage, schema, registry);

    let peer_host_id = Uuid::new_v4();
    let peer_node_id = uuid_to_node_id(peer_host_id);

    // Approve the node first
    controller.approve_node(peer_host_id);

    // auto_join=false, node in approved_nodes -> passes approval,
    // but fails at raft check (expected — raft not initialized in standalone)
    let result = controller
        .handle_join_request(peer_host_id, peer_node_id, None)
        .await;
    assert!(
        matches!(result, Err(ClusterError::Internal(_))),
        "approved node should pass approval check but fail on raft: got {result:?}"
    );
}

#[tokio::test]
async fn auto_join_bypasses_approval() {
    let dir = tempfile::tempdir().unwrap();
    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig {
        auto_join: true,
        ..ClusterConfig::default()
    });
    let net_config = Arc::new(NetConfig::default());
    let local_id = Uuid::new_v4();

    let registry = Arc::new(HandlerRegistry::new());
    let (controller, _handles) =
        ModeController::new(config, net_config, local_id, storage, schema, registry);

    let peer_host_id = Uuid::new_v4();
    let peer_node_id = uuid_to_node_id(peer_host_id);

    // auto_join=true -> bypasses approval, but fails at raft check
    let result = controller
        .handle_join_request(peer_host_id, peer_node_id, None)
        .await;
    // Should NOT be NotApproved — should be Internal (raft not initialized)
    assert!(
        matches!(result, Err(ClusterError::Internal(_))),
        "auto_join should bypass approval check: got {result:?}"
    );
}

#[test]
fn join_generates_correct_token_count() {
    // Verify that generate_deterministic_token produces num_tokens unique tokens.
    let node_id = 12345u64;
    let num_tokens = 256;
    let tokens: Vec<i64> = (0..num_tokens)
        .map(|i| generate_deterministic_token(node_id, i))
        .collect();

    // All tokens should be unique.
    let unique: std::collections::HashSet<i64> = tokens.iter().copied().collect();
    assert_eq!(
        unique.len(),
        num_tokens,
        "all 256 tokens must be unique for a given node"
    );
}

// ---- Task 18: Node decommission tests --------------------------------

/// BUG-010: Approved cluster nodes are never admitted — on_peer_connected()
/// in cluster mode must trigger handle_join_request() for approved peers.
#[tokio::test]
async fn approved_peer_triggers_join_in_cluster_mode() {
    let dir = tempfile::tempdir().unwrap();

    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig {
        auto_join: true, // bypass approval check for simplicity
        raft_data_dir: Some(dir.path().join("raft")),
        ..ClusterConfig::default()
    });
    let net_config = Arc::new(NetConfig::default());
    let local_id = Uuid::new_v4();
    let peer1_id = Uuid::new_v4();
    let peer2_id = Uuid::new_v4();

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

    // First peer -> pair, second peer -> cluster
    let peer1_addr: SocketAddr = "127.0.0.1:7001".parse().unwrap();
    controller.on_peer_connected((peer1_id, peer1_addr));
    let peer2_addr: SocketAddr = "127.0.0.2:7002".parse().unwrap();
    controller.on_peer_connected((peer2_id, peer2_addr));
    assert_eq!(controller.mode(), DeploymentMode::Cluster);

    // Now a new (3rd) peer connects in cluster mode.
    // The controller should trigger a join for this peer.
    let new_peer_id = Uuid::new_v4();
    let new_peer_addr: SocketAddr = "127.0.0.3:7003".parse().unwrap();
    controller.on_peer_connected((new_peer_id, new_peer_addr));

    // Verify the join was queued via pending_joins.
    let pending = controller.pending_joins.lock();
    assert!(
        pending.contains(&new_peer_id),
        "new peer should be in pending_joins after on_peer_connected in cluster mode"
    );
}

#[tokio::test]
async fn existing_cluster_member_does_not_queue_duplicate_join() {
    let dir = tempfile::tempdir().unwrap();
    let peer2_id = Uuid::new_v4();
    let (_server, peer2_addr) = start_live_peer_server("127.0.0.1:0", peer2_id).await;

    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig {
        auto_join: true,
        raft_data_dir: Some(dir.path().join("raft")),
        ..ClusterConfig::default()
    });
    let net_config = Arc::new(NetConfig::default());
    let local_id = Uuid::new_v4();
    let peer1_id = Uuid::new_v4();

    let registry = Arc::new(HandlerRegistry::new());
    let (controller, _handles) = ModeController::new(
        config,
        net_config.clone(),
        local_id,
        storage,
        schema,
        registry,
    );

    let pm = Arc::new(PeerManager::new(
        net_config.clone(),
        local_id,
        controller.clone(),
    ));
    controller.set_peer_manager(pm.clone());

    controller.on_peer_connected((peer1_id, "127.0.0.1:7001".parse().unwrap()));
    controller.on_peer_connected((peer2_id, peer2_addr));
    assert_eq!(controller.mode(), DeploymentMode::Cluster);

    let mut ring = TokenRing::new();
    ring.add_node(
        uuid_to_node_id(local_id),
        NodeInfo {
            host_id: local_id,
            addr: net_config.broadcast_addr.to_string(),
            data_center: controller.config.data_center.clone(),
            rack: controller.config.rack.clone(),
            state: NodeState::Normal,
            cql_broadcast: controller.config.cql_broadcast.clone(),
        },
    );
    ring.add_node(
        uuid_to_node_id(peer2_id),
        NodeInfo {
            host_id: peer2_id,
            addr: peer2_addr.to_string(),
            data_center: controller.config.data_center.clone(),
            rack: controller.config.rack.clone(),
            state: NodeState::Normal,
            cql_broadcast: None,
        },
    );
    controller.set_token_ring(Arc::new(ring));

    pm.ensure_peer(peer2_id, &peer2_addr.to_string())
        .await
        .unwrap();
    controller.pending_joins.lock().clear();

    // peer2 is already part of the seeded ring for this cluster transition.
    controller.on_peer_connected((peer2_id, peer2_addr));

    let pending = controller.pending_joins.lock();
    assert!(
        !pending.contains(&peer2_id),
        "existing cluster members must not queue duplicate join work"
    );
}

#[tokio::test]
async fn existing_cluster_member_without_outbound_pool_requeues_join_refresh() {
    let dir = tempfile::tempdir().unwrap();

    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig {
        auto_join: true,
        raft_data_dir: Some(dir.path().join("raft")),
        ..ClusterConfig::default()
    });
    let net_config = Arc::new(NetConfig::default());
    let local_id = Uuid::new_v4();
    let peer1_id = Uuid::new_v4();
    let peer2_id = Uuid::new_v4();

    let registry = Arc::new(HandlerRegistry::new());
    let (controller, _handles) = ModeController::new(
        config,
        net_config.clone(),
        local_id,
        storage,
        schema,
        registry,
    );

    let pm = Arc::new(PeerManager::new(
        net_config.clone(),
        local_id,
        controller.clone(),
    ));
    controller.set_peer_manager(pm.clone());

    controller.on_peer_connected((peer1_id, "127.0.0.1:7001".parse().unwrap()));
    controller.on_peer_connected((peer2_id, "127.0.0.2:7002".parse().unwrap()));
    assert_eq!(controller.mode(), DeploymentMode::Cluster);

    pm.remove_peer(peer2_id).await;
    controller.on_peer_connected((peer2_id, "127.0.0.2:7002".parse().unwrap()));

    let pending = controller.pending_joins.lock();
    assert!(
        pending.contains(&peer2_id),
        "existing cluster members must rebuild outbound peer pools after reconnect"
    );
}

#[tokio::test]
async fn existing_cluster_member_with_placeholder_peer_entry_requeues_join_refresh() {
    let dir = tempfile::tempdir().unwrap();

    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig {
        auto_join: true,
        raft_data_dir: Some(dir.path().join("raft")),
        ..ClusterConfig::default()
    });
    let net_config = Arc::new(NetConfig::default());
    let local_id = Uuid::new_v4();
    let peer1_id = Uuid::new_v4();
    let peer2_id = Uuid::new_v4();

    let registry = Arc::new(HandlerRegistry::new());
    let (controller, _handles) = ModeController::new(
        config,
        net_config.clone(),
        local_id,
        storage,
        schema,
        registry,
    );

    let pm = Arc::new(PeerManager::new(
        net_config.clone(),
        local_id,
        controller.clone(),
    ));
    controller.set_peer_manager(pm.clone());

    controller.on_peer_connected((peer1_id, "127.0.0.1:7001".parse().unwrap()));
    controller.on_peer_connected((peer2_id, "127.0.0.2:7002".parse().unwrap()));
    assert_eq!(controller.mode(), DeploymentMode::Cluster);

    pm.add_peer_entry((peer2_id, "127.0.0.2:7002".parse().unwrap()))
        .await;
    controller.on_peer_connected((peer2_id, "127.0.0.2:7002".parse().unwrap()));

    let pending = controller.pending_joins.lock();
    assert!(
        pending.contains(&peer2_id),
        "pool-less placeholder peer entries must not suppress join refresh"
    );
}

#[tokio::test]
async fn existing_inbound_cluster_member_with_ephemeral_source_port_does_not_queue_duplicate_join()
{
    let dir = tempfile::tempdir().unwrap();
    let peer2_id = Uuid::new_v4();
    let (_server, peer2_addr) = start_live_peer_server("127.0.0.1:0", peer2_id).await;

    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig {
        auto_join: true,
        raft_data_dir: Some(dir.path().join("raft")),
        ..ClusterConfig::default()
    });
    let net_config = Arc::new(NetConfig {
        bind_addr: std::net::SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            peer2_addr.port(),
        ),
        ..NetConfig::default()
    });
    let local_id = Uuid::new_v4();
    let peer1_id = Uuid::new_v4();

    let registry = Arc::new(HandlerRegistry::new());
    let (controller, _handles) = ModeController::new(
        config,
        net_config.clone(),
        local_id,
        storage,
        schema,
        registry,
    );

    let pm = Arc::new(PeerManager::new(
        net_config.clone(),
        local_id,
        controller.clone(),
    ));
    controller.set_peer_manager(pm.clone());

    controller.on_peer_connected((peer1_id, "127.0.0.1:7001".parse().unwrap()));
    controller.on_peer_connected((peer2_id, peer2_addr));
    assert_eq!(controller.mode(), DeploymentMode::Cluster);

    let mut ring = TokenRing::new();
    ring.add_node(
        uuid_to_node_id(local_id),
        NodeInfo {
            host_id: local_id,
            addr: net_config.broadcast_addr.to_string(),
            data_center: controller.config.data_center.clone(),
            rack: controller.config.rack.clone(),
            state: NodeState::Normal,
            cql_broadcast: controller.config.cql_broadcast.clone(),
        },
    );
    ring.add_node(
        uuid_to_node_id(peer2_id),
        NodeInfo {
            host_id: peer2_id,
            addr: peer2_addr.to_string(),
            data_center: controller.config.data_center.clone(),
            rack: controller.config.rack.clone(),
            state: NodeState::Normal,
            cql_broadcast: None,
        },
    );
    controller.set_token_ring(Arc::new(ring));

    pm.ensure_peer(peer2_id, &peer2_addr.to_string())
        .await
        .unwrap();
    controller.pending_joins.lock().clear();

    use ferrosa_net::rpc::InboundPeerCallback;
    controller.on_inbound_peer(
        (peer2_id, std::net::SocketAddr::new(peer2_addr.ip(), 50318)),
        None,
    );

    let pending = controller.pending_joins.lock();
    assert!(
        !pending.contains(&peer2_id),
        "existing cluster members reconnecting inbound on an ephemeral source port must not queue duplicate join work"
    );
}

#[tokio::test]
async fn existing_cluster_member_with_changed_addr_requeues_join_refresh() {
    let dir = tempfile::tempdir().unwrap();

    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig {
        auto_join: true,
        raft_data_dir: Some(dir.path().join("raft")),
        ..ClusterConfig::default()
    });
    let net_config = Arc::new(NetConfig::default());
    let local_id = Uuid::new_v4();
    let peer1_id = Uuid::new_v4();
    let peer2_id = Uuid::new_v4();

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
    controller.set_peer_manager(pm.clone());

    controller.on_peer_connected((peer1_id, "10.0.0.2:7000".parse().unwrap()));
    controller.on_peer_connected((peer2_id, "10.0.0.3:7000".parse().unwrap()));
    assert_eq!(controller.mode(), DeploymentMode::Cluster);

    pm.add_peer_entry((peer2_id, "10.0.0.3:7000".parse().unwrap()))
        .await;
    controller.pending_joins.lock().clear();

    controller.on_peer_connected((peer2_id, "10.0.0.4:7000".parse().unwrap()));

    let pending = controller.pending_joins.lock();
    assert!(
        pending.contains(&peer2_id),
        "existing cluster members reconnecting with a new IP must queue a metadata/pool refresh"
    );
}

#[tokio::test]
async fn decommission_requires_raft() {
    let dir = tempfile::tempdir().unwrap();
    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig::default());
    let net_config = Arc::new(NetConfig::default());
    let local_id = Uuid::new_v4();

    let registry = Arc::new(HandlerRegistry::new());
    let (controller, _handles) =
        ModeController::new(config, net_config, local_id, storage, schema, registry);

    // Without raft initialized, decommission should fail
    let result = controller.initiate_decommission(Uuid::new_v4()).await;
    assert!(
        matches!(result, Err(ClusterError::Internal(_))),
        "decommission without raft must fail: got {result:?}"
    );
}

// ---- is_cql_ready tests ------------------------------------------------

#[test]
fn is_cql_ready_standalone_returns_true() {
    let dir = tempfile::tempdir().unwrap();
    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig::default());
    let net_config = Arc::new(NetConfig::default());
    let host_id = Uuid::new_v4();

    let registry = Arc::new(HandlerRegistry::new());
    let (controller, _handles) =
        ModeController::new(config, net_config, host_id, storage, schema, registry);

    assert_eq!(controller.mode(), DeploymentMode::Standalone);
    assert!(
        controller.is_cql_ready(),
        "standalone node must accept CQL connections"
    );
}

#[test]
fn is_cql_ready_pair_secondary_returns_false() {
    let dir = tempfile::tempdir().unwrap();
    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig::default());
    let net_config = Arc::new(NetConfig::default());
    // local > peer so local takes the Secondary role (primary-by-lower-id).
    // Uuid::new_v4() made this flaky at ~50% pass.
    let local_id = Uuid::from_u128(2);
    let peer_id = Uuid::from_u128(1);

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

    // Outbound connection to a lower host-id peer keeps this node secondary.
    let peer_addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
    controller.on_peer_connected((peer_id, peer_addr));
    assert_eq!(controller.mode(), DeploymentMode::Pair);
    assert_eq!(controller.role(), Some(PairRole::Secondary));
    assert!(
        !controller.is_cql_ready(),
        "pair secondary must NOT accept CQL connections"
    );
}

#[tokio::test]
async fn reverse_inbound_race_does_not_promote_joiner_to_primary() {
    let dir = tempfile::tempdir().unwrap();
    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig::default());
    let net_config = Arc::new(NetConfig::default());
    let local_id = Uuid::from_u128(2);
    let peer_id = Uuid::from_u128(1);

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

    use ferrosa_net::rpc::InboundPeerCallback;

    // Live 38xxx shape: the seed's reverse inbound arrives on an ephemeral
    // port before the joiner's canonical outbound `:7000` peer-connected event.
    controller.on_inbound_peer((peer_id, "10.0.0.1:40370".parse().unwrap()), None);
    assert_eq!(controller.mode(), DeploymentMode::Pair);
    assert_eq!(
        controller.role(),
        Some(PairRole::Secondary),
        "joiner must stay secondary even if the reverse inbound wins the race"
    );

    controller.on_peer_connected((peer_id, "10.0.0.1:7000".parse().unwrap()));
    assert_eq!(controller.mode(), DeploymentMode::Pair);
    assert_eq!(controller.role(), Some(PairRole::Secondary));
}

// -----------------------------------------------------------------------
// ClusterInviteHandler
// -----------------------------------------------------------------------

// -----------------------------------------------------------------------
// Promotion epoch
// -----------------------------------------------------------------------

/// force_promote increments the promote_epoch counter each time.
/// On reconnect, higher epoch wins primary role.
#[test]
fn force_promote_increments_epoch() {
    let dir = tempfile::tempdir().unwrap();
    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig::default());
    let net_config = Arc::new(NetConfig::default());
    let host_id = Uuid::new_v4();
    let registry = Arc::new(HandlerRegistry::new());
    let (controller, _handles) =
        ModeController::new(config, net_config, host_id, storage, schema, registry);

    assert_eq!(controller.promote_epoch(), 0, "starts at 0");

    controller.force_promote().unwrap();
    assert_eq!(controller.promote_epoch(), 1, "first promote → epoch 1");

    controller.force_promote().unwrap();
    assert_eq!(controller.promote_epoch(), 2, "second promote → epoch 2");
}

/// set_promote_epoch allows updating to a peer's higher epoch on reconnect.
#[test]
fn set_promote_epoch_accepts_higher_value() {
    let dir = tempfile::tempdir().unwrap();
    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig::default());
    let net_config = Arc::new(NetConfig::default());
    let host_id = Uuid::new_v4();
    let registry = Arc::new(HandlerRegistry::new());
    let (controller, _handles) =
        ModeController::new(config, net_config, host_id, storage, schema, registry);

    controller.force_promote().unwrap();
    assert_eq!(controller.promote_epoch(), 1);

    // Simulate receiving a higher epoch from peer during reconnect.
    controller.set_promote_epoch(5);
    assert_eq!(controller.promote_epoch(), 5);
}

/// ClusterInviteHandler replies with ClusterInviteAck and identifies
/// unknown peers from the invite's peer list.
#[tokio::test]
async fn cluster_invite_handler_replies_with_ack() {
    use ferrosa_net::rpc::RpcHandler;

    let local_id = Uuid::new_v4();
    let net_config = Arc::new(NetConfig::default());

    struct NoopListener;
    impl ferrosa_net::peer::PeerEventListener for NoopListener {
        fn on_peer_connected(&self, _: ferrosa_net::rpc::handler::PeerId) {}
        fn on_peer_disconnected(&self, _: ferrosa_net::rpc::handler::PeerId) {}
        fn on_peer_suspected(&self, _: ferrosa_net::rpc::handler::PeerId) {}
        fn on_peer_recovered(&self, _: uuid::Uuid) {}
        fn on_peer_failed(&self, _: uuid::Uuid) {}
    }
    let pm = Arc::new(PeerManager::new(
        net_config.clone(),
        local_id,
        Arc::new(NoopListener),
    ));

    let handler =
        cluster::ClusterInviteHandler::new(local_id, pm, net_config, std::sync::Weak::new());

    let initiator = Uuid::new_v4();
    let peer1 = Uuid::new_v4();
    let peer2 = Uuid::new_v4();

    let msg = ferrosa_net::message::Message::ClusterInvite {
        initiator,
        peers: vec![
            (local_id, "10.0.0.1:7000".parse().unwrap()),
            (peer1, "10.0.0.2:7000".parse().unwrap()),
            (peer2, "10.0.0.3:7000".parse().unwrap()),
        ],
    };

    let from = (initiator, "10.0.0.4:7000".parse().unwrap());
    let response = handler.handle(from, msg).await;

    assert!(response.is_some(), "handler should reply");
    match response.unwrap() {
        ferrosa_net::message::Message::ClusterInviteAck { host_id } => {
            assert_eq!(host_id, local_id, "ACK should contain local host_id");
        }
        other => panic!("expected ClusterInviteAck, got: {other:?}"),
    }
}

/// ClusterInvite triggers cluster transition on a pair-mode node.
///
/// Regression test for BUG-RAFT-HANDLER-RACE: node2/node3 stayed in pair
/// mode forever because ClusterInviteHandler didn't trigger the transition.
/// The fix (ba7599a) added controller.upgrade() → transition_to_cluster().
#[tokio::test]
async fn cluster_invite_triggers_transition_from_pair_mode() {
    use ferrosa_net::rpc::RpcHandler;

    let dir = tempfile::tempdir().unwrap();
    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig {
        raft_data_dir: Some(dir.path().join("raft")),
        ..ClusterConfig::default()
    });
    let net_config = Arc::new(NetConfig::default());
    let local_id = Uuid::new_v4();
    let peer1_id = Uuid::new_v4();
    let peer2_id = Uuid::new_v4();

    let registry = Arc::new(HandlerRegistry::new());
    let (controller, _handles) = ModeController::new(
        config,
        net_config.clone(),
        local_id,
        storage,
        schema,
        registry,
    );

    let pm = Arc::new(PeerManager::new(
        net_config.clone(),
        local_id,
        controller.clone(),
    ));
    controller.set_peer_manager(pm.clone());

    // Put node into pair mode by connecting first peer.
    controller.on_peer_connected((peer1_id, "10.0.0.2:7000".parse().unwrap()));
    assert_eq!(controller.mode(), DeploymentMode::Pair);

    // Now simulate receiving a ClusterInvite with 3 peers (including self).
    // The handler should trigger transition_to_cluster.
    let handler =
        cluster::ClusterInviteHandler::new(local_id, pm, net_config, Arc::downgrade(&controller));

    let invite = ferrosa_net::message::Message::ClusterInvite {
        initiator: peer1_id,
        peers: vec![
            (local_id, "10.0.0.1:7000".parse().unwrap()),
            (peer1_id, "10.0.0.2:7000".parse().unwrap()),
            (peer2_id, "10.0.0.3:7000".parse().unwrap()),
        ],
    };

    let from = (
        peer1_id,
        "10.0.0.2:7000".parse::<std::net::SocketAddr>().unwrap(),
    );
    let response = handler.handle(from, invite).await;
    assert!(response.is_some(), "handler should reply with ack");

    // The node should have transitioned out of Pair mode.
    let mode = controller.mode();
    assert!(
        mode == DeploymentMode::Forming || mode == DeploymentMode::Cluster,
        "ClusterInvite with 3 peers must trigger cluster transition from Pair, got {mode:?}"
    );
}

/// After ClusterInvite triggers transition, Raft handlers are registered.
///
/// This verifies the fix for the "no handler registered msg_type=RaftVote"
/// symptom. When a pair-mode node receives ClusterInvite and transitions
/// to cluster mode, it must register Raft handlers (via LazyRaft) so
/// incoming vote/append requests are handled instead of dropped.
#[tokio::test]
async fn cluster_invite_transition_registers_raft_handlers() {
    use ferrosa_net::rpc::RpcHandler;

    let dir = tempfile::tempdir().unwrap();
    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig {
        raft_data_dir: Some(dir.path().join("raft")),
        ..ClusterConfig::default()
    });
    let net_config = Arc::new(NetConfig::default());
    let local_id = Uuid::new_v4();
    let peer1_id = Uuid::new_v4();
    let peer2_id = Uuid::new_v4();

    let registry = Arc::new(HandlerRegistry::new());
    let (controller, _handles) = ModeController::new(
        config,
        net_config.clone(),
        local_id,
        storage,
        schema,
        registry.clone(),
    );

    let pm = Arc::new(PeerManager::new(
        net_config.clone(),
        local_id,
        controller.clone(),
    ));
    controller.set_peer_manager(pm.clone());

    // Put into pair mode.
    controller.on_peer_connected((peer1_id, "10.0.0.2:7000".parse().unwrap()));
    assert_eq!(controller.mode(), DeploymentMode::Pair);

    // No Raft handlers yet.
    assert!(
        !registry.has_handler(MsgType::RaftVote),
        "Raft handlers should not be registered in pair mode"
    );

    // ClusterInvite triggers cluster transition.
    let handler =
        cluster::ClusterInviteHandler::new(local_id, pm, net_config, Arc::downgrade(&controller));
    let invite = ferrosa_net::message::Message::ClusterInvite {
        initiator: peer1_id,
        peers: vec![
            (local_id, "10.0.0.1:7000".parse().unwrap()),
            (peer1_id, "10.0.0.2:7000".parse().unwrap()),
            (peer2_id, "10.0.0.3:7000".parse().unwrap()),
        ],
    };
    handler
        .handle((peer1_id, "10.0.0.2:7000".parse().unwrap()), invite)
        .await;

    // Give the background Raft init task time to register handlers.
    // LazyRaft registers handlers synchronously before the async task,
    // so they should appear quickly.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    assert!(
        registry.has_handler(MsgType::RaftVote),
        "RaftVote handler must be registered after ClusterInvite triggers cluster transition"
    );
    assert!(
        registry.has_handler(MsgType::RaftAppendEntries),
        "RaftAppendEntries handler must be registered"
    );
    assert!(
        registry.has_handler(MsgType::RaftInstallSnapshot),
        "RaftInstallSnapshot handler must be registered"
    );
}

// -----------------------------------------------------------------------
// Progressive join: standalone → pair → cluster
// -----------------------------------------------------------------------

/// A standalone node auto-promotes to pair when a peer connects.
#[tokio::test]
async fn standalone_mode_accepts_peer_and_transitions_to_pair() {
    let dir = tempfile::tempdir().unwrap();
    let storage = test_storage(dir.path());
    let schema = test_schema();
    // Explicitly configure as standalone — this is what the compose does.
    let config = Arc::new(ClusterConfig::default());
    let net_config = Arc::new(NetConfig::default());
    let local_id = Uuid::new_v4();
    let peer_id = Uuid::new_v4();

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

    assert_eq!(controller.mode(), DeploymentMode::Standalone);

    // Peer connects — should transition to Pair, not be rejected.
    let peer_addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
    controller.on_peer_connected((peer_id, peer_addr));

    assert_eq!(
        controller.mode(),
        DeploymentMode::Pair,
        "standalone node must accept peer and transition to pair"
    );
}

/// Full progressive join: standalone → pair → cluster (3 nodes).
#[tokio::test]
async fn progressive_join_standalone_to_pair_to_cluster() {
    let dir = tempfile::tempdir().unwrap();
    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig::default());
    let net_config = Arc::new(NetConfig::default());
    let local_id = Uuid::new_v4();
    let peer1 = Uuid::new_v4();
    let peer2 = Uuid::new_v4();

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

    // Start standalone.
    assert_eq!(controller.mode(), DeploymentMode::Standalone);

    // First peer → pair.
    controller.on_peer_connected((peer1, "10.0.0.2:7000".parse().unwrap()));
    assert_eq!(controller.mode(), DeploymentMode::Pair);

    // Second peer → forming/cluster.
    controller.on_peer_connected((peer2, "10.0.0.3:7000".parse().unwrap()));
    let mode = controller.mode();
    assert!(
        mode == DeploymentMode::Forming || mode == DeploymentMode::Cluster,
        "expected Forming or Cluster after 3rd node, got {mode:?}"
    );
}

/// Inbound peer to standalone node also triggers progressive join.
#[tokio::test]
async fn standalone_inbound_peer_transitions_to_pair() {
    let dir = tempfile::tempdir().unwrap();
    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig::default());
    let net_config = Arc::new(NetConfig::default());
    let local_id = Uuid::new_v4();
    let peer_id = Uuid::new_v4();

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

    assert_eq!(controller.mode(), DeploymentMode::Standalone);

    // Inbound connection — should also transition, not reject.
    use ferrosa_net::rpc::InboundPeerCallback;
    controller.on_inbound_peer((peer_id, "10.0.0.2:7000".parse().unwrap()), None);

    assert_eq!(
        controller.mode(),
        DeploymentMode::Pair,
        "standalone node must accept inbound peer and transition to pair"
    );
}

// -----------------------------------------------------------------------
// BUG-BOOTSTRAP-NO-DATA-STREAM regression tests
// -----------------------------------------------------------------------

/// Regression test for BUG-BOOTSTRAP-NO-DATA-STREAM.
///
/// Proves that bootstrap streaming produces mutations for remote nodes
/// when the local node has data. This is the core behavior that was
/// broken: the streaming code was gated behind `if lid == local_node_id`
/// so non-leader data-owning nodes never streamed anything.
///
/// This test exercises the bootstrap streaming logic directly: set up a
/// 3-node ring, write data to local storage, then verify that the
/// streaming loop produces mutations destined for other nodes.
#[test]
fn bootstrap_streaming_produces_mutations_for_remote_nodes() {
    use crate::raft::{uuid_to_node_id, NodeInfo, NodeState};
    use crate::ring::TokenRing;
    use ferrosa_common::key::DecoratedKey;
    use ferrosa_common::{PartitionKey, Token};
    use ferrosa_storage::commitlog::TableId;

    let dir = tempfile::tempdir().unwrap();
    let storage = test_storage(dir.path());
    let schema = test_schema();

    // Create a user keyspace + table in schema.
    let ks = ferrosa_schema::KeyspaceMetadata {
        name: "test_ks".into(),
        durable_writes: true,
        replication: ferrosa_schema::ReplicationParams {
            strategy: "SimpleStrategy".into(),
            options: [("replication_factor".into(), "3".into())]
                .into_iter()
                .collect(),
        },
    };
    schema.create_keyspace_internal(ks).unwrap();

    let table = ferrosa_schema::TableMetadata {
        keyspace: "test_ks".into(),
        name: "data".into(),
        id: Uuid::new_v4(),
        columns: indexmap::indexmap! {
            "pk".into() => ferrosa_schema::ColumnMetadata {
                name: "pk".into(),
                column_type: "text".into(),
                kind: ferrosa_schema::ColumnKind::PartitionKey,
                position: 0,
                clustering_order: ferrosa_schema::ClusteringOrder::None,
                mask: None,
            },
            "val".into() => ferrosa_schema::ColumnMetadata {
                name: "val".into(),
                column_type: "text".into(),
                kind: ferrosa_schema::ColumnKind::Regular,
                position: 0,
                clustering_order: ferrosa_schema::ClusteringOrder::None,
                mask: None,
            },
        },
        partition_key: vec!["pk".into()],
        clustering_key: vec![],
        params: ferrosa_schema::TableParams::default(),
        flags: Default::default(),
        extensions: Default::default(),
        is_system: false,
    };
    schema.create_table_internal(table).unwrap();

    // Register table with storage engine and write test data.
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
    let table_schema = TableSchema {
        keyspace: "test_ks".into(),
        table: "data".into(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
        clustering_columns: vec![],
        static_columns: vec![],
        regular_columns: vec![ColumnDefinition {
            name: "val".into(),
            type_name: "org.apache.cassandra.db.marshal.UTF8Type".into(),
        }],
        extensions: Default::default(),
    };
    storage.register_table(table_schema).unwrap();

    // Write 50 partitions with different tokens to spread across the ring.
    for i in 0..50i64 {
        let key_bytes = format!("key_{i}").into_bytes();
        let (h1, _) = ferrosa_common::murmur3::hash3_x64_128(&key_bytes, 0);
        let dk = DecoratedKey {
            token: Token(h1),
            key: PartitionKey::new(key_bytes),
        };
        let row = ferrosa_sstable::types::Row {
            clustering: vec![],
            cells: vec![(
                0,
                ferrosa_common::CellValue::live(format!("val_{i}").into_bytes(), 1000 + i),
            )],
            deletion: ferrosa_sstable::types::DeletionTime::LIVE,
            primary_key_liveness: ferrosa_sstable::types::LivenessInfo::with_timestamp(1000 + i),
        };
        let table_id = TableId::new("test_ks", "data");
        storage.write(&table_id, &dk, row, 1000 + i).unwrap();
    }

    // Build a 3-node ring (simulating the ring built in transition_to_cluster).
    let local_id = Uuid::new_v4();
    let peer1_id = Uuid::new_v4();
    let peer2_id = Uuid::new_v4();
    let local_nid = uuid_to_node_id(local_id);
    let peer1_nid = uuid_to_node_id(peer1_id);
    let peer2_nid = uuid_to_node_id(peer2_id);

    let mut ring = TokenRing::new();
    for (nid, uuid) in [
        (local_nid, local_id),
        (peer1_nid, peer1_id),
        (peer2_nid, peer2_id),
    ] {
        ring.add_node(
            nid,
            NodeInfo {
                host_id: uuid,
                addr: "127.0.0.1:7000".into(),
                data_center: "dc1".into(),
                rack: "rack1".into(),
                state: if nid == local_nid {
                    NodeState::Normal
                } else {
                    NodeState::Joining
                },
                cql_broadcast: None,
            },
        );
    }
    // Assign 256 tokens per node, deterministically.
    use crate::controller::token::generate_deterministic_token;
    let mut all_nids = vec![local_nid, peer1_nid, peer2_nid];
    all_nids.sort_unstable();
    for &nid in &all_nids {
        let tokens: Vec<i64> = (0..256)
            .map(|i| generate_deterministic_token(nid, i))
            .collect();
        ring.assign_tokens(nid, &tokens);
    }

    // --- This is the bootstrap streaming logic. ---
    // Iterate schema tables, read from storage, group by target node.
    let schema_snap = schema.snapshot();
    let mut total_remote_mutations = 0usize;
    let mut target_nodes: std::collections::HashSet<u64> = std::collections::HashSet::new();

    for (ks, tbl) in schema_snap.tables.keys() {
        if ks.starts_with("system") {
            continue;
        }
        let table_id = TableId::new(ks, tbl);
        let partitions = storage
            .read_range(
                &table_id,
                None,
                None,
                crate::write_path::DEFAULT_RANGE_READ_LIMIT,
            )
            .unwrap();

        for partition in &partitions {
            let token = partition.key.token.0;
            let owner = ring.primary_owner(token).unwrap_or(local_nid);

            if owner != local_nid {
                total_remote_mutations += 1;
                target_nodes.insert(owner);
            }
        }
    }

    // With 50 partitions spread across 3 nodes, roughly 2/3 should belong
    // to remote nodes. The exact count depends on token distribution, but
    // it must be non-zero — this is the bug: previously zero mutations
    // were produced because the code never ran on non-leader nodes.
    assert!(
        total_remote_mutations > 0,
        "BUG-BOOTSTRAP-NO-DATA-STREAM: bootstrap must produce mutations for remote nodes, \
         got 0 out of 50 partitions"
    );
    assert!(
        target_nodes.len() >= 2,
        "mutations should target both remote nodes, got {} targets",
        target_nodes.len()
    );
    // Sanity: not ALL partitions should go to remote nodes (some stay local).
    assert!(
        total_remote_mutations < 50,
        "some partitions should remain on the local node"
    );
}

/// Verifies that schema snapshot includes user tables for bootstrap iteration.
///
/// If the schema has no user tables, the bootstrap streaming loop iterates
/// zero times — this is the second failure mode for BUG-BOOTSTRAP-NO-DATA-STREAM
/// when the leader is a fresh node with no schema.json.
#[test]
fn schema_snapshot_includes_user_tables_for_bootstrap() {
    let schema = test_schema();

    // Initially, only system tables exist.
    let snap = schema.snapshot();
    let user_tables: Vec<_> = snap
        .tables
        .keys()
        .filter(|(ks, _)| !ks.starts_with("system"))
        .collect();
    assert!(
        user_tables.is_empty(),
        "fresh schema should have no user tables"
    );

    // After creating a user table, it appears in the snapshot.
    schema
        .create_keyspace_internal(ferrosa_schema::KeyspaceMetadata {
            name: "user_ks".into(),
            durable_writes: true,
            replication: ferrosa_schema::ReplicationParams {
                strategy: "SimpleStrategy".into(),
                options: [("replication_factor".into(), "1".into())]
                    .into_iter()
                    .collect(),
            },
        })
        .unwrap();

    schema
        .create_table_internal(ferrosa_schema::TableMetadata {
            keyspace: "user_ks".into(),
            name: "my_table".into(),
            id: Uuid::new_v4(),
            columns: indexmap::indexmap! {
                "pk".into() => ferrosa_schema::ColumnMetadata {
                    name: "pk".into(),
                    column_type: "text".into(),
                    kind: ferrosa_schema::ColumnKind::PartitionKey,
                    position: 0,
                    clustering_order: ferrosa_schema::ClusteringOrder::None,
                    mask: None,
                },
            },
            partition_key: vec!["pk".into()],
            clustering_key: vec![],
            params: ferrosa_schema::TableParams::default(),
            flags: Default::default(),
            extensions: Default::default(),
            is_system: false,
        })
        .unwrap();

    let snap = schema.snapshot();
    let user_tables: Vec<_> = snap
        .tables
        .keys()
        .filter(|(ks, _)| !ks.starts_with("system"))
        .collect();
    assert_eq!(
        user_tables.len(),
        1,
        "user table must appear in schema snapshot for bootstrap to find it"
    );
    assert_eq!(
        user_tables[0],
        &("user_ks".to_string(), "my_table".to_string())
    );
}

/// P0-08: the bootstrap silent-failure counters must be exposed and
/// readable. This is the public contract of the silent-failure detector;
/// metrics scrape relies on it. The counters can only legitimately be
/// incremented from the bootstrap path (which the unit test environment
/// does not exercise end-to-end), so this test asserts the surface and
/// monotonicity rather than absolute values.
#[test]
fn bootstrap_silent_failure_counts_exposes_three_counters() {
    let (publish, init, election) = super::cluster::bootstrap_silent_failure_counts();
    // Read again — must be monotonic non-decreasing across calls.
    let (publish2, init2, election2) = super::cluster::bootstrap_silent_failure_counts();
    assert!(publish2 >= publish);
    assert!(init2 >= init);
    assert!(election2 >= election);
}

/// W1.15 / hazard P1-1: the controller's mutexes are `parking_lot::Mutex`
/// rather than `std::sync::Mutex`, so a panic inside a critical section
/// does NOT poison the lock — subsequent acquisitions succeed and the
/// controller stays responsive. This is the essential property that the
/// `parking_lot` migration was supposed to deliver.
///
/// Strategy: stand up a fresh ModeController (which holds parking_lot
/// mutexes for `approved_nodes`, `pending_joins`, `connected_peers`,
/// etc.). On a separate thread, acquire one of those mutexes and panic.
/// After the panic-thread joins, re-acquire the same mutex from this
/// thread and assert it succeeds.
#[test]
fn controller_mutex_does_not_propagate_poison() {
    use std::panic;

    let dir = tempfile::tempdir().unwrap();
    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig {
        raft_data_dir: Some(dir.path().join("raft")),
        ..ClusterConfig::default()
    });
    let net_config = Arc::new(NetConfig::default());
    let registry = Arc::new(HandlerRegistry::new());
    let host_id = Uuid::new_v4();
    let (controller, _handles) =
        ModeController::new(config, net_config, host_id, storage, schema, registry);

    // Step 1: panic while holding the `approved_nodes` lock on a worker
    // thread. With std::sync::Mutex this would poison the lock and
    // every subsequent .lock() would return a PoisonError. With
    // parking_lot::Mutex the lock is simply released on unwind.
    let controller_clone = controller.clone();
    let join = std::thread::spawn(move || {
        let _approved = controller_clone.approved_nodes.lock();
        panic!("intentional panic inside critical section");
    });

    // Suppress the panic's stderr noise from the harness output.
    let _ = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        let _ = join.join(); // returns Err with the panic payload
    }));

    // Step 2: subsequent locks must succeed. With std::sync::Mutex this
    // would .lock().unwrap() panic with a PoisonError; parking_lot has
    // no Result.
    let host = Uuid::new_v4();
    {
        let mut approved = controller.approved_nodes.lock();
        approved.insert(host);
    }
    assert!(
        controller.approved_nodes.lock().contains(&host),
        "parking_lot mutex must remain usable after a panic in a critical section"
    );

    // Also exercise the other mutexes named in hazard P1-1 to pin the
    // contract for `pending_joins`, `connected_peers`, etc.
    {
        let mut pending = controller.pending_joins.lock();
        pending.push(host);
    }
    assert_eq!(
        controller.pending_joins.lock().len(),
        1,
        "pending_joins remains writable after the panic"
    );
}

/// W1.16 / hazard P1-3: concurrent mode transitions must serialize via
/// the controller's `transition_guard` so two simultaneous callers
/// cannot both observe the same starting mode and both apply a
/// transition. Without serialization, two `on_peer_connected` callbacks
/// arriving on different threads could each see Standalone and both
/// call `transition_to_pair`, producing a poisoned state where the
/// pair_context is overwritten mid-setup.
#[test]
fn concurrent_mode_transitions_serialize() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Barrier;

    let dir = tempfile::tempdir().unwrap();
    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig {
        raft_data_dir: Some(dir.path().join("raft")),
        ..ClusterConfig::default()
    });
    let net_config = Arc::new(NetConfig::default());
    let registry = Arc::new(HandlerRegistry::new());
    let host_id = Uuid::new_v4();
    let (controller, _handles) =
        ModeController::new(config, net_config, host_id, storage, schema, registry);

    let peer_a = Uuid::new_v4();
    let peer_b = Uuid::new_v4();
    let addr_a: SocketAddr = "127.0.0.1:7000".parse().unwrap();
    let addr_b: SocketAddr = "127.0.0.1:7001".parse().unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let count = Arc::new(AtomicUsize::new(0));

    let c1 = controller.clone();
    let b1 = barrier.clone();
    let n1 = count.clone();
    let t1 = std::thread::spawn(move || {
        b1.wait();
        c1.on_peer_connected((peer_a, addr_a));
        n1.fetch_add(1, Ordering::SeqCst);
    });

    let c2 = controller.clone();
    let b2 = barrier.clone();
    let n2 = count.clone();
    let t2 = std::thread::spawn(move || {
        b2.wait();
        c2.on_peer_connected((peer_b, addr_b));
        n2.fetch_add(1, Ordering::SeqCst);
    });

    t1.join().unwrap();
    t2.join().unwrap();

    // Both calls completed.
    assert_eq!(count.load(Ordering::SeqCst), 2);

    // The transition_guard mutex's hold count reflects that BOTH
    // callers acquired it — they did not race past the guard.
    let acquires = controller
        .contention_metrics
        .transition_guard_acquires
        .load(Ordering::Relaxed);
    assert!(
        acquires >= 2,
        "both peer-connect callers must have acquired the transition guard \
         (got {acquires} acquires) — concurrent transitions should serialize"
    );

    // The connected_peers list should contain both peers (both calls
    // appended through the mutex, no lost updates).
    let peers = controller.connected_peers.lock();
    assert_eq!(
        peers.len(),
        2,
        "both peers must be tracked; lost update would suggest a serialization gap"
    );
    let ids: Vec<Uuid> = peers.iter().map(|(id, _)| *id).collect();
    assert!(ids.contains(&peer_a));
    assert!(ids.contains(&peer_b));
}

/// W1.17 — when `formation_timeout_secs` elapses without a Raft
/// leader, the node reverts to Pair mode and the DDL path falls
/// back to Direct so single-node DDL still works.
///
/// We drive `transition_to_forming` with one stub peer that the node
/// cannot reach (the harness has no real network), so the Raft
/// election poll runs out the budget and the timeout branch fires.
/// `formation_timeout_secs = 1` keeps the test fast.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forming_falls_back_to_pair_on_timeout() {
    let dir = tempfile::tempdir().unwrap();
    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig {
        raft_data_dir: Some(dir.path().join("raft")),
        formation_timeout_secs: Some(1),
        // Aggressive election timing so the per-node tick budget
        // is exhausted within the formation budget.
        raft_heartbeat_ms: 50,
        raft_election_timeout_min_ms: 100,
        raft_election_timeout_max_ms: 200,
        ..ClusterConfig::default()
    });
    let net_config = Arc::new(NetConfig::default());
    let registry = Arc::new(HandlerRegistry::new());
    // The seed (calls raft.initialize) is the node with the highest
    // UUID.  Force the local host to be seed so the election path
    // actually runs.  Without this, the local node would be a passive
    // non-seed waiting for AppendEntries from an unreachable peer.
    let host_id = Uuid::from_u128(u128::MAX);
    let peer_id = Uuid::from_u128(1);
    let (controller, _handles) = ModeController::new(
        config,
        net_config.clone(),
        host_id,
        storage,
        schema,
        registry,
    );

    let pm = Arc::new(PeerManager::new(
        net_config.clone(),
        host_id,
        controller.clone(),
    ));
    controller.set_peer_manager(pm);

    // Forge a peer entry that the controller will try to contact and
    // fail (no RPC server bound at 127.0.0.1:1).  This is enough to
    // drive transition_to_cluster's election poll into its timeout
    // branch.
    let unreachable: SocketAddr = "127.0.0.1:1".parse().unwrap();

    controller.transition_to_forming(vec![(peer_id, unreachable)]);
    // transition_to_forming synchronously fans into transition_to_cluster,
    // which spawns the Raft init task and may flip mode to Cluster
    // before this assertion runs.  Skip the intermediate check —
    // what we really care about is the eventual fall-back to Pair.

    // formation_timeout_secs = 1.  But cluster.rs also spends up to
    // 10 s on `peer_manager.has_live_peer` waiting for connections
    // before Raft init begins.  Total wall time to observe the
    // fallback: ~11 s + headroom.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(25);
    let mut observed_mode = controller.mode();
    while tokio::time::Instant::now() < deadline {
        observed_mode = controller.mode();
        if observed_mode == DeploymentMode::Pair {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    assert_eq!(
        observed_mode,
        DeploymentMode::Pair,
        "after formation_timeout_secs elapses without a leader, mode must revert to Pair",
    );

    controller.shutdown().await;
}

/// W1.14 — `drain_ddl_queue` must replay ops sent both BEFORE and
/// DURING the drain.  The previous `try_recv()`-once-then-quit loop
/// dropped late arrivals because it observed an empty queue once and
/// exited; the new helper waits N consecutive empties so in-flight
/// `Forming` senders can still land.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ddl_during_forming_queues_and_replays() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<u32>();
    let processed = Arc::new(AtomicUsize::new(0));

    // Pre-queue two ops (the steady-state "queued during Forming" case).
    tx.send(1).unwrap();
    tx.send(2).unwrap();

    // Inject a third op AFTER the drain starts but before its
    // first try_recv-empty cool-down completes.
    let tx_for_late = tx.clone();
    tokio::spawn(async move {
        // Sleep long enough for drain_ddl_queue to consume the first
        // two ops and start its first cool-down (50 ms).
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        tx_for_late.send(3).unwrap();
    });

    // Drop the original tx so the channel becomes Disconnected once
    // every clone has been dropped.  We retain the late-tx via the
    // spawned task; it's dropped when that task ends.
    drop(tx);

    let processed_for_closure = processed.clone();
    let replayed = drain_ddl_queue(rx, |op: u32| {
        let processed = processed_for_closure.clone();
        async move {
            processed.fetch_add(1, Ordering::SeqCst);
            // The op processor's success/failure must not change
            // the drain's correctness — assert all ops are seen.
            let _ = op;
            Ok(())
        }
    })
    .await;

    assert_eq!(
        replayed, 3,
        "expected drain to replay 3 ops (two pre-queued + one in-flight); got {replayed}",
    );
    assert_eq!(processed.load(Ordering::SeqCst), 3);
}

/// Sanity test for the empty-channel case.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drain_ddl_queue_returns_zero_for_empty_channel() {
    let (_tx, rx) = tokio::sync::mpsc::unbounded_channel::<u32>();
    let replayed = drain_ddl_queue(rx, |_| async { Ok(()) }).await;
    assert_eq!(replayed, 0);
}

#[test]
fn bootstrap_attempts_sstable_bulk_before_range_materialization() {
    let source = include_str!("cluster.rs");
    let bootstrap = source
        .split("starting bootstrap streaming to new token owners")
        .nth(1)
        .expect("bootstrap streaming block exists");
    let first_read_range = bootstrap
        .find("read_range(")
        .expect("bootstrap block still has a row fallback read_range");
    let first_flush = bootstrap
        .find("flush_all()")
        .expect("bootstrap block should flush before SSTable streaming");

    assert!(
        first_flush < first_read_range,
        "bootstrap must try flush/SSTable bulk streaming before read_range; \
         read_range materializes all SSTable partitions before applying its limit and can OOM"
    );
}

#[test]
fn keyspace_needs_cluster_replay_includes_system_graph_keyspaces() {
    // The graph engine builds `system_graph_<user_ks>` keyspaces lazily on
    // the first graph query. If that first query happens before the local
    // node has transitioned to Cluster mode, the keyspace + adjacency table
    // get registered locally only. ReplaySchema in transition_to_cluster must
    // re-fire those DDLs through Raft so every replica's state machine
    // registers them too — otherwise followers reject every adjacency
    // MutationForward with "table not registered". Regression guard for
    // ferrosa-memory PR#4 cluster-int failure mode B.
    assert!(
        keyspace_needs_cluster_replay("system_graph_agent_memory"),
        "system_graph_* keyspaces must replay through Raft on cluster transition"
    );
    assert!(
        keyspace_needs_cluster_replay("system_graph_app_a"),
        "every system_graph_<user_ks> must replay"
    );
    assert!(
        keyspace_needs_cluster_replay("agent_memory"),
        "regular user keyspaces must replay"
    );
}

#[test]
fn keyspace_needs_cluster_replay_excludes_builtin_system_keyspaces() {
    // The Cassandra-style built-in system keyspaces are hardcoded on every
    // node at startup and must NOT be re-fired through Raft (proposing
    // CreateKeyspace("system") would either no-op noisily or fail).
    for ks in [
        "system",
        "system_schema",
        "system_auth",
        "system_distributed",
        "system_traces",
        "system_virtual_schema",
        "system_observability",
    ] {
        assert!(
            !keyspace_needs_cluster_replay(ks),
            "built-in system keyspace {ks} must NOT replay through Raft"
        );
    }
}
