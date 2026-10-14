//! The jsonb capability verdict must not stay STALE after a rolling upgrade.
//!
//! The defect this file reproduces: the only place a peer's capability bits
//! were kept was its outbound pool, snapshotted when WE dialled it. After a
//! rolling upgrade the peer restarts with a newer build and dials US (inbound)
//! — a fresh advertisement — but nothing recorded it, so
//! `PeerManager::peer_capabilities(peer)` kept reporting the OLD build's bits
//! and the jsonb DDL gate stayed refused even once every member was upgraded.
//!
//! These tests drive the REAL acceptance path: a real `RpcServer` whose
//! `InboundPeerCallback` is the production `ModeController`, fed by a real
//! internode handshake over loopback. Nothing here names a seam introduced by
//! the fix — the assertion is the same public `peer_capabilities` the jsonb
//! gate reads (`ferrosa-cluster/src/controller/jsonb_gate.rs`), so the tests
//! fail on the behaviour and not on a missing symbol.
//!
//! Harness-level note: the outbound half is a real `RpcServer` answering with
//! its configured `advertised_capabilities`, so the observer's pool snapshot is
//! genuinely taken over the wire, not conjured.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use uuid::Uuid;

use ferrosa_cluster::config::ClusterConfig;
use ferrosa_cluster::controller::{ModeController, ModeControllerHandles};
use ferrosa_net::codec::InternodeCodec;
use ferrosa_net::config::NetConfig;
use ferrosa_net::handshake::{initiate_handshake, CAP_JSONB_V1};
use ferrosa_net::peer::PeerManager;
use ferrosa_net::rpc::server::RpcServer;
use ferrosa_net::rpc::HandlerRegistry;
use ferrosa_storage::engine::{StorageEngine, StorageEngineConfig};
use ferrosa_storage::{CommitLogConfig, CompactionConfig};
use tokio_util::codec::Framed;

// ---------------------------------------------------------------------------
// fixtures
// ---------------------------------------------------------------------------

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
        cache_hot_window_secs: 900,
    };
    Arc::new(StorageEngine::new(config, None).unwrap())
}

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

fn test_net_config() -> NetConfig {
    NetConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        broadcast_addr: "127.0.0.1:0".parse().unwrap(),
        ..NetConfig::default()
    }
}

/// The observer under test: a real controller, its real peer manager, and a
/// real internode server whose inbound callback is the production controller.
struct Observer {
    peer_manager: Arc<PeerManager>,
    server: Arc<RpcServer>,
    addr: SocketAddr,
    _controller: Arc<ModeController>,
    _handles: ModeControllerHandles,
    _dir: tempfile::TempDir,
}

async fn start_observer() -> Observer {
    let dir = tempfile::tempdir().unwrap();
    let storage = test_storage(dir.path());
    let schema = test_schema();
    let config = Arc::new(ClusterConfig::default());
    let net_config = Arc::new(test_net_config());
    let host_id = Uuid::new_v4();
    let registry = Arc::new(HandlerRegistry::new());

    let (controller, handles) = ModeController::new(
        config,
        net_config.clone(),
        host_id,
        storage,
        schema,
        registry.clone(),
    );
    let peer_manager = Arc::new(PeerManager::with_weak_listener(
        net_config.clone(),
        host_id,
        controller.as_peer_listener(),
    ));
    controller.set_peer_manager(peer_manager.clone());

    let server = Arc::new(
        RpcServer::new((*net_config).clone(), host_id, registry)
            .with_inbound_callback(controller.clone()),
    );
    let addr = server.start_and_get_addr().await.unwrap();

    Observer {
        peer_manager,
        server,
        addr,
        _controller: controller,
        _handles: handles,
        _dir: dir,
    }
}

/// Start a peer's internode server advertising `advertised_capabilities`. This
/// is the peer's build as the observer learns it from an OUTBOUND dial (the
/// observer reads the bits out of this server's HandshakeAck).
async fn start_peer(host_id: Uuid, advertised_capabilities: u32) -> (Arc<RpcServer>, SocketAddr) {
    let config = NetConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised_capabilities,
        ..NetConfig::default()
    };
    let registry = Arc::new(HandlerRegistry::new());
    let server = Arc::new(RpcServer::new(config, host_id, registry));
    let addr = server.start_and_get_addr().await.unwrap();
    (server, addr)
}

/// The peer RESTARTS with `advertised_capabilities` and dials the observer.
/// Same host id (a restart, not a replacement), inbound — this is the handshake
/// whose advertisement was being dropped.
async fn peer_redials(observer_addr: SocketAddr, peer_id: Uuid, advertised_capabilities: u32) {
    let client_config = NetConfig {
        advertised_capabilities,
        ..NetConfig::default()
    };
    let stream = tokio::net::TcpStream::connect(observer_addr)
        .await
        .expect("peer dials the observer");
    let mut framed = Framed::new(
        stream,
        InternodeCodec::new(client_config.max_frame_body_size),
    );
    initiate_handshake(&mut framed, &client_config, peer_id)
        .await
        .expect("inbound handshake completes");
    // Keep the connection alive briefly: the observer records the advertisement
    // from the accepted connection, and dropping it immediately could close the
    // socket before the accept task has run the callback.
    tokio::time::sleep(Duration::from_millis(100)).await;
}

/// Bounded wait for the observer's verdict to become `Some(expected)`.
///
/// The advertisement is recorded by the accept task (asynchronously, like the
/// other inbound-handshake facts the callback stores), so the assertion is a
/// bounded poll on the system's own condition, never a fixed sleep.
async fn await_capabilities(peer_manager: &PeerManager, peer_id: Uuid, expected: u32) -> u32 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match peer_manager.peer_capabilities(peer_id).await {
            Some(caps) if caps == expected => return caps,
            other => {
                if tokio::time::Instant::now() > deadline {
                    panic!(
                        "peer_capabilities({peer_id}) = {other:?} after 5s; expected \
                         {expected:#x}. The observer never recorded the peer's inbound \
                         advertisement, so a rolling upgrade leaves the jsonb gate refused."
                    );
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// RED — the rolling-upgrade case
// ---------------------------------------------------------------------------

/// RED (taken before the fix, and it fails on the behaviour, not on a symbol):
/// the observer's pool snapshot for the peer is the OLD build's bits, and the
/// peer then dials the observer with the NEW build's bits. `peer_capabilities`
/// must report the newer advertisement — the whole point of the rolling
/// upgrade — but today nothing records the inbound advertisement, so it keeps
/// reporting the stale snapshot and the DDL gate stays refused.
#[tokio::test]
async fn a_peer_that_redials_with_a_newer_build_is_reported_with_its_new_capabilities() {
    let observer = start_observer().await;
    let peer_id = Uuid::new_v4();

    // The peer's OLD build: no capabilities at all.
    let (old_peer, old_addr) = start_peer(peer_id, 0).await;
    // The observer dials it (this is how the stale snapshot is taken).
    observer
        .peer_manager
        .ensure_peer(peer_id, &old_addr.to_string())
        .await
        .unwrap();
    assert_eq!(
        observer.peer_manager.peer_capabilities(peer_id).await,
        Some(0),
        "precondition: the observer's pool snapshot is the OLD build's bits"
    );

    // The peer restarts with the NEW build (same host id) and dials the
    // observer, advertising CAP_JSONB_V1.
    peer_redials(observer.addr, peer_id, CAP_JSONB_V1).await;

    assert_eq!(
        await_capabilities(&observer.peer_manager, peer_id, CAP_JSONB_V1).await,
        CAP_JSONB_V1,
        "the newer inbound advertisement must win over the stale pool snapshot"
    );

    observer.server.shutdown(Duration::from_millis(50)).await;
    old_peer.shutdown(Duration::from_millis(50)).await;
}

// ---------------------------------------------------------------------------
// MIRROR CONTROLS — a fix that ORs, or that clears the value, is not the fix
// ---------------------------------------------------------------------------

/// MIRROR CONTROL (fails before the fix as well, but in the fail-closed
/// direction): the pool snapshot HAS the bit, and the peer then dials in with a
/// build that does NOT. Latest-wins must report the peer as LACKING. A fix that
/// ORs the two would keep reporting capability here, which would let jsonb DDL
/// replicate to a peer that cannot decode it. This pins that the fix is
/// latest-wins, not a union.
#[tokio::test]
async fn a_peer_that_redials_without_the_bit_is_reported_as_lacking_it() {
    let observer = start_observer().await;
    let peer_id = Uuid::new_v4();

    let (peer, addr) = start_peer(peer_id, CAP_JSONB_V1).await;
    observer
        .peer_manager
        .ensure_peer(peer_id, &addr.to_string())
        .await
        .unwrap();
    assert_eq!(
        observer.peer_manager.peer_capabilities(peer_id).await,
        Some(CAP_JSONB_V1),
        "precondition: the pool snapshot advertises the bit"
    );

    // The peer comes back WITHOUT the bit (an older build / the bit withdrawn).
    peer_redials(observer.addr, peer_id, 0).await;

    assert_eq!(
        await_capabilities(&observer.peer_manager, peer_id, 0).await,
        0,
        "the newer advertisement (no bit) must win: a union would wrongly report capability"
    );

    observer.server.shutdown(Duration::from_millis(50)).await;
    peer.shutdown(Duration::from_millis(50)).await;
}

/// MIRROR CONTROL (regression guard — passes before and after the fix): with no
/// inbound advertisement at all, a peer whose pool snapshot LACKS the bit is
/// still reported as lacking. The fix must not clear or fabricate a value when
/// no newer advertisement exists.
#[tokio::test]
async fn a_peer_whose_pool_snapshot_lacks_the_bit_stays_lacking_without_a_newer_advertisement() {
    let observer = start_observer().await;
    let peer_id = Uuid::new_v4();

    let (peer, addr) = start_peer(peer_id, 0).await;
    observer
        .peer_manager
        .ensure_peer(peer_id, &addr.to_string())
        .await
        .unwrap();

    assert_eq!(
        observer.peer_manager.peer_capabilities(peer_id).await,
        Some(0),
        "an un-upgraded peer must still be seen as lacking, with no newer advertisement"
    );

    observer.server.shutdown(Duration::from_millis(50)).await;
    peer.shutdown(Duration::from_millis(50)).await;
}
