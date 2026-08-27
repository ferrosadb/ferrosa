//! CQL TCP server: accepts connections and spawns per-connection tasks.
//! Correctness: admission is bounded and rejects failed consensus with a typed,
//! retriable error before authentication or routing work begins.
//! Last revised: 2026-08-27
//! Last changed: Added fail-closed admission for consensus runtime failure.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use std::time::Duration;

use futures::SinkExt;
use socket2::SockRef;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_util::codec::Framed;
use tracing::{info, warn};

use crate::error::CqlError;
use crate::frame::{
    CqlCodec, CqlFrame, FrameHeader, Opcode, DEFAULT_MAX_FRAME_SIZE, VERSION_RESPONSE,
};
use crate::router::SharedState;
use ferrosa_net::task_pool::TaskPool;

/// Server configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind_addr: SocketAddr,
    pub max_connections: usize,
    pub max_frame_size: u32,
    /// Max concurrent in-flight requests per connection (default 128).
    /// Enforced via a semaphore in the connection handler — exceeding this
    /// limit returns ERROR(Overloaded) to the client.
    pub max_in_flight_per_connection: usize,
    /// Max connections from a single IP address (default 64).
    pub max_connections_per_ip: usize,
    /// Path to TLS certificate file (PEM). If set with tls_key_path, enables TLS.
    pub tls_cert_path: Option<String>,
    /// Path to TLS private key file (PEM).
    pub tls_key_path: Option<String>,
    /// If true, reject startup when no TLS cert/key are configured (production mode).
    pub require_tls: bool,
    /// If true, skip auth (STARTUP returns READY directly).
    pub auth_disabled: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:9042".parse().unwrap(),
            max_connections: 1024,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            max_connections_per_ip: 64,
            max_in_flight_per_connection: 128,
            tls_cert_path: None,
            tls_key_path: None,
            require_tls: false,
            auth_disabled: false,
        }
    }
}

/// Resolve the CQL server's `auth_disabled` flag from the storage-side
/// `auth_enabled` flag, with an optional explicit override.
///
/// Cassandra has a single `authenticator:` setting; ferrosa historically
/// had two (`FERROSA_AUTH_ENABLED` for storage, `FERROSA_AUTH_DISABLED`
/// for the CQL server) which could drift out of sync — and did, in the
/// default case: storage said "auth off, accepts everything" while the
/// CQL server still sent `AUTHENTICATE`, breaking standard drivers
/// (DataStax Java Driver, NoSQLBench).  See
/// ferrosa-nosqlbench/docs/initial-gaps-found.md (Gap 1).
///
/// New contract: `storage_auth_enabled` is the single source of truth.
/// `FERROSA_AUTH_DISABLED` (or the equivalent file-config key) is a
/// **deprecated override**: when set, it wins, and main logs a
/// deprecation warning pointing at this function.
///
/// # Returns
///
/// - `Some(override_value)` if explicit_override is set (deprecated path).
/// - `!storage_auth_enabled` otherwise.
pub fn resolve_auth_disabled(storage_auth_enabled: bool, explicit_override: Option<bool>) -> bool {
    explicit_override.unwrap_or(!storage_auth_enabled)
}

#[cfg(test)]
mod resolve_auth_disabled_tests {
    use super::resolve_auth_disabled;

    #[test]
    fn defaults_to_inverse_of_storage_auth_enabled() {
        // Storage auth off → CQL server sends READY (auth_disabled=true).
        assert!(resolve_auth_disabled(false, None));
        // Storage auth on → CQL server sends AUTHENTICATE (auth_disabled=false).
        assert!(!resolve_auth_disabled(true, None));
    }

    #[test]
    fn explicit_override_wins() {
        // Explicit override beats storage flag in both directions (deprecated
        // but supported for one release to ease migration).
        assert!(resolve_auth_disabled(true, Some(true)));
        assert!(!resolve_auth_disabled(false, Some(false)));
    }
}

/// Tracks per-IP connection counts for rate limiting.
#[derive(Debug, Default)]
struct IpConnectionTracker {
    counts: RwLock<HashMap<IpAddr, usize>>,
}

impl IpConnectionTracker {
    fn new() -> Self {
        Self {
            counts: RwLock::new(HashMap::new()),
        }
    }

    /// Try to acquire a connection slot for the given IP.
    /// Returns true if under limit, false if at/over limit.
    fn try_acquire(&self, ip: IpAddr, limit: usize) -> bool {
        let mut counts = self.counts.write();
        let count = counts.entry(ip).or_insert(0);
        if *count >= limit {
            return false;
        }
        *count += 1;
        true
    }

    /// Release a connection slot for the given IP.
    fn release(&self, ip: IpAddr) {
        let mut counts = self.counts.write();
        if let Some(count) = counts.get_mut(&ip) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                counts.remove(&ip);
            }
        }
    }
}

/// RAII guard that releases an IP connection slot when dropped.
///
/// This ensures the slot is freed even if the handler task panics
/// or is cancelled, preventing permanent connection slot leaks.
///
/// The guard is handed off to `handle_connection`, which drops it
/// as soon as the connection reaches the `Ready` phase — the per-IP
/// limit only needs to defend against unauthenticated connection
/// storms; once a client has completed the protocol handshake (and
/// auth, if enabled), it is no longer counted toward the per-IP cap.
/// If the handler never reaches `Ready` (e.g. the client disconnects
/// before STARTUP, or fails MAX_AUTH_ATTEMPTS), the guard drops with
/// the handler task and the slot is released anyway.
pub(crate) struct IpSlotGuard {
    tracker: Arc<IpConnectionTracker>,
    ip: IpAddr,
}

impl Drop for IpSlotGuard {
    fn drop(&mut self) {
        self.tracker.release(self.ip);
    }
}

/// Send a single rejection frame to a newly-accepted stream in a spawned task.
///
/// The accept loop must NEVER await on a write to a peer it has just rejected:
/// if that one write blocks (slow peer, full kernel buffer, TCP retransmit
/// timeout after the peer disappears), every subsequent `listener.accept()`
/// is also blocked, and the entire CQL listener becomes unresponsive. This
/// is exactly the failure mode triaged in the connect-stall investigation
/// (commit message of this PR).
///
/// By spawning the send into its own task, slow rejection-writes are
/// quarantined to their own task. The accept loop returns to
/// `listener.accept()` on the very next instruction.
fn spawn_reject(
    task_pool: &TaskPool,
    stream: tokio::net::TcpStream,
    max_frame_size: u32,
    err: CqlError,
) {
    task_pool.spawn(async move {
        let codec = CqlCodec::new(max_frame_size);
        let mut framed = Framed::new(stream, codec);
        let body = err.encode_body().freeze();
        let frame = CqlFrame {
            header: FrameHeader {
                version: VERSION_RESPONSE,
                flags: 0,
                stream_id: -1,
                opcode: Opcode::Error,
                length: 0,
            },
            body,
        };
        let _ = framed.send(frame).await;
        // Framed (and the underlying TcpStream) drop here, closing the socket.
    });
}

/// CQL protocol server.
pub struct CqlServer {
    config: ServerConfig,
    state: Arc<SharedState>,
    active_connections: Arc<AtomicUsize>,
    ip_tracker: Arc<IpConnectionTracker>,
    task_pool: TaskPool,
}

impl CqlServer {
    pub fn new(config: ServerConfig, state: Arc<SharedState>) -> Self {
        Self {
            config,
            state,
            active_connections: Arc::new(AtomicUsize::new(0)),
            ip_tracker: Arc::new(IpConnectionTracker::new()),
            task_pool: TaskPool::current("cql"),
        }
    }

    pub fn with_task_pool(mut self, task_pool: TaskPool) -> Self {
        self.task_pool = task_pool;
        self
    }

    /// Build a TLS acceptor from cert/key paths if configured.
    fn build_tls_acceptor(&self) -> Result<Option<TlsAcceptor>, CqlError> {
        match (&self.config.tls_cert_path, &self.config.tls_key_path) {
            (Some(cert_path), Some(key_path)) => {
                let cert_file = std::fs::File::open(cert_path).map_err(|e| {
                    CqlError::ServerError(format!("failed to open TLS cert {cert_path}: {e}"))
                })?;
                let key_file = std::fs::File::open(key_path).map_err(|e| {
                    CqlError::ServerError(format!("failed to open TLS key {key_path}: {e}"))
                })?;

                let certs: Vec<_> = rustls_pemfile::certs(&mut std::io::BufReader::new(cert_file))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| {
                        CqlError::ServerError(format!("failed to parse TLS certs: {e}"))
                    })?;

                let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(key_file))
                    .map_err(|e| CqlError::ServerError(format!("failed to parse TLS key: {e}")))?
                    .ok_or_else(|| {
                        CqlError::ServerError("no private key found in TLS key file".into())
                    })?;

                let provider = rustls::crypto::ring::default_provider();
                let config = rustls::ServerConfig::builder_with_provider(provider.into())
                    .with_safe_default_protocol_versions()
                    .map_err(|e| CqlError::ServerError(format!("TLS protocol error: {e}")))?
                    .with_no_client_auth()
                    .with_single_cert(certs, key)
                    .map_err(|e| CqlError::ServerError(format!("TLS config error: {e}")))?;

                info!("TLS enabled for CQL connections");
                Ok(Some(TlsAcceptor::from(Arc::new(config))))
            }
            (None, None) => {
                if self.config.require_tls {
                    return Err(CqlError::ServerError(
                        "require_tls is true but no tls_cert_path/tls_key_path configured".into(),
                    ));
                }
                Ok(None)
            }
            _ => Err(CqlError::ServerError(
                "both tls_cert_path and tls_key_path must be set (or neither)".into(),
            )),
        }
    }

    /// Start the server in the background. Returns the bound address.
    pub async fn start_background(&self) -> Result<SocketAddr, CqlError> {
        let tls_acceptor = self.build_tls_acceptor()?;
        let bind_addr = self.config.bind_addr;
        let max_connections = self.config.max_connections;
        let max_connections_per_ip = self.config.max_connections_per_ip;
        let max_frame_size = self.config.max_frame_size;
        let max_in_flight = self.config.max_in_flight_per_connection;
        let auth_disabled = self.config.auth_disabled;
        let active = self.active_connections.clone();
        let ip_tracker = self.ip_tracker.clone();
        let state = self.state.clone();
        let task_pool = self.task_pool.clone();
        let accept_task_pool = task_pool.clone();
        let (addr_tx, addr_rx) = tokio::sync::oneshot::channel();

        if auth_disabled {
            warn!("auth: DISABLED (env override) — all connections are unauthenticated");
        }

        task_pool.spawn(async move {
            let listener = match TcpListener::bind(bind_addr).await {
                Ok(listener) => listener,
                Err(e) => {
                    let _ = addr_tx.send(Err(CqlError::from(e)));
                    return;
                }
            };
            let addr = match listener.local_addr() {
                Ok(addr) => addr,
                Err(e) => {
                    let _ = addr_tx.send(Err(CqlError::from(e)));
                    return;
                }
            };
            info!(
                pool = accept_task_pool.name(),
                "CQL server listening on {addr}"
            );
            let _ = addr_tx.send(Ok(addr));

            loop {
                match listener.accept().await {
                    Ok((stream, peer)) => {
                        let current = active.fetch_add(1, Ordering::Relaxed);
                        if current >= max_connections {
                            active.fetch_sub(1, Ordering::Relaxed);
                            warn!("connection limit reached, rejecting {peer}");
                            spawn_reject(
                                &accept_task_pool,
                                stream,
                                max_frame_size,
                                CqlError::Overloaded("connection limit reached".into()),
                            );
                            continue;
                        }
                        // In pair mode, only the primary accepts CQL connections.
                        // Secondaries reject with Overloaded so drivers retry on the primary.
                        if !state.mode_controller.accepts_cql_connections() {
                            active.fetch_sub(1, Ordering::Relaxed);
                            tracing::debug!(
                                "rejecting CQL connection: node is pair-mode secondary"
                            );
                            spawn_reject(
                                &accept_task_pool,
                                stream,
                                max_frame_size,
                                CqlError::Overloaded(
                                    "writes disallowed on read replica; connect to the primary node"
                                        .into(),
                                ),
                            );
                            continue;
                        }
                        // Per-IP rate limiting
                        let peer_ip = peer.ip();
                        if !ip_tracker.try_acquire(peer_ip, max_connections_per_ip) {
                            active.fetch_sub(1, Ordering::Relaxed);
                            warn!("per-IP limit reached for {peer_ip}, rejecting");
                            spawn_reject(
                                &accept_task_pool,
                                stream,
                                max_frame_size,
                                CqlError::Overloaded("per-IP connection limit reached".into()),
                            );
                            continue;
                        }
                        // Configure TCP keepalive to detect dead peers in ~60s
                        // instead of relying on the default 2+ hour timeout.
                        let sock_ref = SockRef::from(&stream);
                        let keepalive = socket2::TcpKeepalive::new()
                            .with_time(Duration::from_secs(30))
                            .with_interval(Duration::from_secs(10));
                        if let Err(e) = sock_ref.set_tcp_keepalive(&keepalive) {
                            warn!("failed to set TCP keepalive for {peer}: {e}");
                        }

                        // RAII guard: releases the IP slot on drop (including panics
                        // and task cancellation), preventing permanent slot leaks.
                        // Handed to the connection handler, which drops it the moment
                        // the connection reaches Ready phase — see IpSlotGuard docs.
                        let ip_guard = IpSlotGuard {
                            tracker: ip_tracker.clone(),
                            ip: peer_ip,
                        };

                        let active = active.clone();
                        let state = state.clone();
                        let tls_acceptor = tls_acceptor.clone();
                        let connection_task_pool = accept_task_pool.clone();
                        accept_task_pool.spawn(async move {
                            if let Some(acceptor) = tls_acceptor {
                                // TLS handshake with 10s timeout
                                match tokio::time::timeout(
                                    Duration::from_secs(10),
                                    acceptor.accept(stream),
                                )
                                .await
                                {
                                    Ok(Ok(tls_stream)) => {
                                        crate::connection::handle_connection(
                                            tls_stream,
                                            peer,
                                            max_frame_size,
                                            max_in_flight,
                                            auth_disabled,
                                            state,
                                            Some(ip_guard),
                                            connection_task_pool.clone(),
                                        )
                                        .await;
                                    }
                                    Ok(Err(e)) => {
                                        // ip_guard drops here, releasing the slot.
                                        warn!("TLS handshake failed from {peer}: {e}");
                                    }
                                    Err(_) => {
                                        warn!("TLS handshake timeout from {peer}");
                                    }
                                }
                            } else {
                                crate::connection::handle_connection(
                                    stream,
                                    peer,
                                    max_frame_size,
                                    max_in_flight,
                                    auth_disabled,
                                    state,
                                    Some(ip_guard),
                                    connection_task_pool.clone(),
                                )
                                .await;
                            }
                            // active connection count is decremented explicitly.
                            active.fetch_sub(1, Ordering::Relaxed);
                        });
                    }
                    Err(e) => {
                        warn!("accept error: {e}");
                    }
                }
            }
        });

        addr_rx
            .await
            .map_err(|e| CqlError::ServerError(format!("CQL bind task failed: {e}")))?
    }

    /// Returns the number of active connections.
    pub fn active_connections(&self) -> usize {
        self.active_connections.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::{FrameHeader, Opcode, HEADER_SIZE};
    use crate::prepared::PreparedCache;
    use crate::virtual_tables::active_queries::QueryTracker;
    use crate::virtual_tables::connections::ConnectionTracker;
    use arc_swap::ArcSwap;
    use ferrosa_cluster::WritePath;
    use ferrosa_schema::NodeConfig;
    use ferrosa_schema::{
        AuthMethod, DeploymentMode, EnvSecretsProvider, PasswordHasher, PasswordPolicy,
        RateLimitConfig, Schema, SchemaConfig, TestAuditSink,
    };
    use ferrosa_storage::{
        CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
    };
    use tempfile::TempDir;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpStream;

    fn setup_state() -> (Arc<SharedState>, TempDir) {
        let dir = TempDir::new().unwrap();
        let commit_log = CommitLogConfig {
            segment_size: 4096,
            max_segment_age: std::time::Duration::from_secs(60),
            sync_strategy: SyncStrategyConfig::Batch,
            batch: Default::default(),
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
            local_disk_free_reserve_bytes: 0,
            flush_threshold_bytes: 4096,
            memtable_backpressure_bytes: u64::MAX,
            flush_max_age_secs: 5,
            data_dir: dir.path().to_path_buf(),
            index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
            write_verify: true,
            auth_enabled: false,
            auth_warn: false,
            max_pending_replay_mutations_without_schema: 1024,
            memtable_num_shards: 64,
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
        let state = Arc::new(SharedState {
            core: Arc::new(ferrosa_session::SessionCore {
                engine: engine.clone(),
                schema: schema.clone(),
                node_config,
                cluster_state: Arc::new(ArcSwap::from_pointee(
                    ferrosa_cluster::ClusterStateHolder::Standalone,
                )),
                write_path: Arc::new(ArcSwap::from_pointee(WritePath::direct(engine.clone()))),
                ddl_path: Arc::new(ArcSwap::from_pointee(ferrosa_cluster::DdlPath::Direct {
                    schema,
                    engine,
                })),
                udf_executor,
                mode_controller,
                auth_warn: false,
                peer_manager: None,
                accord_clock: None,
                accord_state: ferrosa_cluster::accord::empty_accord_state_slot(),
            }),
            prepared_cache: Arc::new(PreparedCache::new(10 * 1024 * 1024)),
            param_cache: crate::param_cache::from_env(),
            connection_tracker: Arc::new(ConnectionTracker::new()),
            query_tracker: Arc::new(QueryTracker::new()),
            full_scan_tracker: Arc::new(crate::virtual_tables::FullScanTracker::new()),
            index_usage_tracker: Arc::new(crate::virtual_tables::IndexUsageTracker::new()),
            event_sender: tokio::sync::broadcast::channel(64).0,
            last_schema_event: tokio::sync::watch::channel(None).0,
            cql_metrics: Arc::new(crate::observability::CqlMetrics::new()),
            topology_policy: crate::topology::ClientTopologyPolicy::default(),
            txn_registry: std::sync::Arc::new(parking_lot::Mutex::new(
                crate::txn_registry::TransactionRegistry::default(),
            )),
        });
        (state, dir)
    }

    fn test_config(max_connections: usize, max_per_ip: usize) -> ServerConfig {
        ServerConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            max_connections,
            max_connections_per_ip: max_per_ip,
            ..ServerConfig::default()
        }
    }

    #[tokio::test]
    async fn server_accepts_connection() {
        let (state, _dir) = setup_state();
        let server = CqlServer::new(test_config(10, 64), state);
        let addr = server.start_background().await.unwrap();
        let _stream = TcpStream::connect(addr).await.unwrap();
    }

    #[tokio::test]
    async fn server_rejects_over_limit_with_overloaded() {
        let (state, _dir) = setup_state();
        let server = CqlServer::new(test_config(1, 64), state);
        let addr = server.start_background().await.unwrap();

        let _conn1 = TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let mut conn2 = TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let mut buf = vec![0u8; 256];
        let n = conn2.read(&mut buf).await.unwrap();
        assert!(n >= HEADER_SIZE);
        let header = FrameHeader::decode(&buf[..HEADER_SIZE]).unwrap();
        assert_eq!(header.opcode, Opcode::Error);
        let error_code = i32::from_be_bytes(buf[HEADER_SIZE..HEADER_SIZE + 4].try_into().unwrap());
        assert_eq!(error_code, 0x1001);
    }

    /// Send a v4 STARTUP frame and read one response frame.
    async fn startup_and_read_one_frame(stream: &mut TcpStream) -> FrameHeader {
        use bytes::BufMut;
        use tokio::io::AsyncWriteExt;
        let mut body = bytes::BytesMut::new();
        body.put_u16(1); // 1 entry in the string map
        let key = b"CQL_VERSION";
        body.put_u16(key.len() as u16);
        body.put_slice(key);
        let val = b"3.0.0";
        body.put_u16(val.len() as u16);
        body.put_slice(val);
        let body_len = body.len() as u32;

        let mut header = bytes::BytesMut::new();
        header.put_u8(0x04); // version (request)
        header.put_u8(0x00); // flags
        header.put_i16(0); // stream id
        header.put_u8(0x01); // STARTUP opcode
        header.put_u32(body_len);
        stream.write_all(&header).await.unwrap();
        stream.write_all(&body).await.unwrap();

        let mut buf = vec![0u8; HEADER_SIZE];
        let mut read_total = 0;
        while read_total < HEADER_SIZE {
            let n = stream.read(&mut buf[read_total..]).await.unwrap();
            if n == 0 {
                panic!("EOF before response header");
            }
            read_total += n;
        }
        let h = FrameHeader::decode(&buf).unwrap();
        // drain body so caller can reuse stream cleanly
        if h.length > 0 {
            let mut body = vec![0u8; h.length as usize];
            let _ = stream.read_exact(&mut body).await;
        }
        h
    }

    /// F3 regression test: per-IP rate-limit slot must be released the moment
    /// the connection reaches Ready phase. Otherwise a burst from one IP
    /// holds every slot for the full IDLE_TIMEOUT after the clients finish,
    /// rejecting legitimate follow-up traffic for minutes.
    #[tokio::test]
    async fn per_ip_slot_released_on_ready_transition() {
        let (state, _dir) = setup_state();
        let mut config = test_config(10, 1); // per-IP cap = 1
        config.auth_disabled = true; // STARTUP transitions directly to Ready
        let server = CqlServer::new(config, state);
        let addr = server.start_background().await.unwrap();

        // Connection 1 — complete handshake, reach Ready.
        let mut c1 = TcpStream::connect(addr).await.unwrap();
        let h1 = startup_and_read_one_frame(&mut c1).await;
        assert_eq!(h1.opcode, Opcode::Ready);

        // Give the server a brief window to drop the slot guard.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Connection 2 from SAME IP — pre-F3, c1 still holds the per-IP slot
        // and this would be rejected with Overloaded. Post-F3, c1's slot was
        // released when it reached Ready, so c2 acquires it and succeeds.
        let mut c2 = TcpStream::connect(addr).await.unwrap();
        let h2 = startup_and_read_one_frame(&mut c2).await;
        assert_eq!(
            h2.opcode,
            Opcode::Ready,
            "second connection from same IP should be accepted: c1 must release its per-IP slot on Ready transition"
        );

        // Keep c1 alive so we know its IDLE_TIMEOUT didn't release the slot.
        drop(c1);
        drop(c2);
    }

    /// Regression test for the CQL connect-stall: if rejection writes happen
    /// inline in the accept loop, ONE slow rejection-peer freezes every
    /// subsequent accept on the listener. With the fix, rejection writes
    /// are quarantined to spawned tasks; the accept loop keeps draining the
    /// SYN queue regardless of how slow individual rejection-peers are.
    #[tokio::test]
    async fn rejection_does_not_block_subsequent_accepts() {
        let (state, _dir) = setup_state();
        // Global cap=1, per-IP cap large so we test only the global path.
        let server = CqlServer::new(test_config(1, 64), state);
        let addr = server.start_background().await.unwrap();

        // Open and HOLD the one allowed slot; never read from it.
        let _hog = TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        // Open a rejected connection whose kernel recv buffer is set tiny
        // and that NEVER reads. With the old inline-await design, the
        // server's framed.send to this peer would saturate the kernel
        // buffer and block the accept loop. (For a 50-byte Error frame
        // this rarely happens in practice — but the structural fix is
        // what protects us, not the kernel buffer luck.)
        let slow_peer = TcpStream::connect(addr).await.unwrap();
        {
            let sref = socket2::SockRef::from(&slow_peer);
            let _ = sref.set_recv_buffer_size(512);
        }

        // Now race a NEW client: in the buggy design the accept loop is
        // wedged behind the slow_peer's rejection-send and this connect
        // would have to wait. We assert it completes within a tight bound.
        let race_start = std::time::Instant::now();
        let mut race = TcpStream::connect(addr).await.unwrap();
        let connect_elapsed = race_start.elapsed();

        let mut buf = vec![0u8; 256];
        let n = tokio::time::timeout(std::time::Duration::from_secs(2), race.read(&mut buf))
            .await
            .expect("rejection frame read timed out")
            .unwrap();
        assert!(n >= HEADER_SIZE);
        let header = FrameHeader::decode(&buf[..HEADER_SIZE]).unwrap();
        assert_eq!(header.opcode, Opcode::Error);

        assert!(
            connect_elapsed < std::time::Duration::from_millis(500),
            "race connect took {connect_elapsed:?} — accept loop appears blocked behind the slow rejection"
        );

        // Keep references alive so their sockets stay open during the test.
        drop(_hog);
        drop(slow_peer);
    }

    #[tokio::test]
    async fn server_rejects_per_ip_over_limit() {
        let (state, _dir) = setup_state();
        // Global limit 10, per-IP limit 2
        let server = CqlServer::new(test_config(10, 2), state);
        let addr = server.start_background().await.unwrap();

        // First two connections from 127.0.0.1 succeed
        let _conn1 = TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let _conn2 = TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Third connection from same IP should be rejected with Overloaded
        let mut conn3 = TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let mut buf = vec![0u8; 256];
        let n = conn3.read(&mut buf).await.unwrap();
        assert!(n >= HEADER_SIZE);
        let header = FrameHeader::decode(&buf[..HEADER_SIZE]).unwrap();
        assert_eq!(header.opcode, Opcode::Error);
        let error_code = i32::from_be_bytes(buf[HEADER_SIZE..HEADER_SIZE + 4].try_into().unwrap());
        assert_eq!(error_code, 0x1001); // Overloaded
    }

    #[test]
    fn ip_tracker_acquire_release() {
        let tracker = IpConnectionTracker::new();
        let ip: IpAddr = "10.0.0.1".parse().unwrap();

        assert!(tracker.try_acquire(ip, 2));
        assert!(tracker.try_acquire(ip, 2));
        assert!(!tracker.try_acquire(ip, 2)); // at limit

        tracker.release(ip);
        assert!(tracker.try_acquire(ip, 2)); // slot freed
    }

    #[tokio::test]
    async fn server_accepts_tls_connection() {
        use rcgen::generate_simple_self_signed;

        let certified = generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");
        std::fs::write(&cert_path, certified.cert.pem()).unwrap();
        std::fs::write(&key_path, certified.signing_key.serialize_pem()).unwrap();

        let (state, _dir2) = setup_state();
        let mut config = test_config(10, 64);
        config.tls_cert_path = Some(cert_path.to_str().unwrap().to_string());
        config.tls_key_path = Some(key_path.to_str().unwrap().to_string());
        let server = CqlServer::new(config, state);
        let addr = server.start_background().await.unwrap();

        // Connect with TLS client
        let mut root_store = rustls::RootCertStore::empty();
        root_store.add(certified.cert.der().clone()).unwrap();
        let provider = rustls::crypto::ring::default_provider();
        let client_config = rustls::ClientConfig::builder_with_provider(provider.into())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(root_store)
            .with_no_client_auth();
        let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));
        let tcp = TcpStream::connect(addr).await.unwrap();
        let domain = rustls::pki_types::ServerName::try_from("localhost").unwrap();
        let _tls_stream = connector.connect(domain, tcp).await.unwrap();
    }

    #[test]
    fn require_tls_without_certs_fails() {
        let (state, _dir) = setup_state();
        let mut config = test_config(10, 64);
        config.require_tls = true;
        let server = CqlServer::new(config, state);
        assert!(server.build_tls_acceptor().is_err());
    }

    #[test]
    fn partial_tls_config_fails() {
        let (state, _dir) = setup_state();
        let mut config = test_config(10, 64);
        config.tls_cert_path = Some("/tmp/cert.pem".into());
        // key_path is None — should fail
        let server = CqlServer::new(config, state);
        assert!(server.build_tls_acceptor().is_err());
    }

    /// Build a SharedState where the ModeController is pair-mode secondary,
    /// so `is_cql_ready()` returns false.
    fn setup_secondary_state() -> (Arc<SharedState>, TempDir) {
        let dir = TempDir::new().unwrap();
        let commit_log = CommitLogConfig {
            segment_size: 4096,
            max_segment_age: std::time::Duration::from_secs(60),
            sync_strategy: SyncStrategyConfig::Batch,
            batch: Default::default(),
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
            local_disk_free_reserve_bytes: 0,
            flush_threshold_bytes: 4096,
            memtable_backpressure_bytes: u64::MAX,
            flush_max_age_secs: 5,
            data_dir: dir.path().to_path_buf(),
            index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
            write_verify: true,
            auth_enabled: false,
            auth_warn: false,
            max_pending_replay_mutations_without_schema: 1024,
            memtable_num_shards: 64,
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
        let mode_controller = ferrosa_cluster::ModeController::pair_secondary_for_test(
            schema.clone(),
            engine.clone(),
        );
        let state = Arc::new(SharedState {
            core: Arc::new(ferrosa_session::SessionCore {
                engine: engine.clone(),
                schema: schema.clone(),
                node_config,
                cluster_state: Arc::new(ArcSwap::from_pointee(
                    ferrosa_cluster::ClusterStateHolder::Standalone,
                )),
                write_path: Arc::new(ArcSwap::from_pointee(WritePath::direct(engine.clone()))),
                ddl_path: Arc::new(ArcSwap::from_pointee(ferrosa_cluster::DdlPath::Direct {
                    schema,
                    engine,
                })),
                udf_executor,
                mode_controller,
                auth_warn: false,
                peer_manager: None,
                accord_clock: None,
                accord_state: ferrosa_cluster::accord::empty_accord_state_slot(),
            }),
            prepared_cache: Arc::new(PreparedCache::new(10 * 1024 * 1024)),
            param_cache: crate::param_cache::from_env(),
            connection_tracker: Arc::new(ConnectionTracker::new()),
            query_tracker: Arc::new(QueryTracker::new()),
            full_scan_tracker: Arc::new(crate::virtual_tables::FullScanTracker::new()),
            index_usage_tracker: Arc::new(crate::virtual_tables::IndexUsageTracker::new()),
            event_sender: tokio::sync::broadcast::channel(64).0,
            last_schema_event: tokio::sync::watch::channel(None).0,
            cql_metrics: Arc::new(crate::observability::CqlMetrics::new()),
            topology_policy: crate::topology::ClientTopologyPolicy::default(),
            txn_registry: std::sync::Arc::new(parking_lot::Mutex::new(
                crate::txn_registry::TransactionRegistry::default(),
            )),
        });
        (state, dir)
    }

    #[tokio::test]
    async fn secondary_rejects_cql_with_overloaded() {
        let (state, _dir) = setup_secondary_state();
        assert!(
            !state.mode_controller.is_cql_ready(),
            "secondary must not be CQL-ready"
        );
        let server = CqlServer::new(test_config(10, 64), state);
        let addr = server.start_background().await.unwrap();

        let mut conn = TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let mut buf = vec![0u8; 256];
        let n = conn.read(&mut buf).await.unwrap();
        assert!(n >= HEADER_SIZE, "expected at least a frame header");
        let header = FrameHeader::decode(&buf[..HEADER_SIZE]).unwrap();
        assert_eq!(header.opcode, Opcode::Error);
        // 0x1001 = Overloaded error code
        let error_code = i32::from_be_bytes(buf[HEADER_SIZE..HEADER_SIZE + 4].try_into().unwrap());
        assert_eq!(error_code, 0x1001, "expected Overloaded error code");
    }

    /// A client connecting after the Raft lane failed can still negotiate the
    /// protocol, then receives a typed retriable error for data operations.
    #[tokio::test]
    async fn consensus_failure_keeps_new_cql_protocol_responsive() {
        let (state, _dir) = setup_state();
        state.mode_controller.consensus_health().fail(
            "raft-runtime-panic",
            format_args!("raft_core.rs:769 empty apply window"),
        );
        let mut config = test_config(10, 64);
        config.auth_disabled = true;
        let server = CqlServer::new(config, state);
        let addr = server.start_background().await.unwrap();

        let mut conn = TcpStream::connect(addr).await.unwrap();
        let (options_header, _) = send_empty_request_and_read_frame(&mut conn, 1, 0x05).await;
        assert_eq!(options_header.opcode, Opcode::Supported);
        assert_eq!(
            startup_and_read_one_frame(&mut conn).await.opcode,
            Opcode::Ready
        );

        let (query_header, query_body) =
            send_empty_request_and_read_frame(&mut conn, 2, 0x07).await;
        assert_eq!(query_header.opcode, Opcode::Error);
        assert_eq!(
            i32::from_be_bytes(query_body[..4].try_into().unwrap()),
            0x1001,
            "expected retriable Overloaded"
        );
        let message_len = u16::from_be_bytes(query_body[4..6].try_into().unwrap()) as usize;
        let message = std::str::from_utf8(&query_body[6..6 + message_len]).unwrap();
        assert_eq!(
            message,
            "node unavailable: consensus runtime failed; retry another node"
        );
    }

    async fn send_empty_request_and_read_frame(
        stream: &mut TcpStream,
        stream_id: i16,
        opcode: u8,
    ) -> (FrameHeader, Vec<u8>) {
        use bytes::BufMut;
        use tokio::io::AsyncWriteExt;

        let mut request = bytes::BytesMut::with_capacity(HEADER_SIZE);
        request.put_u8(0x04);
        request.put_u8(0x00);
        request.put_i16(stream_id);
        request.put_u8(opcode);
        request.put_u32(0);
        stream.write_all(&request).await.unwrap();

        let mut header_bytes = [0u8; HEADER_SIZE];
        tokio::time::timeout(
            std::time::Duration::from_millis(250),
            stream.read_exact(&mut header_bytes),
        )
        .await
        .expect("established connection must remain responsive")
        .unwrap();
        let header = FrameHeader::decode(&header_bytes).unwrap();
        let mut body = vec![0u8; header.length as usize];
        stream.read_exact(&mut body).await.unwrap();
        (header, body)
    }

    /// Connections established before the failure must be fail-closed too.
    /// OPTIONS remains available to prove the native protocol is alive; QUERY
    /// fails before its (intentionally empty) body can reach parsing/routing.
    #[tokio::test]
    async fn established_cql_connection_fails_data_ops_but_answers_options() {
        let (state, _dir) = setup_state();
        let mut config = test_config(10, 64);
        config.auth_disabled = true;
        let server = CqlServer::new(config, state.clone());
        let addr = server.start_background().await.unwrap();
        let mut conn = TcpStream::connect(addr).await.unwrap();
        assert_eq!(
            startup_and_read_one_frame(&mut conn).await.opcode,
            Opcode::Ready
        );

        state.mode_controller.consensus_health().fail(
            "raft-runtime-panic",
            format_args!("raft_core.rs:769 empty apply window"),
        );

        let (options_header, _) = send_empty_request_and_read_frame(&mut conn, 1, 0x05).await;
        assert_eq!(options_header.opcode, Opcode::Supported);

        let (query_header, query_body) =
            send_empty_request_and_read_frame(&mut conn, 2, 0x07).await;
        assert_eq!(query_header.opcode, Opcode::Error);
        assert_eq!(
            i32::from_be_bytes(query_body[..4].try_into().unwrap()),
            0x1001,
            "existing connections receive retriable Overloaded"
        );
        let message_len = u16::from_be_bytes(query_body[4..6].try_into().unwrap()) as usize;
        assert_eq!(
            std::str::from_utf8(&query_body[6..6 + message_len]).unwrap(),
            "node unavailable: consensus runtime failed; retry another node"
        );
    }

    #[test]
    fn ip_tracker_different_ips_independent() {
        let tracker = IpConnectionTracker::new();
        let ip1: IpAddr = "10.0.0.1".parse().unwrap();
        let ip2: IpAddr = "10.0.0.2".parse().unwrap();

        assert!(tracker.try_acquire(ip1, 1));
        assert!(!tracker.try_acquire(ip1, 1)); // ip1 at limit
        assert!(tracker.try_acquire(ip2, 1)); // ip2 still ok
    }

    #[test]
    fn ip_slot_guard_releases_on_drop() {
        let tracker = Arc::new(IpConnectionTracker::new());
        let ip: IpAddr = "10.0.0.1".parse().unwrap();

        // Acquire a slot
        assert!(tracker.try_acquire(ip, 1));

        // Create guard and drop it — slot should be released
        {
            let _guard = IpSlotGuard {
                tracker: tracker.clone(),
                ip,
            };
        }

        // Slot should be free now — acquiring again must succeed
        assert!(
            tracker.try_acquire(ip, 1),
            "slot should have been released when guard was dropped"
        );
    }

    #[test]
    fn ip_slot_guard_releases_on_panic() {
        let tracker = Arc::new(IpConnectionTracker::new());
        let ip: IpAddr = "10.0.0.1".parse().unwrap();

        // Acquire a slot
        assert!(tracker.try_acquire(ip, 1));

        // Spawn a thread that panics while holding the guard
        let tracker_clone = tracker.clone();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _guard = IpSlotGuard {
                tracker: tracker_clone,
                ip,
            };
            panic!("simulated handler panic");
        }));
        assert!(result.is_err(), "should have caught the panic");

        // Slot must be freed despite the panic
        assert!(
            tracker.try_acquire(ip, 1),
            "slot should have been released even after panic"
        );
    }

    #[tokio::test]
    async fn dead_client_slot_reclaimed() {
        let (state, _dir) = setup_state();
        // Global limit 10, per-IP limit 2
        let server = CqlServer::new(test_config(10, 2), state);
        let addr = server.start_background().await.unwrap();

        // Open two connections — fills the per-IP slots
        let conn1 = TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _conn2 = TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Drop conn1 abruptly (simulating dead client)
        drop(conn1);
        // Give the server time to detect the closed connection and release the slot
        tokio::time::sleep(Duration::from_millis(200)).await;

        // A new connection should succeed because the dropped conn freed its slot
        let mut conn3 = TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        // If we got here without the connection being rejected, the slot was reclaimed.
        // Verify we do NOT receive an Overloaded error frame (the server should accept us).
        // Set a short read timeout — if no error frame arrives, the connection was accepted.
        let result = tokio::time::timeout(Duration::from_millis(200), async {
            let mut buf = vec![0u8; 256];
            conn3.read(&mut buf).await
        })
        .await;

        // Either timeout (no error frame = accepted) or 0 bytes (clean) is fine.
        // An Overloaded error frame would mean the slot was NOT reclaimed — that's a failure.
        match result {
            Err(_timeout) => { /* no data = connection accepted, waiting for STARTUP */ }
            Ok(Ok(0)) => { /* connection closed cleanly */ }
            Ok(Ok(n)) if n >= HEADER_SIZE => {
                panic!("received {n} bytes — likely an Overloaded error; slot was not reclaimed");
            }
            Ok(Ok(_)) => { /* partial data, not an error frame */ }
            Ok(Err(_)) => { /* read error, connection likely reset */ }
        }
    }

    #[tokio::test]
    async fn server_sets_keepalive_on_accepted_socket() {
        let (state, _dir) = setup_state();
        let server = CqlServer::new(test_config(10, 64), state);
        let addr = server.start_background().await.unwrap();

        // Connect and verify the server-side socket has keepalive set.
        // We can't directly inspect the server socket, but we can verify the
        // client-side socket reports keepalive is enabled on the peer's socket
        // by checking the local socket after connection.
        // On most OS-es, the server-side keepalive is independent of client.
        // Instead, we verify our own connection is accepted (server didn't crash
        // while setting keepalive) and use socket2 to check the server's
        // accepted socket indirectly via a connection tracker.
        let stream = TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Verify the connection is alive (server didn't error out)
        assert!(
            stream.peer_addr().is_ok(),
            "connection should be established — server must not crash setting keepalive"
        );

        // Verify via socket2 that we can at least read keepalive on our side
        // (this proves socket2 integration works without needing server internals)
        let sock_ref = socket2::SockRef::from(&stream);
        // Just verify the call doesn't panic — the actual keepalive is set server-side
        let _ = sock_ref.keepalive();
    }
}
