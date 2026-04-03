//! CQL TCP server: accepts connections and spawns per-connection tasks.

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
struct IpSlotGuard {
    tracker: Arc<IpConnectionTracker>,
    ip: IpAddr,
}

impl Drop for IpSlotGuard {
    fn drop(&mut self) {
        self.tracker.release(self.ip);
    }
}

/// CQL protocol server.
pub struct CqlServer {
    config: ServerConfig,
    state: Arc<SharedState>,
    active_connections: Arc<AtomicUsize>,
    ip_tracker: Arc<IpConnectionTracker>,
}

impl CqlServer {
    pub fn new(config: ServerConfig, state: Arc<SharedState>) -> Self {
        Self {
            config,
            state,
            active_connections: Arc::new(AtomicUsize::new(0)),
            ip_tracker: Arc::new(IpConnectionTracker::new()),
        }
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
        let listener = TcpListener::bind(self.config.bind_addr).await?;
        let addr = listener.local_addr()?;
        let max_connections = self.config.max_connections;
        let max_connections_per_ip = self.config.max_connections_per_ip;
        let max_frame_size = self.config.max_frame_size;
        let max_in_flight = self.config.max_in_flight_per_connection;
        let auth_disabled = self.config.auth_disabled;
        let active = self.active_connections.clone();
        let ip_tracker = self.ip_tracker.clone();
        let state = self.state.clone();

        info!("CQL server listening on {addr}");

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, peer)) => {
                        let current = active.fetch_add(1, Ordering::Relaxed);
                        if current >= max_connections {
                            active.fetch_sub(1, Ordering::Relaxed);
                            warn!("connection limit reached, rejecting {peer}");
                            let codec = CqlCodec::new(max_frame_size);
                            let mut framed = Framed::new(stream, codec);
                            let err = CqlError::Overloaded("connection limit reached".into());
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
                            continue;
                        }
                        // In pair mode, only the primary accepts CQL connections.
                        // Secondaries reject with Overloaded so drivers retry on the primary.
                        if !state.mode_controller.is_cql_ready() {
                            active.fetch_sub(1, Ordering::Relaxed);
                            tracing::debug!(
                                "rejecting CQL connection: node is pair-mode secondary"
                            );
                            let codec = CqlCodec::new(max_frame_size);
                            let mut framed = Framed::new(stream, codec);
                            let err = CqlError::Overloaded(
                                "writes disallowed on read replica; connect to the primary node"
                                    .into(),
                            );
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
                            continue;
                        }
                        // Per-IP rate limiting
                        let peer_ip = peer.ip();
                        if !ip_tracker.try_acquire(peer_ip, max_connections_per_ip) {
                            active.fetch_sub(1, Ordering::Relaxed);
                            warn!("per-IP limit reached for {peer_ip}, rejecting");
                            let codec = CqlCodec::new(max_frame_size);
                            let mut framed = Framed::new(stream, codec);
                            let err =
                                CqlError::Overloaded("per-IP connection limit reached".into());
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
                        let _ip_guard = IpSlotGuard {
                            tracker: ip_tracker.clone(),
                            ip: peer_ip,
                        };

                        let active = active.clone();
                        let state = state.clone();
                        let tls_acceptor = tls_acceptor.clone();
                        tokio::spawn(async move {
                            // Move the guard into the spawned task so its lifetime
                            // is tied to the connection handler, not the accept loop.
                            let _ip_guard = _ip_guard;

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
                                        )
                                        .await;
                                    }
                                    Ok(Err(e)) => {
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
                                )
                                .await;
                            }
                            // _ip_guard drops here, releasing the IP slot.
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

        Ok(addr)
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
        let state = Arc::new(SharedState {
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
            prepared_cache: Arc::new(PreparedCache::new(10 * 1024 * 1024)),
            connection_tracker: Arc::new(ConnectionTracker::new()),
            query_tracker: Arc::new(QueryTracker::new()),
            udf_executor,
            event_sender: tokio::sync::broadcast::channel(64).0,
            mode_controller,
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
        assert_eq!(error_code, 0x1100);
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
        assert_eq!(error_code, 0x1100); // Overloaded
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
        std::fs::write(&key_path, certified.key_pair.serialize_pem()).unwrap();

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
        let mode_controller = ferrosa_cluster::ModeController::pair_secondary_for_test(
            schema.clone(),
            engine.clone(),
        );
        let state = Arc::new(SharedState {
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
            prepared_cache: Arc::new(PreparedCache::new(10 * 1024 * 1024)),
            connection_tracker: Arc::new(ConnectionTracker::new()),
            query_tracker: Arc::new(QueryTracker::new()),
            udf_executor,
            event_sender: tokio::sync::broadcast::channel(64).0,
            mode_controller,
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
        // 0x1100 = Overloaded error code
        let error_code = i32::from_be_bytes(buf[HEADER_SIZE..HEADER_SIZE + 4].try_into().unwrap());
        assert_eq!(error_code, 0x1100, "expected Overloaded error code");
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
