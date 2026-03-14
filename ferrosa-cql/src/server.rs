//! CQL TCP server: accepts connections and spawns per-connection tasks.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use futures::SinkExt;
use tokio::net::TcpListener;
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
    /// TODO: Enforce in connection handler — reject with ERROR(Overloaded).
    pub max_in_flight_per_connection: usize,
    /// If true, skip auth (STARTUP returns READY directly).
    pub auth_disabled: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:9042".parse().unwrap(),
            max_connections: 1024,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            max_in_flight_per_connection: 128,
            auth_disabled: false,
        }
    }
}

/// CQL protocol server.
pub struct CqlServer {
    config: ServerConfig,
    state: Arc<SharedState>,
    active_connections: Arc<AtomicUsize>,
}

impl CqlServer {
    pub fn new(config: ServerConfig, state: Arc<SharedState>) -> Self {
        Self {
            config,
            state,
            active_connections: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Start the server in the background. Returns the bound address.
    pub async fn start_background(&self) -> Result<SocketAddr, CqlError> {
        let listener = TcpListener::bind(self.config.bind_addr).await?;
        let addr = listener.local_addr()?;
        let max_connections = self.config.max_connections;
        let max_frame_size = self.config.max_frame_size;
        let auth_disabled = self.config.auth_disabled;
        let active = self.active_connections.clone();
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
                            let err = CqlError::Overloaded;
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
                        let active = active.clone();
                        let state = state.clone();
                        tokio::spawn(async move {
                            crate::connection::handle_connection(
                                stream,
                                peer,
                                max_frame_size,
                                auth_disabled,
                                state,
                            )
                            .await;
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
        let state = Arc::new(SharedState {
            engine: engine.clone(),
            schema,
            node_config,
            cluster_state: Arc::new(ArcSwap::from_pointee(
                ferrosa_cluster::ClusterStateHolder::Standalone,
            )),
            write_path: Arc::new(ArcSwap::from_pointee(WritePath::direct(engine))),
            prepared_cache: Arc::new(PreparedCache::new(10 * 1024 * 1024)),
            connection_tracker: Arc::new(ConnectionTracker::new()),
            query_tracker: Arc::new(QueryTracker::new()),
        });
        (state, dir)
    }

    #[tokio::test]
    async fn server_accepts_connection() {
        let (state, _dir) = setup_state();
        let config = ServerConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            max_connections: 10,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            max_in_flight_per_connection: 128,
            auth_disabled: false,
        };
        let server = CqlServer::new(config, state);
        let addr = server.start_background().await.unwrap();
        let _stream = TcpStream::connect(addr).await.unwrap();
    }

    #[tokio::test]
    async fn server_rejects_over_limit_with_overloaded() {
        let (state, _dir) = setup_state();
        let config = ServerConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            max_connections: 1,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            max_in_flight_per_connection: 128,
            auth_disabled: false,
        };
        let server = CqlServer::new(config, state);
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
}
