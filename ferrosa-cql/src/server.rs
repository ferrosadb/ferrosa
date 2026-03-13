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

/// Server configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind_addr: SocketAddr,
    pub max_connections: usize,
    pub max_frame_size: u32,
    /// Max concurrent in-flight requests per connection (default 128).
    /// TODO: Enforce in connection handler (Part B) — reject with ERROR(Overloaded).
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
    active_connections: Arc<AtomicUsize>,
}

impl CqlServer {
    pub fn new(config: ServerConfig) -> Self {
        Self {
            config,
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
                        tokio::spawn(async move {
                            crate::connection::handle_connection(
                                stream,
                                peer,
                                max_frame_size,
                                auth_disabled,
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
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpStream;

    #[tokio::test]
    async fn server_accepts_connection() {
        let config = ServerConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            max_connections: 10,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            max_in_flight_per_connection: 128,
            auth_disabled: false,
        };
        let server = CqlServer::new(config);
        let addr = server.start_background().await.unwrap();
        let _stream = TcpStream::connect(addr).await.unwrap();
    }

    #[tokio::test]
    async fn server_rejects_over_limit_with_overloaded() {
        let config = ServerConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            max_connections: 1,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            max_in_flight_per_connection: 128,
            auth_disabled: false,
        };
        let server = CqlServer::new(config);
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
