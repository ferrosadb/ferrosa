//! Per-connection CQL protocol handler.

use std::net::SocketAddr;

use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tracing::debug;

/// Handle a single CQL connection.
///
/// Stub implementation: holds the connection open until the client
/// disconnects. Will be expanded in Task 11 with the full handshake.
pub async fn handle_connection(
    mut stream: TcpStream,
    peer: SocketAddr,
    _max_frame_size: u32,
    _auth_disabled: bool,
) {
    debug!("new connection from {peer}");
    // Hold connection open until client disconnects (read until EOF).
    let mut buf = [0u8; 1024];
    loop {
        match stream.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
    }
    debug!("connection from {peer} closed");
}
