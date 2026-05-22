use std::fmt;

/// Errors produced by the ferrosa-net transport layer.
#[derive(Debug)]
#[non_exhaustive]
pub enum NetError {
    /// Frame body exceeds MAX_FRAME_BODY_SIZE.
    FrameTooLarge { size: u32, max: u32 },
    /// Unknown message type byte.
    UnknownMessageType(u8),
    /// Invalid lane value (must be 0–2).
    InvalidLane(u8),
    /// Handshake failed (cluster name mismatch, PSK invalid, etc.).
    HandshakeFailed(String),
    /// Connection timed out (per-lane or handshake).
    Timeout(String),
    /// Peer suspected dead (heartbeat timeout).
    PeerSuspected(uuid::Uuid),
    /// Protocol violation (corrupt frame, unexpected state).
    Protocol(String),
    /// Maximum connections reached.
    Overloaded,
    /// I/O error from the transport layer.
    Io(std::io::Error),
    /// Lane is currently reconnecting; request cannot be served.
    Reconnecting,
    /// Lane has exhausted all reconnection attempts and is permanently failed.
    LaneFailed,
    /// Server failed during startup (e.g. bind notification could not be
    /// delivered to the caller because the receiver was dropped before the
    /// listener bound). Carries a human-readable cause for the operator.
    StartupFailed(String),
}

impl fmt::Display for NetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FrameTooLarge { size, max } => {
                write!(f, "frame body too large: {size} bytes (max {max})")
            }
            Self::UnknownMessageType(t) => write!(f, "unknown message type: 0x{t:02x}"),
            Self::InvalidLane(l) => write!(f, "invalid lane: {l} (expected 0-2)"),
            Self::HandshakeFailed(msg) => write!(f, "handshake failed: {msg}"),
            Self::Timeout(msg) => write!(f, "timeout: {msg}"),
            Self::PeerSuspected(id) => write!(f, "peer suspected dead: {id}"),
            Self::Protocol(msg) => write!(f, "protocol error: {msg}"),
            Self::Overloaded => write!(f, "max internode connections reached"),
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Reconnecting => write!(f, "lane is reconnecting; retry later"),
            Self::LaneFailed => {
                write!(f, "lane permanently failed after max reconnection attempts")
            }
            Self::StartupFailed(msg) => write!(f, "startup failed: {msg}"),
        }
    }
}

/// Return an operator-facing diagnostic string for a `bind()` failure.
///
/// Decorates the raw I/O error with information about *why* the bind
/// might have failed and what to do about it. The macOS-specific path
/// covers BUG-001: port 7000 (the historical Cassandra default and the
/// previous ferrosa default) is reserved by macOS ControlCenter on
/// all modern macOS versions, producing an opaque `EADDRINUSE`.
pub fn bind_failure_diagnostic(addr: &std::net::SocketAddr, err: &std::io::Error) -> String {
    let kind = err.kind();
    let is_in_use = kind == std::io::ErrorKind::AddrInUse;
    let port = addr.port();
    if is_in_use && port == 7000 && cfg!(target_os = "macos") {
        return format!(
            "failed to bind internode port {addr}: address in use. On macOS, port 7000 \
             is reserved by ControlCenter. Set FERROSA_INTERNODE_BIND to a high port \
             (e.g. 127.0.0.1:17000) or change [internode] bind in ferrosa.toml. \
             Underlying error: {err}"
        );
    }
    if is_in_use {
        return format!(
            "failed to bind {addr}: address in use. Another process is already listening \
             on this port. Underlying error: {err}"
        );
    }
    format!("failed to bind {addr}: {err}")
}

impl std::error::Error for NetError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for NetError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

pub type Result<T> = std::result::Result<T, NetError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_messages() {
        let e = NetError::FrameTooLarge {
            size: 1000,
            max: 500,
        };
        assert!(e.to_string().contains("1000"));
        assert!(e.to_string().contains("500"));

        let e = NetError::InvalidLane(5);
        assert!(e.to_string().contains("5"));

        let e = NetError::Overloaded;
        assert!(e.to_string().contains("max internode connections"));
    }

    #[test]
    fn io_error_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        let net_err: NetError = io_err.into();
        assert!(matches!(net_err, NetError::Io(_)));
    }

    /// BUG-001: on macOS, an EADDRINUSE on port 7000 must specifically name
    /// ControlCenter and tell the operator what to set.
    #[cfg(target_os = "macos")]
    #[test]
    fn bind_diagnostic_macos_port_7000_names_controlcenter() {
        let addr: std::net::SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let err = std::io::Error::from(std::io::ErrorKind::AddrInUse);
        let msg = bind_failure_diagnostic(&addr, &err);
        assert!(msg.contains("ControlCenter"), "msg: {msg}");
        assert!(msg.contains("FERROSA_INTERNODE_BIND"), "msg: {msg}");
        assert!(msg.contains("17000"), "msg: {msg}");
    }

    /// Other ports getting EADDRINUSE — generic helpful message, no
    /// macOS-specific reference.
    #[test]
    fn bind_diagnostic_other_port_in_use_is_generic() {
        let addr: std::net::SocketAddr = "127.0.0.1:9999".parse().unwrap();
        let err = std::io::Error::from(std::io::ErrorKind::AddrInUse);
        let msg = bind_failure_diagnostic(&addr, &err);
        assert!(msg.contains("9999"), "msg: {msg}");
        assert!(msg.contains("address in use"), "msg: {msg}");
        assert!(!msg.contains("ControlCenter"), "msg: {msg}");
    }

    /// Non-bind errors (e.g. permission denied) pass through with the address.
    #[test]
    fn bind_diagnostic_other_error_kinds_pass_through() {
        let addr: std::net::SocketAddr = "127.0.0.1:80".parse().unwrap();
        let err = std::io::Error::from(std::io::ErrorKind::PermissionDenied);
        let msg = bind_failure_diagnostic(&addr, &err);
        assert!(msg.contains("127.0.0.1:80"), "msg: {msg}");
        assert!(!msg.contains("ControlCenter"), "msg: {msg}");
    }
}
