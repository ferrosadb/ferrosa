use std::fmt;

/// Errors produced by the ferrosa-net transport layer.
#[derive(Debug)]
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
        }
    }
}

impl std::error::Error for NetError {}

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
}
