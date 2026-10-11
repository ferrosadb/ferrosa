// ferrosa-net/src/rpc/error_reply.rs
//! Error-reply frames: how a server tells a requester that it will never send
//! the response.
//!
//! Without them, a request whose handler panicked, whose response failed to
//! encode, or whose body failed to decode got no frame at all. The requester
//! held its stream slot until the lane timeout, and a burst of such failures
//! pinned every peer lane at its in-flight cap (2026-10-03).
//!
//! Wire form: a frame with [`crate::codec::FLAG_RPC_ERROR`] set, the request's
//! `stream_id` and `msg_type`, and this body:
//!
//! ```text
//! kind: u8 | detail: u16-length-prefixed UTF-8 string
//! ```
//!
//! A server sends one only to a peer whose Handshake advertised
//! [`crate::handshake::CAP_RPC_ERROR_REPLY`]. An unknown `kind` from a newer
//! peer decodes as [`RemoteFailureKind::Other`], never as a decode error.

use std::fmt;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::{NetError, Result};
use crate::message::{get_string, put_string};

/// Longest `detail` carried on the wire. Panic messages can embed whole
/// values; the full text is in the server's own log line.
pub const MAX_DETAIL_BYTES: usize = 1024;

/// Why the peer could not answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteFailureKind {
    /// The handler panicked.
    HandlerPanicked,
    /// The handler's response could not be encoded.
    ResponseEncodeFailed,
    /// The request body could not be decoded.
    RequestDecodeFailed,
    /// The encoded response exceeds the frame size limit.
    ResponseTooLarge,
    /// The handler ran and its operation failed (for example openraft refused
    /// an `install_snapshot` chunk). Added 2026-10-10; builds before it decode
    /// code 5 as `Other(5)`.
    HandlerFailed,
    /// The handler cannot serve yet (for example Raft is not initialised).
    /// Added 2026-10-10; builds before it decode code 6 as `Other(6)`.
    NotReady,
    /// A code this build does not know (sent by a newer peer).
    Other(u8),
}

impl RemoteFailureKind {
    pub fn code(self) -> u8 {
        match self {
            Self::HandlerPanicked => 1,
            Self::ResponseEncodeFailed => 2,
            Self::RequestDecodeFailed => 3,
            Self::ResponseTooLarge => 4,
            Self::HandlerFailed => 5,
            Self::NotReady => 6,
            Self::Other(code) => code,
        }
    }

    pub fn from_code(code: u8) -> Self {
        match code {
            1 => Self::HandlerPanicked,
            2 => Self::ResponseEncodeFailed,
            3 => Self::RequestDecodeFailed,
            4 => Self::ResponseTooLarge,
            5 => Self::HandlerFailed,
            6 => Self::NotReady,
            other => Self::Other(other),
        }
    }

    /// Stable metric label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::HandlerPanicked => "handler_panicked",
            Self::ResponseEncodeFailed => "response_encode_failed",
            Self::RequestDecodeFailed => "request_decode_failed",
            Self::ResponseTooLarge => "response_too_large",
            Self::HandlerFailed => "handler_failed",
            Self::NotReady => "not_ready",
            Self::Other(_) => "other",
        }
    }
}

impl fmt::Display for RemoteFailureKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Other(code) => write!(f, "unknown failure (code {code})"),
            known => f.write_str(known.as_str()),
        }
    }
}

/// Body of an error-reply frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcErrorReply {
    pub kind: RemoteFailureKind,
    pub detail: String,
}

impl RpcErrorReply {
    /// Build a reply, truncating `detail` to [`MAX_DETAIL_BYTES`] on a char
    /// boundary.
    pub fn new(kind: RemoteFailureKind, detail: &str) -> Self {
        let mut end = detail.len().min(MAX_DETAIL_BYTES);
        while !detail.is_char_boundary(end) {
            end -= 1;
        }
        Self {
            kind,
            detail: detail[..end].to_string(),
        }
    }

    pub fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        debug_assert!(self.detail.len() <= MAX_DETAIL_BYTES);
        buf.put_u8(self.kind.code());
        put_string(buf, &self.detail)
    }

    pub fn decode(body: &mut Bytes) -> Result<Self> {
        if body.remaining() < 1 {
            return Err(NetError::Protocol("truncated error reply".into()));
        }
        let kind = RemoteFailureKind::from_code(body.get_u8());
        let detail = get_string(body)?;
        Ok(Self { kind, detail })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_reply_roundtrip() {
        let reply = RpcErrorReply::new(RemoteFailureKind::HandlerPanicked, "boom");
        let mut buf = BytesMut::new();
        reply.encode(&mut buf).unwrap();
        let decoded = RpcErrorReply::decode(&mut buf.freeze()).unwrap();
        assert_eq!(decoded, reply);
    }

    /// A newer peer may send a kind this build does not know. It must still
    /// fail the request (as `Other`), never be rejected as malformed.
    #[test]
    fn unknown_kind_decodes_as_other() {
        let mut buf = BytesMut::new();
        buf.put_u8(200);
        put_string(&mut buf, "future").unwrap();
        let decoded = RpcErrorReply::decode(&mut buf.freeze()).unwrap();
        assert_eq!(decoded.kind, RemoteFailureKind::Other(200));
        assert_eq!(decoded.kind.code(), 200);
    }

    /// The decoder exactly as builds before 2026-10-10 shipped it: codes 1-4
    /// known, everything else `Other`.
    fn decode_kind_as_previous_build(code: u8) -> RemoteFailureKind {
        match code {
            1 => RemoteFailureKind::HandlerPanicked,
            2 => RemoteFailureKind::ResponseEncodeFailed,
            3 => RemoteFailureKind::RequestDecodeFailed,
            4 => RemoteFailureKind::ResponseTooLarge,
            other => RemoteFailureKind::Other(other),
        }
    }

    /// Rolling upgrade: a new server sends `HandlerFailed` / `NotReady` to a
    /// peer on the previous build. Their codes must be ones that build did not
    /// know, so it decodes them as `Other` (the request still fails promptly)
    /// rather than misreading them as an older kind.
    #[test]
    fn new_kinds_decode_as_other_on_the_previous_build() {
        for kind in [RemoteFailureKind::HandlerFailed, RemoteFailureKind::NotReady] {
            let reply = RpcErrorReply::new(kind, "raft not ready");
            let mut buf = BytesMut::new();
            reply.encode(&mut buf).unwrap();
            let mut body = buf.freeze();
            let code = body.get_u8();
            assert_eq!(
                decode_kind_as_previous_build(code),
                RemoteFailureKind::Other(code),
                "{kind:?} reuses a code the previous build already assigned"
            );
            assert_eq!(get_string(&mut body).unwrap(), "raft not ready");
            assert_eq!(RemoteFailureKind::from_code(code), kind, "this build round-trips it");
        }
    }

    #[test]
    fn detail_is_truncated_on_a_char_boundary() {
        let long = "é".repeat(MAX_DETAIL_BYTES); // 2 bytes per char
        let reply = RpcErrorReply::new(RemoteFailureKind::HandlerPanicked, &long);
        assert!(reply.detail.len() <= MAX_DETAIL_BYTES);
        assert!(reply.detail.chars().all(|c| c == 'é'));
    }

    #[test]
    fn truncated_body_is_a_protocol_error() {
        let err = RpcErrorReply::decode(&mut Bytes::new()).unwrap_err();
        assert!(matches!(err, NetError::Protocol(_)), "got {err:?}");
    }
}
