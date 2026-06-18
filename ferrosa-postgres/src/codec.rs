//! Framing + (de)serialization for the Postgres v3 wire protocol.
//!
//! Reads consume from a [`BytesMut`] and return `Ok(None)` when more bytes are
//! needed (no partial consumption), `Err(CodecError)` on a protocol violation,
//! and `Ok(Some(msg))` once a full frame is available. A hard upper bound
//! ([`MAX_MESSAGE_LEN`]) guards against unbounded-allocation DoS — the
//! threat-model bounded-length control, mirroring the CQL 256 MiB frame cap.

use std::fmt;

use bytes::{Buf, BytesMut};

use crate::messages::{
    FrontendMessage, StartupFrame, StartupMessage, CANCEL_REQUEST_CODE, SSL_REQUEST_CODE,
};

/// Maximum accepted message length (the value of the i32 length field).
/// Mirrors the CQL frame cap; anything larger is rejected as `MessageTooLarge`.
pub const MAX_MESSAGE_LEN: usize = 256 * 1024 * 1024;

/// A protocol-level decode error. These are violations (fail loud); "need more
/// bytes" is `Ok(None)`, never an error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    /// The length field exceeds [`MAX_MESSAGE_LEN`].
    MessageTooLarge { len: usize },
    /// The length field is smaller than the minimum a valid frame can have.
    LengthTooSmall { len: i32 },
    /// A startup packet used a length-code we do not recognize.
    UnknownStartupCode(i32),
    /// Structurally malformed frame (with a static reason).
    Malformed(&'static str),
    /// A C-string field was not valid UTF-8.
    InvalidUtf8,
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CodecError::MessageTooLarge { len } => {
                write!(f, "message length {len} exceeds maximum {MAX_MESSAGE_LEN}")
            }
            CodecError::LengthTooSmall { len } => write!(f, "frame length {len} too small"),
            CodecError::UnknownStartupCode(c) => write!(f, "unknown startup length-code {c}"),
            CodecError::Malformed(why) => write!(f, "malformed frame: {why}"),
            CodecError::InvalidUtf8 => write!(f, "field was not valid UTF-8"),
        }
    }
}

impl std::error::Error for CodecError {}

/// Read a NUL-terminated C-string from the front of `buf`, consuming it and the
/// terminator. Returns `Ok(None)` if no terminator is present yet.
fn read_cstring(buf: &mut BytesMut) -> Result<Option<String>, CodecError> {
    match buf.iter().position(|&b| b == 0) {
        Some(pos) => {
            let bytes = buf.split_to(pos);
            buf.advance(1); // consume the NUL terminator
            let s = String::from_utf8(bytes.to_vec()).map_err(|_| CodecError::InvalidUtf8)?;
            Ok(Some(s))
        }
        None => Ok(None),
    }
}

/// Try to read the first (untagged) startup frame: a `StartupMessage`, an
/// `SSLRequest`, or a `CancelRequest`. Returns `Ok(None)` if incomplete.
pub fn read_startup(buf: &mut BytesMut) -> Result<Option<StartupFrame>, CodecError> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let len = i32::from_be_bytes(buf[0..4].try_into().unwrap());
    if len < 8 {
        return Err(CodecError::LengthTooSmall { len });
    }
    let len = len as usize;
    if len > MAX_MESSAGE_LEN {
        return Err(CodecError::MessageTooLarge { len });
    }
    if buf.len() < len {
        return Ok(None); // wait for the whole frame; consume nothing
    }

    let mut frame = buf.split_to(len);
    frame.advance(4); // skip the length field
    let code = i32::from_be_bytes(frame[0..4].try_into().unwrap());
    frame.advance(4);

    match code {
        SSL_REQUEST_CODE => Ok(Some(StartupFrame::SslRequest)),
        CANCEL_REQUEST_CODE => {
            if frame.len() < 8 {
                return Err(CodecError::Malformed("cancel request body too short"));
            }
            let process_id = i32::from_be_bytes(frame[0..4].try_into().unwrap());
            let secret_key = i32::from_be_bytes(frame[4..8].try_into().unwrap());
            Ok(Some(StartupFrame::CancelRequest {
                process_id,
                secret_key,
            }))
        }
        protocol_version => {
            let mut parameters = Vec::new();
            loop {
                let key = read_cstring(&mut frame)?
                    .ok_or(CodecError::Malformed("startup parameters not terminated"))?;
                if key.is_empty() {
                    break; // empty key terminates the parameter list
                }
                let value = read_cstring(&mut frame)?
                    .ok_or(CodecError::Malformed("startup parameter missing value"))?;
                parameters.push((key, value));
            }
            Ok(Some(StartupFrame::Startup(StartupMessage {
                protocol_version,
                parameters,
            })))
        }
    }
}

/// Try to read one tagged frontend message (post-startup). Returns `Ok(None)`
/// if incomplete.
pub fn read_frontend(buf: &mut BytesMut) -> Result<Option<FrontendMessage>, CodecError> {
    if buf.len() < 5 {
        return Ok(None); // need tag (1) + length (4)
    }
    let tag = buf[0];
    let len = i32::from_be_bytes(buf[1..5].try_into().unwrap());
    if len < 4 {
        return Err(CodecError::LengthTooSmall { len });
    }
    let len = len as usize;
    if len > MAX_MESSAGE_LEN {
        return Err(CodecError::MessageTooLarge { len });
    }
    let total = 1 + len; // tag byte + (length counts itself + body)
    if buf.len() < total {
        return Ok(None);
    }

    let mut frame = buf.split_to(total);
    frame.advance(5); // skip tag + length; `frame` is now the body

    match tag {
        b'Q' => {
            let query = read_cstring(&mut frame)?
                .ok_or(CodecError::Malformed("query string not NUL-terminated"))?;
            Ok(Some(FrontendMessage::Query(query)))
        }
        b'S' => Ok(Some(FrontendMessage::Sync)),
        b'X' => Ok(Some(FrontendMessage::Terminate)),
        b'p' => Ok(Some(FrontendMessage::SaslResponse {
            data: frame.to_vec(),
        })),
        other => Ok(Some(FrontendMessage::Unknown {
            tag: other,
            body: frame.to_vec(),
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::{BackendMessage, StartupMessage, TransactionStatus};
    use bytes::{BufMut, BytesMut};

    // ---- backend encoding (server -> client) ----

    #[test]
    fn authentication_ok_encodes_to_known_bytes() {
        let mut out = BytesMut::new();
        BackendMessage::AuthenticationOk.encode(&mut out);
        // 'R', length=8 (4 len + 4 payload), payload i32(0)
        assert_eq!(&out[..], &[b'R', 0, 0, 0, 8, 0, 0, 0, 0]);
    }

    #[test]
    fn ready_for_query_idle_encodes_to_known_bytes() {
        let mut out = BytesMut::new();
        BackendMessage::ReadyForQuery(TransactionStatus::Idle).encode(&mut out);
        // 'Z', length=5 (4 len + 1 status), 'I'
        assert_eq!(&out[..], &[b'Z', 0, 0, 0, 5, b'I']);
    }

    #[test]
    fn ready_for_query_in_transaction_status_byte() {
        let mut out = BytesMut::new();
        BackendMessage::ReadyForQuery(TransactionStatus::InTransaction).encode(&mut out);
        assert_eq!(out.last().copied(), Some(b'T'));
    }

    #[test]
    fn backend_key_data_encodes_to_known_bytes() {
        let mut out = BytesMut::new();
        BackendMessage::BackendKeyData {
            process_id: 1,
            secret_key: 2,
        }
        .encode(&mut out);
        // 'K', length=12 (4 + 4 + 4), pid=1, key=2
        assert_eq!(&out[..], &[b'K', 0, 0, 0, 12, 0, 0, 0, 1, 0, 0, 0, 2]);
    }

    #[test]
    fn parameter_status_encodes_name_and_value_cstrings() {
        let mut out = BytesMut::new();
        BackendMessage::ParameterStatus {
            name: "server_version".into(),
            value: "16".into(),
        }
        .encode(&mut out);
        assert_eq!(out[0], b'S');
        // length = 4 + ("server_version\0"=15) + ("16\0"=3) = 22
        let len = i32::from_be_bytes(out[1..5].try_into().unwrap());
        assert_eq!(len, 22);
        assert_eq!(&out[5..], b"server_version\x0016\x00");
    }

    #[test]
    fn authentication_sasl_encodes_subtype_10_and_mechanism_list() {
        let mut out = BytesMut::new();
        BackendMessage::AuthenticationSasl {
            mechanisms: vec!["SCRAM-SHA-256".into()],
        }
        .encode(&mut out);
        assert_eq!(out[0], b'R');
        assert_eq!(i32::from_be_bytes(out[5..9].try_into().unwrap()), 10);
        // mechanism cstring followed by the list terminator
        assert_eq!(&out[9..], b"SCRAM-SHA-256\x00\x00");
    }

    #[test]
    fn authentication_sasl_continue_and_final_carry_subtypes_11_and_12() {
        let mut cont = BytesMut::new();
        BackendMessage::AuthenticationSaslContinue {
            data: b"r=abc".to_vec(),
        }
        .encode(&mut cont);
        assert_eq!(cont[0], b'R');
        assert_eq!(i32::from_be_bytes(cont[5..9].try_into().unwrap()), 11);
        assert_eq!(&cont[9..], b"r=abc");

        let mut fin = BytesMut::new();
        BackendMessage::AuthenticationSaslFinal {
            data: b"v=xyz".to_vec(),
        }
        .encode(&mut fin);
        assert_eq!(i32::from_be_bytes(fin[5..9].try_into().unwrap()), 12);
        assert_eq!(&fin[9..], b"v=xyz");
    }

    #[test]
    fn sasl_response_frontend_message_carries_raw_body() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'p');
        let payload = b"c=biws,r=abc,p=proof";
        buf.put_i32((payload.len() + 4) as i32);
        buf.extend_from_slice(payload);
        assert_eq!(
            read_frontend(&mut buf).unwrap(),
            Some(FrontendMessage::SaslResponse {
                data: payload.to_vec()
            })
        );
    }

    #[test]
    fn error_response_terminates_with_zero_byte() {
        let mut out = BytesMut::new();
        BackendMessage::ErrorResponse {
            fields: vec![(b'S', "FATAL".into()), (b'C', "28000".into())],
        }
        .encode(&mut out);
        assert_eq!(out[0], b'E');
        // final byte is the field-list terminator
        assert_eq!(out.last().copied(), Some(0));
    }

    // ---- startup parsing (client -> server, first frame) ----

    /// Build a StartupMessage byte buffer for the given protocol + params.
    fn build_startup(protocol: i32, params: &[(&str, &str)]) -> BytesMut {
        let mut body = BytesMut::new();
        body.put_i32(protocol);
        for (k, v) in params {
            body.extend_from_slice(k.as_bytes());
            body.put_u8(0);
            body.extend_from_slice(v.as_bytes());
            body.put_u8(0);
        }
        body.put_u8(0); // terminating empty key
        let mut frame = BytesMut::new();
        frame.put_i32(body.len() as i32 + 4); // length counts itself
        frame.extend_from_slice(&body);
        frame
    }

    #[test]
    fn parses_startup_message_with_parameters() {
        let mut buf = build_startup(
            super::super::messages::PROTOCOL_VERSION_3_0,
            &[
                ("user", "postgres"),
                ("database", "ferrosa"),
                ("ferrosa.isolation", "accord"),
            ],
        );
        let frame = read_startup(&mut buf).unwrap().unwrap();
        match frame {
            StartupFrame::Startup(StartupMessage {
                protocol_version,
                parameters,
            }) => {
                assert_eq!(protocol_version, 196608);
                assert_eq!(parameters[0], ("user".to_string(), "postgres".to_string()));
                assert_eq!(
                    parameters[1],
                    ("database".to_string(), "ferrosa".to_string())
                );
                // dotted custom GUC must survive (the D1/D11 connection-time opt-in path)
                assert_eq!(
                    parameters[2],
                    ("ferrosa.isolation".to_string(), "accord".to_string())
                );
            }
            other => panic!("expected Startup, got {other:?}"),
        }
        assert!(buf.is_empty(), "frame should be fully consumed");
    }

    #[test]
    fn startup_returns_none_when_incomplete() {
        let full = build_startup(196608, &[("user", "x")]);
        let mut partial = BytesMut::from(&full[..full.len() - 2]);
        assert_eq!(read_startup(&mut partial).unwrap(), None);
        // nothing consumed on incomplete
        assert_eq!(partial.len(), full.len() - 2);
    }

    #[test]
    fn parses_ssl_request() {
        let mut buf = BytesMut::new();
        buf.put_i32(8);
        buf.put_i32(SSL_REQUEST_CODE);
        assert_eq!(
            read_startup(&mut buf).unwrap(),
            Some(StartupFrame::SslRequest)
        );
    }

    #[test]
    fn parses_cancel_request() {
        let mut buf = BytesMut::new();
        buf.put_i32(16);
        buf.put_i32(CANCEL_REQUEST_CODE);
        buf.put_i32(42);
        buf.put_i32(99);
        assert_eq!(
            read_startup(&mut buf).unwrap(),
            Some(StartupFrame::CancelRequest {
                process_id: 42,
                secret_key: 99
            })
        );
    }

    #[test]
    fn oversized_startup_length_is_rejected() {
        let mut buf = BytesMut::new();
        buf.put_i32((MAX_MESSAGE_LEN + 1) as i32);
        buf.put_i32(196608);
        assert_eq!(
            read_startup(&mut buf),
            Err(CodecError::MessageTooLarge {
                len: MAX_MESSAGE_LEN + 1
            })
        );
    }

    // ---- frontend tagged messages ----

    #[test]
    fn parses_simple_query() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'Q');
        let q = b"SELECT 1\x00";
        buf.put_i32((q.len() + 4) as i32);
        buf.extend_from_slice(q);
        assert_eq!(
            read_frontend(&mut buf).unwrap(),
            Some(FrontendMessage::Query("SELECT 1".to_string()))
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn unknown_frontend_tag_is_surfaced_not_dropped() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'P'); // Parse — not handled in this slice
        buf.put_i32(4 + 3);
        buf.extend_from_slice(&[1, 2, 3]);
        match read_frontend(&mut buf).unwrap().unwrap() {
            FrontendMessage::Unknown { tag, body } => {
                assert_eq!(tag, b'P');
                assert_eq!(body, vec![1, 2, 3]);
            }
            other => panic!("expected Unknown, got {other:?}"),
        }
    }

    #[test]
    fn oversized_frontend_length_is_rejected() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'Q');
        buf.put_i32((MAX_MESSAGE_LEN + 1) as i32);
        assert_eq!(
            read_frontend(&mut buf),
            Err(CodecError::MessageTooLarge {
                len: MAX_MESSAGE_LEN + 1
            })
        );
    }
}
