//! Postgres v3 wire message types.
//!
//! Framing (Postgres protocol v3.0): backend messages and post-startup frontend
//! messages are `[type: u8][length: i32 BE][body]`, where `length` counts itself
//! (its 4 bytes) but **not** the type byte. The very first frontend message —
//! `StartupMessage` / `SSLRequest` / `CancelRequest` — has **no** type byte and
//! is length-prefixed only; see [`StartupFrame`] and `codec::read_startup`.

use bytes::{BufMut, BytesMut};

/// Append a NUL-terminated C-string (the on-wire string encoding).
fn put_cstring(buf: &mut BytesMut, s: &str) {
    buf.extend_from_slice(s.as_bytes());
    buf.put_u8(0);
}

/// Postgres protocol version 3.0, as it appears in the StartupMessage
/// (major << 16 | minor == 3 << 16 | 0 == 196608).
pub const PROTOCOL_VERSION_3_0: i32 = 196608;

/// Magic "length code" identifying an `SSLRequest` startup packet.
pub const SSL_REQUEST_CODE: i32 = 80877103;

/// Magic "length code" identifying a `CancelRequest` startup packet.
pub const CANCEL_REQUEST_CODE: i32 = 80877102;

/// Transaction status reported by `ReadyForQuery` (the `I`/`T`/`E` byte).
///
/// This is the wire signal the blueprint's D11 hangs off of: entering a `T`
/// block is the trigger to route the transaction through Accord.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    /// `I` — idle (not in a transaction block).
    Idle,
    /// `T` — in a transaction block.
    InTransaction,
    /// `E` — in a failed transaction block (until `ROLLBACK`).
    Failed,
}

impl TransactionStatus {
    /// The single status byte sent on the wire.
    pub fn to_byte(self) -> u8 {
        match self {
            TransactionStatus::Idle => b'I',
            TransactionStatus::InTransaction => b'T',
            TransactionStatus::Failed => b'E',
        }
    }
}

/// A backend (server → client) message. First-slice subset sufficient for the
/// handshake and a `ReadyForQuery` turnaround.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendMessage {
    /// `R` with payload `0` — authentication succeeded.
    AuthenticationOk,
    /// `Z` — ready for the next query, carrying the transaction status.
    ReadyForQuery(TransactionStatus),
    /// `K` — cancellation key material for this backend.
    BackendKeyData { process_id: i32, secret_key: i32 },
    /// `S` — a run-time parameter report (e.g. `server_version`).
    ParameterStatus { name: String, value: String },
    /// `E` — an error response; `fields` are `(field-type byte, value)` pairs.
    ErrorResponse { fields: Vec<(u8, String)> },
}

impl BackendMessage {
    /// The message type tag byte.
    pub fn tag(&self) -> u8 {
        match self {
            BackendMessage::AuthenticationOk => b'R',
            BackendMessage::ReadyForQuery(_) => b'Z',
            BackendMessage::BackendKeyData { .. } => b'K',
            BackendMessage::ParameterStatus { .. } => b'S',
            BackendMessage::ErrorResponse { .. } => b'E',
        }
    }

    /// Append this message's body (no tag, no length) to `body`.
    fn encode_body(&self, body: &mut BytesMut) {
        match self {
            BackendMessage::AuthenticationOk => body.put_i32(0),
            BackendMessage::ReadyForQuery(status) => body.put_u8(status.to_byte()),
            BackendMessage::BackendKeyData {
                process_id,
                secret_key,
            } => {
                body.put_i32(*process_id);
                body.put_i32(*secret_key);
            }
            BackendMessage::ParameterStatus { name, value } => {
                put_cstring(body, name);
                put_cstring(body, value);
            }
            BackendMessage::ErrorResponse { fields } => {
                for (field_type, value) in fields {
                    body.put_u8(*field_type);
                    put_cstring(body, value);
                }
                body.put_u8(0); // field-list terminator
            }
        }
    }

    /// Append the fully framed message (`tag + length + body`) to `out`.
    /// `length` counts itself but not the tag byte.
    pub fn encode(&self, out: &mut BytesMut) {
        out.put_u8(self.tag());
        let len_idx = out.len();
        out.put_i32(0); // placeholder; patched once the body length is known
        self.encode_body(out);
        let framed_len = (out.len() - len_idx) as i32; // counts the 4 length bytes
        out[len_idx..len_idx + 4].copy_from_slice(&framed_len.to_be_bytes());
    }
}

/// A frontend (client → server) message after the startup phase. First-slice
/// subset; anything else is surfaced as [`FrontendMessage::Unknown`] rather
/// than silently dropped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrontendMessage {
    /// `Q` — a simple query string.
    Query(String),
    /// `S` — sync (extended-query boundary).
    Sync,
    /// `X` — terminate the connection.
    Terminate,
    /// Any other tagged message, preserved verbatim for later handling.
    Unknown { tag: u8, body: Vec<u8> },
}

/// A StartupMessage: protocol version plus the connection parameters
/// (`user`, `database`, and any dotted custom GUCs such as `ferrosa.isolation`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupMessage {
    pub protocol_version: i32,
    pub parameters: Vec<(String, String)>,
}

impl StartupMessage {
    /// Look up a startup parameter (e.g. `user`, `database`, `ferrosa.isolation`).
    pub fn get(&self, key: &str) -> Option<&str> {
        self.parameters
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
}

/// The first untagged frontend frame: a normal startup, or one of the two
/// magic-coded special packets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupFrame {
    Startup(StartupMessage),
    SslRequest,
    CancelRequest { process_id: i32, secret_key: i32 },
}
