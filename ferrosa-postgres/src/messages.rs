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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TransactionStatus {
    /// `I` — idle (not in a transaction block).
    #[default]
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

/// One field (column) in a `RowDescription`.
///
/// `type_size` is the on-wire fixed length for a fixed-width type (e.g. 4 for
/// int4, 1 for bool) or `-1` for a variable-length type (e.g. text). `type_oid`
/// is the Postgres type OID the driver uses to decode each `DataRow` value.
/// `format_code` is the wire format the matching `DataRow` column is encoded in:
/// `0` = text, `1` = binary. The simple-query path always uses `0`; the extended
/// path honors the portal's result-format codes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldDescription {
    pub name: String,
    pub type_oid: i32,
    pub type_size: i16,
    pub format_code: i16,
}

impl FieldDescription {
    /// A text-format field (`format_code = 0`) — the simple-query default.
    pub fn text(name: impl Into<String>, type_oid: i32, type_size: i16) -> Self {
        Self {
            name: name.into(),
            type_oid,
            type_size,
            format_code: 0,
        }
    }
}

/// A backend (server → client) message. First-slice subset sufficient for the
/// handshake and a `ReadyForQuery` turnaround.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendMessage {
    /// `R` with payload `0` — authentication succeeded.
    AuthenticationOk,
    /// `R` with payload `10` — offer SASL mechanisms (e.g. `SCRAM-SHA-256`).
    AuthenticationSasl { mechanisms: Vec<String> },
    /// `R` with payload `11` — SASL challenge (carries `server-first`).
    AuthenticationSaslContinue { data: Vec<u8> },
    /// `R` with payload `12` — SASL completion (carries `server-final`).
    AuthenticationSaslFinal { data: Vec<u8> },
    /// `Z` — ready for the next query, carrying the transaction status.
    ReadyForQuery(TransactionStatus),
    /// `K` — cancellation key material for this backend.
    BackendKeyData { process_id: i32, secret_key: i32 },
    /// `S` — a run-time parameter report (e.g. `server_version`).
    ParameterStatus { name: String, value: String },
    /// `E` — an error response; `fields` are `(field-type byte, value)` pairs.
    ErrorResponse { fields: Vec<(u8, String)> },
    /// `T` — describes the columns of a result set (one per query).
    RowDescription { fields: Vec<FieldDescription> },
    /// `D` — one data row; each column is `Some(bytes)` (text format) or `None`
    /// for SQL NULL.
    DataRow { columns: Vec<Option<Vec<u8>>> },
    /// `C` — command completion, carrying the command tag (e.g. `"SELECT 2"`).
    CommandComplete { tag: String },
    /// `1` — Parse completed (extended protocol; empty body).
    ParseComplete,
    /// `2` — Bind completed (extended protocol; empty body).
    BindComplete,
    /// `3` — Close completed (extended protocol; empty body).
    CloseComplete,
    /// `t` — ParameterDescription: the parameter type OIDs of a prepared
    /// statement (i16 count + that many i32 OIDs).
    ParameterDescription { type_oids: Vec<i32> },
    /// `n` — NoData: the statement/portal produces no result columns.
    NoData,
    /// `I` — EmptyQueryResponse: the query string was empty.
    EmptyQueryResponse,
}

impl BackendMessage {
    /// The message type tag byte.
    pub fn tag(&self) -> u8 {
        match self {
            BackendMessage::AuthenticationOk
            | BackendMessage::AuthenticationSasl { .. }
            | BackendMessage::AuthenticationSaslContinue { .. }
            | BackendMessage::AuthenticationSaslFinal { .. } => b'R',
            BackendMessage::ReadyForQuery(_) => b'Z',
            BackendMessage::BackendKeyData { .. } => b'K',
            BackendMessage::ParameterStatus { .. } => b'S',
            BackendMessage::ErrorResponse { .. } => b'E',
            BackendMessage::RowDescription { .. } => b'T',
            BackendMessage::DataRow { .. } => b'D',
            BackendMessage::CommandComplete { .. } => b'C',
            BackendMessage::ParseComplete => b'1',
            BackendMessage::BindComplete => b'2',
            BackendMessage::CloseComplete => b'3',
            BackendMessage::ParameterDescription { .. } => b't',
            BackendMessage::NoData => b'n',
            BackendMessage::EmptyQueryResponse => b'I',
        }
    }

    /// Append this message's body (no tag, no length) to `body`.
    fn encode_body(&self, body: &mut BytesMut) {
        match self {
            BackendMessage::AuthenticationOk => body.put_i32(0),
            BackendMessage::AuthenticationSasl { mechanisms } => {
                body.put_i32(10);
                for m in mechanisms {
                    put_cstring(body, m);
                }
                body.put_u8(0); // mechanism-list terminator
            }
            BackendMessage::AuthenticationSaslContinue { data } => {
                body.put_i32(11);
                body.extend_from_slice(data);
            }
            BackendMessage::AuthenticationSaslFinal { data } => {
                body.put_i32(12);
                body.extend_from_slice(data);
            }
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
            BackendMessage::RowDescription { fields } => {
                body.put_i16(fields.len() as i16);
                for field in fields {
                    put_cstring(body, &field.name);
                    body.put_i32(0); // table OID (not from a known relation)
                    body.put_i16(0); // column attribute number
                    body.put_i32(field.type_oid);
                    body.put_i16(field.type_size);
                    body.put_i32(-1); // type modifier (none)
                    body.put_i16(field.format_code); // 0 = text, 1 = binary
                }
            }
            BackendMessage::DataRow { columns } => {
                body.put_i16(columns.len() as i16);
                for col in columns {
                    match col {
                        None => body.put_i32(-1), // SQL NULL: length -1, no bytes
                        Some(bytes) => {
                            body.put_i32(bytes.len() as i32);
                            body.extend_from_slice(bytes);
                        }
                    }
                }
            }
            BackendMessage::CommandComplete { tag } => put_cstring(body, tag),
            // Empty-body extended-protocol acknowledgements.
            BackendMessage::ParseComplete
            | BackendMessage::BindComplete
            | BackendMessage::CloseComplete
            | BackendMessage::NoData
            | BackendMessage::EmptyQueryResponse => {}
            BackendMessage::ParameterDescription { type_oids } => {
                body.put_i16(type_oids.len() as i16);
                for oid in type_oids {
                    body.put_i32(*oid);
                }
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
    /// `P` — Parse: create a prepared statement. `stmt_name` empty ⇒ the unnamed
    /// statement; `param_types` are the client-declared parameter type OIDs
    /// (0 = unspecified, server infers).
    Parse {
        stmt_name: String,
        query: String,
        param_types: Vec<i32>,
    },
    /// `B` — Bind: create a portal from a prepared statement.
    Bind {
        portal: String,
        stmt_name: String,
        /// Per-parameter format codes (0 = text, 1 = binary). Length 0 ⇒ all
        /// text; length 1 ⇒ that one code applies to every parameter; else one
        /// code per parameter.
        param_formats: Vec<i16>,
        /// Parameter values: `None` is SQL NULL (wire length -1).
        param_values: Vec<Option<Vec<u8>>>,
        /// Per-result-column format codes (same 0/1 + 0/1/many fan-out rule).
        result_formats: Vec<i16>,
    },
    /// `D` — Describe a statement (`S`) or portal (`P`) by name.
    Describe { kind: u8, name: String },
    /// `E` — Execute a portal; `max_rows` of 0 means "all rows".
    Execute { portal: String, max_rows: i32 },
    /// `C` — Close a statement (`S`) or portal (`P`) by name.
    Close { kind: u8, name: String },
    /// `S` — sync (extended-query boundary).
    Sync,
    /// `X` — terminate the connection.
    Terminate,
    /// `p` — a SASL message (SASLInitialResponse or SASLResponse). The body is
    /// returned verbatim; the handshake state machine interprets it by phase.
    SaslResponse { data: Vec<u8> },
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_description_frames_one_int_field() {
        let mut out = BytesMut::new();
        BackendMessage::RowDescription {
            fields: vec![FieldDescription::text("id", 23, 4)], // int4, text format
        }
        .encode(&mut out);
        // 'T', i32 length, i16 field-count, then the field body.
        assert_eq!(out[0], b'T');
        // body = field-count(2) + name "id\0"(3) + tableOid(4) + attnum(2)
        //        + typeOid(4) + typeSize(2) + typeMod(4) + format(2) = 23
        // length counts itself (4) + body(23) = 27
        let len = i32::from_be_bytes(out[1..5].try_into().unwrap());
        assert_eq!(len, 27);
        let body = &out[5..];
        assert_eq!(i16::from_be_bytes(body[0..2].try_into().unwrap()), 1); // 1 field
        assert_eq!(&body[2..5], b"id\x00"); // name cstring
        assert_eq!(i32::from_be_bytes(body[5..9].try_into().unwrap()), 0); // table oid
        assert_eq!(i16::from_be_bytes(body[9..11].try_into().unwrap()), 0); // attnum
        assert_eq!(i32::from_be_bytes(body[11..15].try_into().unwrap()), 23); // type oid
        assert_eq!(i16::from_be_bytes(body[15..17].try_into().unwrap()), 4); // type size
        assert_eq!(i32::from_be_bytes(body[17..21].try_into().unwrap()), -1); // type modifier
        assert_eq!(i16::from_be_bytes(body[21..23].try_into().unwrap()), 0); // format = text
    }

    #[test]
    fn data_row_encodes_value_and_null() {
        let mut out = BytesMut::new();
        BackendMessage::DataRow {
            columns: vec![Some(b"abc".to_vec()), None],
        }
        .encode(&mut out);
        // 'D', i32 length, i16 col-count, col0 (len 3 + "abc"), col1 (len -1, NULL)
        assert_eq!(out[0], b'D');
        // body = col-count(2) + [len(4)+"abc"(3)] + [len(4)] = 13; length = 4 + 13 = 17
        let len = i32::from_be_bytes(out[1..5].try_into().unwrap());
        assert_eq!(len, 17);
        let body = &out[5..];
        assert_eq!(i16::from_be_bytes(body[0..2].try_into().unwrap()), 2); // 2 columns
        assert_eq!(i32::from_be_bytes(body[2..6].try_into().unwrap()), 3); // col0 len
        assert_eq!(&body[6..9], b"abc");
        assert_eq!(i32::from_be_bytes(body[9..13].try_into().unwrap()), -1); // col1 NULL
        assert_eq!(body.len(), 13, "no bytes follow a NULL length");
    }

    #[test]
    fn row_description_field_can_be_binary_format() {
        let mut out = BytesMut::new();
        BackendMessage::RowDescription {
            fields: vec![FieldDescription {
                name: "id".to_string(),
                type_oid: 23,
                type_size: 4,
                format_code: 1, // binary
            }],
        }
        .encode(&mut out);
        let body = &out[5..];
        // The trailing format code (last 2 bytes of the field) is 1 = binary.
        assert_eq!(i16::from_be_bytes(body[21..23].try_into().unwrap()), 1);
    }

    #[test]
    fn empty_body_extended_acks_frame_with_zero_length_body() {
        for (msg, tag) in [
            (BackendMessage::ParseComplete, b'1'),
            (BackendMessage::BindComplete, b'2'),
            (BackendMessage::CloseComplete, b'3'),
            (BackendMessage::NoData, b'n'),
            (BackendMessage::EmptyQueryResponse, b'I'),
        ] {
            let mut out = BytesMut::new();
            msg.encode(&mut out);
            // tag + length(=4, body empty); nothing follows.
            assert_eq!(out[0], tag, "tag byte for {msg:?}");
            assert_eq!(&out[..], &[tag, 0, 0, 0, 4]);
        }
    }

    #[test]
    fn parameter_description_encodes_count_and_oids() {
        let mut out = BytesMut::new();
        BackendMessage::ParameterDescription {
            type_oids: vec![23, 25],
        }
        .encode(&mut out);
        assert_eq!(out[0], b't');
        // length = 4 + count(2) + 2*oid(4) = 14
        let len = i32::from_be_bytes(out[1..5].try_into().unwrap());
        assert_eq!(len, 14);
        let body = &out[5..];
        assert_eq!(i16::from_be_bytes(body[0..2].try_into().unwrap()), 2);
        assert_eq!(i32::from_be_bytes(body[2..6].try_into().unwrap()), 23);
        assert_eq!(i32::from_be_bytes(body[6..10].try_into().unwrap()), 25);
    }

    #[test]
    fn command_complete_encodes_tag_cstring() {
        let mut out = BytesMut::new();
        BackendMessage::CommandComplete {
            tag: "SELECT 2".to_string(),
        }
        .encode(&mut out);
        // 'C', i32 length, "SELECT 2\0"
        assert_eq!(out[0], b'C');
        // length = 4 + ("SELECT 2\0" = 9) = 13
        let len = i32::from_be_bytes(out[1..5].try_into().unwrap());
        assert_eq!(len, 13);
        assert_eq!(&out[5..], b"SELECT 2\x00");
    }
}
