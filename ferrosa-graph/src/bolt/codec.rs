//! PackStream binary serialization and chunked framing for the Bolt protocol.
//!
//! PackStream is a binary presentation format for exchanging richly-typed data.
//! Chunked framing wraps serialized messages into length-prefixed chunks for
//! transport over TCP.

use std::fmt;

// ── Errors ──────────────────────────────────────────────────────────

/// Errors that can occur during PackStream encoding/decoding or chunk framing.
#[derive(Debug)]
pub enum CodecError {
    /// Ran out of bytes before completing a value.
    UnexpectedEnd,
    /// Encountered an unrecognized marker byte.
    InvalidMarker(u8),
    /// A string contained invalid UTF-8.
    InvalidUtf8,
    /// A message exceeded the maximum allowed size.
    MessageTooLarge(usize),
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEnd => write!(f, "unexpected end of data"),
            Self::InvalidMarker(m) => write!(f, "invalid marker byte: 0x{m:02X}"),
            Self::InvalidUtf8 => write!(f, "invalid UTF-8 in string"),
            Self::MessageTooLarge(size) => {
                write!(f, "message too large: {size} bytes")
            }
        }
    }
}

impl std::error::Error for CodecError {}

// ── PackStream Values ───────────────────────────────────────────────

/// A PackStream value — the fundamental unit of Bolt wire data.
#[derive(Debug, Clone, PartialEq)]
pub enum PackValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Bytes(Vec<u8>),
    String(String),
    List(Vec<PackValue>),
    Map(Vec<(String, PackValue)>),
    /// Bolt structure: tag byte + ordered fields.
    Structure {
        tag: u8,
        fields: Vec<PackValue>,
    },
}

// ── Marker Constants ────────────────────────────────────────────────

// Null / Bool
const MARKER_NULL: u8 = 0xC0;
const MARKER_FALSE: u8 = 0xC2;
const MARKER_TRUE: u8 = 0xC3;

// Float
const MARKER_FLOAT_64: u8 = 0xC1;

// Integer
const MARKER_INT_8: u8 = 0xC8;
const MARKER_INT_16: u8 = 0xC9;
const MARKER_INT_32: u8 = 0xCA;
const MARKER_INT_64: u8 = 0xCB;

// String
const MARKER_TINY_STRING: u8 = 0x80; // 0x80..=0x8F
const MARKER_STRING_8: u8 = 0xD0;
const MARKER_STRING_16: u8 = 0xD1;
const MARKER_STRING_32: u8 = 0xD2;

// List
const MARKER_TINY_LIST: u8 = 0x90; // 0x90..=0x9F
const MARKER_LIST_8: u8 = 0xD4;
const MARKER_LIST_16: u8 = 0xD5;
const MARKER_LIST_32: u8 = 0xD6;

// Map
const MARKER_TINY_MAP: u8 = 0xA0; // 0xA0..=0xAF
const MARKER_MAP_8: u8 = 0xD8;
const MARKER_MAP_16: u8 = 0xD9;
const MARKER_MAP_32: u8 = 0xDA;

// Bytes
const MARKER_BYTES_8: u8 = 0xCC;
const MARKER_BYTES_16: u8 = 0xCD;
const MARKER_BYTES_32: u8 = 0xCE;

// Structure
const MARKER_TINY_STRUCT: u8 = 0xB0; // 0xB0..=0xBF
const MARKER_STRUCT_8: u8 = 0xDC;
const MARKER_STRUCT_16: u8 = 0xDD;

// ── Encoder ─────────────────────────────────────────────────────────

/// Encode a [`PackValue`] into PackStream bytes, appending to `buf`.
pub fn encode(value: &PackValue, buf: &mut Vec<u8>) {
    match value {
        PackValue::Null => buf.push(MARKER_NULL),

        PackValue::Bool(false) => buf.push(MARKER_FALSE),
        PackValue::Bool(true) => buf.push(MARKER_TRUE),

        PackValue::Int(v) => encode_int(*v, buf),
        PackValue::Float(v) => encode_float(*v, buf),
        PackValue::Bytes(v) => encode_bytes(v, buf),
        PackValue::String(v) => encode_string(v, buf),
        PackValue::List(v) => encode_list(v, buf),
        PackValue::Map(v) => encode_map(v, buf),
        PackValue::Structure { tag, fields } => encode_structure(*tag, fields, buf),
    }
}

fn encode_int(v: i64, buf: &mut Vec<u8>) {
    // Tiny int: -16..=127 as a single byte
    if (-16..=127).contains(&v) {
        buf.push(v as u8);
    } else if i8::MIN as i64 <= v && v <= i8::MAX as i64 {
        buf.push(MARKER_INT_8);
        buf.push(v as i8 as u8);
    } else if i16::MIN as i64 <= v && v <= i16::MAX as i64 {
        buf.push(MARKER_INT_16);
        buf.extend_from_slice(&(v as i16).to_be_bytes());
    } else if i32::MIN as i64 <= v && v <= i32::MAX as i64 {
        buf.push(MARKER_INT_32);
        buf.extend_from_slice(&(v as i32).to_be_bytes());
    } else {
        buf.push(MARKER_INT_64);
        buf.extend_from_slice(&v.to_be_bytes());
    }
}

fn encode_float(v: f64, buf: &mut Vec<u8>) {
    buf.push(MARKER_FLOAT_64);
    buf.extend_from_slice(&v.to_be_bytes());
}

fn encode_bytes(v: &[u8], buf: &mut Vec<u8>) {
    let len = v.len();
    if len <= 0xFF {
        buf.push(MARKER_BYTES_8);
        buf.push(len as u8);
    } else if len <= 0xFFFF {
        buf.push(MARKER_BYTES_16);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf.push(MARKER_BYTES_32);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    }
    buf.extend_from_slice(v);
}

fn encode_string(v: &str, buf: &mut Vec<u8>) {
    let len = v.len();
    if len <= 0x0F {
        buf.push(MARKER_TINY_STRING | len as u8);
    } else if len <= 0xFF {
        buf.push(MARKER_STRING_8);
        buf.push(len as u8);
    } else if len <= 0xFFFF {
        buf.push(MARKER_STRING_16);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf.push(MARKER_STRING_32);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    }
    buf.extend_from_slice(v.as_bytes());
}

fn encode_list(v: &[PackValue], buf: &mut Vec<u8>) {
    let len = v.len();
    if len <= 0x0F {
        buf.push(MARKER_TINY_LIST | len as u8);
    } else if len <= 0xFF {
        buf.push(MARKER_LIST_8);
        buf.push(len as u8);
    } else if len <= 0xFFFF {
        buf.push(MARKER_LIST_16);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf.push(MARKER_LIST_32);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    }
    for item in v {
        encode(item, buf);
    }
}

fn encode_map(v: &[(String, PackValue)], buf: &mut Vec<u8>) {
    let len = v.len();
    if len <= 0x0F {
        buf.push(MARKER_TINY_MAP | len as u8);
    } else if len <= 0xFF {
        buf.push(MARKER_MAP_8);
        buf.push(len as u8);
    } else if len <= 0xFFFF {
        buf.push(MARKER_MAP_16);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf.push(MARKER_MAP_32);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    }
    for (key, val) in v {
        encode_string(key, buf);
        encode(val, buf);
    }
}

fn encode_structure(tag: u8, fields: &[PackValue], buf: &mut Vec<u8>) {
    let len = fields.len();
    if len <= 0x0F {
        buf.push(MARKER_TINY_STRUCT | len as u8);
    } else if len <= 0xFF {
        buf.push(MARKER_STRUCT_8);
        buf.push(len as u8);
    } else {
        buf.push(MARKER_STRUCT_16);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    }
    buf.push(tag);
    for field in fields {
        encode(field, buf);
    }
}

// ── Decoder ─────────────────────────────────────────────────────────

/// Decode a [`PackValue`] from PackStream bytes.
///
/// Returns the decoded value and the number of bytes consumed.
pub fn decode(data: &[u8]) -> Result<(PackValue, usize), CodecError> {
    if data.is_empty() {
        return Err(CodecError::UnexpectedEnd);
    }

    let marker = data[0];
    match marker {
        // ── Null / Bool ──
        MARKER_NULL => Ok((PackValue::Null, 1)),
        MARKER_FALSE => Ok((PackValue::Bool(false), 1)),
        MARKER_TRUE => Ok((PackValue::Bool(true), 1)),

        // ── Float ──
        MARKER_FLOAT_64 => {
            ensure_len(data, 9)?;
            let v = f64::from_be_bytes(data[1..9].try_into().unwrap());
            Ok((PackValue::Float(v), 9))
        }

        // ── Integer sizes ──
        MARKER_INT_8 => {
            ensure_len(data, 2)?;
            Ok((PackValue::Int(data[1] as i8 as i64), 2))
        }
        MARKER_INT_16 => {
            ensure_len(data, 3)?;
            let v = i16::from_be_bytes(data[1..3].try_into().unwrap());
            Ok((PackValue::Int(v as i64), 3))
        }
        MARKER_INT_32 => {
            ensure_len(data, 5)?;
            let v = i32::from_be_bytes(data[1..5].try_into().unwrap());
            Ok((PackValue::Int(v as i64), 5))
        }
        MARKER_INT_64 => {
            ensure_len(data, 9)?;
            let v = i64::from_be_bytes(data[1..9].try_into().unwrap());
            Ok((PackValue::Int(v), 9))
        }

        // ── Bytes ──
        MARKER_BYTES_8 => {
            ensure_len(data, 2)?;
            let len = data[1] as usize;
            ensure_len(data, 2 + len)?;
            Ok((PackValue::Bytes(data[2..2 + len].to_vec()), 2 + len))
        }
        MARKER_BYTES_16 => {
            ensure_len(data, 3)?;
            let len = u16::from_be_bytes(data[1..3].try_into().unwrap()) as usize;
            ensure_len(data, 3 + len)?;
            Ok((PackValue::Bytes(data[3..3 + len].to_vec()), 3 + len))
        }
        MARKER_BYTES_32 => {
            ensure_len(data, 5)?;
            let len = u32::from_be_bytes(data[1..5].try_into().unwrap()) as usize;
            ensure_len(data, 5 + len)?;
            Ok((PackValue::Bytes(data[5..5 + len].to_vec()), 5 + len))
        }

        // ── String sizes ──
        MARKER_STRING_8 => decode_string_n(data, 1, 2),
        MARKER_STRING_16 => decode_string_n(data, 2, 3),
        MARKER_STRING_32 => decode_string_n(data, 4, 5),

        // ── List sizes ──
        MARKER_LIST_8 => {
            ensure_len(data, 2)?;
            let len = data[1] as usize;
            decode_list_items(data, len, 2)
        }
        MARKER_LIST_16 => {
            ensure_len(data, 3)?;
            let len = u16::from_be_bytes(data[1..3].try_into().unwrap()) as usize;
            decode_list_items(data, len, 3)
        }
        MARKER_LIST_32 => {
            ensure_len(data, 5)?;
            let len = u32::from_be_bytes(data[1..5].try_into().unwrap()) as usize;
            decode_list_items(data, len, 5)
        }

        // ── Map sizes ──
        MARKER_MAP_8 => {
            ensure_len(data, 2)?;
            let len = data[1] as usize;
            decode_map_entries(data, len, 2)
        }
        MARKER_MAP_16 => {
            ensure_len(data, 3)?;
            let len = u16::from_be_bytes(data[1..3].try_into().unwrap()) as usize;
            decode_map_entries(data, len, 3)
        }
        MARKER_MAP_32 => {
            ensure_len(data, 5)?;
            let len = u32::from_be_bytes(data[1..5].try_into().unwrap()) as usize;
            decode_map_entries(data, len, 5)
        }

        // ── Structure sizes ──
        MARKER_STRUCT_8 => {
            ensure_len(data, 3)?; // marker + 1-byte len + tag
            let len = data[1] as usize;
            let tag = data[2];
            decode_struct_fields(data, tag, len, 3)
        }
        MARKER_STRUCT_16 => {
            ensure_len(data, 4)?; // marker + 2-byte len + tag
            let len = u16::from_be_bytes(data[1..3].try_into().unwrap()) as usize;
            let tag = data[3];
            decode_struct_fields(data, tag, len, 4)
        }

        // ── Tiny types (ranges) ──
        _ => {
            // Tiny int: values 0x00..=0x7F (0..=127) and 0xF0..=0xFF (-16..=-1)
            if marker <= 0x7F || marker >= 0xF0 {
                Ok((PackValue::Int(marker as i8 as i64), 1))
            }
            // Tiny string: 0x80..=0x8F
            else if (MARKER_TINY_STRING..MARKER_TINY_STRING + 0x10).contains(&marker) {
                let len = (marker & 0x0F) as usize;
                ensure_len(data, 1 + len)?;
                let s =
                    std::str::from_utf8(&data[1..1 + len]).map_err(|_| CodecError::InvalidUtf8)?;
                Ok((PackValue::String(s.to_owned()), 1 + len))
            }
            // Tiny list: 0x90..=0x9F
            else if (MARKER_TINY_LIST..MARKER_TINY_LIST + 0x10).contains(&marker) {
                let len = (marker & 0x0F) as usize;
                decode_list_items(data, len, 1)
            }
            // Tiny map: 0xA0..=0xAF
            else if (MARKER_TINY_MAP..MARKER_TINY_MAP + 0x10).contains(&marker) {
                let len = (marker & 0x0F) as usize;
                decode_map_entries(data, len, 1)
            }
            // Tiny struct: 0xB0..=0xBF
            else if (MARKER_TINY_STRUCT..MARKER_TINY_STRUCT + 0x10).contains(&marker) {
                let len = (marker & 0x0F) as usize;
                ensure_len(data, 2)?; // marker + tag
                let tag = data[1];
                decode_struct_fields(data, tag, len, 2)
            } else {
                Err(CodecError::InvalidMarker(marker))
            }
        }
    }
}

fn ensure_len(data: &[u8], required: usize) -> Result<(), CodecError> {
    if data.len() < required {
        Err(CodecError::UnexpectedEnd)
    } else {
        Ok(())
    }
}

/// Decode a sized string (STRING_8 / STRING_16 / STRING_32).
/// `size_bytes` is how many bytes encode the length (1, 2, or 4).
/// `header` is the total header length (marker + size bytes).
fn decode_string_n(
    data: &[u8],
    size_bytes: usize,
    header: usize,
) -> Result<(PackValue, usize), CodecError> {
    ensure_len(data, header)?;
    let len = match size_bytes {
        1 => data[1] as usize,
        2 => u16::from_be_bytes(data[1..3].try_into().unwrap()) as usize,
        4 => u32::from_be_bytes(data[1..5].try_into().unwrap()) as usize,
        _ => unreachable!(),
    };
    ensure_len(data, header + len)?;
    let s =
        std::str::from_utf8(&data[header..header + len]).map_err(|_| CodecError::InvalidUtf8)?;
    Ok((PackValue::String(s.to_owned()), header + len))
}

fn decode_list_items(
    data: &[u8],
    count: usize,
    mut offset: usize,
) -> Result<(PackValue, usize), CodecError> {
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        let (val, consumed) = decode(&data[offset..])?;
        items.push(val);
        offset += consumed;
    }
    Ok((PackValue::List(items), offset))
}

fn decode_map_entries(
    data: &[u8],
    count: usize,
    mut offset: usize,
) -> Result<(PackValue, usize), CodecError> {
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        // Key must be a string
        let (key_val, consumed) = decode(&data[offset..])?;
        offset += consumed;
        let key = match key_val {
            PackValue::String(s) => s,
            _ => return Err(CodecError::InvalidMarker(data[offset - consumed])),
        };
        let (val, consumed) = decode(&data[offset..])?;
        offset += consumed;
        entries.push((key, val));
    }
    Ok((PackValue::Map(entries), offset))
}

fn decode_struct_fields(
    data: &[u8],
    tag: u8,
    count: usize,
    mut offset: usize,
) -> Result<(PackValue, usize), CodecError> {
    let mut fields = Vec::with_capacity(count);
    for _ in 0..count {
        let (val, consumed) = decode(&data[offset..])?;
        fields.push(val);
        offset += consumed;
    }
    Ok((PackValue::Structure { tag, fields }, offset))
}

// ── Chunked Framing ─────────────────────────────────────────────────

/// Default maximum chunk size in bytes.
pub const DEFAULT_MAX_CHUNK_SIZE: usize = 65_535;

/// Encode a serialized message into Bolt chunked framing.
///
/// The message is split into chunks of at most `max_chunk_size` bytes.
/// Each chunk is prefixed with a 2-byte big-endian length. A zero-length
/// chunk (two zero bytes) terminates the message.
pub fn chunk_encode(message: &[u8], max_chunk_size: usize) -> Vec<u8> {
    let chunk_size = max_chunk_size.min(DEFAULT_MAX_CHUNK_SIZE);
    // Pre-allocate: data + 2 bytes per chunk header + 2 bytes terminator
    let num_chunks = (message.len() + chunk_size - 1).max(1) / chunk_size.max(1);
    let mut out = Vec::with_capacity(message.len() + num_chunks * 2 + 2);

    let mut remaining = message;
    while !remaining.is_empty() {
        let take = remaining.len().min(chunk_size);
        out.extend_from_slice(&(take as u16).to_be_bytes());
        out.extend_from_slice(&remaining[..take]);
        remaining = &remaining[take..];
    }
    // Zero-length terminator
    out.extend_from_slice(&[0x00, 0x00]);
    out
}

/// Accumulates chunked Bolt data and yields complete messages.
///
/// Feed raw TCP bytes in via [`ChunkDecoder::feed`]. When a zero-length
/// terminator chunk is encountered, the accumulated message is returned.
pub struct ChunkDecoder {
    /// Raw bytes not yet consumed.
    buffer: Vec<u8>,
    /// Current message being assembled.
    current: Vec<u8>,
}

impl ChunkDecoder {
    /// Create a new chunk decoder.
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            current: Vec::new(),
        }
    }

    /// Feed raw bytes and extract any complete messages.
    ///
    /// Returns a (possibly empty) vector of complete message payloads.
    pub fn feed(&mut self, data: &[u8]) -> Vec<Vec<u8>> {
        self.buffer.extend_from_slice(data);
        let mut messages = Vec::new();

        loop {
            if self.buffer.len() < 2 {
                break;
            }
            let chunk_len = u16::from_be_bytes([self.buffer[0], self.buffer[1]]) as usize;

            if chunk_len == 0 {
                // End-of-message marker
                self.buffer.drain(..2);
                if !self.current.is_empty() {
                    messages.push(std::mem::take(&mut self.current));
                }
            } else {
                // Need the full chunk
                if self.buffer.len() < 2 + chunk_len {
                    break;
                }
                self.current
                    .extend_from_slice(&self.buffer[2..2 + chunk_len]);
                self.buffer.drain(..2 + chunk_len);
            }
        }

        messages
    }
}

impl Default for ChunkDecoder {
    fn default() -> Self {
        Self::new()
    }
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Helper ──

    fn roundtrip(value: &PackValue) -> PackValue {
        let mut buf = Vec::new();
        encode(value, &mut buf);
        let (decoded, consumed) = decode(&buf).expect("decode failed");
        assert_eq!(consumed, buf.len(), "did not consume all bytes");
        decoded
    }

    // ── Null / Bool ──

    #[test]
    fn encode_decode_null() {
        assert_eq!(roundtrip(&PackValue::Null), PackValue::Null);
    }

    #[test]
    fn encode_decode_bool() {
        assert_eq!(roundtrip(&PackValue::Bool(true)), PackValue::Bool(true));
        assert_eq!(roundtrip(&PackValue::Bool(false)), PackValue::Bool(false));
    }

    // ── Int ──

    #[test]
    fn encode_decode_int_tiny() {
        // Positive tiny ints
        for v in 0..=127_i64 {
            assert_eq!(roundtrip(&PackValue::Int(v)), PackValue::Int(v));
        }
        // Negative tiny ints
        for v in -16..=-1_i64 {
            assert_eq!(roundtrip(&PackValue::Int(v)), PackValue::Int(v));
        }
        // Tiny int encodes as 1 byte
        let mut buf = Vec::new();
        encode(&PackValue::Int(42), &mut buf);
        assert_eq!(buf.len(), 1);
    }

    #[test]
    fn encode_decode_int_large() {
        let values: &[i64] = &[
            -17,
            -128,
            128,
            255,
            -32768,
            32767,
            -2_147_483_648,
            2_147_483_647,
            i64::MIN,
            i64::MAX,
        ];
        for &v in values {
            assert_eq!(
                roundtrip(&PackValue::Int(v)),
                PackValue::Int(v),
                "failed for {v}"
            );
        }
    }

    // ── Float ──

    #[test]
    fn encode_decode_float() {
        let values: &[f64] = &[0.0, -0.0, 1.5, -1.5, f64::INFINITY, f64::NEG_INFINITY];
        for &v in values {
            assert_eq!(
                roundtrip(&PackValue::Float(v)),
                PackValue::Float(v),
                "failed for {v}"
            );
        }
        // NaN special case
        let nan_rt = roundtrip(&PackValue::Float(f64::NAN));
        match nan_rt {
            PackValue::Float(v) => assert!(v.is_nan()),
            _ => panic!("expected Float"),
        }
    }

    // ── String ──

    #[test]
    fn encode_decode_string_tiny() {
        assert_eq!(
            roundtrip(&PackValue::String(String::new())),
            PackValue::String(String::new())
        );
        assert_eq!(
            roundtrip(&PackValue::String("hello".into())),
            PackValue::String("hello".into())
        );
        // Max tiny string (15 chars)
        let s = "a".repeat(15);
        assert_eq!(
            roundtrip(&PackValue::String(s.clone())),
            PackValue::String(s)
        );
    }

    #[test]
    fn encode_decode_string_long() {
        // STRING_8 range
        let s256 = "x".repeat(200);
        assert_eq!(
            roundtrip(&PackValue::String(s256.clone())),
            PackValue::String(s256)
        );
        // STRING_16 range
        let s_large = "y".repeat(300);
        assert_eq!(
            roundtrip(&PackValue::String(s_large.clone())),
            PackValue::String(s_large)
        );
    }

    // ── Bytes ──

    #[test]
    fn encode_decode_bytes() {
        let cases: Vec<Vec<u8>> = vec![vec![], vec![0x00], vec![0xFF; 300]];
        for b in cases {
            assert_eq!(roundtrip(&PackValue::Bytes(b.clone())), PackValue::Bytes(b));
        }
    }

    // ── List ──

    #[test]
    fn encode_decode_list() {
        let empty = PackValue::List(vec![]);
        assert_eq!(roundtrip(&empty), empty);

        let mixed = PackValue::List(vec![
            PackValue::Int(1),
            PackValue::String("two".into()),
            PackValue::Bool(true),
            PackValue::Null,
        ]);
        assert_eq!(roundtrip(&mixed), mixed);

        // Nested list
        let nested = PackValue::List(vec![
            PackValue::List(vec![PackValue::Int(1), PackValue::Int(2)]),
            PackValue::List(vec![]),
        ]);
        assert_eq!(roundtrip(&nested), nested);
    }

    // ── Map ──

    #[test]
    fn encode_decode_map() {
        let empty = PackValue::Map(vec![]);
        assert_eq!(roundtrip(&empty), empty);

        let m = PackValue::Map(vec![
            ("name".into(), PackValue::String("Alice".into())),
            ("age".into(), PackValue::Int(30)),
            ("active".into(), PackValue::Bool(true)),
        ]);
        assert_eq!(roundtrip(&m), m);
    }

    // ── Structure ──

    #[test]
    fn encode_decode_structure() {
        let s = PackValue::Structure {
            tag: 0x01,
            fields: vec![PackValue::Map(vec![(
                "user_agent".into(),
                PackValue::String("test".into()),
            )])],
        };
        assert_eq!(roundtrip(&s), s);

        // Empty struct
        let empty = PackValue::Structure {
            tag: 0xFF,
            fields: vec![],
        };
        assert_eq!(roundtrip(&empty), empty);
    }

    // ── Chunked Framing ──

    #[test]
    fn chunk_encode_single() {
        let msg = b"hello";
        let encoded = chunk_encode(msg, DEFAULT_MAX_CHUNK_SIZE);
        // Expected: [0x00, 0x05] + b"hello" + [0x00, 0x00]
        assert_eq!(&encoded[0..2], &[0x00, 0x05]);
        assert_eq!(&encoded[2..7], b"hello");
        assert_eq!(&encoded[7..9], &[0x00, 0x00]);
    }

    #[test]
    fn chunk_encode_large_message() {
        let msg = vec![0xAA; 100];
        let encoded = chunk_encode(&msg, 30);
        let mut decoder = ChunkDecoder::new();
        let messages = decoder.feed(&encoded);
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0], msg);
    }

    #[test]
    fn chunk_decoder_single_message() {
        let msg = b"test data";
        let encoded = chunk_encode(msg, DEFAULT_MAX_CHUNK_SIZE);
        let mut decoder = ChunkDecoder::new();
        let messages = decoder.feed(&encoded);
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0], msg);
    }

    #[test]
    fn chunk_decoder_partial_feed() {
        let msg = b"hello world";
        let encoded = chunk_encode(msg, DEFAULT_MAX_CHUNK_SIZE);
        let mut decoder = ChunkDecoder::new();

        // Feed one byte at a time
        let mut all_messages = Vec::new();
        for &byte in &encoded {
            all_messages.extend(decoder.feed(&[byte]));
        }
        assert_eq!(all_messages.len(), 1);
        assert_eq!(all_messages[0], msg);
    }

    #[test]
    fn chunk_decoder_multiple_messages() {
        let m1 = b"first";
        let m2 = b"second";
        let mut wire = chunk_encode(m1, DEFAULT_MAX_CHUNK_SIZE);
        wire.extend_from_slice(&chunk_encode(m2, DEFAULT_MAX_CHUNK_SIZE));

        let mut decoder = ChunkDecoder::new();
        let messages = decoder.feed(&wire);
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0], m1);
        assert_eq!(messages[1], m2);
    }

    // ── Error Cases ──

    #[test]
    fn decode_empty_returns_unexpected_end() {
        assert!(matches!(decode(&[]), Err(CodecError::UnexpectedEnd)));
    }

    #[test]
    fn decode_truncated_int16() {
        // INT_16 marker but only 1 byte of value
        assert!(matches!(
            decode(&[MARKER_INT_16, 0x00]),
            Err(CodecError::UnexpectedEnd)
        ));
    }

    // ── Proptest ──

    mod proptests {
        use super::*;
        use proptest::prelude::*;

        fn arb_pack_value() -> impl Strategy<Value = PackValue> {
            let leaf = prop_oneof![
                Just(PackValue::Null),
                any::<bool>().prop_map(PackValue::Bool),
                any::<i64>().prop_map(PackValue::Int),
                any::<f64>().prop_map(PackValue::Float),
                proptest::collection::vec(any::<u8>(), 0..64).prop_map(PackValue::Bytes),
                "[a-zA-Z0-9 ]{0,64}".prop_map(PackValue::String),
            ];
            leaf.prop_recursive(3, 32, 8, |inner| {
                prop_oneof![
                    proptest::collection::vec(inner.clone(), 0..8).prop_map(PackValue::List),
                    proptest::collection::vec(("[a-z]{1,8}", inner.clone()), 0..8,).prop_map(
                        |entries| PackValue::Map(
                            entries
                                .into_iter()
                                .map(|(k, v)| (k.to_string(), v))
                                .collect()
                        )
                    ),
                    (0..16u8, proptest::collection::vec(inner, 0..8))
                        .prop_map(|(tag, fields)| PackValue::Structure { tag, fields }),
                ]
            })
        }

        proptest! {
            #[test]
            fn packvalue_roundtrip(value in arb_pack_value()) {
                let mut buf = Vec::new();
                encode(&value, &mut buf);
                let (decoded, consumed) = decode(&buf).expect("decode failed");
                prop_assert_eq!(consumed, buf.len());
                // NaN != NaN, so we compare Debug strings for Float
                prop_assert_eq!(
                    format!("{:?}", value),
                    format!("{:?}", decoded)
                );
            }
        }
    }
}
